#
# Copyright 2013-2015 BloomReach, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import Queue
import subprocess
import threading
import time
import traceback

from boto.emr.connection import EmrConnection
from boto.emr.step import JarStep
from boto.emr.instance_group import InstanceGroup
from boto.emr.bootstrap_action import BootstrapAction

import fs
import process
from coreutils import *
from rate_limit_lock import *
from node import *
from properties import *
from qubole import *


class EMRCluster(object):
  '''Representation of an EMR cluster.
     TODO: add bridge to boto interface for unit test.
  '''
  emr_status_delay = 10      # in sec
  emr_status_max_delay = 60  # in sec
  emr_status_max_error = 30  # number of errors
  emr_max_idle = 10 * 60     # 10 min (in sec)
  rate_limit_lock = RateLimitLock()

  def __init__(self, prop):
    '''Constructor, initialize EMR connection.'''
    self.prop = prop
    self.conn = EmrConnection(self.prop.ec2.key, self.prop.ec2.secret)
    self.jobid = None
    self.retry = 0
    self.level = 0
    self.last_update = -1

  @property
  def priority(self):
    '''The priority used in EMRManager.
       The lower value, the higher priority.
    '''
    with EMRCluster.rate_limit_lock:
      if self.jobid is None:
        return 1
      return 0

  def get_instance_groups(self):
    '''Get instance groups to start a cluster.
       It calculates the price with self.level, which indicates the
       price upgrades from the original price.
    '''
    instance_groups = []
    for group in self.prop.emr.instance_groups:
      (num, group_name, instance_type) = group
      level = max(0, min(self.level, len(self.prop.emr.price_upgrade_rate) - 1))  # 0 <= level < len(...)
      bprice = self.prop.emr.prices[instance_type] * self.prop.emr.price_upgrade_rate[level]
      name = '%s-%s@%f' % (group_name, 'SPOT', bprice)

      # Use on-demand instance if prices are zero.
      if bprice > 0:
        ig = InstanceGroup(num, group_name, instance_type, 'SPOT', name, '%.3f' % bprice)
      else:
        ig = InstanceGroup(num, group_name, instance_type, 'ON_DEMAND', name)

      instance_groups.append(ig)      

    return instance_groups

  def get_bootstrap_actions(self):
    '''Get list of bootstrap actions from property'''
    actions = []
    for bootstrap_action in self.prop.emr.bootstrap_actions:
      assert len(bootstrap_action) >= 2, 'Wrong bootstrap action definition: ' + str(bootstrap_action)
      actions.append(BootstrapAction(bootstrap_action[0], bootstrap_action[1], bootstrap_action[2:]))
    return actions

  @synchronized
  def start(self):
    '''Start a EMR cluster.'''
    # emr.project_name is required
    if self.prop.emr.project_name is None:
      raise ValueError('emr.project_name is not set')

    self.last_update = time.time()
    with EMRCluster.rate_limit_lock:
      self.jobid = self.conn.run_jobflow(name=self.prop.emr.cluster_name,
                                         ec2_keyname=self.prop.emr.keyname,
                                         log_uri=self.prop.emr.log_uri,
                                         ami_version=self.prop.emr.ami_version,
                                         bootstrap_actions=self.get_bootstrap_actions(),
                                         keep_alive=True,
                                         action_on_failure='CONTINUE',
                                         api_params={'VisibleToAllUsers': 'true'},
                                         instance_groups=self.get_instance_groups())
    message('Job flow created: %s', self.jobid)

    # Tag EC2 instances to allow future analysis
    tags = {'FlowControl': 'Briefly',
            'Project': self.prop.emr.project_name}
    if self.prop.emr.tags is not None:
      assert isinstance(self.prop.emr.tags, dict)
      tags = dict(tags.items() + self.prop.emr.tags.items())
    self.conn.add_tags(self.jobid, tags)

  @synchronized
  def terminate(self, level_upgrade=0):
    '''Terminate this EMR cluster.'''
    if self.jobid is None:
      return

    self.level += level_upgrade # upgrade to another price level

    message('Terminate jobflow: %s', self.jobid)
    for i in xrange(3):
      try:
        with EMRCluster.rate_limit_lock:
          self.conn.terminate_jobflow(self.jobid)
        break
      except Exception, e:
        message('Unable to terminate job flow: %s', self.jobid)
        message(traceback.format_exc())
    # We have to set jobid as None to create new cluster;
    # otherwise, run_steps will keep launching jobs on the bad cluster.
    self.jobid = None

  def is_idle(self):
    '''Check if this EMR cluster is idle?'''
    return (not self.jobid is None) and ((time.time() - self.last_update) > self.emr_max_idle)

  def get_steps(self, node):
    '''Get the jar step from the node.'''
    step = JarStep(name=node.config.sub(node.config.emr.step_name, node_hash=node.hash()),
                   main_class=node.config.main_class,
                   jar=node.config.hadoop.jar,
                   action_on_failure='CONTINUE',
                   step_args=node.process_args(*node.config.args))
    return [step]

  def get_step_index(self, step_id):
    '''Get the index of a step given step_id (1 based)'''
    steps = [step.id for step in reversed(self.conn.list_steps(self.jobid).steps) if step.status is not None]

    # revert the index since latest step is on top of the list
    return steps.index(step_id) + 1

  def run_steps(self, node, wait=True):
    '''Main loop to execute a node.
       It will block until step complete or failure, and will raise
       exception for failures so that the step will be retried.
       TODO: add timeouts for each step?
       TODO: dynamic increase cluster size?
    '''
    if not self.jobid:
      self.start()

    try:
      with EMRCluster.rate_limit_lock:
        # Here we just add single step. And get the step_id for fallowing checks.
        step_id = self.conn.add_jobflow_steps(self.jobid, self.get_steps(node)).stepids[0].value
        assert step_id is not None
    except Exception, e:
      node.log('Unable to add jobflow steps: %s', node.hash())
      node.log('%s', traceback.format_exc())
      raise HadoopFailure()

    status_error_counter = 0
    step_status = 'PENDING'
    step_index = None
    step_start = time.time()

    # notify the node with status.
    node.notify_status('Running on EMR: %s', self.jobid)

    while wait and step_status in ['PENDING', 'RUNNING']:
      try:
        # wait first for the status turning to 'RUNNING' from 'WAITING'. Exponential delay for errors.
        # Cap delay to a predefined limit.
        delay = min(self.emr_status_delay * (2 ** status_error_counter), self.emr_status_max_delay)
        time.sleep(delay)

        # Keep current cluster alive.
        self.last_update = time.time()

        # Get current cluster status. May raise exception due to EMR request throttle.
        cluster_state = self.conn.describe_cluster(self.jobid).status.state

        if step_index is None:
          step_index = self.get_step_index(step_id)
          node.log('Step #: %d', step_index)
          node.log('Log URI: %s/%s/steps/%d/', node.config.emr.log_uri, self.jobid, step_index)
        
        step_status = self.conn.describe_step(self.jobid, step_id).status.state
        status_error_counter = 0 # reset counter
        node.log("%s: %s %s", self.jobid, cluster_state, step_status)

        if cluster_state in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']: # cluster kill (maybe due to spot price), upgrade.
          self.terminate(1)
          break

        if (time.time() - step_start) > node.config.emr.step_timeout: # Step running too long? EMR cluster idle.
          node.log('Step running too long. Restart with new cluster')
          self.terminate()
          break

      except KeyboardInterrupt:
        raise
      except Exception, e:
        node.log('EMR loop exception: %d error(s)', status_error_counter)
        status_error_counter += 1
        if status_error_counter > self.emr_status_max_error:
          self.terminate()
          node.log('Too many errors in EMR loop')
          node.log('Exception: %s', traceback.format_exc())
          raise

    if step_status != 'COMPLETED':
      raise HadoopFailure()

class IdleTimer(threading.Thread):
  '''A timer to periodically invoke a function.'''

  def __init__(self, check_func, delay=60):
    '''Constructor. Set thread properties and start.'''
    super(IdleTimer, self).__init__()
    self.daemon = True
    self.check_func = check_func
    self.delay = delay
    self.terminated = threading.Event()
    self.start()

  def run(self):
    '''Periodically check if we have idle clusters.'''
    while not self.terminated.wait(self.delay):
      try:
        self.check_func()
      except Exception, e:
        message('Exception checking idle clusters: %s', str(e))

  def terminate(self):
    '''Terminate this timer'''
    self.terminated.set()

class EMRManager(object):
  '''A collection of EMRCluster.
     This class controls a pool of EMR clusters for this pipeline. It also
     provides thread-safe methods to execute a single step.
  '''

  def __init__(self):
    '''Constructor, initialize cluster pool.'''
    self.idle_timer = None
    self.reset()

  def reset(self):
    '''Cleanup and re-initialize internal states'''
    self.clusters = []
    self.pool = Queue.PriorityQueue()
    self.nodes = []
    self.prop = None

  def add_node(self, node):
    '''Add a node to be executed. This is used to check
       if we can shutdown clusters as soon as we don't
       have any remaining hadoop processes.
    '''
    if self.prop is None:
      self.prop = node.prop
    self.nodes.append(node)

  @synchronized
  def get_cluster(self):
    '''Get a cluster to run a jar step.
       It will create new clusters if allowed. Otherwise, it will block
       until any cluster available for execution.
    '''
    if self.pool.empty() and len(self.clusters) < self.prop.emr.max_cluster:
      new_cluster = EMRCluster(self.prop)
      self.clusters.append(new_cluster)
      self.pool.put((new_cluster.priority, new_cluster))
      self.check_idle_clusters() # kick off idle timer if necessary

    return self.pool.get()[1]

  @synchronized
  def check_idle_clusters(self):
    '''Check idle clusters. Terminate those clusters if necessary.'''
    for cluster in self.clusters:
      if cluster.is_idle():
        message('Idle cluster detected: %s', cluster.jobid)
        cluster.terminate()

    # Start/Stop idle check timer.
    if (len(self.clusters) > 0) and (self.idle_timer is None):
      self.idle_timer = IdleTimer(self.check_idle_clusters)
    elif ((len(self.clusters) <= 0)) and (not self.idle_timer is None):
      self.idle_timer.terminate()
      self.idle_timer = None

  def run_steps(self, node, wait=True):
    '''Get an available EMR cluster to execute a node.'''
    cluster = self.get_cluster()
    try:
      cluster.run_steps(node, wait)
    finally:
      self.pool.put((cluster.priority, cluster))

  def close(self):
    '''Close and terminate all EMR clusters'''
    for cluster in self.clusters:
      cluster.terminate()
    self.reset()

class HadoopProcess(process.Process):
  '''A single hadoop process.
     This process will run locally or remotely depending on prop.hadoop.runner property.
  '''
  local_hadoop_lock = threading.Lock()
  emr_manager = EMRManager()
  qubole_manager = QuboleManager()

  def __init__(self, *args, **kargs):
    '''Constructor, initialize hadoop configs.'''
    super(HadoopProcess, self).__init__(*args, **kargs)
    self.config = Properties()

  def configure(self):
    '''Configure this node, check for all inputs.'''
    super(HadoopProcess, self).configure()
    self.config.defaults(**self.prop.get_data())
    self.emr_manager.add_node(self)
    self.qubole_manager.set_properties(self)
    self.objs.add_resource(self.emr_manager)
    self.objs.add_resource(self.qubole_manager)

  def add_dep(self, dep):
    '''Check sources, inject extrace process to upload files.
       XXX Refactor this function with RemoteProcess
    '''
    if self.prop.hadoop.runner == 'emr' and isinstance(dep, Node) and (not dep.output is None) and not fs.is_s3url(dep.output):
      s3p = dep | process.S3Process()
      dep = s3p.check_configure()
    return super(HadoopProcess, self).add_dep(dep)

  def run_local(self):
    '''Run a hadoop step locally.
       Only single hadoop process is allowed at one time (controlled with local_hadoop_lock).
    '''
    cmdlist = [self.config.hadoop.bin, 'jar', self.config.hadoop.jar, self.config.main_class] + \
              self.process_args(*self.config.args)
    logf = open(self.log_file, 'a')
    with self.local_hadoop_lock:
      subprocess.call(cmdlist, stdout=logf, stderr=logf)
    logf.close()

  def run_emr(self):
    '''Run a hadoop step remotely on EMR.'''
    self.emr_manager.run_steps(self)

  def run_qubole(self):
    '''Run a hadoop step remotely on Qubole.'''
    self.qubole_manager.run_steps(self)

  def execute(self):
    '''Execute this hadoop process locally or remotely.'''
    self.log('jar = %s', self.config.hadoop.jar)
    self.log('main_class = %s', self.config.main_class)
    self.log('args = %s', self.process_args(*self.config.args))

    if self.config.hadoop.runner == 'emr':
      self.run_emr()
    elif self.config.hadoop.runner == 'qubole':
      self.run_qubole()
    else:
      self.run_local()

  def check(self):
    '''See if we can skip this process.
       It should always False if we want to overwrite output.
    '''
    if self.config.overwrite:
      return False

    return super(HadoopProcess, self).check()

  def execute_error(self, exp):
    # TODO: delete output directory for retry.
    self.log('Output (incomplete): %s', self.output)
