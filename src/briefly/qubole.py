#
# Copyright 2014-2015 BloomReach, Inc.
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
import uuid

from coreutils import *
from rate_limit_lock import *

import qds_sdk.qubole
import qds_sdk.commands
import qds_sdk.cluster

class QuboleCluster(InstanceSynchronizedHelper):
  '''Representation of an Qubole cluster.
     Multiple jobs can be executed on one Qubole cluster concurrently.
  '''
  status_interval = 5 # Time interval between status query.
  max_status_error = 10  # Number of status query errors before rasing exception.
  rate_limit_lock = RateLimitLock()

  def __init__(self, manager):
    '''Constructor.'''
    super(QuboleCluster, self).__init__()
    self.id = None
    self.job_id = None
    self.label = None
    self.manager = manager
    self.prop = manager.prop
    self.job_count = 0
    self.level = 0

  @property
  def priority(self):
    '''Number of jobs being executed on the cluster, used as the priority in QuboleManager.
       The lower the value, the higher the priority.
    '''
    return self.job_count

  def get_hadoop_settings(self):
    '''Get Hadoop settings for a Qubole clusters.
       It calculates the price with self.level, which indicates the
       price upgrades from the original price.
    '''
    bid_price_percentage = 0.0
    if self.level < len(self.prop.qubole.price_upgrade_rate):
      bid_price_percentage = self.prop.qubole.price_upgrade_rate[self.level] * 100.0
    slave_request_type = 'spot'
    # Use on-demand instance if prices are zero.
    if bid_price_percentage <= 0:
      bid_price_percentage = 1.0 # API doesn't accept 0.
      slave_request_type = 'ondemand'

    hadoop_settings = self.prop.qubole.hadoop_settings
    hadoop_settings['custom_config'] = '\n'.join(
      ['%s=%s' % (key, value) for (key, value) in self.prop.qubole.hadoop_custom_config.items()])
    hadoop_settings['slave_request_type'] = slave_request_type

    return (bid_price_percentage, hadoop_settings)

  @instance_synchronized
  def start(self):
    '''Starts a Qubole cluster.
       Would raise exception if failed.
    '''
    if self.id is not None:
      # Cluster was already started by another thread.
      return

    # For persistent clusters, get the label from properties
    if self.prop.qubole.cluster_id is not None:
      self.id = self.prop.qubole.cluster_id
      self.label = self.id + '_label'

      if self.has_cluster():
        return

      message('Cluster with label "%s" does not exist, creating one now.' % self.label)

    if self.manager.closed:
      # KeyboardInterrupt happened when the thread was blocked trying to get the instance lock.
      raise HadoopFailure('WARN: Pipeline cancelled when waiting to start cluster.')

    if self.prop.qubole.project_name is None:
      # All Qubole pipelines are supposed to have a project name for tagging cluster and future analysis.
      # TODO Implement actual tagging with new Qubole API.
      raise HadoopFailure('ERROR: Please provide project name.')

    self.create_cluster()

  def has_cluster(self):
    ''' Given that self.label is defined. Check if the cluster with this label exists.'''
    result = qds_sdk.cluster.Cluster.list()
    return any((self.label in cluster['cluster']['label']) for cluster in result)

  def create_cluster(self):
    ''' Send an API request to create a cluster.
    Randomly create a label if it is not provided.
    '''
    if self.label is None:
      self.label = str(uuid.uuid1()).replace('-', '_')

    cluster_info = qds_sdk.cluster.ClusterInfo(
      label = self.label,
      aws_access_key_id = self.prop.ec2.key,
      aws_secret_access_key = self.prop.ec2.secret,
      # Don't allow Qubole to auto-terminate cluster if cluster_id is not provided, i.e. using a persistent cluster.
      disallow_cluster_termination = self.prop.qubole.cluster_id is not None)

    (bid_price_percentage, hadoop_settings) = self.get_hadoop_settings()
    cluster_info.set_hadoop_settings(**hadoop_settings)
    cluster_info.set_ec2_settings(
      aws_region = self.prop.qubole.aws_region,
      aws_availability_zone = self.prop.qubole.aws_availability_zone)
    cluster_info.set_spot_instance_settings(
      maximum_bid_price_percentage = bid_price_percentage,
      timeout_for_request = self.prop.qubole.timeout_for_request)
    cluster_info.set_security_settings(persistent_security_groups = self.prop.qubole.persistent_security_groups)

    with QuboleCluster.rate_limit_lock:
      result = qds_sdk.cluster.Cluster.create(cluster_info.minimal_payload()) # Could fail and raise exception.

    self.id = result['cluster']['id']
    message('Created Qubole cluster with id: %s', self.id)

  @instance_synchronized
  def terminate(self, level_upgrade=0):
    '''Terminates the Qubole cluster.
       Swallows exception.
    '''
    if self.prop.qubole.cluster_id is not None:
      message('Persistent cluster. Do not terminate the cluster.')
      message('Canceling self.job_id %s', self.job_id)
      qds_sdk.commands.HadoopCommand.cancel_id(self.job_id)
      message('Canceled job %s', self.job_id)
      return

    if self.id is None:
      # Cluster was already deleted.
      return

    message('Terminating cluster: %s', self.id)
    self.level += level_upgrade
    with QuboleCluster.rate_limit_lock:
      try:
        qds_sdk.cluster.Cluster.terminate(self.id)
      except Exception, e:
        message('ERROR: Failed to send termination request to cluster: %s', self.id)

  @instance_synchronized
  def delete(self):
    '''Deletes the Qubole cluster.
       Swallows exception.
    '''
    if self.id is None:
      # Cluster was already deleted.
      return

    if self.prop.qubole.cluster_id is not None:
      message('Persistent cluster. Not deleting the cluster')
      return

    termination_start = time.time()
    while qds_sdk.cluster.Cluster.status(self.id)['state'] != 'DOWN':
      if (time.time() - termination_start) > self.prop.qubole.termination_timeout:
        message('ERROR: Failed to delete cluster: %s', str(self.id))
        break
      time.sleep(self.status_interval)

    message('Deleting cluster: %s', self.id)
    with QuboleCluster.rate_limit_lock:
      try:
        qds_sdk.cluster.Cluster.delete(self.id)
        self.id = None
      except Exception, e:
        message('ERROR: Failed to delete cluster: %s', self.id)

  def run_steps(self, node, wait=True):
    '''Main loop to execute a node.
       It will block until step complete or failure, and will raise
       exception for failures so the job will be retried.
    '''

    if self.id is None:
      self.start()

    args = ['--cluster-label', self.label, '--name', node.config.sub(node.config.qubole.step_name, node_hash=node.hash()), 'jar', node.config.hadoop.jar, node.config.main_class]
    args += node.process_args(*node.config.args)
    args = qds_sdk.commands.HadoopCommand.parse(args)

    with QuboleCluster.rate_limit_lock:
      result = qds_sdk.commands.HadoopCommand.create(**args)

    self.job_id = result.id
    node.log('Executing job %s on cluster id: %s', self.job_id, self.id)

    # notify the node with status.
    node.notify_status('Running on Quoble: %s', self.job_id)

    step_start = time.time()
    step_status = 'running'
    status_error_counter = 0
    while step_status != 'error' and step_status != 'done' and step_status != 'cancelled':
      try:
        with QuboleCluster.rate_limit_lock:
          step_status = qds_sdk.commands.HadoopCommand.find(self.job_id).status # Could fail and raise exception.
        # Reset counter.
        status_error_counter = 0
      except Exception, e:
        # Had issue accessing status. Try a few times until the count reaches max_status_error.
        status_error_counter += 1
        if status_error_counter > self.max_status_error:
          node.log('ERROR: Had too many errors when querying status. Try terminating the job.')
          step_status = 'error'

      if (time.time() - step_start) > self.prop.qubole.step_timeout:
        node.log('ERROR: Step running too long. Try terminating the job %s on cluster: %s', self.job_id, self.id)
        step_status = 'error'

      if self.manager.closed:
        # KeyboardInterrupt happened.
        node.log('WARN: Pipeline cancelled.')
        step_status = 'cancelled'

      time.sleep(self.status_interval)

    if step_status == 'done':
      node.log('Finished job %s on cluster: %s', self.job_id, self.id)
     
    else:
      if step_status == 'error':
        self.terminate()
        self.delete()

      node.log('ERROR:\n%s', result.get_log())
      node.log('ERROR: Job %s on cluster %s exited with status: %s', self.job_id, self.id, step_status)
      raise HadoopFailure('Job exited with status: %s' % step_status)

class QuboleManager(InstanceSynchronizedHelper):
  '''A collection of QuboleCluster.
     This class controls a pool of Qubole clusters for this pipeline. It also
     provides thread-safe methods to execute a single step.
  '''
  def __init__(self):
    '''Constructor, initialize cluster queue.'''
    super(QuboleManager, self).__init__()
    self.clusters = []
    self.cluster_queue = Queue.PriorityQueue()
    self.prop = None
    self.closed = False

  def set_properties(self, node):
    '''Configures properties.'''
    if self.prop is None:
      self.prop = node.prop
      qds_sdk.qubole.Qubole.configure(
        api_token = self.prop.qubole.api_token,
        api_url = self.prop.qubole.api_url,
        version = self.prop.qubole.api_version)

  @instance_synchronized
  def create_cluster(self):
    '''If there there is room left for more clusters, create a QuboleCluster object and add it to clusters.
       This has to be executed inside a instance lock because it changes clusters, which might happen at the same time
       when close() is iterating through it.
    '''
    cluster = None
    if self.cluster_queue.empty() and len(self.clusters) < self.prop.qubole.max_cluster:
      cluster = QuboleCluster(self)
      self.clusters.append(cluster)
    return cluster

  def get_cluster(self):
    '''Get a cluster to run a jar step.
       It will create new clusters if allowed. Otherwise, it will block
       until any cluster available for execution.
    '''
    cluster = self.create_cluster()
    if cluster is None:
      # Executor threads get blocked here.
      cluster = self.cluster_queue.get()[1]
      message('Dequeued cluster: %s', cluster.id if cluster.id is not None else 'N/A')

    return cluster

  def run_steps(self, node, wait=True):
    '''Get an available Qubole cluster to execute a node.'''
    cluster = self.get_cluster()
    self.update_cluster_job_count(cluster, 1)

    if self.closed:
      # Keyboard interrupt happened when current thread got blocked in get_cluster(). 
      return

    try:
      cluster.run_steps(node, wait)
    finally:
      self.update_cluster_job_count(cluster, -1)

  @instance_synchronized
  def update_cluster_job_count(self, cluster, delta):
    '''Increment or decrement job_count of cluster, depends on the delta.
       If the cluster didn't reach concurrency limit, inserts it into cluster queue.
    '''
    cluster.job_count += delta
    if cluster.job_count < self.prop.qubole.max_job_per_cluster:
      self.cluster_queue.put((cluster.priority, cluster))
      message('Enqueued cluster: %s', cluster.id if cluster.id is not None else 'N/A')

  @instance_synchronized
  def close(self):
    '''Terminates and deletes all Qubole clusters.
       it's done in two for loops instead of one because it takes a couple minutes for a cluster to shut down, so it's
       more efficient to send termination request to all clusters first, then wait in delete() probabaly only once.
    '''
    self.closed = True
    for cluster in self.clusters:
      cluster.terminate()
    for cluster in self.clusters:
      cluster.delete()
