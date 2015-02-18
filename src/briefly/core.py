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

import sys
import time
import traceback
import threading
import optparse
import Queue

from properties import *
from process import *
from defaults import *
from coreutils import *
import dag

BRIEFLY_VERSION = '1.0'

class NodeExecutor(threading.Thread):
  '''Common node executor. This object is created per each execution thread.
     Dealing with the common logs and retry logic. Will raise exception for
     last unsuccesful trial.
  '''

  def __init__(self, service, task_done_callback=None):
    '''Constructor, initialize executor'''
    super(NodeExecutor, self).__init__()
    self.service = service
    self.daemon = True
    self.task_done = task_done_callback
    self.start()

  def run(self):
    '''Wait and obtain node from pending queue. Execute until we see a termination
       node. Note the worker thread will block until queue items available.
    '''
    while True:
      try:
        node = self.service.get_next_job()
        if node is None:
          break
        self.execute_node(node)
      finally:
        if node is not None:
          self.service.complete_execute(node)

  def execute_node(self, node):
    '''Execute a node num_retry times'''
    node.reset_log()

    if node.prop.test_run:
      node.test_execute()
      return

    # Check all the dependencies are met. Otherwise, we can skip.
    if any([dep.exe_error is not None for dep in node.deps]):
      log(' - %s : skipped (depencency error)', node.hash())
      node.exe_error = Exception("Dependency error")
      return

    exec_exp = None
    for i in range(node.prop.num_retry):
      log(' - %s : executing', node.hash())
      if i > 0:
        node.log('Try again #%d...', i)
        log(' - %s : try again #%d', node.hash(), i)
      try:
        node.check_execute()
        log(' - %s : done', node.hash())
        break
      except Exception, e:
        log(' - %s : exception: %s', node.hash(), str(e))
        # Last try? Output more detailed trace for debugging.
        if i == node.prop.num_retry - 1:
          log(' - %s : %s', node.hash(), traceback.format_exc())
          exec_exp = e

    if self.task_done:
      self.task_done(node, exec_exp)


class ExecutorService(object):
  '''Sevice of a single execution session. This class will create the dependency
     map for execution, and control the execution flow. Each node will be assigned
     to a separate thread for execution. Number of concurrent threads are controlled
     by run_threads property.
  '''

  def __init__(self, objs, task_done_callback=None):
    '''Constructor. Create and initialize members for execution.'''
    self.number_of_threads = objs.prop.run_threads
    self.dag = dag.DependencyGraph()
    self.executor_factory = NodeExecutor
    self.lock = threading.Lock()
    self.pending = Queue.PriorityQueue()
    self.task_done_callback = task_done_callback
    self.order = 1
    
  def get_next_job(self):
    '''Blocks if no jobs; otherwise, pops a pending job without dependencies (or already finished).
       returns None when terminated.
    '''
    order, node = self.pending.get()
    return node

  def build_dependency_graph(self, node):
    '''Create dependency map recursively.'''
    if node.executed or node in self.dag:
      return

    self.dag.add_node(node, self.order)
    self.order += 1

    for dep in node.deps:
      self.build_dependency_graph(dep)
      if not dep.executed:
        self.dag.add_edge(dep, node)
    
  def complete_execute(self, node):
    '''Finalize the execution of a node. Update the dependency map
       and trigger new execution if necessary.
    '''
    if self.empty():
      return # early termination

    with self.lock:
      bridge_nodes = self.dag.get_bridge_nodes(node)
      self.dag.remove(node)
      self.enqueue_pending(bridge_nodes)

      # Don't call self.empty() outside the lock.
      if len(self.dag) == 0:
        self.terminate()

  def enqueue_pending(self, nodes):
    ''' Put the nodes into the queue
        We have to make sure that when putting nodes into the queue, it's in the correct oder. 
    '''
    nodes = list(nodes)
    nodes.sort(key=lambda x: x[1])

    ''' Put iterable nodes into pending priority queue.'''
    for ready_node, order in nodes:
      self.pending.put((order, ready_node))

  def empty(self):
    '''Chceck if we still have pending executions.'''
    with self.lock:
      return len(self.dag) == 0

  def terminate(self):
    '''Terminate the execution service. Should be called with self.lock being hold.'''
    for i in xrange(self.number_of_threads):
      self.pending.put((sys.maxint, None))
      
  def execute(self, targets):
    '''Start execution of targets. Prepare the dependency map
       and initialize worker threads. Put seed processes into
       pending queue for execution.
    '''
    for target in targets:
      target.check_configure()
      self.build_dependency_graph(target)

    if self.empty():
      return

    assert self.pending.empty()
    # Enqueu start nodes into pending queue
    self.enqueue_pending(self.dag.get_start_nodes())

    # Create worker threads.
    running_threads = []
    for i in xrange(self.number_of_threads):
      running_threads.append(self.executor_factory(self, self.task_done_callback))

    try:
      # Wait for completition.
      for thread in running_threads:
        # it's okay to use long wait since it will recrive notification.
        while thread.is_alive() and not thread.join(60):
          pass
    finally:
      # disable reuse of this executor
      self.pending = None

class Pipeline(object):
  '''Main job flow pipeline class.
     This class encaptulate the properties and the flow dependencies.
     Also provides utilities to run targets in paralle. It also
     manages resources and release them after running.
  '''

  resources = set() # global resources set for all pipelines.

  def __init__(self, name, pprop=None):
    '''Constructor, initialize pipeline properties.'''
    self.name = name
    self.targets = None
    self.task_done_callback = None

    if pprop: # properties from parent pipeline?
      self.prop = pprop.get_copy()
    else:
      self.prop = PIPELINE_DEFAULT_PROPERTIES.get_copy()
      self.load_args()

  def load_args(self):
    '''Parse and load system arguments.'''
    if not sys.argv[0]: sys.argv[0] = ''  # Workaround for running with optparse from egg

    parser = optparse.OptionParser(description='Briefly Script Engine. Version %s' % BRIEFLY_VERSION)
    parser.add_option('-D', '--define', help='Add a property setting.', action='append', default=[])
    parser.add_option('-p', '--property', help='property file', action='append', default=[])
    opt, args = parser.parse_args()

    for pf in opt.property:
      self.prop.load(pf)
    for d in opt.define:
      self.prop.parse(d)

  def set_task_done_callback(self, task_done_callback):
    '''Set the callback method, the callback method should be of this form:
       task_done_callback(node, exception)
    '''
    assert callable(task_done_callback)
    self.task_done_callback = task_done_callback

  def add_resource(self, resource):
    '''Add closable resources into resource set.'''
    self.resources.add(resource)

  def set_targets(self, targets):
    '''Set the targets to be executed'''
    self.targets = targets

  def run_targets(self):
    '''Execute targets in paralle.
       Create executor service to execute targets and wait for completion.
       Also, release managed resources.
    '''

    # Create ${build_dir} if not exists.
    if not os.path.exists(self.prop.build_dir):
      try:
        os.makedirs(self.prop.build_dir)
      except OSError, ose:
        if ose.errno != errno.EEXIST:
          raise

    init_log(self.prop)
    log('Running pipeline: %s', self.name)
    message('Configuring targets...')

    try:
      # Start executiong all targets.
      service = ExecutorService(self, self.task_done_callback)
      service.execute(self.targets)
    except KeyboardInterrupt:
      message('User interrupted')
    finally:
      # clean up global resources for this pipelien
      message('Releasing resources...')
      for resource in self.resources:
        resource.close()
      self.resource = set()

    message('%d total target(s), %d executed.', len(self.targets), len([t for t in self.targets if t.executed]))

  def run(self, *targets):
    '''Execute list of targets'''
    self.set_targets(targets)
    self.run_targets()

  def source(self, uri):
    '''A simple source node with given URL (a local or s3 path)'''
    return SourceProcess(self, uri)

  def phony(self):
    '''A dummy target which does nothing. It can be used for nodes without any input.'''
    return PhonyProcess(self)

  def __or__(self, node):
    '''A short cut of phony target.'''
    return self.phony() | node
