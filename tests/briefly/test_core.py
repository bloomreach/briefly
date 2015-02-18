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

from briefly.core import *
import unittest
import time

class PrinterExecutor(NodeExecutor):
  '''A class for debug.
  '''
  def __init__(self, service, task_done_callback=None):
    '''Constructor, initialize executor'''
    super(PrinterExecutor, self).__init__(service)
    service.execution_queue = []
    self.verbose = False

  def execute_node(self, node):
    '''Simulate executing a node.'''
    time.sleep(node.identifier * 0.1)
    self.executed = True
    self.service.execution_queue.append(node)
    #print ' - %s : executed.' % (node.hash(),)

class MockTarget(object):
  DEFAULT_RETRY_COUNT = 3
  def __init__(self, identifier):
    self.identifier = identifier
    self.deps = []
    self.configured = True
    self.executed = False
    self.exe_error = None

  def __getattribute__(self, attr):
    if attr == 'prop':
      class property(object):
        pass
      prop = property()
      prop.num_retry = self.DEFAULT_RETRY_COUNT
      return prop
    else:
      return super(MockTarget, self).__getattribute__(attr)

  def hash(self):
    return self.identifier

  def log(self, *args):
    pass

  def check_configure(self):
    return self

  def check_execute(self):
    pass

  def reset_log(self):
    pass

class TestCaseWithSetup(unittest.TestCase):
  def get_edges(self):
    return tuple()

  def get_exeuctor_service(self):
    prop = Properties(
              run_threads = 5,
              )
    self.prop = prop
    return ExecutorService(self)

  def setUp(self):
    self.service = self.get_exeuctor_service()
    self.service.executor_factory = PrinterExecutor
    self.edges = self.get_edges()
    self.nodes = set()
    self.target_map = {}

    for p, c in self.edges:
      self.nodes.add(p)
      self.nodes.add(c)

    self.targets = []
    for n in self.nodes:
      target = MockTarget(n)
      self.target_map[n] = target
      self.targets.append(target)

    for p, c in self.edges:
      self.target_map[c].deps.append(self.target_map[p])

class TestCaseExecutionService(TestCaseWithSetup):
  def get_edges(self):
    return ((11,2),
            (11,9),
            (11,10),
            (8,9),
            (7,11),
            (7,8),
            (5,11),
            (3,8),
            (3,10))

  def get_exeuctor_service(self):
    prop = Properties(
              run_threads = 5,
              )
    self.prop = prop
    return ExecutorService(self)

  def test_setup(self):
    for p, c in self.edges:
      child = self.target_map[c]
      parent = self.target_map[p]
      self.assertTrue(parent in child.deps)

  def test_run(self):
    self.service.execute(self.targets)
    execution_sequence = self.service.execution_queue
    results = tuple(e.identifier for e in execution_sequence)
    self.assertTrue(results == (3, 5, 7, 8, 11, 2, 9, 10))

class TestCaseExecutionServiceParallel(TestCaseWithSetup):
  def get_edges(self):
    return ((1,2),
            (3,4),
            (5,6),
            (7,8))

  def get_exeuctor_service(self):
    prop = Properties(
              run_threads = 1,
              )
    self.prop = prop
    return ExecutorService(self)

  def test_run(self):
    self.service.execute(self.targets)
    execution_sequence = self.service.execution_queue
    results = tuple(e.identifier for e in execution_sequence)
    self.assertTrue(results == (1, 2, 3, 4, 5, 6, 7, 8))


if __name__ == '__main__':
  unittest.main()
