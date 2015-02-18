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

import fs
import datetime
import threading
import traceback

from coreutils import *

Events = enum(
  RuntimeNotification='1'
)

class Node(object):
  '''A node is the basic building block for a job flow.
     It is a abstraction of an executable task with dependencies with
     a pre-defined way to determine the timestamp of last successful
     executoin.
  '''

  def __init__(self):
    '''Constructor, initialize a node.'''
    self.objs = None
    self.prop = None
    self.configured = False
    self.executed = False
    self.main_src = None
    self.deps = []
    self.output = None
    self.id = None
    self.log_file = None
    self.exe_error = None
    self.callbacks = {}

  def add_dep(self, dep):
    '''Add a dependency to this node. Also get the properties from
       the dependency.
    '''
    if not isinstance(dep, Node):
      return dep

    self.deps.append(dep)
    return dep

  def get_timestamp(self):
    '''Return the timestamp of last successful execution'''
    try:
      return fs.getmtime(self.output)
    except:
      # Unable to determine timestamp.
      return None

  def check(self):
    '''Check if we can skip the execution.
       Child class should override this function to provide
       customized check.
    '''
    self_ts = self.get_timestamp()
    if not self_ts:
      return False

    for dep in self.deps:
      dep_ts = dep.get_timestamp()
      if not dep_ts:
        return False

      if dep_ts > self_ts:
        return False

    return True

  def build_dep(self):
    '''Build dependency map recursively'''
    if isinstance(self.main_src, Node):
      self.main_src = self.add_dep(self.main_src.check_configure())

  def check_configure(self):
    '''Main recursive function to configure a job flow.
       It configures source nodes first and then configure itself.
    '''
    if self.configured:
      return self

    self.build_dep() # Recursively create dependencymap

    self.configure()
    self.configured = True
    return self

  def check_execute(self):
    '''Check the if we can skip this node and then execute. The
       executed flag will be set the True only for succesfully
       execution. Catch the exeception for logging and rethrow
       to the caller.
    '''
    if self.executed:
      return

    if not self.check():
      self.log('Start execution: %s', self.hash())
      try:
        self.execute()
        self.log('Execution completed: %s', type(self).__name__)
        self.exe_error = None
      except Exception, e:
        self.exe_error = e
        self.execute_error(e)
        self.log('Exception: %s', e)
        self.log('%s', traceback.format_exc())
        raise
    else:
      self.log('Execution skipped: %s', type(self).__name__)
      log(' - %s : skipped', self.hash())

    self.executed = True

  def test_execute(self):
    '''Emulate a succesful run. Log necessary information if needed.'''
    self.log('Test Execution')
    self.exe_error = None
    self.executed = True

  def __or__(self, other):
    '''Utility function to pipe to another node.'''
    other.main_src = self # prepend as main source.
    other.objs = self.objs
    other.prop = self.prop
    return other

  def configure(self):
    '''Main configuration method.
       Setup the dependencies and create hash id here.
       Child class can override this method to modify the job flow.
    '''
    self.attempt = 0
    self.id = self.prop.sub('${build_dir}/%s' % self.hash())
    self.output = self.id + '.dat'
    self.log_file = self.id + '.log'

  def format_log(self, fmt, *args):
    '''Utility function to format log messages.'''
    now = datetime.datetime.now()
    fmtstr = fmt % args
    return '%s: %s' % (now, fmtstr)

  def reset_log(self):
    '''Utility function to delete and restart the log for this node.'''
    logf = open(self.log_file, 'w')
    logf.write(self.format_log('Execution started.'))
    logf.close()

  def log(self, fmt, *args):
    '''Log utility function for this node.'''
    logf = open(self.log_file, 'a')
    for line in paragraph(self.format_log(fmt, *args)):
      logf.write(line)
    logf.close()

  def execute(self):
    '''Execute this node. Child class should override.'''
    raise NotImplementedError()

  def hash(self):
    '''Hadecode of this node. Child class should override.'''
    raise NotImplementedError()

  def execute_error(self, exp):
    '''Handler execution error elegantly. Remove incomplete outputs.
       Child class should override.
    '''
    pass

  def notify_status(self, fmt, *args):
    '''Notify the client about the running status'''
    message = fmt % args
    self.invoke_callback(Events.RuntimeNotification, message)

  def register_event(self, event, callback):
    '''Register the event with the callback'''
    self.callbacks[event] = callback

  def invoke_callback(self, event, *args, **kargs):
    ''' Invoking callback regarding the event '''
    if event in self.callbacks:
      self.callbacks[event](*args, **kargs)
