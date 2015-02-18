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

import threading
import subprocess

from properties import *

# Global constants.
MAIN_LOG_FILE = None

def enum(**enums):
  '''Simple enum generator.'''
  return type('Enum', (), enums)

def synchronized(func):
  '''Decorator to synchronize function.'''
  func.__lock__ = threading.Lock()
  def synced_func(*args, **kargs):
    with func.__lock__:
      return func(*args, **kargs)
  return synced_func

def instance_synchronized(func):
  '''Decorator to place an instance based lock around a function.'''
  def synced_func(self, *args, **kargs):
    with self.__lock__:
      return func(self, *args, **kargs)
  return synced_func

class InstanceSynchronizedHelper(object):
  '''Base class with __lock__ attribute, required for instance_synchronized.'''
  def __init__(self):
    self.__lock__  = threading.Lock()

def init_log(prop):
  '''Initialize the log file from properties.'''
  global MAIN_LOG_FILE
  MAIN_LOG_FILE = prop.log

def paragraph(msgs):
  '''Group multi-line messages with proper indent.'''
  for lineno, line in enumerate(msgs.split('\n')):
    if lineno > 0:
      line = '   | ' + line
    yield line + '\n'

@synchronized
def log(fmt, *args):
  '''Main log function for pipeline execution.'''
  if MAIN_LOG_FILE is None:
    return
  with open(MAIN_LOG_FILE, 'a') as logf:
    for line in paragraph(fmt % args):
      logf.write(line)

def message(fmt, *args):
  '''Default output for pipeline execution.'''
  msg = fmt % args
  log(fmt, *args)
  print msg

class HadoopFailure(Exception):
  '''Hadoop execution failure.'''
  pass

def exec_external(cmdlist, config, output):
  '''Utility to run a command with given parameters'''

  try:
    if '${output}' in config.args: # Ignore stdout if ${output} is specified.
      return _exec_external(cmdlist, config)
    else:
      with open(output, 'w') as outf:
        return _exec_external(cmdlist, config, outf)
  except Exception, e:
    if not config.ignore_error:
      raise e

def _exec_external(cmdlist, config, outf=None):
  '''Internal wrapper to execute a command with timeout.'''

  exec_timeout = False
  def process_timeout(proc):
    '''Timeout callback to kill running process.'''
    try:
      proc.kill()
      exec_timeout = True
    except:
      pass # Okay, already terminated.

  proc = subprocess.Popen(cmdlist,
                          stdout=outf,
                          stderr=config.stderr,
                          env=config.env, # env variables
                          cwd=config.cwd, # directory to execute
                          preexec_fn=config.preexec_fn)

  if not config.timeout is None:
    timer = threading.Timer(config.timeout, process_timeout, [proc])
    timer.start()
  else:
    # TODO: Provide a default timeout.
    timer = None

  proc.wait() # Wait for termination or timeout.

  if not timer is None:
    timer.cancel()

  if exec_timeout:
    raise TimeoutError('Process timeout: %d sec(s)' % config.timeout)

  return proc.returncode
