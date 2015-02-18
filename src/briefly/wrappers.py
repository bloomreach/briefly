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

import process
import hadoop
import java
import shell
import os
import stat

from core import *

#
# Common wrappers to create simple process.
#

class simple_pipeline(object):
  '''Create a simple subpipeline.'''

  def __init__(self, objs):
    '''Constructor, save parent pipeline.'''
    self.pobjs = objs

  def __call__(self, func):
    '''Augment the function to pass in a child pipeline.'''    
    def pipeline_wrapper(*args, **kargs):
      objs = Pipeline('Child Pipeline:' + func.__name__, self.pobjs.prop)
      return func(objs, *args, **kargs)
    return pipeline_wrapper

def simple_process(func):
  '''A simple local running process.
     The wrapped function is the do_execute() of the process.
  '''
  class process_wrapper(process.SimpleProcess):
    def do_execute(self):
      func(self, *self.args, **self.kargs)

  process_wrapper.__name__ = func.__name__
  return process_wrapper

def simple_hadoop_process(func):
  '''A simple hadoop jobs.
     The wrapped function is to configure the hadoop parameters.
  '''
  class process_wrapper(hadoop.HadoopProcess):
    def configure(self):
      super(process_wrapper, self).configure()
      func(self, *self.args, **self.kargs)
      if not self.config.output is None:
        self.output = self.config.output
  process_wrapper.__name__ = func.__name__
  return process_wrapper

def simple_java_process(func):
  '''A simple java process.
     The wrapped function is to configure the java parameters.
  '''
  class process_wrapper(java.JavaProcess):
    def configure(self):
      super(process_wrapper, self).configure()
      func(self, *self.args, **self.kargs)
  process_wrapper.__name__ = func.__name__
  return process_wrapper

def simple_shell_process(func):
  '''A simple shell process.
     The wrapped function is to configure the shell parameters.
     The custom function can also return a full shell script to
     be executed with shell.runner.
  '''
  class process_wrapper(shell.ShellProcess):
    def configure(self):
      super(process_wrapper, self).configure()
      script = func(self, *self.args, **self.kargs)
      if script is None:
        return

      # Handle the case when returning a shell script to execute.
      script_file = self.prop.sub('${build_dir}/%s.sh' % self.hash())
      with open(script_file, 'w') as outf:
        outf.write(script)
      os.chmod(script_file, stat.S_IRWXU)
      self.config.defaults(
        runner = self.prop.shell.runner,
        cmd = script_file,
        args = [])

  process_wrapper.__name__ = func.__name__
  return process_wrapper
