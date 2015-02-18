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

from properties import *
from coreutils import *

class ShellProcess(process.LocalProcess):
  '''A simple process for shell execution.'''

  def __init__(self, *args, **kargs):
    '''Constructor, initialize shell configs.'''
    super(ShellProcess, self).__init__(*args, **kargs)
    self.config = Properties()    

  def execute(self):
    '''Execute this shell process.'''
    shell_runner = [self.config.runner] if self.config.runner else []
    cmdlist = shell_runner + [self.config.cmd] + self.process_args(*self.config.args)
    exec_external(cmdlist, self.config, self.output)
