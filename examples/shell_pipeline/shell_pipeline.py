#
# Copyright 2015 BloomReach, Inc.
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

from briefly import *
from briefly.common import *

objs = Pipeline('Simple shell pipeline')
prop = objs.prop

@simple_process
def pretty_print(self, header=None):
  '''A simple process that wrap the output with header and footer.'''
  self.write('**** %s ****\n' % header)
  for line in self.read():
    self.write(line)
  self.write('\n')

@simple_process
def dump(self):
  '''Dump result to standard output.'''
  for line in self.read():
    self.write(line)
    print line,

@simple_process
def split(self):
  '''Split individual alphabet words and output them in each line.'''
  for line in self.read():
    for word in line.split(' '):
      if word.isalpha():
        self.write(word.strip() + '\n')

@simple_shell_process
def get_date(self, days_ago):
  '''A shell process to run "date" command to get output string.'''
  self.config.defaults(
      cmd = 'date',
      timeout = 10,
      args = ['--date', '%d days ago' % days_ago, '+"%Y/%m/%d"']
    )

def date_list():
  '''Get a list of targets running get_date() for the past few days.'''
  targets = []
  for i in range(5):
    targets.append(objs | get_date(i))
  return targets

#
# Demo target 1
# Find each line with @bloomreach.com from the source and with * in in the beginning.
#
target1 = objs.source(prop.input_doc) | filter('@bloomreach.com') | filter(re.compile(r'^\s+\*')) | pretty_print('Target 1')

#
# Demo target 2
# Get a list of dates by running date command with shell process.
#
target2 = objs | cat(*date_list()) | pretty_print('Target 2')

#
# Demo target 3
# Generate histogram for the input file. See common.py for more utility processes.
#
target3 = objs.source(prop.input_doc) | split() | uniq_count() | sort(key=2, numeric=True, reverse=True) | head(5) | pretty_print('Target 3')

# Combine outputs from all targets into a single output.
final_output = objs | cat(target1, target2, target3) | dump()
objs.run(final_output)
