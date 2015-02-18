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
def dump(self):
  '''Dump result to standard output.'''
  for line in self.read():
    self.write(line)
    print line,

@simple_process
def alpha_only(self):
  '''Filter the alphabet only words.'''
  for line in self.read():
    word, count = line.split('\t')
    if word.isalpha():
      self.write('%s\t%s', word, count)

@simple_hadoop_process
def word_count(self):
  self.config.defaults(
    main_class = 'wordcount', # This is special for hadoop-examples.jar. Use full class name instead.
    args = ['${input}', '${output}']
  )

#
# Demo target
# Generate histogram for the input file with local hadoop process. See common.py for more utility processes.
#
target = objs.source(prop.input_doc) | word_count() | alpha_only() | sort(key=2, numeric=True, reverse=True) | head(10) | dump()
objs.run(target)
