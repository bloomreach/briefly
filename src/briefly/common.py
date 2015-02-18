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

import random

from wrappers import *
import process

@simple_process
def cat(self, *sources):
  '''Concatnate all sources'''
  for dep in self.deps:
    if dep.output is None:
      continue
    for line in self.read(dep.output):
      self.write('%s', line)

@simple_process
def head(self, limit=10):
  '''Get the first n lines'''
  count = 0
  for line in self.read():
    self.write('%s', line)
    count += 1
    if count >= limit:
      break

@simple_process
def cut(self, column=1, sep='\t'):
  '''Get certain field seperated by dep'''
  index = column - 1
  for line in self.read():
    fields = line.strip().split(sep)
    self.write('%s\n', fields[index])

@simple_shell_process
def sort(self, key='1', numeric=False, reverse=False):
  '''Sort file, call gnu sort command'''
  args = ['-k%s' % key]
  if numeric:
    args += ['-n']
  if reverse:
    args += ['-r']    
  self.config.defaults(
    cmd = 'sort',
    args = args + ['${input}', '-o', '${output}'])

class SortedProcess(SimpleProcess):
  '''A process that is running locally.
     It ensures all sources are sorted.
  '''
  def add_dep(self, dep):
    '''Check sources, inject sort process.'''
    if isinstance(dep, Node):
      sp = dep | sort()
      dep = sp.check_configure()      
    return super(SortedProcess, self).add_dep(dep) 

def simple_sorted_process(func):
  '''A simple local running process.
     The wrapped function is the do_execute() of the process.
  '''
  class process_wrapper(SortedProcess):
    def do_execute(self):
      func(self, *self.args, **self.kargs)

  process_wrapper.__name__ = func.__name__
  return process_wrapper

def line_count(reader):
  '''Count number of repeat with uniq lines. It will
     emit (line, count) tuple for further processing.
  '''
  uniq = None
  count = 0
  for line in reader:
    if uniq != line:
      if not uniq is None:
        yield (uniq, count)
      uniq = line
      count = 1
    else:
      count += 1
  if not uniq is None and count > 0:
    yield (uniq, count)

@simple_sorted_process
def uniq(self):
  '''Get unique set of lines'''
  for line, count in line_count(self.read()):
    self.write('%s', line)

@simple_sorted_process
def uniq_only(self):
  '''Get only the unique lines from source'''
  for line, count in line_count(self.read()):
    if count == 1:
      self.write('%s', line)

@simple_sorted_process
def dup_only(self):
  '''Get only the duplicated lines from source'''
  for line, count in line_count(self.read()):
    if count > 1:
      self.write('%s', line)

@simple_sorted_process
def uniq_count(self, sep='\t'):
  '''Output the unique line count'''
  for line, count in line_count(self.read()):
    self.write('%s%s%d\n', line.strip(), sep, count)

@simple_process
def sample(self, limit=10):
  '''Resevoir sampling'''
  buffer = []
  for lineno, line in enumerate(self.read()):
    if lineno < limit:
      buffer.append(line)
    else:
      index = random.randrange(lineno)
      if index < limit:
        buffer[index] = line        

  for line in buffer:
    self.write(line)

@simple_process
def filter(self, predicate, inverse=False):
  '''Filter out lines with given predicate.
     Supported predicate types are string of regular expression,
     regular expression object, or a boolean function.
  '''
  def re_gen(pattern):
    def re_pred(line):
      return not pattern.search(line) is None
    return re_pred

  def str_gen(pattern):
    return re_gen(re.compile(pattern))
  ptype = type(predicate)
  pfunc = {
    type(re.compile('')): re_gen,
    str: str_gen
  }

  if pfunc.has_key(ptype):
    predicate = pfunc[ptype](predicate)

  for line in self.read():
    if predicate(line) != inverse: # xor here.
      self.write(line)

@simple_process
def count(self, desc='count'):
  '''Count total number of lines'''
  count = 0
  for line in self.read():
    count += 1

  self.write('%s\t%d\n' % (desc, count))

@simple_shell_process
def diff(self, other):
  '''Diff two files, call join command'''
  self.config.defaults(
    cmd = 'diff',
    args = ['${input}', other.output])

@simple_sorted_process
def join(self, other, f1='1', f2='1', flags=[]):
  '''Join two files, call join command'''
  args = ['-1 %s' % f1, '-2 %s' % f2] + flags
  self.config.defaults(
    cmd = 'join',
    args = args + ['${input}', other.output])

class wait(Process):
  ''' Join and wait the dependents. '''
  def configure(self):
    ''' Set the output to None. '''
    super(wait, self).configure()
    self.output = None

  def check(self):
    ''' Never skip this step since it's no-op anyway. '''
    return False

  def execute(self):
    '''Execute join process, do nothing.'''
    self.executed = True

class s4copy(Process):
  ''' Perform s4 copy from s3 to s3 location. '''
  def __init__(self, tos3):
    ''' tos3: s3 destination location. '''
    super(s4copy, self).__init__()
    self.tos3 = tos3

  def configure(self):
    ''' Set the output to the s3. '''
    super(s4copy, self).configure()
    self.output = self.tos3

  def execute(self):
    ''' Copy from s3 to s3. '''
    self.log('%s => %s', self.main_src.output, self.output)
    fs.s3sync(self.main_src.output, self.output)
