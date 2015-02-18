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

import os
import sys
import hashlib
import fs

from node import *

PROCESS_DEFS = []

class Process(Node):
  '''Process is a node with parameters.
     It implements the hash with given inputs and its classname,
     also provides utilities for processing parameters.
  '''

  def __init__(self, *args, **kargs):
    '''Constructor, initialize this process.'''
    super(Process, self).__init__()
    self.args = args
    self.kargs = kargs
    self.hashcode = None

  def build_dep(self):
    '''Build dependency map'''
    super(Process, self).build_dep()
    args = []
    kargs = {}

    for arg in self.args:
      if isinstance(arg, Node):
        arg.check_configure()
      args.append(self.add_dep(arg))
    for k, v in self.kargs.iteritems():
      if isinstance(v, Node):
        v.check_configure()
      kargs[k] = self.add_dep(v)

    self.args = args
    self.kargs = kargs

  def hash(self):
    '''Calculate the hash id of this process.'''
    if self.hashcode:
      return self.hashcode

    op = type(self).__name__
    elems = []
    for v in self.deps + list(self.args) + self.kargs.values():
      if isinstance(v, Process):
        elems.append(v.hash())
      else:
        elems.append(str(v))

    m = hashlib.md5()
    m.update('%s(%s)' % (op, ','.join(elems)))
    self.hashcode = '%s-%s' % (op, m.hexdigest()[:16])

    return self.hashcode

  def process_args(self, *args):
    '''Process parameters to replace default input/output.
       This function is used to call external process such as shell.
    '''
    results = []
    for arg in args:
      results.append(self.prop.sub(arg, input=self.main_src.output, output=self.output))
    return results

  def execute(self):
    '''Execute this process. Child class should override.'''
    raise NotImplementedError()

class LocalProcess(Process):
  '''A process that is running locally.
     It ensures all sources are downloaded to local filesystem.
  '''

  def add_dep(self, dep):
    '''Check sources, inject extra process to download files.'''
    if isinstance(dep, Node) and (not dep.output is None) and fs.is_s3url(dep.output):
      s3p = dep | S3Process()
      dep = s3p.check_configure()
    return super(LocalProcess, self).add_dep(dep) 

  def execute(self):
    '''Execute this process. Child class should override.'''
    raise NotImplementedError()

  def execute_error(self, exp):
    '''Remove partial output when error'''
    try:
      os.remove(self.output)
    except:
      pass # okay.

class RemoteProcess(Process):
  '''A process that is running remotely.
     It ensures all sources are uploaded to s3.
  '''

  def add_dep(self, dep):
    '''Check sources, inject extrace process to upload files.'''
    if isinstance(dep, Node) and (not dep.output is None) and not fs.is_s3url(dep.output):
      s3p = dep | S3Process()
      dep = s3p.check_configure()
    return super(RemoteProcess, self).add_dep(dep) 

  def execute(self):
    '''Execute this process. Child class should override.'''
    raise NotImplementedError()

class SimpleProcess(LocalProcess):
  '''A process that provides simple utilities for reading inputs and
     writing outputs. It is running locally.
  '''

  def read(self, filename=None):
    '''Generator of reading input files.'''
    if filename is None:
      filename = self.main_src.output
    if os.path.isdir(filename):
      filelist = [os.path.join(filename, f) for f in os.listdir(filename) if not f.startswith('.')]
    else:
      filelist = [filename]

    for f in filelist:
      inf = open(f, 'r')
      for line in inf:
        yield line
      inf.close()

  def write(self, fmt, *args):
    '''Write a single line to output file.'''
    self.outf.write(fmt % args)

  def execute(self):
    '''Execute this process by calling do_execute()'''
    self.outf = open(self.output, 'w')
    try:
      self.do_execute()
    finally:
      self.outf.close()

  def do_execute(self):
    '''Execute this process. Child class should override.'''
    raise NotImplementedError()

  def __str__(self):
    '''Utility to read file content to be used as parameter.'''
    with open(self.output, 'r') as outf:
      return outf.read()

  def iter(self):
    '''Iterate line by line of the output.'''
    with open(self.output, 'r') as outf:
      for line in outf:
        yield line

class SourceProcess(Process):
  '''Nodes with parameters'''
  def __init__(self, objs, uri):
    super(SourceProcess, self).__init__(uri) # send uri for hashing.
    self.uri = str(uri)
    self.objs = objs
    self.prop = objs.prop

  def configure(self):
    '''Configure source node, set output as the URI parameter.'''
    super(SourceProcess, self).configure()
    self.output = self.uri
    self.executed = True # no need for actual execution.

  def execute(self):
    '''Execute this node, just add log here.'''
    self.log('URI: %s', self.uri)


class PhonyProcess(Process):
  '''Nodes with parameters'''
  def __init__(self, objs):
    super(PhonyProcess, self).__init__()
    self.objs = objs
    self.prop = objs.prop

  def configure(self):
    '''Configure source node, set output to None.'''
    super(PhonyProcess, self).configure()
    self.output = None
    self.executed = True # no need for actual execution.

  def get_timestamp(self):
    '''Return the timestamp of last successful execution, always zero'''
    return 0

  def execute(self):
    '''Execute phony process, do nothing.'''
    pass

class S3Process(Process):
  def configure(self):
    '''Set the output to the opposite of the input.
       This process will upload or download a file from/to s3.
    '''
    super(S3Process, self).configure()
    if not fs.is_s3url(self.main_src.output):
      self.output = self.prop.sub('${s3_root}/%s' % self.output)

  def execute(self):
    '''Execute S3Process, upload or download a file.'''
    self.log('%s => %s', self.main_src.output, self.output)
    fs.s3sync(self.main_src.output, self.output)
