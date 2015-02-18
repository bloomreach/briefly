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
import calendar
import time
import threading

from boto.s3.connection import S3Connection
try:
  from br.s4cmd.s4cmd import *
except:
  # Open source repo compatible
  from s4cmd import *

# Initialize keys first.
S3Handler.init_s3_keys()

# Shared filesystem/s3 utilities.
def is_s3url(path):
  '''Check if the path is a value s3 URL.'''
  return (not path is None) and S3URL.is_valid(path)

def get_fs(path):
  '''Get a Filesystem node depending on the path.'''
  if is_s3url(path):
    return S3FileSystem(path)
  return LocalFileSystem(path)

def exists(path):
  '''Check existence of a path.'''
  return get_fs(path).exists()

def getmtime(path):
  '''Get modified time of a path.'''
  fs = get_fs(path)
  if not fs.exists():
    return None
  return fs.getmtime()

def s3sync(source, target):
  '''Upload or download from/to S3, or copy from one s3 location to another S3 location'''
  if is_s3url(source) and is_s3url(target):
    get_fs(source).copy_files(source, target)
  elif is_s3url(source):
    get_fs(source).get_files(target)
  elif is_s3url(target):
    get_fs(target).put_files(source)
  else:
    raise Exception('Unkown s3 operation')

class FileSystem(object):
  '''A common filesystem interface'''
  def __init__(self, path):
    self.path = path

  def exists(self):
    '''Check if the path exists'''
    raise NotImplementedError()

  def getmtime(self):
    '''Get the last modified timestamp of the path'''
    raise NotImplementedError()

  def size(self):
    '''Number of items for the given path'''
    raise NotImplementedError()

class LocalFileSystem(FileSystem):
  def exists(self):
    '''Check if the path exists'''
    return os.path.exists(self.path)

  def getmtime(self):
    '''Get the last modified timestamp of the path'''
    return int(os.path.getmtime(self.path))

  def size(self):
    '''Number of items for the given path'''
    if os.path.isdir(self.path):
      return len(os.listdir(self.path))
    return 1

class S3FileSystem(FileSystem):
  calendar_lock = threading.Lock()

  def __init__(self, path):
    '''Constructor, initialize s3 path and cache.'''
    super(S3FileSystem, self).__init__(path)
    assert is_s3url(self.path)
    self.data = None

  def get_s3handle(self):
    '''Get a s3 handle for s3 operation.'''    
    return S3Handler()

  def _s3walk(self):
    '''Walk s3 path to get detailed information, cache the result.'''
    if self.data is None:
      # XXX Calling s4cmd s3walk() cause race condition in thread pool.
      #     use boto library directly here for now. Will fit it later.
      conn = S3Connection(*S3Handler.S3_KEYS)
      s3url = S3URL(self.path)
      bucket = conn.lookup(s3url.bucket)
      self.data = []
      for key in  bucket.list(s3url.path, PATH_SEP):
        self.data.append(key)
      conn.close()

    return self.data

  def exists(self):
    '''Check if the path exists'''
    return self.size() > 0

  def size(self):
    '''Number of items for the given path'''
    return len(self._s3walk())

  def getmtime(self):
    '''Get the last modified timestamp of the path'''
    if not self._s3walk() or not self._s3walk()[0].last_modified:
      return None

    with self.calendar_lock: # XXX strptime not thread-safe, see http://bugs.python.org/issue7980
      result = calendar.timegm(time.strptime(self._s3walk()[0].last_modified, '%Y-%m-%dT%H:%M:%S.000Z'))
    return result

  def get_files(self, target):
    '''Download s3 files'''
    handle = self.get_s3handle()
    handle.opt.recursive = True
    handle.opt.sync_check = True
    handle.opt.force = True
    handle.opt.num_threads = 1
    handle.get_files(self.path, target)

  def put_files(self, source):
    '''Upload s3 files'''
    handle = self.get_s3handle()
    handle.opt.recursive = True
    handle.opt.sync_check = True
    handle.opt.force = True
    handle.opt.num_threads = 1
    handle.put_files(source, self.path)

  def copy_files(self, source, target):
    '''Copy files from s3 to s3 location'''
    handle = self.get_s3handle()
    handle.opt.recursive = True
    handle.opt.force = True
    handle.opt.num_threads = 1
    handle.cp_files(source, target)
