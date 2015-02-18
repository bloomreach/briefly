#
# Copyright 2014-2015 BloomReach, Inc.
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
import time

class RateLimitLock:
  '''EMR and Qubole throttle the number of request we can send.
     This class intends to rate limit the requests sent to avoid failures
     caused by failed requests.
  '''
  MAX_DELAY = 5 * 60

  def __init__(self, delay=1.0):
    '''Constructor.'''
    self.lock = threading.Lock()
    self.delay = delay
    self.err_count = 0

  def __enter__(self):
    '''Invoked at the start of with block.'''
    self.lock.acquire()

  def __exit__(self, type, value, traceback):
    '''Invoked at the end of with block.
       If exception happened, sleep for an exponentially extended amount of
       time and then the caller would raise the exception.
    '''
    if isinstance(value, Exception):
      self.err_count += 1
    else:
      self.err_count = 0

    delay = min((2 ** self.err_count) * self.delay, self.MAX_DELAY)
    if self.err_count > 0:
      print 'delay: %f' % delay

    time.sleep(delay)
    self.lock.release()