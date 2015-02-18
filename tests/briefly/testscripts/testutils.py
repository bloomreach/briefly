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

# Utility for testing briefly

from briefly.coreutils import *
from briefly import *
import threading

@synchronized
def test_log(fmt, *args):
  msg = fmt % args
  print '>> %s' % (msg)

@simple_process
def test(self, tid, *deps):
  deps = [d.executed and d.exe_error is None for d in self.deps]
  test_log('%d %s', tid, str(deps))
