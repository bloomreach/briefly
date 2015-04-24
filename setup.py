#!/usr/bin/env python

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

"""
Briefly Job Flow Control
"""

from setuptools import setup

__author__ = 'Chou-han Yang'
__author_email__ = 'chyang@bloomreach.com'
__copyright__ = 'Copyright 2015 BloomReach, Inc.'
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'
__version__ = '1.0'
__maintainer__ = __author__
__status__ = 'Development'

setup(name='briefly',
      version=__version__,
      description='Briefly Job Flow Control',
      author=__author__,
      license=__license__,
      url='https://github.com/bloomreach/briefly',
      packages=['briefly'],
      package_dir={'briefly': 'src/briefly'},
      install_requires=['boto>=2.3.0', 's4cmd>=1.5.20', 'qds_sdk==1.4.0'],
      dependency_links=['git+https://github.com/qubole/qds-sdk-py.git@v1.4.0#egg=qds_sdk-1.4.0']
     )
