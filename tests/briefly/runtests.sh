#!/bin/bash

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

#
# Script to execute Briefly test cases
#

testcases=${1-testscripts/test-*.test *.py}

rm -rf build
mkdir build

for test in ${testcases}
do
  # Special cases for Python unit tests
  if [[ $test == *.py ]]; then
    echo "#> Unit test: $test"
    PYTHONPATH=../../src python $test
    continue
  fi

  build_dir=build/$(basename ${test%.*})
  result=$(basename ${test%.*}).out

  mkdir -p ${build_dir}
  grep '^#>' ${test}
  PYTHONPATH=../../src python $test -Dbuild_dir=${build_dir} | grep '^>>' > ${build_dir}/${result}

  diff <(env LC_ALL="C" sort ${build_dir}/${result}) testscripts/${result}
  if [[ $? -eq 0 ]]; then
    echo "   - ${test} OK"
  else
    echo "   - ${test} failed"
  fi
done
