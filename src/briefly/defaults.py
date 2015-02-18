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

import getpass

from properties import *

# Bare minimum system settings for any pipeline
PIPELINE_DEFAULT_PROPERTIES = Properties(
  # System-wide default values.
  build_dir = "build",
  num_retry = 3,
  retry_delay = 10,
  username = getpass.getuser(),
  log = "${build_dir}/execute.log",

  run_threads = 2,  # Execution threads
  debug = False,    # Extra debug information
  test_run = False, # Dry-run for check execution flow

  # Default values for shell process.
  shell = Properties(
    max_process = 4,
    runner = "/bin/sh",
  ),

  # Default values for java process.
  java = Properties(
    max_process = 2,
    classpath = ["."], # full list of classpath
    runner = "java", # full path to java executable.
  ),

  # Default values for hadoop process (local or remote).
  hadoop = Properties(
    runner = "emr",
    jar = None, # s3://<BUCKET>/path/hadoop_jobs.jar
    root = "${build_dir}",
    bin = "hadoop", # Full path to hadoop binary to execute (local mode only)
  ),

  # Default values for EMR cluster.
  emr = Properties(
    max_cluster = 2,
    cluster_name = "${username}-cluster",
    step_name = "${node_hash}",
    project_name = None, # Team name or project name to track costs
    tags = None, # EC2 tags for the EMR cluster
    keyname = None,
    instance_groups = [[1, "MASTER", "m1.small"]], # List of instance groups [num, MASTER/CORE, type]
    bootstrap_actions = [], # List of bootstrap actions: [[name1, action1, args...], [name2, action2, args...], ...]

    # Regular EC2 instance prices. See http://www.ec2instances.info/.
    prices = {"t2.micro":    0.01,
              "t1.micro":    0.02,
              "t2.small":    0.02,
              "m1.small":    0.04,
              "t2.medium":   0.05,
              "m3.medium":   0.07,
              "m1.medium":   0.08,
              "c3.large":    0.10,
              "c1.medium":   0.13,
              "m3.large":    0.14,
              "m1.large":    0.17,
              "r3.large":    0.17,
              "c3.xlarge":   0.21,
              "m2.xlarge":   0.24,
              "m3.xlarge":   0.28,
              "m1.xlarge":   0.35,
              "r3.xlarge":   0.35,
              "c3.2xlarge":  0.42,
              "m2.2xlarge":  0.49,
              "c1.xlarge":   0.52,
              "m3.2xlarge":  0.56,
              "g2.2xlarge":  0.65,
              "r3.2xlarge":  0.70,
              "c3.4xlarge":  0.84,
              "i2.xlarge":   0.85,
              "m2.4xlarge":  0.98,
              "r3.4xlarge":  1.40,
              "c3.8xlarge":  1.68,
              "i2.2xlarge":  1.70,
              "cc2.8xlarge": 2.00,
              "cg1.4xlarge": 2.10,
              "r3.8xlarge":  2.80,
              "hi1.4xlarge": 3.10,
              "i2.4xlarge":  3.41,
              "cr1.8xlarge": 3.50,
              "hs1.8xlarge": 4.60,
              "i2.8xlarge":  6.82,},
    # Price multiplier for each level. 0 means on-demand instances.
    price_upgrade_rate = [0.8, 1.5, 0],

    log_uri = None, # S3 location for mapreduce logs e.g. "s3://<BUCKET>/${username}/mr-logs"
    ami_version = "2.4.2",
    step_timeout = 12 * 60 * 60, # 12 HR (in sec)
  ),

  # Default values for Qubole cluster.
  qubole = Properties(
    api_url = "https://api2.qubole.com/api",
    api_version = "latest",
    api_token = None,
    aws_region = "us-east-1",
    aws_availability_zone = None,
    persistent_security_groups = "ElasticMapReduce-slave",
    max_cluster = 1,
    max_job_per_cluster = 1,
    termination_timeout = 5 * 60, # Wait 5 min for cluster termination (in sec). 

    project_name = None,
    hadoop_custom_config = {}, # Custom hadoop configs. Example: {"mapred.output.compress": "true", "mapred.output.compression.type": "BLOCK"} 
    hadoop_settings = {"master_instance_type": "m1.small", "slave_instance_type": "m1.small", "initial_nodes": 1, "max_nodes": 1}, # Num/type config for the cluster
    bootstrap_actions = [], # List of bootstrap actions: [[name1, action1, args...], [name2, action2, args...], ...]
    price_upgrade_rate = [0.8, 1.5, 0], # Price multiplier for each level. 0 means on-demand instances.
    timeout_for_request = 15, # Timeout for spot instance requests (in min).
    log_uri = None, # S3 location for mapreduce logs e.g. "s3://<BUCKET>/${username}/mr-logs"
    step_timeout = 43200, # 43200 = 12 * 60 * 60 = 12 HR (in sec).
    cluster_id = None,  # Default None. If a value is passed, the job is executed on that cluster, and the cluster is not terminated
    step_name = "${node_hash}", # If a value is passed, it will be displayed on the qubole analyzer and will help in debugging
  ),

  # Default values for EC2/boto commands.
  ec2 = Properties(
    key = "S3_ACCESS_KEY",
    secret = "S3_SECRET_KEY",
  ),
)
