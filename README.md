Briefly Job Flow Control
========================

[![Join the chat at https://gitter.im/bloomreach/briefly](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/bloomreach/briefly?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Chou-han Yang (chyang@bloomreach.com)
(Version 1.0)

Overview
--------

Briefly is a Python based meta programming library designed to manage complex workflows with various type of tasks, such as Hadoop (local, Amazon EMR, or Qubole), Java processes, and shell commands, with a minimal and elegant [Hartman pipeline](http://en.wikipedia.org/wiki/Hartmann_pipeline) syntax. Briefly provides resource management for job execution i.e. infrastructure and operational logic so that developers can focus on their job logic.

At [BloomReach](http://www.bloomreach.com), we run thousands of hadoop jobs everyday on clusters with hundreds of machines. We use Briefly to manage our critical pipelines, and to maintain the robustness and efficiency of data processing jobs.

#### Supported types of processes

  * Simple python process
  * Local hadoop process
  * Amazon Elastic Mapreduce hadoop process
  * Qubole hadoop process
  * Java process
  * Shell process

Each process is capable of defining its own dependencies and declares its own output. The library provides a lot of default wrappers to help start off with minimal customization.

#### Features

  - Use of a [Hartman pipeline](http://en.wikipedia.org/wiki/Hartmann_pipeline) to create job flow.
  - Resource management for multiple Hadoop clusters ([Amazon EMR](http://aws.amazon.com/elasticmapreduce/), [Qubole](http://www.qubole.com/)) for parallel execution, also allowing customized Hadoop cluster.
  - Individual logs for each process to make debugging easier.
  - Fully resumable pipeline with customizable execution check and error handling.
  - Encapsulated local and remote filesystem (s3) for unified access.
  - Automatic download of files from s3 for local process and upload of files to s3 for remote process with [s4cmd](https://github.com/bloomreach/s4cmd).
  - Automatic fail/retry logic for all failed processes.
  - Automatic price upgrades for EMR clusters with spot instances.
  - Timeout for Hadoop jobs to prevent long-running clusters.
  - Dynamic cluster size adjustment for EMR steps (to be implemented)

External and Third-Party Library Requirements
---------------------------------------------

  - s4cmd (>=1.5.20): [https://github.com/bloomreach/s4cmd](https://github.com/bloomreach/s4cmd)
  - boto (>=2.30.0): [https://github.com/boto/boto](https://github.com/boto/boto)
  - Qubole SDK (>=1.4.0): [https://github.com/qubole/qds-sdk-py](https://github.com/qubole/qds-sdk-py)

Required libraries will be installed automatically with setup.py.

Installation
------------

Clone briefly from github

```
git clone https://github.com/bloomreach/briefly.git
```

Install python package

```
cd briefly
python setup.py install
```

Getting Started
---------------

### Your First Briefly Pipeline

```python
from briefly import *
from briefly.common import *

objs = Pipeline("My first pipeline")
prop = objs.prop

@simple_process
def dump(self):
  '''Dump all data from the source node'''
  for line in self.read():
    self.write(line)
    print line,

target = objs.source(prop.input) | sort() | dump()
objs.run(target)
```

Run your pipeline:

```shell
mkdir build
python pipeline.py -Dinput=demo.txt
```

You will see the sorted output of the file:

```
Configuring targets...
Ann     5003
Jonh    5001
Lee     5004
Smith   5002
Releasing resources...
1 total target(s), 1 executed.
```

Note that if you run it again, you won't see anything because Briefly checks the source and target date and see there is no need to generate the target again.

```
Configuring targets...
Releasing resources...
1 total target(s), 1 executed.
```

You just need to delete the content of build directory to execute it again.

### Create a Propery File for Options

As the pipeline grows more complex, we will have more configurations. It makes sense to put all pipeline configurations into a file (or multiple files). We can create demo.conf:

```
my_input = "demo.txt"
some_opt1 = ["1", "2", "3"]
some_opt2 = {"a": 1, "b": 2, "c": 3}
some_opt3 = true
```

The value of each option can be any valid JSON object. See next chapter for full usage on property files and how to read values out from it. Now you can execute again with *-p* option:

```
python pipeline.py -pdemo.conf
```

Multiple configuration files can be passed in and the configuration passed in later will overwrite the one load previously:

```
python pipeline.py -p demo.conf -p foo.conf -p bar.conf -Dopt1=xxx -Dopt2=yyy
```

### Chain to hadoop process in pipeline

OK, so now we have some basic pipelines running. We can add more complex processes such as hadoop process. We are going to use Hadoop's Word Count in hadoop-examples.jar. First, we have to set a property in demo.conf:

```
my_input = "demo.txt"

# This tells Briefly to run hadoop locally. Valid options are local, emr, and qubole
hadoop.runner = "local"
```

Now we can chain the pipeline with our first hadoop job:

```python
from briefly import *
from briefly.common import *

objs = Pipeline("My first hadoop pipeline")
prop = objs.prop

@simple_process
def dump(self):
  for line in self.read():
    self.write(line)
    print line,

@simple_hadoop_process
def word_count(self):
  self.config.hadoop.jar = 'hadoop-examples.jar' # path to your local jar
  self.config.defaults(
    main_class = 'wordcount', # This is special for hadoop-examples.jar. Use full class name instead.
    args = ['${input}', '${output}']
  )

target = objs.source(prop.my_input) | sort() | dump()
target2 = objs.source(prop.my_input) | word_count() | dump()

objs.run(target, target2)
```

Run it again, we will see the output:

```txt
Configuring targets...
Ann 5003
Jonh  5001
Lee 5004
Smith 5002
5001  1
5002  1
5003  1
5004  1
Ann 1
Jonh  1
Lee 1
Smith 1
Releasing resources...
2 total target(s), 2 executed.
```

Full log for each process can be found in build directory. You can found the main execution log in build/execute.log:

```txt
Running pipeline: My first pipeline
Configuring targets...
 - sort-6462031860a6f17b : executing
 - word_count-7f55d503e26321d7 : executing
 - sort-6462031860a6f17b : done
 - dump-ea94f33ba627c5f6 : executing
 - dump-ea94f33ba627c5f6 : done
 - word_count-7f55d503e26321d7 : done
 - dump-7f06c703ee1089fb : executing
 - dump-7f06c703ee1089fb : done
Releasing resources...
2 total target(s), 2 executed.
```

Congratulations! Now that you've completed the demo pipeline, it is easy to go from here to build more complex pipelines. Things you can do:

  * Create multiple property files for test, staging, and production runs
  * Put your files on Amazon S3
  * Change hadoop.runner to use Amazone EMR or Qubole
  * And more...

Pipeline Basics
---------------

Briefly pipelines always bind to a property collection. The collection provides basic settings and environment for the entire pipeline. You can set the default value and combine with the properties loaded from *-p* command line parameter to override. See properties section for more details.

Internally, Briefly creates a DAG (directed acyclic graph) to resolve the dependencies between processes.
Every function-like process in Briefly is actually a node class factory. So all the executions are deferred until you call

```python
objs.run(targets)
```

### Pipeline Creation

To create a new pipeline is simple:

```python
objs = Pipeline("Name of your pipeline")
prop = objs.prop
```

The pipeline constructor will also generate a new property collection, from which you can get or set values.

### Node Executions

Each process you chain in the pipeline will be augmented to a class by the decorator, such as *@simple_process*. Different types of process require different initialization. For example, a function with *@simple_hadoop_process* will just let you configure all the necessary parameters to invoke hadoop process instead of actually processing data inside the function.

The *Node* class in Briefly looks like:

```python
class Node(object):
  ...
  def configure(self):
    '''Main configuration method.
       Setup the dependencies and create hash id here.
       Child class can override this method to modify the job flow.
    '''
    ...

  def check(self):
    '''Check if we can skip the execution.
       Child class should override this function to provide
       customized check.
    '''
    ...

  def execute(self):
    '''Execute this node. Child class should override.'''
    ...
```

  * `configure()` -- setup a node for execution, rewire process flow and setup necessary parameters.
  * `check()` -- to verify if a node has been executed before by checking file timestamps, therefore can be skipped.
  * `execute()` -- actual implementation of the process execution.

### Phases of execution

There are 3 major phases for pipeline constructions and execution:

  * `DAG construction` - Follow the code execution order of the Python program, your job flow will be constructed with a DAG as internal representation. This phase runs in single thread.
  * `Node Configure` - Check if each node meets the dependency requirements for execution. Also rewire the DAG for automatic file transfer if needed. This phase runs in single thread.
  * `Node Execute` - Execute all processes with the order and precedence with the DAG. This runs with multiple threads. Each node execution will hold a thread during execution.

### Default Properties

All Briefly system configurations have default values set in **defaults.py**. This file also provides good references of what options provided by Briefly.

Property Collection
-------------------

As a job flows get more complicated, we may want to have more options to control a job flow. More and more control options and parameters will be included into the property collection. Briefly properties are designed to make complex configuration simple.

#### Features
  * Python name binding - Use python symbol name such as prop.your_options instead of `prop['your_options']`, although both methods are provided.
  * Hierarchical name space - You can have embedded collection, such as `prop.some_group.some_settings`.
  * Stackable properties - Multiple configuration files can be combined, providing a common configuration with a special one-off configuration for override.
  * Multiple value types - Augment JSON as values for parsing. This allows other platforms such as Java to read and parse configuration easily.
  * String substitution with lazy evaluation - String substitution is needed especially for path composition, such as `'s3://${your_bucket}/${your_option}/somewhere'`. This is basically the pointer between properties, which reduce the requirement of string concatenation.
  * Simple setup utility function - Take advantage of Python's keyword parameter of a function. You can simply do `prop.set(opt1=xxx, opt2=yyy)`.

### Create and Set Properties

The constructor or the Properties class can be used to set values:

```python
prop = Properties(
  a = 3,
  b = 'xxx',
  c = Properties(
    d = 5
  )
)
```

Then you can retrieve the values with several different type of method:

```python
print prop.a # ==> 3
print prop['a'] # ==> 3
print prop.c.d # ==> 5
print prop['c.d'] # ==> 5
```

Use *set()* method to set multiple values:

```python
prop.set(
  x = [1, 2, 3],
  y = true
)
```

You can also set individual properties:

```python
prop.a = 100
prpp.new_entry = 200
prop['a'] = 50
prop['c.d'] = 'abcd'
prop['new_entr2'] = 'new'
```

Or use string substitution for lazy evaluation:

```python
prop.path = '${b}/ddd/${c.d}'
print prop.path # ==> 'xxx/ddd/5'
```

### Load and Save Properties

The same properties can be loaded from or saved to a config file. The format will be,

```
opt1 = <<JSON value>>
opt2 = <<JSON value>>

group1.op1 = <<JSON value>>
group1.op2 = <<JSON value>>
```

You can also use `@import` directive to import other property files:

```
@import = "other.conf"

foo = "xxx"
bar = "yyy"
```

TODO: Note that during serialization, Briefly is not going to create subgroups for now. So it is required to provide default values before load from configuration file:

```python
prop.defaults(
  opt1 = 0,
  opt2 = 0,
  group1 = Properties(
    opt1 = 0,
    opt2 = 0
  )
)
```

Unrecognized JSON string will be seen as raw strings. For example:

```
opt1 = foo bar
opt2 = 'foo bar'
```

So those values will be:

```python
print prop.opt1 # ==> "foo bar"
print prop.opt2 # ==> "'foo bar'"
```

Noted that JSON string always use double quote. So single quote is not valid (TODO: This may be changed).

To save a property collection back to a file simply use *save()*:

```python
prop.save('foo.conf')
```

Execution and Flow Control
--------------------------

You can use Briefly to create very complex job flows. But remember, execution will be deferred until you invoke `run()`. So during the job flow construction, you may decide to change the job flow topology based on some conditions or checks. But after the execution starts, Briefly will control all the execution assuming the job flow is already fixed. Changing the job flow topology during execution is not recommended and my cause unknown execution results or errors.

### Node Alias

All process nodes need to have a source node. The pipeline object can be used as a dummy source node if the leading process doesn't have any dependencies:

```python
target = objs | first_process() | second_process()
```

The variable `target` is going to point to the last execution node, which is `second_process`.

Each execution node can be assigned to a variable as an alias:

```python
target = objs | first_process()
target = target | second_process()
```

which is equivalent to previous topology. Sometimes, it is cleaner and more readable to create alias for each steps.

### Node Branch

Alias can also be used to create branches:

```python
target = objs | first_process()
foo = target | foo_process()
bar = target | bar_process()
```

When executing the job flow with multiple available nodes, Briefly will try to execute with the creation order. Therefore, `foo_process()` will have higher precedence than `bar_process()`. Of course, if multiple threads are available, they will be executed at the same time.

### Process Dependencies

To create dependencies between nodes, you can simply chain them. When you have multiple dependencies, you can just pass into the node creators as parameters:

```python
foo = objs | foo_process()
bar = bar | bar_process()
target = combine_process(foo, bar)
```

This way, Briefly will detect the parameters are actual nodes for execution, so `combine_process` needs to wait for the completion of `foo_process` and `bar_process`.

### Node Hash

Every process node in Briefly requires a unique hash id so that it can be used to create output files and logs. It also uses the timestamps of the files to check if a process can be skipped. Briefly also use node hash to identify same process from the same sources to avoid duplicate executing the same process against the same set of data:

```python
input = objs.source(prop.input_file) | preprocess()
target1 = input | sort() | dump()
target2 = input | sort() | dump()
```

In this case, Briefly will identify the `sort` process in two pipelines have the same sources, therefore, it is not necessary to execute `sort` process twice.

Wrappers
--------

### What are Wrappers?

To make job flow construction easier, Briefly also provides a set of wrappers to help create process node class instead of defining node classes over and over again in Python. Wrappers leverages Python's decorator syntax. Those functions will be augmented to a node class:

```python
@simple_process
def dump(self):
  '''Dump all data from the source node'''
  for line in self.read():
    self.write(line)
    print line,
```

will be augmented to (conceptually):

```python
class dump(process.SimpleProcess):
  def do_execute(self):
   for line in self.read():
     self.write(line)
     print line,
```

The definition of `@simple_process` can be found in **wrappers.py**

```python
def simple_process(func):
  '''A simple local running process.
     The wrapped function is the do_execute() of the process.
  '''
  class process_wrapper(process.SimpleProcess):
    def do_execute(self):
      func(self, *self.args, **self.kargs)

  process_wrapper.__name__ = func.__name__
  return process_wrapper
```

### Hadoop Wrapper

To invoke external hadoop process, you have to prepare several config parameters:

  * Jar file to execute
  * Main class name
  * Arguments for the job

So `@simple_hadoop_process` wrapped these inside a function so you can provide enough information for the execution:

```python
@simple_hadoop_process
def my_hadoop_job(self):
  self.config.hadoop.jar = 'hadoop-uber-jar.jar'
  self.config.defaults(
    main_class = 'com.bloomreach.HadoopJob',
    args = ['${input}', '${output}', 'foo', 'bar']
  )
```

will be augmented to (conceptually):

```python
class my_hadoop_job(hadoop.HadoopProcess):
  def configure(self):
    self.config.hadoop.jar = 'hadoop-uber-jar.jar'
    self.config.defaults(
      main_class = 'com.bloomreach.HadoopJob',
      args = ['${input}', '${output}', 'foo', 'bar']
    )
```

The actual invocation to hadoop cluster depends on the property `hadoop.runner`. So it can be used to run hadoop locally, remotely on Amazon EMR, or on Qubole.

### Java Wrapper

To execute Java process, you can set the corresponding properties for java process:

```
java.runner = "/path/to/java"
java.max_process = 3
```

`runner` set the path to the java binary, and `max_process` controls number of concurrent Java process allowed for execution.

Then simply use `@simiple_java_process` to customize the parameters

```python
@simple_java_process
def my_java_job(self):
  self.config.defaults(
    main_class = 'com.bloomreach.SomeClass'
    args = ['args', '${input}', '${output}']
  )
```

Use `${input}` to reference to the input file from the source if you need it. And optionally, use `${output}` to save the final output to a file if the Java process doesn't output directly to console.

### Shell Wrapper

To execute shell commands, simply use `@simple_shell_process`:

```python
@simple_shell_process
def list_file(self):
  self.config.defaults(
    cmd = '/bin/ls', # binary of the executable
    timeout = 60,
    args = ['/tmp']
  )
```

Use `${output}` in `args` if the process doesn't output to console directly. Set timeout to make sure that the shell process ends correctly in given time period, otherwise, it will fail and retry. More common shell process examples can be found in **common.py**.

For some shell script with a lot of input variables, you can also sent entire property collection with environment varialbe. In this case, your shell script can easily access all the properties from Briefly process:

```python
@simple_shell_process
def shell_script_with_env(self):
  # Complex parameters going to be sent
  vars = Properties()
  vars.param1 = 'xxx'
  vars.param2 = 'yyy'

  self.config.defaults(
    cmd = 'path/to/your/script.sh',
    env = dict(os.environ.items() + vars.get_data().items()),
    args = ['arg1', 'arg2']
  )
```

Therefore, you can access `${param1}` and `${param2}` directly in the shell script.

You can also dynamically running a shell by returning a shell script from the method:

```python
@simple_shell_process
def run_dynamic_shell(self):
  self.config.defaults(
    args = ['${output}']
  )
  return '''\
#!/bin/bash
echo "Hello, world" > $1
'''
```

Hadoop Runner
-------------

In order to control very large hadoop clusters on Amazon EMR, Briefly also manages the clusters internally. So the complex operational issues can be isolated from the job flow logic. To control the execution of hadoop jobs, there are several properties to be set:

```
# Where to execute hadoop, valid values can be ['local', 'emr', 'qubole']
hadoop.runner = "emr"

# Location to the jar file. Use S3 path to run hadoop on EMR or Qubole.
hadoop.jar = "your_hadoop.jar"

# Default output path.
hadoop.root = "path/to/default/hadoop/output"
```

### Running Hadoop Locally

As the example shown in the first section. We can set the hadoop runner to local:

```
hadoop.runner = "local"
```

Then Briefly will invoke hadoop directly on the executing machine.

### Amazon EMR

To execute your hadoop job on Amazon EMR, you need couple of properties setup:

```
hadoop.runner = "emr"

# Max number of concurrent EMR clusters to be created
emr.max_cluster = 10

# Instances groups for each cluster
emr.instance_groups = [[1, "MASTER", "m2.2xlarge"], [9, "CORE", "m2.2xlarge"]]

# Name of your EMR cluster
emr.cluster_name = "my-emr-cluster"

# A unique name for the project for cost tracking purpose
emr.project_name = "my-emr-project"

# Where EMR is going to put yoru log
emr.log_uri = "s3://log-bucket/log-path/"

# EC2 key pairs if you with to login into your EMR cluster
emr.keyname = "ec2-keypair"

# Spot instnace price upgrade strategy. The multipliers to the EC2 ondemand price you want
# to bid against the spot instances. 0 means use ondemand instances.
emr.price_upgrade_rate = [1.5, 2.0, 0]
```

You also need to set the keys for your EC2 access:

```
ec2.key = "your_ec2_key"
ec2.secret = "your_ec2_secret"
```

Not that the number max of EMR clusters will be bounded by the number of threads you have. Therefore, you need to increase number of threads along with `emr.max_cluster`:

```
run_threads = 16
```

Please refer to **defaults.py** for all configurations you need to run hadoop on Amazon EMR.

### Qubole

To execute your hadoop job on Qubole, you need couple of properties setup:

```
hadoop.runner = "qubole"

# Qubole execution api token
qubole.api_token = "qubole_token"

# EC2 access keys
qubole.aws_access_key_id = "your_ec2_key"
qubole.aws_secret_access_key = "your_ec2_secret"

# Max concurrent clusters
qubole.max_cluster = 10

# Max jobs per cluster
qubole.max_job_per_cluster = 1

# Project name
qubole.project = "your-qubolep-project"

# Qubole cluster settings
qubole.hadoop_settings = { \
  "master_instance_type": "m2.2xlarge", \
  "slave_instance_type": "m2.2xlarge", \
  "initial_nodes": 1, \
  "max_nodes": 9}
```

Common Utility Processes
------------------------

Please import the utility processes from **common.py** separately.

#### cat(*sources)

Concatenate all output from all sources.

```python
foo = objs.source(prop.source1) | foo_process()
bar = objs.source(prop.source2) | bar_process()
target = cat(foo, bar)
```

#### head(limit=10)

Cut first few list from the source.

```python
target = objs.source(prop.source1) | head(limit=20)
```

#### cut(column=1, sep='\t')

Cut out specific columns from the source.

```python
target = objs.source(prop.source1) | cut(5)
```

#### sort(key='1', numeric=False, reverse=False)

Sort the source input.

```python
target = objs.source(prop.source1) | sort(reverse=True)
```

#### uniq()

Uniquify the source. The source will be sorted automatically.

```python
target = objs.source(prop.source1) | uniq()
```

#### uniq_only()

Select unique lines from the source. The source will be sorted automatically.

```python
target = objs.source(prop.source1) | uniq_only()
```

#### dup_only()

Select duplicated lines from the source. The source will be sorted automatically.

```python
target = objs.source(prop.source1) | dup_only()
```

#### uniq_count(sep='\t')

Generate the count for each unique line from source. The source will be sorted automatically.

```python
target = objs.source(prop.source1) | uniq_count()
```

#### sample(limit=10)

Reservior sampler from the source.

```python
target = objs.source(prop.source1) | sample(limit=20)
```

#### filter(predicate, inverse=False)

Filter is similar to `grep` command, but provide more versatile predicate. The predicate can be:

  * `Python function` - return True or False with given line as input.
  * `String` - Look for a substring in lines.
  * `Regular Expression` - Use regular expression to match lines.

```python
target = objs.source(prop.source1) | filter(re.compile('$www\..*\.com'))
```

#### count()

Count the number of lines in the source

```python
target = objs.source(prop.source1) | count()
```

#### diff(diff)

Compare source input with another. Invoke `diff` command.

```python
foo = objs.source(prop.source1)
target = objs.source(prop.source2).diff(foo)
```

#### join(other, f1='1', f2='1', flags=[])

Join two inputs with `join` command. Both sources will be sorted automatically.

```python
foo = objs.source(prop.source1)
target = objs.source(prop.source2).join(foo)
```

Contributors
------------

  * Chou-han Yang (chyang@bloomreach.com) - Main developer/Project owner
  * Shao-Chuan Wang (shaochuan.wang@bloomreach.com)
  * Yiran Wang (yiran.wang@bloomreach.com)
  * Kazuyuki Tanimura (kazu@bloomreach.com)
  * Sumeet Khullar (sumeet@bloomreach.com)
  * Prateek Gupta (prateek@bloomreach.com)
  * Amit Kumar (amit.kumar@bloomreach.com)
  * Ching-lun Lin (ching-lun.lin@bloomreach.com)
  * Viksit Gaur (viksit@bloomreach.com)

More Information
----------------

   Bloomreach Engineering Blog: [http://engineering.bloomreach.com/briefly-python-dsl-scale-mapreduce-pipelines/](http://engineering.bloomreach.com/briefly-python-dsl-scale-mapreduce-pipelines/)

License
-------

  Apache License Version 2.0
  http://www.apache.org/licenses/LICENSE-2.0
