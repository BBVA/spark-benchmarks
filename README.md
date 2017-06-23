Spark Benchmarks
================

Overview
--------

Spark Benchmarks is a benchmarking suite specific for Apache Spark that helps to evaluate a Spark deployment 
in terms of speed, throughput and system resource utilization. It comprises a representative set of Spark workloads 
including DFSIO, TeraSort, etc.

Motivation
----------

There already exists other benchmarks suites in the community that helps to evaluate different big data 
frameworks. The more representative one is [HiBench](https://github.com/intel-hadoop/HiBench) which contains a set of 
Hadoop, Spark and streaming workloads suited for benchmarking different use cases: sorting, machine learning 
algorithms, web searches, graphs and so on. 

However, not all workloads are implemented using only Spark jobs and rely on Hadoop MapReduce framework assuming Spark
is running on top of a YARN cluster. Concretely, DFSIO benchmark, that tests the throughput of a HDFS cluster by 
generating a large number of tasks performing writes and reads simultaneously, does not have a Spark corresponding 
implementation.

The purpose of this suite is to help users to stress different scenarios of Spark combined with a distributed 
file system (HDFS, Alluxio, etc), regardless of whether it runs on Mesos, YARN or Spark Standalone. Moreover, it enables
an exhaustive study and comparision for different platform and hardware setups, sizing tuning and system optimizations, 
making easier the evaluation of their performance implications and the identification of bottlenecks.

Workloads
---------

Currently, there is only one workload available:

1. [TestDFSIO](./docs/TestDFSIO.md)

Getting started
---------------

Please visit the documentation associated to the corresponding workload.

Building Spark Benchmarks
-------------------------

### Pre-Requisites

The followings are needed for building Spark Benchmarks

* JDK 8
* [SBT 0.13.15](http://www.scala-sbt.org/0.13.15/docs/Getting-Started/Setup.html)

### Supported Spark/Hadoop releases:

* Spark 2.1.x
* Hadoop HDFS 2.x

### Building

To build all modules in Spark Benchmarks, use the below command.

```bash
sbt clean package
```

If you are only interested in a single workload you can build a single module. For example, the below command only
builds the dfsio workload.

```bash
sbt dfsio/clean dfsio/package
```


## TODO:

* Include unit tests (if necessary)
* Implement TeraSort benchmark
* Implement NNBench benchmark
* Implement PageRank benchmark

## Contributing

You can contribute to Spark Benchmarks in a few different ways:

* Submit issues through [issue tracker](https://github.com/BBVA/spark-benchmarks/issues) on Github.
* If you wish to make code changes, or contribute something new, please follow the 
[GitHub Forks / Pull requests model](https://help.github.com/articles/fork-a-repo/): 
fork the spark-benchmarks repo, make the change and propose it back by submitting a pull request.


## License

Spark Benchmarks is Open Source and available under the Apache 2 License.