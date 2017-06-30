TestDFSIO
=========

Overview
--------

TestDFSIO is the canonical example of a benchmark that attempts to measure the HDFS's capacity for reading and 
writing bulk data. It is also helpful to discover performance network bottlenecks in your network and to stress the
hardware, OS and Spark/Hadoop setup of your cluster machines. The test can measure the time taken to create a given 
number of large files, and then use those same files as inputs to a test to measure the read performance an HDFS 
instance can sustain.

The original version is included in the Hadoop's MapReduce job client library. However, since we could run the tests 
on a Spark Standalone cluster, we need to use a modified version of this benchmark based entirely on Spark and fully 
compatible with the Alluxio filesystem.

Getting started
---------------

### How it works

The TestDFSIO benchmark is used for measuring I/O (read/write) performance and it does this by using Spark jobs to read
and write files in parallel. It intentionally avoids any overhead or optimizations induced by Spark and therefore, it 
assumes certain initial requirements. For instance, files should be replicated and spread to nodes relatively evenly. As
a result, if it runs at least one task per node on the cluster, it will write out data evenly assuring correct locality.

When a write test is run via 


### How to submit the benchmark

In order 

  --master spark://spark-master:7077 \
  --class com.bbva.spark.benchmarks.dfsio.TestDFSIO \
  --total-executor-cores $total_executor_cores \
  --executor-cores $executor_cores \
  --driver-memory 1g \
  --executor-memory 1g \
  --conf spark.locality.wait=30s \
  --conf spark.driver.extraJavaOptions=-Dalluxio.user.file.writetype.default=$write_type \
  --conf spark.executor.extraJavaOptions=-Dalluxio.user.file.writetype.default=$write_type \
  --packages org.alluxio:alluxio-core-client:1.4.0 \
  "http://hdfs-httpfs:14000/webhdfs/v1/jobs/dfsio.jar?op=OPEN&user.name=openshift" \
  write --numFiles $num_files --fileSize $file_size --outputDir  alluxio://alluxio-master:19998/benchmarks/DFSIO


### Interpreting the results

Now, let's have a look at how the benchmark shows the resulting performance statistics. 

Here follows an example of a result log after a write test and a subsequent read test have been run:

```bash
----- TestDFSIO ----- : write
           Date & time: Mon May 08 2017
       Number of files: 1000
Total MBytes processed: 1000000
     Throughput mb/sec: 4.989
Average IO rate mb/sec: 5.185
 IO rate std deviation: 0.960
    Test exec time sec: 1113.53
    
----- TestDFSIO ----- : read
           Date & time: Mon May 08 2017
       Number of files: 1000
Total MBytes processed: 1000000
     Throughput mb/sec: 11.349
Average IO rate mb/sec: 22.341
 IO rate std deviation: 119.231
    Test exec time sec: 544.842
```

The most significant metrics are  *Throughput mb/sec* and *Average IO rate mb/sec*. Both of them are based on the file size
written or read by every individual task and the elapsed time to do so. Therefore, it excludes the scheduling time of the tasks 
from the calculations.

Having this in mind, the *Throughput* metric for the whole job, where *N* is the number of tasks, is defined as follows:

<p align="center">
  <img src="./throughput.gif"/>
</p>


The *Average IO rate* is defined as:

<p align="center">
  <img src="./average_io.gif"/>
</p>