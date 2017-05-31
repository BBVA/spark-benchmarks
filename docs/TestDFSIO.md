TestDFSIO
=========

Overview
--------

TestDFSIO is the canonical example of a benchmark that attempts to measure the HDFS's capacity for reading and 
writing bulk data. The test can measure the time taken to create a given number of large files, and then use those same 
files as inputs to a test to measure the read performance an HDFS instance can sustain.

The original version is included in the Hadoop's MapReduce job client library. However, since we were running the tests 
on a Spark Standalone cluster, we needed using a modified version of this benchmark based entirely on Spark and fully 
compatible with the Alluxio filesystem.

Setup
-----