package com.bbva.spark.benchmarks.alluxio

abstract class TestJob(conf: TestAlluxioIOConf) {
  def run(): Unit
}
