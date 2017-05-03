/*
 * Copyright 2017 BBVA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bbva.spark.benchmarks.alluxio

import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.json4s.BuildInfo

/**
  * Distributed I/O benchmark.
  *
  * This test application writes into or reads from a specified number of files. The number of bytes to write or read
  * is also specified as a parameter to the test. By default, each file is accessed in a separate spark task.
  *
  */
object TestAlluxioIO extends App with LazyLogging {

  Logger.getLogger("akka").setLevel(Level.WARN)

  TestAlluxioIOConfParser.parseAndRun(args) { conf =>

    printOptions(conf)

    System.setProperty("alluxio.user.file.writetype.default", conf.writeBehavior)
    System.setProperty("alluxio.user.file.readtype.default", conf.readBehavior)

    val job: TestJob = conf.mode match {
      case Clean => new DataCleaner(conf)
      case Write => new EnhancedDataWriter(conf)
      //case Read =>
      //case _ => // ignore
    }

    job.run()

  }

  private def printOptions(conf: TestAlluxioIOConf): Unit = {
    logger.info(s"${TestAlluxioIO.getClass.getSimpleName}.${BuildInfo.version}")
    logger.info("Test mode = {}", conf.mode.command)
    logger.info("numFiles = {}", conf.numFiles)
    logger.info("fileSize = {}", conf.fileSize)
    conf.compression.foreach(codec => logger.info("compression = {}", codec))
    conf.mode match {
      case Write =>
        logger.info("outputDir = {}", conf.benchmarkDir)
        logger.info("writeBehavior = {}", conf.writeBehavior)
      case Read =>
        logger.info("inputDir = {}", conf.benchmarkDir)
        logger.info("readBehavior = {}", conf.readBehavior)
      case _ => // ignore
    }
  }

}
