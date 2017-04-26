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
import org.apache.spark.{SparkConf, SparkContext}

object TestAlluxioIO extends App with LazyLogging {

  Logger.getLogger("akka").setLevel(Level.WARN)

  TestAlluxioIOConfParser.parseAndRun(args) { conf =>

    val sparkConf = new SparkConf()
      .setAppName("TestAlluxioIO")
      .set("spark.logConf", "true")
      .set("spark.driver.port", "51000")
      .set("spark.fileserver.port", "51100")
      .set("spark.broadcast.port", "51200")
      .set("spark.blockManager.port", "51400")
      .set("spark.executor.port", "51500")

    implicit val sc = SparkContext.getOrCreate(sparkConf)

    sc.hadoopConfiguration.set("alluxio.user.file.writetype.default", "CACHE_THROUGH")
    sc.hadoopConfiguration.set("alluxio.user.block.size.bytes.default", "32MB")

    conf.mode match {
      case Clean =>
      case Write => DataWriter.generate(conf.outputDir, conf.numFiles, conf.fileSize, 10)
      case Read =>
    }

    sc.stop()

  }

}
