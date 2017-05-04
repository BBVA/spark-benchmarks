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

package com.bbva.spark.benchmarks.dfsio

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SerializableWritable
import org.apache.spark.rdd.RDD

abstract class IOTestBase(hadoopConf: Configuration, dataDir: String) extends Serializable with LazyLogging {

  protected val DefaultBufferSize: Int = 100000
  protected val wrappedConf = new SerializableWritable(hadoopConf)

  def runIOTest(inputPaths: RDD[(Text, LongWritable)]): RDD[Stats] = {

    inputPaths.mapPartitions { partition =>

      implicit val conf = wrappedConf.value
      implicit val fs = FileSystem.get(conf)

      partition.map { case (file, size) =>

        val fileName: String = file.toString
        val fileSize: Long = size.get

        val tStart: Long = System.currentTimeMillis()  // TODO create function measure

        val output = doIO(fileName, fileSize)

        val execTime: Long = System.currentTimeMillis() - tStart

        collectStats(fileName, execTime, output)

      }
    }

  }

  def collectStats(fileName: String, execTime: Long, totalSize: BytesSize): Stats = {

    val ioRateMbSec: Float = totalSize.toFloat * 1000 / (execTime * 0x100000) // MEGA 0x100000

    logger.info("Number of bytes processed = {}", totalSize)
    logger.info("Exec time = {}", execTime)
    logger.info("IO rate = {}", ioRateMbSec)

    Stats(tasks = 1, size = totalSize, time = execTime, rate = ioRateMbSec * 1000,
      sqRate = ioRateMbSec * ioRateMbSec * 1000)

  }

  def doIO(fileName: String, fileSize: BytesSize)(implicit conf: Configuration, fs: FileSystem): BytesSize

}