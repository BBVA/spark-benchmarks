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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.util.Random

object DataWriter {

  def writeTests(outputDir: String, numFiles: Int, fileSize: Long, numPartitions: Int, writeBehavior: String): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("TestAlluxioIO")
      .set("spark.logConf", "true")
      .set("spark.driver.port", "51000")
      .set("spark.fileserver.port", "51100")
      .set("spark.broadcast.port", "51200")
      .set("spark.blockManager.port", "51400")
      .set("spark.executor.port", "51500")

    implicit val sc = SparkContext.getOrCreate(sparkConf)

    sc.hadoopConfiguration.set("alluxio.user.file.writetype.default", writeBehavior)
    sc.hadoopConfiguration.set("alluxio.user.block.size.bytes.default", "32MB")

    val bufferSize = 20

    val dataSet: RDD[(String, String)] = sc.parallelize(1 to numFiles, numFiles)
      .mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => {
        val random = new Random()
        it.map(_ => (1 to fileSize.toInt).map(_ => random.nextPrintableChar()).mkString("")).map(s => (index.toString, s))
      }
    , preservesPartitioning = true)

    dataSet.saveAsTextFileByKey(outputDir)

    sc.stop()

  }

  implicit class RichRDD[T](val self: RDD[T]) extends AnyVal {
    def saveAsTextFileByKey[K, V](path: String)(implicit ev: RDD[T] => PairRDDFunctions[K, V]): Unit =
      self.saveAsHadoopFile(path, classOf[NullWritable], classOf[Text], classOf[RDDMultipleTextOutputFormat])
  }

}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  override def generateActualKey(key: Any, value: Any): Any = NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    new Path(key.toString, name).toString

}