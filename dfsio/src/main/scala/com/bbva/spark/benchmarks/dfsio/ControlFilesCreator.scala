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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

object ControlFilesCreator {

  val BaseFileName = "test_io_"

  def createFiles(controlDirPath: String, numFiles: Int, fileSize: Long)(implicit sc: SparkContext): Unit = {
    sc.parallelize(0 until numFiles, numFiles).map(getFileName).map { fileName =>
      val controlFilePath = new Path(controlDirPath, s"in_file_$fileName")
      (controlFilePath.toString, new LongWritable(fileSize))
    }.saveAsSequenceFileByKey(controlDirPath)
  }

  implicit class RichRDD[T](val self: RDD[T]) extends AnyVal {
    def saveAsSequenceFileByKey[K, V](path: String)(implicit ev: RDD[T] => PairRDDFunctions[K, V]): Unit =
      self.saveAsHadoopFile(path, classOf[Text], classOf[LongWritable], classOf[RDDMultipleSequenceFileOutputFormat])
  }

  private def getFileName(fileIndex: Int): String = BaseFileName + fileIndex

  class RDDMultipleSequenceFileOutputFormat extends MultipleSequenceFileOutputFormat[Any, Any] {

    override def generateActualKey(key: Any, value: Any): Any = new Text(key.toString.split("/").last)

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      new Path(key.toString).toString

  }
}
