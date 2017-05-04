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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, SequenceFile, Text}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Distributed I/O benchmark.
  *
  * This test application writes into or reads from a specified number of files. The number of bytes to write or read
  * is also specified as a parameter to the test. By default, each file is accessed in a separate spark task.
  *
  */
object TestDFSIO extends App with LazyLogging {

  val BaseFileName = "test_io_"
  val ControlDir = "io_control"
  val WriteDir = "io_write"
  val ReadDir =  "io_read"
  val DataDir = "io_data"

  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  TestDFSIOConfParser.parseAndRun(args) { conf =>

    val sparkConf = new SparkConf().setAppName("TestDFSIO").set("spark.logConf", "true")

    implicit val sc = SparkContext.getOrCreate(sparkConf)
    implicit val hadoopConf = new Configuration(sc.hadoopConfiguration)

    // set buffer size
    hadoopConf.setInt("test.io.file.buffer.size", conf.bufferSize)

    // set compression codec
    conf.compression.foreach { codec =>
      hadoopConf.setBoolean(FileOutputFormat.COMPRESS, true)
      hadoopConf.set(FileOutputFormat.COMPRESS_CODEC, getCompressionCodecClass(codec))
      hadoopConf.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.toString)
    }

    conf.mode match {
      case Clean =>
        cleanUp(conf.benchmarkDir)
      case Write =>
        createControlFiles(conf.benchmarkDir, conf.fileSize, conf.numFiles)
        // TODO MEASURE AND ANALYZE
        val stats = runWriteTest(conf.benchmarkDir)
        println(stats)
      case Read =>
        val stats = runReadTest(conf.benchmarkDir)
        println(stats)
      case _ => // ignore
    }

  }

  private def cleanUp(benchmarkDir: String)(implicit hadoopConf: Configuration): Unit = {
    logger.info("Cleaning up test files")
    val fs = FileSystem.get(hadoopConf)
    fs.delete(new Path(benchmarkDir), true)
  }

  private def createControlFiles(benchmarkDir: String, fileSize: Long, numFiles: Int)
                                (implicit hadoopConf: Configuration): Unit = {

    val controlDirPath: Path = new Path(benchmarkDir, "io_control")

    logger.info("Deleting any previous control directory...")
    val fs = FileSystem.get(hadoopConf)
    fs.delete(controlDirPath, true)

    logger.info("Creating control files: {} bytes, {} files", fileSize.toString, numFiles.toString)
    (0 until numFiles).map(getFileName).foreach(createControlFile(hadoopConf, controlDirPath, fileSize))
    logger.info("Control files created for: {}  files", numFiles.toString)

  }

  private def createControlFile(hadoopConf: Configuration, controlDir: Path, fileSize: Long)(fileName: String): Unit = {

    val controlFilePath = new Path(controlDir, s"in_file_$fileName")
    logger.info("Creating control file in path {}, with size {} bytes", controlFilePath.toString, fileSize.toString)

    val writer: Writer = SequenceFile.createWriter(hadoopConf,
      Writer.file(controlFilePath),
      Writer.keyClass(classOf[Text]),
      Writer.valueClass(classOf[LongWritable]),
      Writer.compression(CompressionType.NONE)
    )

    try {
      writer.append(new Text(fileName), new LongWritable(fileSize))
    } finally {
      writer.close()
    }

    logger.info("Control file created in path {}, with size {} bytes", controlFilePath.toString, fileSize.toString)
  }

  private def runWriteTest(benchmarkDir: String)
                          (implicit hadoopConf: Configuration, sc: SparkContext): Stats = {

    val controlDirPath: Path = new Path(benchmarkDir, "io_control")
    val dataDirPath: Path = new Path(benchmarkDir, DataDir)
    val writeDirPath: Path = new Path(benchmarkDir, WriteDir)

    logger.info("Deleting any previous data and write directories...")
    val fs = FileSystem.get(hadoopConf)
    fs.delete(dataDirPath, true)
    fs.delete(writeDirPath, true)

    val files: RDD[(Text, LongWritable)] = sc.sequenceFile(controlDirPath.toString, classOf[Text], classOf[LongWritable])

    val stats: RDD[Stats] = new IOWriter(hadoopConf, dataDirPath.toString).runIOTest(files)

    StatsAccumulator.accumulate(stats)
    // TODO should i write the accumulated stats to hdfs (one field per line) and then read again to analyze them?
  }

  private def runReadTest(benchmarkDir: String)(implicit hadoopConf: Configuration, sc: SparkContext): Stats = {

    val controlDirPath: Path = new Path(benchmarkDir, "io_control")
    val dataDirPath: Path = new Path(benchmarkDir, DataDir)
    val readDirPath: Path = new Path(benchmarkDir, ReadDir)

    logger.info("Deleting any previous read directories...")
    val fs = FileSystem.get(hadoopConf)
    fs.delete(readDirPath, true)

    val files: RDD[(Text, LongWritable)] = sc.sequenceFile(controlDirPath.toString, classOf[Text], classOf[LongWritable])

    val stats: RDD[Stats] = new IOReader(hadoopConf, dataDirPath.toString).runIOTest(files)

    StatsAccumulator.accumulate(stats)

  }

  private def getFileName(fileIndex: Int): String = BaseFileName + fileIndex

  private def getCompressionCodecClass(codec: String): String = getCompressionCodec(codec).getName

  private def getCompressionCodec(codec: String): Class[_ <: CompressionCodec] =
    codec match {
      case "gzip" => classOf[GzipCodec]
      case "snappy" => classOf[SnappyCodec]
      case "lz4" => classOf[Lz4Codec]
      case "bzip2" => classOf[BZip2Codec]
    }

}
