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

import java.io.DataOutputStream

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.{LongWritable, SequenceFile, Text}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class EnhancedDataWriter(conf: TestAlluxioIOConf) extends TestJob(conf) with LazyLogging {

  val DefaultBufferSize: Int = 100000 // TODO should be moved to conf?
  val BaseFileName = "test_io_"
  val FsConfig: Configuration = new Configuration()
  val ControlDir: Path = new Path(conf.benchmarkDir, "io_control")
  val WriteDir: Path = new Path(conf.benchmarkDir, "io_write")
  val ReadDir: Path = new Path(conf.benchmarkDir, "io_read")
  val DataDir: Path = new Path(conf.benchmarkDir, "io_data")

  def run() = {

    val sparkConf = new SparkConf()
      .setAppName("TestAlluxioIO")
      .set("spark.logConf", "true")
      .set("spark.driver.port", "51000")
      .set("spark.fileserver.port", "51100")
      .set("spark.broadcast.port", "51200")
      .set("spark.blockManager.port", "51400")
      .set("spark.executor.port", "51500")

    implicit val sc = SparkContext.getOrCreate(sparkConf)

    sc.hadoopConfiguration.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem") // TODO REMOVE THIS!!

//    import scala.collection.JavaConversions._
//    sc.hadoopConfiguration.iterator().foreach(entry => println(s"${entry.getKey} : ${entry.getValue}"))

    val hadoopFs = FileSystem.get(sc.hadoopConfiguration)

    createControlFiles(hadoopFs, conf.fileSize, conf.numFiles)

    // TODO MEASURE
    runWriteTest(hadoopFs, DefaultBufferSize, conf.compression)

  }

 // TODO complete docs
  /**
    * This method creates a sequence file per numFiles with the file name as key and the value
    */
  private def createControlFiles(fs: FileSystem, fileSize: Long, numFiles: Int): Unit = {

    logger.info("Deleting any previous control directory...")
    fs.delete(ControlDir, true)

    logger.info("Creating control files: {} bytes, {} files", fileSize.toString, numFiles.toString)

    (0 until numFiles).map(getFileName).foreach(createControlFile(fs, fileSize))

    logger.info("Control files created for: {}  files", numFiles.toString)

  }

  private def createControlFile(fs: FileSystem, fileSize: Long)(fileName: String): Unit = {

    val controlFile = new Path(ControlDir, s"in_file_$fileName")
    logger.info("Creating control file in path {}, with size {} bytes", controlFile.toString, fileSize.toString)

    val writer: Writer = SequenceFile.createWriter(FsConfig,
      Writer.file(controlFile),
      Writer.keyClass(classOf[Text]),
      Writer.valueClass(classOf[LongWritable]),
      Writer.compression(CompressionType.NONE)
    )

    try {
      writer.append(new Text(fileName), new LongWritable(fileSize))
    } finally {
      writer.close()
    }

    logger.info("Control file created in path {}, with size {} bytes", controlFile.toString, fileSize.toString)
  }

  private def runWriteTest(fs: FileSystem, bufferSize: Int, compression: Option[String])
                       (implicit sc: SparkContext): Unit = {

    logger.info("Deleting any previous data and write directories...")
    fs.delete(DataDir, true)
    fs.delete(WriteDir, true)

    val hadoopConf = new Configuration(sc.hadoopConfiguration)

    // set buffer size
    hadoopConf.setInt("test.io.file.buffer.size", bufferSize)

    // set compression codec
    compression match {
      case Some(codec) =>
        hadoopConf.setBoolean(FileOutputFormat.COMPRESS, true)
        hadoopConf.set(FileOutputFormat.COMPRESS_CODEC, getCompressionCodecClass(codec))
        hadoopConf.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.toString)
      case None => // ignore
    }

    val files: RDD[(Text, LongWritable)] = sc.sequenceFile(ControlDir.toString, classOf[Text], classOf[LongWritable])

    files.saveAsNewAPIHadoopFile(DataDir.toString, classOf[Text], classOf[LongWritable], classOf[DfsIOTextOutputFormat],
        hadoopConf)


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

private[alluxio] class DfsIOTextOutputFormat extends FileOutputFormat[Text, LongWritable] {

  private class WholeFileRecordWriter(job: TaskAttemptContext) extends RecordWriter[Text, LongWritable] {

    private val DefaultBufferSize: Int = 100000

    private val conf = job.getConfiguration
    private val bufferSize: Int = conf.getInt("test.io.file.buffer.size", DefaultBufferSize)
    private val out: DataOutputStream = getFileOutputStream(job)

    def write(key: Text, value: LongWritable): Unit = {

      val buffer: Array[Byte] = Array.tabulate[Byte](bufferSize)(i => ('0' + i % 50).toByte)
      val fileName = key.toString
      val fileSize = value.get

     // out = getFileOutputStream(job)

      for (remaining <- fileSize to 0 by -bufferSize) {
        val currentSize = if (bufferSize.toLong < remaining) bufferSize else remaining.toInt
        out.write(buffer, 0, currentSize)
      }

    }

    def close(context: TaskAttemptContext): Unit = out.close()

    private def getFileOutputStream(job: TaskAttemptContext): DataOutputStream = {
      val isCompressed = FileOutputFormat.getCompressOutput(job)
      if (isCompressed) {
        val codecClass = FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
        val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, conf)
        val extension = codec.getDefaultExtension
        val file = getDefaultWorkFile(job, extension)
        val fs = file.getFileSystem(conf)
        val fileOut = fs.create(file, false)
        new DataOutputStream(codec.createOutputStream(fileOut))
      } else {
        val file = getDefaultWorkFile(job, "")
        val fs = file.getFileSystem(conf)
        val fileOut = fs.create(file, false)
        fileOut
      }
    }

  }

  def getRecordWriter(job: TaskAttemptContext): RecordWriter[Text, LongWritable] = new WholeFileRecordWriter(job)

}

//class TestFileWriter(@transient private val rdd: RDD[(String, Long)]) extends Serializable with LazyLogging {
//
//  def write(configuration: Configuration, fs: FileSystem, dataDir: Path, bufferSize: Int): Unit =
//    rdd.foreach { case (fileName, fileSize) =>
//
//      val buffer: Array[Byte] = Array.tabulate[Byte](bufferSize)(i => ('0' + i % 50).toByte)
//
//      val filePath = new Path(dataDir, fileName)
//      logger.info("Creating file {}", filePath.toString)
//      val out = fs.create(filePath, true, bufferSize)
//
//      managed(out) { out =>
//        for (remaining <- fileSize to 0 by bufferSize) {
//          val currentSize = if (bufferSize.toLong < remaining) bufferSize else remaining.toInt
//          out.write(buffer, 0, currentSize)
//        }
//      }
//
//    }
//
//  // TODO improve this
//  def managed[A <: java.io.Closeable](resource: A)(f: A => Unit): Unit = {
//    try {
//      f(resource)
//    } finally {
//      resource.close()
//    }
//  }
//
//}