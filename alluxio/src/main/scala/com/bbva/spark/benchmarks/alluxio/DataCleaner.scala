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

import alluxio.AlluxioURI
import alluxio.client.file.FileSystem
import alluxio.client.file.options.DeleteOptions
import com.typesafe.scalalogging.LazyLogging

class DataCleaner(conf: TestAlluxioIOConf) extends TestJob(conf) with LazyLogging {

  def run() = {
    logger.info("Cleaning up test files")
    val fs = FileSystem.Factory.get()
    val path = new AlluxioURI(conf.benchmarkDir)
    if (fs.exists(path)) fs.delete(path, DeleteOptions.defaults().setRecursive(true))
  }

}
