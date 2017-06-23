/*
 * Copyright 2017 Banco Bilbao Vizcaya Argentaria S.A.
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

import org.apache.spark.rdd.RDD

case class Stats(tasks: Long, size: BytesSize, time: Long, rate: Float, sqRate: Float)

object StatsAccumulator {

  def accumulate(rdd: RDD[Stats]): Stats = rdd.reduce {
    (s1, s2) => s1.copy(
      tasks = s1.tasks + s2.tasks,
      size = s1.size + s2.size,
      time = s1.time + s2.time,
      rate = s1.rate + s2.rate,
      sqRate = s1.sqRate + s2.sqRate
    )
  }

}
