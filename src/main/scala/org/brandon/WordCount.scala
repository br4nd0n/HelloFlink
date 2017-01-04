package org.brandon

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(2)

    // get input data
    //val text = env.fromElements("Far out in the uncharted backwaters of the unfashionable end of the western spiral arm of the Galaxy lies a small unregarded yellow sun. Orbiting this at a distance of roughly ninety-two million miles is an utterly insignificant little blue green planet whose ape-descended life forms are so am")
    val file: DataSet[String] = env.read

    val counts: DataSet[(String,Int)] = file.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0) //parameter defines the field to group by. the key.
      .sum(1) //parameter defines the field to aggregate. the value.
      .filter { _._2 > 0}

    // emit result
    counts.print()
    counts.writeAsCsv("filtered-counts") //saves one file per degree of parallelism in the execution environment


    // execute program
    env.execute("Filtered WordCount Example")
  }
}
