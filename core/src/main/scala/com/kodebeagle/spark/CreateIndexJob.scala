/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kodebeagle.spark

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.Repository
import com.kodebeagle.indexer.JavaASTBasedIndexer
import com.kodebeagle.spark.SparkIndexJobHelper._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object CreateIndexJob {

  case class SourceFile(repoId: Int, fileName: String, fileContent: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("CreateIndexJob")

    val sc: SparkContext = createSparkContext(conf)

    val zipFileExtractedRDD: RDD[(List[(String, String)],
      Option[Repository], List[String])] = makeZipFileExtractedRDD(sc)

    // Create indexes for elastic search.
    zipFileExtractedRDD.map { f =>
      val (files, repo, packages) = f
      (repo, new JavaASTBasedIndexer()
        .generateTokens(files.toMap, packages, repo), mapToSourceFiles(repo, files))
    }.flatMap { case (Some(a), b, c) =>
      Seq(toJson(a, isToken = false), toJson(b, isToken = true), toJson(c, isToken = false))
    case _ => Seq()
    }.saveAsTextFile(KodeBeagleConfig.sparkIndexOutput)

  }
}
