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

package com.kodebeagle.spark.job

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.logging.Logger
import com.kodebeagle.spark.producer.{JavaIndexProducer, ScalaIndexProducer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

object UpdateExistingIndicesJob extends Logger {

  import com.kodebeagle.spark.util.SparkIndexJobHelper._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("UpdateExistingIndicesJob")

    val sc = createSparkContext(conf)

    val diffMap: Broadcast[Map[String, (List[String], List[String])]] = sc.broadcast(sc.textFile(
      s"${KodeBeagleConfig.gitDiffDir}/git-repo-diff-file.txt").
      map(x => {
        val (repoName: JValue, addedFiles: JValue, deleteFiles: JValue, modifiedFiles: JValue,
        renamedFiles: JValue, copiedFiles: JValue, renamedFilesForAdd: List[Nothing],
        renamedFilesForDelete: List[Nothing]) = extractChanges(x)
        implicit val formats = DefaultFormats
        renamedFiles.extract[List[String]].
          map(x => {
            renamedFilesForAdd ++ x.substring(x.indexOf("->") + 2)
            renamedFilesForDelete ++ x.substring(0, x.indexOf("->"))
          })

        (repoName.extract[String], (addedFiles.extract[List[String]],
          copiedFiles.extract[List[String]], deleteFiles.extract[List[String]],
          modifiedFiles.extract[List[String]], renamedFilesForAdd, renamedFilesForDelete))
      }).map { case (x, y) => (x, (y._1 ++ y._2 ++ y._4 ++ y._5, y._3 ++ y._6)) }.collectAsMap()
    )

    val rdd: RDD[(String, (String, String))] = makeRDD(sc, args(0))

    val (addIndexRdd, removeIndexRdd) = addAndDeleteRdd(rdd, diffMap)

    log.info(s"re-Indexing ${addIndexRdd.count} from ${rdd.count} total files in this batch")
    val repoIndex = sc.broadcast(createRepoIndex(addIndexRdd, args(0)))
    val (javaRDD, scalaRDD) = (addIndexRdd.filter { case (_, file) => file._1.endsWith(".java") },
      addIndexRdd.filter { case (_, file) => file._1.endsWith(".scala") })
    JavaIndexProducer.createIndices(javaRDD, args(0), repoIndex.value)
    ScalaIndexProducer.createIndices(scalaRDD, args(0), repoIndex.value)
  }

  def addAndDeleteRdd(rdd: RDD[(String, (String, String))], diffMap:
  Broadcast[Map[String, (List[String], List[String])]]): (RDD[(String, (String, String))],
    RDD[(String, (String, String))]) = {
    (rdd.filter { case (x, y) => {
      val file = diffMap.value.get(x)
      if (file != None) {
        file.get._1.contains(y._1.substring(y._1.indexOf("/") + 1))
      }
      else {
        false
      }
    }
    },
      rdd.filter { case (x, y) => {
        val file = diffMap.value.get(x)
        if (file != None) {
          file.get._2.contains(y._1.substring(y._1.indexOf("/") + 1))
        }
        else {
          false
        }
      }
      }
      )
  }

  def extractChanges(x: String): (JValue, JValue, JValue, JValue,
    JValue, JValue, List[Nothing], List[Nothing]) = {
    val repoDiff = parse(x)
    val repoId = repoDiff \ "repoId"
    val repoName = repoDiff \ "repoFileName"
    val addedFiles = repoDiff \ "addedFiles"
    val deleteFiles = repoDiff \ "deleteFiles"
    val modifiedFiles = repoDiff \ "modifiedFiles"
    val renamedFiles = repoDiff \ "renamedFiles"
    val copiedFiles = repoDiff \ "copiedFiles"
    val renamedFilesForAdd = List()
    val renamedFilesForDelete = List()
    (repoName, addedFiles, deleteFiles, modifiedFiles, renamedFiles,
      copiedFiles, renamedFilesForAdd, renamedFilesForDelete)
  }
}
