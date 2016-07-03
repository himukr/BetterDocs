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

import java.io.{File, PrintWriter}

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.indexer.{RepoFileNameInfo, Repository}
import com.kodebeagle.logging.Logger
import com.kodebeagle.parser.RepoFileNameParser
import com.kodebeagle.spark.producer.{JavaIndexProducer, ScalaIndexProducer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

import scala.collection.Map

object UpdateExistingIndicesJob extends Logger {

  import com.kodebeagle.spark.util.SparkIndexJobHelper._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("UpdateExistingIndicesJob")

    val sc = createSparkContext(conf)
    val batch = args(0)

    val diffMap: Broadcast[Map[String, (List[String], List[String])]] = sc.broadcast(sc.textFile(
      s"${KodeBeagleConfig.gitDiffDir}/git-repo-diff-file-$batch.txt").
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
      }).map { case (x, y) => (x, (y._1 ++ y._2 ++ y._4 ++ y._5, y._3 ++ y._4 ++ y._6)) }
      .collectAsMap()
    )

    val totalIndexRDD: RDD[(String, (String, String))] = makeRDD(sc, batch)
    val addIndexRdd: RDD[(String, (String, String))] = makeAddRDD(totalIndexRDD, diffMap)

    val repoIndex = sc.broadcast(createRepoIndex(totalIndexRDD, batch + "-diff"))
    val (javaRDD, scalaRDD) = (addIndexRdd.filter { case (_, file) => file._1.endsWith(".java") },
      addIndexRdd.filter { case (_, file) => file._1.endsWith(".scala") })
    JavaIndexProducer.createIndices(javaRDD, batch + "-diff", repoIndex.value)
    ScalaIndexProducer.createIndices(scalaRDD, batch + "-diff", repoIndex.value)

    deleteIndices(diffMap.value, batch + "-diff")

  }

  def extractChanges(x: String): (JValue, JValue, JValue, JValue,
    JValue, JValue, List[Nothing], List[Nothing]) = {
    val repoDiff = parse(x)
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

  private def deleteIndices(diffMapValue: Map[String, (List[String], List[String])],
                            batch: String) {
    case class RepoId(repoId: String)
    case class FileName(file: String)
    case class ShouldMatch(match_phrase: FileName)
    case class MustMatch(term: RepoId)

    new File(s"${KodeBeagleConfig.sparkIndexOutput}/$batch").mkdirs()
    new PrintWriter(s"${KodeBeagleConfig.sparkIndexOutput}/${batch}/deleteIndexFiles") {
      write(diffMapValue.filter { case (x, y) => y._2.size > 0 }
        .map { case (repoName, y) => {
          val maybeFileInfo: Option[RepoFileNameInfo] = RepoFileNameParser(repoName)
          (MustMatch(RepoId(extractRepoId(repoName))),
            y._2.map(deletedFile =>
              ShouldMatch(FileName(fileNameToUrl(deletedFile, maybeFileInfo)))))
        }
        }
        .map(x => toDeleteIndexTypeJson(x._1, x._2)).mkString("\n")
      )
      close()
    }
  }

  def makeAddRDD(rdd: RDD[(String, (String, String))], diffMap:
  Broadcast[Map[String, (List[String], List[String])]]): RDD[(String, (String, String))] = {
    val addRdd = rdd
      .filter { case (x, y) => {
        val file = diffMap.value.get(x)
        if (file != None) {
          file.get._1.contains(y._1.substring(y._1.indexOf("/") + 1))
        }
        else {
          false
        }
      }
      }.cache()
    addRdd
  }

  private def extractRepoDirName(x: String) = x.substring(0, x.indexOf('/'))

  private def extractRepoId(repoName: String) = repoName.split("~")(3)

  private def fileNameToUrl(deletedFile: String, maybeFileInfo: Option[RepoFileNameInfo]) = {
    val maybeFileUrl=maybeFileInfo.
      map(fileInfo=>
        s"""${fileInfo.login}/${fileInfo.name}/blob/${fileInfo.defaultBranch}/$deletedFile""")
    maybeFileUrl.getOrElse("")
  }


}
