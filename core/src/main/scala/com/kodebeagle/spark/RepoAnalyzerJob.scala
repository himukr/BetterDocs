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

import java.io.{File, PrintWriter}

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.indexer.SourceFile
import com.kodebeagle.logging.Logger
import com.kodebeagle.model.GithubRepo.GithubRepoInfo
import com.kodebeagle.model.{GithubRepo, JavaRepo, RepoIndexStatus}
import com.kodebeagle.util.SparkIndexJobHelper._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SerializableWritable, SparkConf}
import org.eclipse.jgit.diff.DiffEntry

import scala.util.Try

object RepoAnalyzerJob extends Logger {

  private val language = "Java"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("RepoAnalyzerJob")
    val sc = createSparkContext(conf)
    sc.hadoopConfiguration.set("dfs.replication", "1")
    val sqlContext = new SQLContext(sc)

    val metaDataLoc = Try {
      val arg = args(0)
      if (arg.contains(",")) {
        arg.split(",").map(s => s"${KodeBeagleConfig.repoMetaDataHdfsPath}/${s.trim}")
      } else if (arg.contains("-")) {
        // This is a range
        val rangeArr = arg.split("-").map(_.trim.toInt)
        val start = rangeArr.head
        val end = rangeArr.last
        val chunkSize = KodeBeagleConfig.chunkSize.toInt
        val completeRange = (start until end).grouped(chunkSize).toList
        completeRange.map(r => {
          val head = r.head
          s"${head}-${head + KodeBeagleConfig.chunkSize.toInt -1}"
        }).map(s => s"${KodeBeagleConfig.repoMetaDataHdfsPath}/${s.trim}").toArray
      } else {
        Array(arg)
      }
    }.toOption.getOrElse(Array(KodeBeagleConfig.repoMetaDataHdfsPath))

    val df = sqlContext.read.json(metaDataLoc: _*)
    val repoInfos = selectFromDf(df)

    val confBroadcast = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))
    // filter: size < 1 GB, and stars > 5 and language in ('Scala', 'Java')
    val javaReposRDD = repoInfos.filter(filterRepo)
      .flatMap(rInfo => GithubRepo(confBroadcast.value.value, rInfo))
      .map(repo => new JavaRepo(repo))
    handleJavaIndices(confBroadcast, javaReposRDD)
  }

  private def selectFromDf(df: DataFrame) = {
    val repoInfos = df.select("id", "name", "full_name", "owner.login",
      "private", "fork", "size", "stargazers_count", "watchers_count",
      "forks_count", "subscribers_count", "default_branch", "language")
      .rdd.map {r =>
        val id: Long = Option(r.get(0)).get.asInstanceOf[Long]
        val name: String = Option(r.get(1)).get.asInstanceOf[String]
        val fullName: String = Option(r.get(2)).get.asInstanceOf[String]
        val owner_Login: String = Option(r.get(3)).get.asInstanceOf[String]
        val isPrivate: Boolean = Option(r.get(4)).get.asInstanceOf[Boolean]
        val isFork: Boolean = Option(r.get(5)).get.asInstanceOf[Boolean]
        val size: Long = Option(r.get(6)).get.asInstanceOf[Long]
        val stars: Long = Option(r.get(7)).getOrElse(0L).asInstanceOf[Long]
        val watchers: Long = Option(r.get(8)).getOrElse(0L).asInstanceOf[Long]
        val forks: Long = Option(r.get(9)).getOrElse(0L).asInstanceOf[Long]
        val subscribers: Long = Option(r.get(10)).getOrElse(0L).asInstanceOf[Long]
        val defaultBranch: String = Option(r.get(11)).get.asInstanceOf[String]
        val language: String = Option(r.get(12)).getOrElse("").asInstanceOf[String]

        GithubRepoInfo(
          id, owner_Login, name, fullName, isPrivate, isFork, size, watchers,
          language, forks, subscribers, defaultBranch, stars)
    }
    repoInfos
  }

  // **** Helper methods for the job ******// 
  private def filterRepo(ri: GithubRepoInfo): Boolean = {
    /*(ri.size / 1000 < 1000) && ri.stargazersCount > 25 && Option(ri.language).isDefined &&*/
      Seq("Java", "Scala").exists(_.equalsIgnoreCase(ri.language))
  }

  private def handleJavaIndices(confBroadcast: Broadcast[SerializableWritable[Configuration]],
                                javafileIndicesRDD: RDD[JavaRepo]) = {

    javafileIndicesRDD.foreachPartition((repos: Iterator[JavaRepo]) => {
      val fs = FileSystem.get(confBroadcast.value.value)
      repos.foreach(handleJavaRepos(fs))
    })
  }

  private def handleJavaRepos(fs: FileSystem)(javarepo: JavaRepo) = {
    import scala.sys.process._
    val login = javarepo.baseRepo.repoInfo.get.login
    val repoName = javarepo.baseRepo.repoInfo.get.name
    val srchRefFileName = s"/tmp/kodebeagle-srch-$login~$repoName"
    val srcFileName = s"/tmp/kodebeagle-src-$login~$repoName"
    val metaFileName = s"/tmp/kodebeagle-meta-$login~$repoName"
    val typesInfoFileName = s"/tmp/kodebeagle-typesInfo-$login~$repoName"
    val deleteIndicesFileName = s"/tmp/kodebeagle-deleteindices-$login~$repoName"
    val srchrefWriter = new PrintWriter(new File(srchRefFileName))
    val srcWriter = new PrintWriter(new File(srcFileName))
    val metaWriter = new PrintWriter(new File(metaFileName))
    val typesInfoWriter = new PrintWriter(new File(typesInfoFileName))
    val deleteIndicesWriter = new PrintWriter(new File(deleteIndicesFileName))
    try {
      if (javarepo.files.isEmpty) {
        log.info(s"Repo $login/$repoName does not seem to contain anything java.")
      }
      javarepo.files.foreach(file => {
        val srchRefEntry = toJson(file.searchableRefs,
          Option(file.searchableRefs.file))
        val metaDataEntry = toJson(file.fileMetaData,
          Option(file.fileMetaData.fileName))
        val sourceEntry = toJson(SourceFile(file.repoId,
          file.repoFileLocation, file.fileContent))
        val typesInfoEntry = toJson(file.typesInFile)
        srchrefWriter.write(srchRefEntry + "\n")
        metaWriter.write(metaDataEntry + "\n")
        srcWriter.write(sourceEntry + "\n")
        typesInfoWriter.write(typesInfoEntry + "\n")
      })
      if(RepoIndexStatus.EXISTING == javarepo.baseRepo.repoIndexStatus){
        handleDeleteIndices(javarepo, deleteIndicesWriter)
      }
    }finally {
      Seq(srchrefWriter, srcWriter, metaWriter,
        deleteIndicesWriter,typesInfoWriter).foreach(_.close())
    }
    val moveIndex: (String, String) => Unit = moveFromLocal(login, repoName, fs)
    moveIndex(srcFileName, "sources")
    moveIndex(srchRefFileName, "tokens")
    moveIndex(metaFileName, "meta")
    moveIndex(typesInfoFileName, "typesinfo")
    moveIndex(deleteIndicesFileName, "deleteindices")
    val repoCleanCmd = s"rm -rf /tmp/kodebeagle/$login/$repoName"
    log.info(s"Executing command: $repoCleanCmd")
    repoCleanCmd.!!
    s"rm -f $srcFileName $srchRefFileName $metaFileName $deleteIndicesFileName".!!
  }

  def handleDeleteIndices(javarepo: JavaRepo, deleteIndicesWriter: PrintWriter): Unit = {
    val deletedFiles = javarepo.baseRepo.gitHubDeletedFiles.get
    deletedFiles.foreach(file=> {
      val deleteEntry = toDeleteIndexJson(file.repoFileLocation)
      deleteIndicesWriter.write(deleteEntry + "\n")
    })
  }

  private def moveFromLocal(login: String, repoName: String, fs: FileSystem)
                           (indxFileName: String, indxName: String) = {
    fs.moveFromLocalFile(new Path(indxFileName),
      new Path(s"${KodeBeagleConfig.repoIndicesHdfsPath}$language/$indxName/$login~$repoName"))
  }
}
