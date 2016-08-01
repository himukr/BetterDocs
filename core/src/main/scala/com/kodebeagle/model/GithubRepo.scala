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

package com.kodebeagle.model

import java.io.File
import java.nio.file.{Path, Paths}

import com.kodebeagle.logging.Logger
import com.kodebeagle.model.GithubRepo.GithubRepoInfo
import com.kodebeagle.model.RepoIndexStatus.RepoIndexStatus
import org.apache.hadoop.conf.Configuration
import org.eclipse.jgit.lib.{ObjectId, ObjectLoader, Ref, Repository}
import org.eclipse.jgit.revwalk.{RevCommit, RevTree, RevWalk}
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.treewalk.TreeWalk

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * This is an abstraction over a github repo that is to be analyzed.
  *
  * By default it will read the repo from the hdfs cluster and will throw
  * RepoNotFoundException if it is not found.
  *
  * However, if update is set to `true` then it will first clone the repo
  * from Github, replace the existing repo on Hdfs and read from that repo.
  *
  */
class GithubRepo protected()
  extends Repo with Logger with LazyLoadSupport {

  private var _files: Option[List[GithubFileInfo]] = None
  private var _stats: Option[RepoStatistics] = None
  private var _languages: Option[Set[String]] = None
  protected var _repoGitFiles: Option[List[String]] = None
  var repoInfo: Option[GithubRepoInfo] = None
  var repoIndexStatus: RepoIndexStatus = RepoIndexStatus.NEW

  private def init(configuration: Configuration,
                   githubRepoInfo: GithubRepoInfo): Option[GithubRepo] = {
    val repoUpdateHelper = new GithubRepoUpdateHelper(configuration, githubRepoInfo.fullName)
    var createRepoIndex=false
    if (repoUpdateHelper.shouldCreate()) {
      repoUpdateHelper.create()
      createRepoIndex=true
    }else if (repoUpdateHelper.shouldUpdate()){
      repoIndexStatus=RepoIndexStatus.EXISTING
      repoUpdateHelper.update()
      createRepoIndex=true
    }
    if(createRepoIndex) {
      _repoGitFiles = Option(repoUpdateHelper.downloadLocalFromDfs())
      repoInfo = Option(githubRepoInfo)
      Some(this)
    }else{
      None
    }
  }

  override def files: List[GithubFileInfo] = {
    getOrCompute(_files, () => {
      _files = Option(readProject())
      _files.get
    })
  }

  override def statistics: RepoStatistics = {
    getOrCompute(_stats, () => {
      _stats = Option(calculateStats(files))
      _stats.get
    })
  }

  override def languages: Set[String] = {
    getOrCompute(_languages, () => {
      _languages = Option(extractLanguages(files))
      _languages.get
    })
  }

  def repository: Repository = {
    val repoPath: String = _repoGitFiles.get(0)
    val builder: FileRepositoryBuilder = new FileRepositoryBuilder
    builder.setGitDir(new File(s"$repoPath/.git")).readEnvironment.findGitDir.build
  }

  def calculateStats(files: List[GithubFileInfo]): RepoStatistics = {
    var slocSum: Int = 0
    var sizeSum: Long = 0

    for (fileInfo <- files) {
      slocSum += fileInfo.sloc
      sizeSum += fileInfo.fileContent.getBytes.length
    }
    val repoStatistics: RepoStatistics = new RepoStatistics {

      override def sloc: Int = slocSum

      override def fileCount: Int = files.size

      override def size: Long = sizeSum
    }
    repoStatistics

  }

  def extractLanguages(files: List[GithubFileInfo]): Set[String] = {
    val fileLanguages: mutable.Set[String] = mutable.Set[String]()
    files.map(gitHubFileInfo => fileLanguages
      .add(gitHubFileInfo.extractLang()))
    fileLanguages.toSet
  }


  def readProject(): List[GithubFileInfo] = {
    val gitRepo = repository

    val gitHubFilesInfo: ArrayBuffer[GithubFileInfo] =
      mutable.ArrayBuffer[GithubFileInfo]()


    val fileListOpt = Try {
      val head: Ref = gitRepo.getRef("HEAD")

      // a RevWalk allows to walk over commits based on some filtering that is
      // defined
      val walk: RevWalk = new RevWalk(gitRepo)

      val commit: RevCommit = walk.parseCommit(head.getObjectId)
      val tree: RevTree = commit.getTree

      // Now use a TreeWalk to iterate over all files in the Tree recursively
      // We can set Filters to narrow down the results if needed
      val treeWalk: TreeWalk = new TreeWalk(gitRepo)
      treeWalk.addTree(tree)
      treeWalk.setRecursive(true)
      while (treeWalk.next) {
        val githubFileInfo = new GithubFileInfo(treeWalk.getPathString,
          treeWalk.getObjectId(0), gitRepo,
          repoInfo.get)
        gitHubFilesInfo.append(githubFileInfo)
      }

      gitHubFilesInfo.toList
    }

    fileListOpt match {
      case Success(list: List[GithubFileInfo]) => list
      case Failure(e: Throwable) => {
        log.warn(
          s"""Unable to read file list from repo ${repoInfo};
             | empty file list will be returned.
             | (This is possibly due to repo clone being empty)""".stripMargin,
          e)
        List.empty
      }
    }
  }
}

object GithubRepo {

  case class GithubRepoInfo(id: Long, login: String, name: String, fullName: String,
                            isPrivate: Boolean, isFork: Boolean, size: Long, watchersCount: Long,
                            language: String, forksCount: Long, subscribersCount: Long,
                            defaultBranch: String, stargazersCount: Long)

  def apply(configuration: Configuration, githubRepoInfo: GithubRepoInfo): Option[GithubRepo] = {
    new GithubRepo().init(configuration, githubRepoInfo)
  }

}

class GithubFileInfo(filePath: String, objectId: ObjectId, repository: Repository,
                     val githubRepoInfo: GithubRepoInfo) extends BaseFileInfo(filePath) {

  override def extractFileName(): String = {
    val p: Path = Paths.get(filePath)
    p.getFileName.toString
  }

  override def readFileContent(): String = {
    val loader: ObjectLoader = repository.open(objectId)
    new String(loader.getBytes(), "UTF-8")
  }

  override def extractLang(): String = {
    val fileType: Array[String] = filePath.split("\\.")
    if (fileType.length > 1) {
      fileType(1)
    } else {
      UNKNOWN_LANG
    }
  }

  override def readSloc(): Int = {
    Source.fromString(fileContent).getLines().size
  }

  override def repoFileLocation: String = {
    s"${githubRepoInfo.login}/${githubRepoInfo.name}/blob/${githubRepoInfo.defaultBranch}/$filePath"
  }

  override def repoId: Long = {
    githubRepoInfo.id
  }
}

