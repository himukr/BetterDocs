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

package com.kodebeagle.crawler

import java.io._

import akka.actor.{Actor, ActorSystem, Props}
import akka.routing.RoundRobinRouter
import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.logging.Logger
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.io.{FileUtils, IOUtils}
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.ResetCommand.ResetType
import org.eclipse.jgit.diff.DiffEntry
import org.eclipse.jgit.diff.DiffEntry.ChangeType
import org.eclipse.jgit.lib.{ObjectId, ObjectReader, Repository}
import org.eclipse.jgit.treewalk.CanonicalTreeParser
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


object GitRepoDiffFinder extends App with Logger {
  val noOfWorker = 10

  sealed trait Message

  case class GitDiff(gitRepoDir: File) extends Message

  case class Diff(file: File) extends Message

  case class GitDiffResult(diffEntries: List[DiffEntry], fileName: String) extends Message

  case class RepoDiffInfo(repoFileName: String, deleteFiles: ListBuffer[String],
                          addedFiles: ListBuffer[String], modifiedFiles: ListBuffer[String],
                          renamedFiles: ListBuffer[String], copiedFiles: ListBuffer[String])

  var totalGitFileCount = 0

  def startFindingDiffs(batch: String) {
    val system = ActorSystem("GitDiffFinder")
    val repoBatchDir = new File(s"${KodeBeagleConfig.githubDir}/$batch")
    val master = system.actorOf(Props(new MasterDiffFinder(noOfWorker,batch)), "master")
    repoBatchDir.listFiles().
      foreach(x => totalGitFileCount += 1)
    log.info(s"Started processing $totalGitFileCount files")
    master ! GitDiff(repoBatchDir)
  }

  startFindingDiffs(args(0))
}

class MasterDiffFinder(noOfWorker: Int,batch: String) extends Actor {

  import com.kodebeagle.crawler.GitRepoDiffFinder._

  val workerRouter = context.actorOf(Props[WorkerDiffFinder].
    withRouter(RoundRobinRouter(noOfWorker)), "workers")

  override def receive: Receive = {
    case GitDiff(gitRepoDir) =>
      gitRepoDir.listFiles().foreach(x => {
        log.info(s"sending task to worker: repo => ${x.getName}");
        workerRouter ! Diff(x)
      })

    case GitDiffResult(diffEntries, fileName) =>
      if (diffEntries != Nil && diffEntries.size > 0) {
        val (deletedFiles: ListBuffer[String], modifiedFiles: ListBuffer[String],
        renamedFiles: ListBuffer[String], addedFiles: ListBuffer[String],
        copiedFiles: ListBuffer[String]) = extractDiffTypes(diffEntries)

        writeDiffToFile(fileName, deletedFiles, modifiedFiles,
                            renamedFiles, addedFiles, copiedFiles)
      }
      totalGitFileCount -= 1
      if (totalGitFileCount == 0) {
        log.info(s"Done processing")
        context.system.shutdown()
      }
  }

  def extractDiffTypes(diffEntries: List[DiffEntry]):
  (ListBuffer[String], ListBuffer[String],
    ListBuffer[String], ListBuffer[String], ListBuffer[String]) = {
    val deletedFiles = ListBuffer[String]()
    val modifiedFiles = ListBuffer[String]()
    val renamedFiles = ListBuffer[String]()
    val addedFiles = ListBuffer[String]()
    val copiedFiles = ListBuffer[String]()

    for (entry <- diffEntries) {
      entry.getChangeType match {
        case ChangeType.ADD =>
          addedFiles += entry.getNewPath
        case ChangeType.DELETE =>
          deletedFiles += (entry.getOldPath)
        case ChangeType.MODIFY =>
          modifiedFiles += entry.getNewPath
        case ChangeType.RENAME =>
          renamedFiles += entry.getOldPath + "->" + entry.getNewPath
        case ChangeType.COPY =>
          copiedFiles += entry.getNewPath
      }
    }
    (deletedFiles, modifiedFiles, renamedFiles, addedFiles, copiedFiles)
  }

  def writeDiffToFile(fileName: String, deletedFiles: ListBuffer[String],
                      modifiedFiles: ListBuffer[String], renamedFiles: ListBuffer[String],
                      addedFiles: ListBuffer[String], copiedFiles: ListBuffer[String]): Unit = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val json = Serialization.write(RepoDiffInfo(fileName.replace(".zip",""),
      deletedFiles, addedFiles, modifiedFiles, renamedFiles, copiedFiles))
    val writer = new PrintWriter(
      new FileWriter
      (s"${KodeBeagleConfig.gitDiffDir}/git-repo-diff-file-$batch.txt", true))
    writer.write(json.toString + "\n")
    writer.close()
    log.info(s"diff added to git diff file : repo => $fileName")
  }
}

class WorkerDiffFinder extends Actor {

  import com.kodebeagle.crawler.GitRepoDiffFinder._

  def unzip(file: File): Unit = {

    val mayBeZipFile = Try(new ZipFile(file))
    if(mayBeZipFile.isSuccess){
      val zipFile=mayBeZipFile.get
        val zipParentPath = file.getParent
        try {
          val entries = zipFile.getEntries
          while (entries.hasMoreElements) {
            val entry = entries.nextElement
            val entryDestination = new File(s"${zipParentPath}/${entry.getName}")
            if (entry.isDirectory) {
              entryDestination.mkdirs()
            } else {
              entryDestination.getParentFile.mkdirs
              val in = zipFile.getInputStream(entry)
              val out = new FileOutputStream(entryDestination)
              IOUtils.copy(in, out)
              IOUtils.closeQuietly(in)
              out.close()
            }
          }
        } finally {
          zipFile.close()
        }
    }else{
        log.info("Not a zip file, continue finding diff")
    }
  }

  private def getGitDiff(file: File) = {
    unzip(file)
    val git = Git.open(new File(s"${file.getPath.replace(".zip","")}"))
    git.fetch().call
    val repo: Repository = git.getRepository
    val fetchHead = repo.resolve("refs/remotes/origin/" + file.getName.split("~")(6)
      + "^{tree}" )
    val head: ObjectId = repo.resolve("refs/heads/" + file.getName.split("~")(6) +
      "^{tree}")
    val reader: ObjectReader = repo.newObjectReader
    val oldTreeIter: CanonicalTreeParser = new CanonicalTreeParser
    oldTreeIter.reset(reader, head)
    val newTreeIter: CanonicalTreeParser = new CanonicalTreeParser
    newTreeIter.reset(reader, fetchHead)
    git.diff.setNewTree(newTreeIter).setOldTree(oldTreeIter).call
  }

  import scala.collection.JavaConversions._

  private def getLatestRevision(diffEntries: List[DiffEntry], fromFile: File) = {
    val git = Git.open(fromFile)
    for (entry <- diffEntries) {
      if (entry.getChangeType == ChangeType.ADD || entry.getChangeType == ChangeType.MODIFY
        || entry.getChangeType == ChangeType.RENAME || entry.getChangeType == ChangeType.COPY) {
        git.reset().setMode(ResetType.HARD).setRef("refs/remotes/origin/" +
          fromFile.getName.split("~")(6)).call()
      }
    }
  }

  import scala.util.{Failure, Success, Try}

  override def receive: Actor.Receive = {
    case Diff(file) =>
      val diffEntries: List[DiffEntry] = Try(getGitDiff(file).toList) match {
        case Success(result) => result
        case Failure(fail) => log.info(fail.printStackTrace().toString)
          Nil
      }
      sender() ! GitDiffResult(diffEntries, file.getName)
      val unzipedFile = new File(s"${file.getPath.replace(".zip","")}")
      if (unzipedFile.exists()) {
        if (diffEntries != Nil && diffEntries.size > 0) {
          getLatestRevision(diffEntries, unzipedFile)
        }
      }
      FileUtils.forceDelete(file)
      log.info(s"worker completed the task: repo => ${file.getName}")
  }
}
