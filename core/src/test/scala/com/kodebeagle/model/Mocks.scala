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

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.model.GithubRepo.GithubRepoInfo
import org.apache.hadoop.conf.Configuration
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.Repository

import sys.process._

class MockedGithubRepo() extends GithubRepo() {
  val mockGithubRepoInfo = new GithubRepoInfo(1, "himukr", "google-grp-scraper",
    "himukr/google-grp-scraper", false, false, 100, 5, "", 1, 5,"master", 5)
  def init(configurationTest: Configuration, repoPathTest: String): MockedGithubRepo = {
    _repoGitFiles = Option(
      List(s"${KodeBeagleConfig.repoCloneDir}/himukr/google-grp-scraper"))
    repoInfo = Option(mockGithubRepoInfo)
    this
  }
}

class MockedGithubUpdateHelper(configuration: Configuration,repoPath: String)
  extends GithubRepoUpdateHelper(configuration,repoPath){
  override def localCloneDir: String =s"/tmp/test${KodeBeagleConfig.repoCloneDir}"
  override def fsStoreDir: String =s"/tmp/test${KodeBeagleConfig.repoStoreDir}"

  override protected def performGitFetch(git: Git): Unit = {
    val repoName = repoPath.split("/")(1)
    val gitFetchedFile=Thread.currentThread().
      getContextClassLoader.getResource("GitRepoFETCH-git.tar.gz").getPath
    GithubRepoUpdateHelper.bashCmdsFromDir("",Seq(
      s"""cp $gitFetchedFile $localRepoPath/$repoName""",
      s"""cd $localRepoPath/$repoName""",
      s"""tar -xvf GitRepoFETCH-git.tar.gz"""
    )).!!
  }

  override protected def performGitMerge(repo: Repository, git: Git): Unit ={
    val repoName = repoPath.split("/")(1)
    val gitMergedFile=Thread.currentThread().
      getContextClassLoader.getResource("GitRepoMERGE-git.tar.gz").getPath
    GithubRepoUpdateHelper.bashCmdsFromDir("",Seq(
      s"""cp $gitMergedFile $localRepoPath/$repoName""",
      s"""cd $localRepoPath/$repoName""",
      s"""tar -xvf GitRepoMERGE-git.tar.gz"""
    )).!!
  }

  // scalastyle:off
  override def buildCloneCommand(repoName: String, cloneUrl: String): Seq[String] = {
    val repoName = repoPath.split("/")(1)

    GithubRepoUpdateHelper.bashCmdsFromDir(s"${localRepoPath}/${repoName}",Seq(
      s"""cp ${Thread.currentThread().getContextClassLoader.getResource("GitRepoTest-git.tar.gz").getPath} ${localRepoPath}""",
      s"""cd ${localRepoPath}""",
      s"""tar -xvf GitRepoTest-git.tar.gz -C ${repoName}"""
    ),true)
  }
  // scalastyle:on

}
