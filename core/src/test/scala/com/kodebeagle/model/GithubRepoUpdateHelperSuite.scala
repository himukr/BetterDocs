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

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import sys.process._

class GithubRepoUpdateHelperSuite
  extends FunSuite with BeforeAndAfterAll with GithubRepoUpdateHelperMockSupport{
  var updateHelper: Option[GithubRepoUpdateHelper]=None

  override def beforeAll: Unit ={
    """rm -rf /tmp/test/""".!!
    updateHelper=Option(mockGithubRepoUpdateHelper)
  }

  test("test create or update git hub repo flow"){

    // 1. test the should create method
    assert(updateHelper.get.shouldCreate())


    // 2. test the create method
    updateHelper.get.create()
    val clonedTar=new File(s"${updateHelper.get.localRepoPath}/git.tar.gz");
    val storedTar=new File(s"${updateHelper.get.fsRepoPath}/git.tar.gz")
    assert(!clonedTar.exists() && storedTar.exists())


    // 3. test the create method after creating git repo
    assert(!updateHelper.get.shouldCreate())


    // 4. test the should update method immediately after creating
    """rm -rf /tmp/test/tmp/kodebeagle""".!!
    assert(!updateHelper.get.shouldUpdate())


    // 5. test the should command after changing the modified time of repo file
    s"""touch -t 201212101830.55 ${updateHelper.get.fsRepoPath}/git.tar.gz""".!!
    assert(updateHelper.get.shouldUpdate())

    // 6. perform update on the git repo after adding merged file
    s"""rm -rf ${updateHelper.get.fsRepoPath}""".!! // removed repo from fs
    updateHelper.get.update()
    assert(new File(s"${updateHelper.get.fsRepoPath}/git.tar.gz").exists())

  }

  /**
    * clean everything
    */
  override def afterAll(): Unit ={
    """rm -rf /tmp/test/""".!!
  }
}

trait GithubRepoUpdateHelperMockSupport {
  def mockGithubRepoUpdateHelper: GithubRepoUpdateHelper={
    val mocked=new MockedGithubUpdateHelper(new Configuration(),"himukr/google-grp-scraper")

    GithubRepoUpdateHelper.bashCmdsFromDir("",
      Seq(s"""mkdir -p ${mocked.localCloneDir}""",
      s"""mkdir -p ${mocked.fsStoreDir}""")
    ).!!
    mocked
  }
}
