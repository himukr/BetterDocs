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

trait Repo extends Serializable {

  def files: List[FileInfo]

  def statistics: RepoStatistics

  def languages: Set[String]
}

trait FileInfo extends Serializable {

  /**
    * A name for this file in repository that uniquely identifies it.
    *
    * e.g. the path of file in the repository hierarchy.
    *
    * repoFileLocation is complete Repository URL of particular file
    *
    * @return
    */
  def fileName: String

  def language: String

  def fileContent: String

  def sloc: Int

  def repoId: Long

  def repoFileLocation: String
}

trait RepoStatistics {

  def sloc: Int

  def fileCount: Int

  def size: Long
}

trait LazyLoadSupport {

  def getOrCompute[T](maybeVal: Option[T], compute: () => T): T = {
    maybeVal match {
      case Some(value) => value
      case None => compute()
    }
  }
}

object RepoIndexStatus extends Enumeration{
  type RepoIndexStatus=Value
  val NEW, EXISTING =Value
}
