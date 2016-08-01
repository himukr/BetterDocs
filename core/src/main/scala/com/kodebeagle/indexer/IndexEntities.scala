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

package com.kodebeagle.indexer

import java.util

case class Line(line: Int, startCol: Int, endCol: Int)

trait Property

case class ContextProperty(name: String) extends Property

case class PayloadProperty(name: String, lines: Set[Line]) extends Property

trait Type {
  type T <: Property

  def name: String

  def props: Set[T]
}

case class ContextType(name: String, props: Set[ContextProperty]) extends Type {
  type T = ContextProperty
}

case class PayloadType(name: String, props: Set[PayloadProperty]) extends Type {
  type T = PayloadProperty
}

case class Payload(types: Set[PayloadType])

case class Context(text: String, types: Set[ContextType])

case class TypeReference(contexts: Set[Context], payload: Payload, score: Long, file: String)

case class SourceFile(repoId: Long, fileName: String, fileContent: String)

case class RepoFileNameInfo(login: String, id: Int, name: String, fork: Boolean, language: String,
                            defaultBranch: String, stargazersCount: Int)

case class Repository(login: String, id: Int, name: String, fork: Boolean, language: String,
                      defaultBranch: String, stargazersCount: Int, sloc: Int, fileCount: Int,
                      size: Long)

case class Statistics(sloc: Int, fileCount: Int, size: Long)

/** For testing */
object Repository {
  def invalid: Repository =
    Repository("n-a", -1, "n-a", fork = false, "Java", "n-a", 0, -1, -1, -1)
}

/* File Metadata related entities */
case class RepoSource(repoId: Long, fileName: String, fileContent: String)

case class TypeDeclaration(fileType: String, loc: String)

case class ExternalRef(id: Int, fqt: String)

case class VarTypeLocation(loc: String, id: Int)

case class MethodTypeLocation(loc: String, id: Int, method: String, argTypes: List[String])

case class MethodDefinition(loc: String, method: String, argTypes: List[String])

case class InternalRef(childLine: String, parentLine: String)


case class SuperType(typeName: String, supers: List[String])

case class FileMetaData(repoId: Long, fileName: String, superTypes: List[SuperType],
                        fileTypes: util.List[TypeDeclaration],
                        externalRefList: List[ExternalRef],
                        typeLocationList: List[VarTypeLocation],
                        methodTypeLocation: List[MethodTypeLocation],
                        methodDefinitionList: List[MethodDefinition],
                        internalRefList: List[InternalRef])

case class JavaFileIndices(searchableRefs: Set[TypeReference],
                           fileMetaData: FileMetaData, sourceFile: SourceFile, repo: String)





