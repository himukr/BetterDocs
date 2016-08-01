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

import java.util

import com.kodebeagle.indexer._
import com.kodebeagle.javaparser.JavaASTParser.ParseType
import com.kodebeagle.javaparser.{JavaASTParser, SingleClassBindingResolver}
import com.kodebeagle.logging.Logger
import com.kodebeagle.util.Utils
import org.eclipse.jdt.core.dom.CompilationUnit

import scala.util.Try

class JavaRepo(val baseRepo: GithubRepo) extends Repo with Logger
  with LazyLoadSupport {

  // TODO: This constants needs to go somewhere else
  private val JAVA_LANGUAGE = "java"

  override def files: List[JavaFileInfo] = {
    if (languages.contains(JAVA_LANGUAGE)) {
      baseRepo.files
        .filter(_.fileName.endsWith(".java"))
        .map(f => new JavaFileInfo(f))
    } else {
      Nil
    }
  }

  override def statistics: JavaRepoStatistics = new JavaRepoStatistics(baseRepo.statistics)

  override def languages: Set[String] = baseRepo.languages
}

class JavaFileInfo(baseFile: GithubFileInfo) extends FileInfo with LazyLoadSupport with Logger {

  assert(baseFile.fileName.endsWith(".java"),
    s"A java file is expected. Actual file: ${baseFile.fileName}")

  private var _searchableRefs: Option[TypeReference] = None

  private var _fileMetaData: Option[FileMetaData] = None

  private var _imports: Option[Set[String]] = None

  private var _repoPath: Option[String] = None

  def searchableRefs: TypeReference = {
    getOrCompute(_searchableRefs, () => {
      parse()
      _searchableRefs.get
    })
  }

  def fileMetaData: FileMetaData = {
    getOrCompute(_fileMetaData, () => {
      parse()
      _fileMetaData.get
    })
  }

  def imports: Set[String] = {
    getOrCompute(_imports, () => {
      parse()
      _imports.get
    })
  }

  def repoPath: String = {
    getOrCompute(_repoPath, () => {
      parse()
      _repoPath.get
    })
  }

  override def fileName: String = baseFile.fileName

  override def sloc: Int = baseFile.sloc

  override def fileContent: String = baseFile.fileContent

  override def language: String = baseFile.language

  override def repoId: Long = baseFile.repoId

  override def repoFileLocation: String = baseFile.repoFileLocation

  def isTestFile(): Boolean = imports.exists(_.contains("org.junit"))

  /**
    * This method parses the java file and updates all that needs to be exposed by this class.
    *
    * The method is called lazily on when the data depending on parse is first requested, and
    * then all such data is computed and stored in the fields of the class.
    *
    * @return
    */
  private def parse() = {
    import scala.collection.JavaConversions._

    val parser: JavaASTParser = new JavaASTParser(true)
    _repoPath = Option(s"${baseFile.githubRepoInfo.login}/${baseFile.githubRepoInfo.name}")

    // The file may not even be well formed, so the parser may throw an
    // IllegalArgumentException. Need to handle such a case.
    val cu: Option[CompilationUnit] = Try {
      parser.getAST(fileContent, ParseType.COMPILATION_UNIT)
        .asInstanceOf[CompilationUnit]
    }.toOption

    if (cu.isEmpty) {
      import collection.JavaConverters._
      log.error(s"Compilation unit is null for $fileName")
      _imports = Option(Set.empty)

      _searchableRefs = None
      _fileMetaData = Option(new FileMetaData(repoId, fileName,
        List.empty,
        List.empty[TypeDeclaration], List.empty, List.empty, List.empty,
        List.empty, List.empty))
    } else {
      val scbr: SingleClassBindingResolver = new SingleClassBindingResolver(cu.get)
      scbr.resolve()

      val nodeVsType = scbr.getTypesAtPosition
      val score = baseFile.githubRepoInfo.stargazersCount
      val externalTypeRefs = extractTypeReference(scbr, cu.get, score)
      val fileMetaData = FileMetaDataIndexer.generateMetaData(scbr, cu.get, repoId, fileName)

      _imports = Option(scbr.getImports.toSet)
      _searchableRefs = Option(externalTypeRefs)
      _fileMetaData = Option(fileMetaData)
    }

  }

  private def extractTypeReference(scbr: SingleClassBindingResolver,
                                   cu: CompilationUnit, score: Long): TypeReference = {
    val contexts = generateContexts(scbr)
    val payload = generatePayload(scbr, cu)
    TypeReference(contexts, payload, score, repoFileLocation)
  }


  def generateContexts(scbr: SingleClassBindingResolver): Set[Context] = {
    import scala.collection.JavaConversions._
    scbr.getMethodInvoks.map { case (methodDecl, methodCalls) =>
      // for every method in the class
      val contextTypes = methodCalls.groupBy(_.getTargetType)
        // then map the grp to:
        .map { case (typ, grp) =>
        val props = grp.groupBy(_.getMethodName).keys.map(ContextProperty).toSet
        ContextType(typ, props)
      }.toSet
      Context(Utils.generateMethodSignature(methodDecl), contextTypes)
    }.toSet
  }


  def generatePayload(scbr: SingleClassBindingResolver, cu: CompilationUnit): Payload = {
    import scala.collection.JavaConversions._
    val payloadTypes = scbr.getMethodInvoks.values().flatten
      .groupBy(_.getTargetType).map { case (typ, grp) =>
      val propSet = grp.groupBy(_.getMethodName).map { case (prop, pgrp) =>
        PayloadProperty(prop, pgrp.map(t => {
          val line = cu.getLineNumber(t.getLocation)
          val col = cu.getColumnNumber(t.getLocation)
          Line(line, col, col + t.getLength)
        }).toSet)
      }.toSet
      PayloadType(typ, propSet)
    }.toSet
    Payload(payloadTypes)
  }
}

class JavaRepoStatistics(repoStatistics: RepoStatistics) extends RepoStatistics {

  override def sloc: Int = repoStatistics.sloc

  override def fileCount: Int = repoStatistics.fileCount

  override def size: Long = repoStatistics.size
}

