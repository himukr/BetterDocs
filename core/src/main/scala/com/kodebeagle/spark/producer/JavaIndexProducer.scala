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

package com.kodebeagle.spark.producer

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.indexer.{ExternalTypeReference, FileMetaDataIndexer, InternalTypeReference, JavaExternalTypeRefIndexer, JavaInternalTypeRefIndexer, Repository, SourceFile}
import com.kodebeagle.spark.util.SparkIndexJobHelper._
import org.apache.spark.rdd.RDD

object JavaIndexProducer extends IndexProducer {

  val javaInternalIndexer = new JavaInternalTypeRefIndexer()
  val javaExternalIndexer = new JavaExternalTypeRefIndexer()

  override def createIndices(javaRDD: RDD[(String, (String, String))], batch: String,
                             repoIndex: Map[String, (Option[Repository], List[String])]): Unit = {
    javaRDD.map {
      case (repo, file) =>
        val maybeTuple = repoIndex.get(repo)
        val mayBeRepo: Option[Repository] = maybeTuple.get._1
        val packages: List[String] = maybeTuple.get._2
        val internal = javaInternalIndexer.generateTypeReferences(file, packages, mayBeRepo)
        val external = javaExternalIndexer.generateTypeReferences(file, packages, mayBeRepo)
        val sourceFile = mapToSourceFile(mayBeRepo, file)
        val metadata = FileMetaDataIndexer.generateMetaData(sourceFile)
        (internal.asInstanceOf[Set[InternalTypeReference]],
          external.asInstanceOf[Set[ExternalTypeReference]],
          metadata, sourceFile)
    }.flatMap {
      case (internal, external, metadata, sourceFile) =>
        Seq(toIndexTypeJson(TYPEREFS, "javainternal", internal, isToken = false),
          toIndexTypeJson(TYPEREFS, "javaexternal", external, isToken = false),
          toJson(metadata, isToken = false),
          toJson(sourceFile, isToken = false))
    }.saveAsTextFile(s"${KodeBeagleConfig.sparkIndexOutput}/${batch}/javaIndex")
  }
}
