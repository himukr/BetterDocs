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
import com.kodebeagle.indexer.{ExternalTypeReference, InternalTypeReference, Repository, ScalaExternalTypeRefIndexer, ScalaInternalTypeRefIndexer}
import com.kodebeagle.spark.util.SparkIndexJobHelper._
import org.apache.spark.rdd.RDD

object ScalaIndexProducer extends IndexProducer {

  val scalaInternalIndexer = new ScalaInternalTypeRefIndexer
  val scalaExternalIndexer = new ScalaExternalTypeRefIndexer

  def createIndices(scalaRDD: RDD[(String, (String, String))], batch: String,
                    repoIndex: Map[String, (Option[Repository], List[String])]): Unit = {
    scalaRDD.map {
      case (repo, file) =>
        val maybeTuple = repoIndex.get(repo)
        val mayBeRepo: Option[Repository] = maybeTuple.get._1
        val packages = maybeTuple.get._2
        val internal = scalaInternalIndexer.generateTypeReferences(file, packages, mayBeRepo)
        val external = scalaExternalIndexer.generateTypeReferences(file, packages, mayBeRepo)
        (internal.asInstanceOf[Set[InternalTypeReference]],
          external.asInstanceOf[Set[ExternalTypeReference]],
          mapToSourceFile(mayBeRepo, file))
    }.flatMap {
      case (internal, external, sourceFile) =>
        Seq(toIndexTypeJson(TYPEREFS, "scalainternal", internal, isToken = false),
          toIndexTypeJson(TYPEREFS, "scalaexternal", external, isToken = false),
          toJson(sourceFile, isToken = false))
    }.saveAsTextFile(s"${KodeBeagleConfig.sparkIndexOutput}/${batch}/scalaIndex")
  }
}
