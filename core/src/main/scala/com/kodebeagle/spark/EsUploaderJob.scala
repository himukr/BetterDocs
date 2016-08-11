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

import java.net.URLEncoder

import com.kodebeagle.configuration.KodeBeagleConfig
import org.apache.spark.SparkConf
import com.kodebeagle.util.SparkIndexJobHelper._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

import scala.util.Try

object EsUploaderJob {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(KodeBeagleConfig.sparkMaster).
      setAppName("EsUploaderJob").
      set("spark.serializer", classOf[KryoSerializer].getName).
      set("spark.kryoserializer.buffer.max", "128m")

    val sc = createSparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val javaDeleteIndicesLoc =
      s"${KodeBeagleConfig.repoIndicesHdfsPath}/Java/${KodeBeagleIndices.DELETEINDICES}"
    val javaMetaIndicesLoc =
      s"${KodeBeagleConfig.repoIndicesHdfsPath}/Java/${KodeBeagleIndices.META}"
    val javaSourcesIndicesLoc =
      s"${KodeBeagleConfig.repoIndicesHdfsPath}/Java/${KodeBeagleIndices.SOURCES}"
    val javaTokensIndicesLoc =
      s"${KodeBeagleConfig.repoIndicesHdfsPath}/Java/${KodeBeagleIndices.TOKENS}"
    val javaTypesInfoIndicesLoc =
      s"${KodeBeagleConfig.repoIndicesHdfsPath}/Java/${KodeBeagleIndices.TYPESINFO}"

    deleteIndices(sqlContext, javaDeleteIndicesLoc)

    val uploadToJava = uploadIndices(sqlContext, "java") _
    uploadToJava(javaMetaIndicesLoc, "filemetadata", "fileName")
    uploadToJava(javaSourcesIndicesLoc, "sourcefile", "fileName")
    uploadToJava(javaTokensIndicesLoc, "typereference", "file")
    uploadToJava(javaTypesInfoIndicesLoc, "typesinfo", "fileName")

    moveIndicesToBckUp(sc.hadoopConfiguration)
  }

  def deleteIndices(sqlContext: SQLContext, deleteIndicesLoc: String): Unit = {
    import sys.process._
    val df = Try(sqlContext.read.json(deleteIndicesLoc).select("delete._id")).foreach {
      _.foreach {
        row =>
          val id = URLEncoder.encode(row.get(0).asInstanceOf[String], "UTF-8")
          // scalastyle:off
          s"curl -XDELETE http://${KodeBeagleConfig.esNodes}:${KodeBeagleConfig.esPort}/java/filemetadata/$id".!!
          s"curl -XDELETE http://${KodeBeagleConfig.esNodes}:${KodeBeagleConfig.esPort}/java/sourcefile/$id".!!
          s"curl -XDELETE http://${KodeBeagleConfig.esNodes}:${KodeBeagleConfig.esPort}/java/typereference/$id".!!
          s"curl -XDELETE http://${KodeBeagleConfig.esNodes}:${KodeBeagleConfig.esPort}/java/typesinfo/$id".!!
          // scalastyle:on
      }
    }
  }

  def uploadIndices(sqlContext: SQLContext,
                    indexName: String)(indicesLoc: String, indexType: String,
                                       idMappingField: String): Unit = {
    val maybeDF = Try(sqlContext.read.json(indicesLoc))

    maybeDF.map(df=>df.saveToEs(
      s"$indexName/$indexType",
      Map(
        ES_NODES -> KodeBeagleConfig.esNodes,
        ES_PORT -> KodeBeagleConfig.esPort,
        ES_WRITE_OPERATION -> ES_OPERATION_UPSERT,
        ES_MAPPING_ID -> idMappingField)))
  }

  def moveIndicesToBckUp(hadoopConfig: Configuration): Unit = {
    val fs=FileSystem.get(hadoopConfig)
    val timeInMillis=System.currentTimeMillis()
    fs.rename(new Path(s"${KodeBeagleConfig.repoIndicesHdfsPath}"),
      new Path(s"${KodeBeagleConfig.repoIndicesBackupPath}/indices-$timeInMillis"))
  }
}

object KodeBeagleIndices {
  val META = "meta"
  val SOURCES = "sources"
  val TOKENS = "tokens"
  val TYPESINFO = "typesinfo"
  val DELETEINDICES = "deleteindices"
}

