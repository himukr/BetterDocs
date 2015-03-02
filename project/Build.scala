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

import sbt._
import sbt.Keys._

object BetterDocsBuild extends Build {

  lazy val betterDocs = Project("betterDocs", file("core"), settings = betterDocsSettings)

  def betterDocsSettings = Defaults.defaultSettings ++ Seq (
      name                                   :=  "BetterDocs",
      organization                           :=  "io.betterdocs",
      version                                :=  "0.0.1-SNAPSHOT",
      scalaVersion                           :=  "2.11.5",
      scalacOptions                          :=  Seq("-encoding", "UTF-8", "-unchecked", "-optimize", "-deprecation", "-feature"),
      retrieveManaged                        :=  true,
      libraryDependencies                    += Dependencies.spark,
      crossPaths                             :=  false,
      fork                                   :=  true
    )
}

object Dependencies {

  val spark = "org.apache.spark" %% "spark-core" % "1.2.1"

// transitively uses commons-lang3-3.3.2
// commons-httpclient-3.1
// commons-io-2.4
// json4s-jackson_2.11-3.2.10
// json4s-ast_2.11-3.2.10.jar
// commons-compress-1.4.1

}