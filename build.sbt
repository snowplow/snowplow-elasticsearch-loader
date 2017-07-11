/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

lazy val commonDependencies = Seq(
  // Java
  Dependencies.Libraries.config,
  Dependencies.Libraries.slf4j,
  Dependencies.Libraries.log4jOverSlf4j,
  Dependencies.Libraries.kinesisClient,
  Dependencies.Libraries.kinesisConnector,
  Dependencies.Libraries.validator,
  // Scala
  Dependencies.Libraries.scopt,
  Dependencies.Libraries.scalaz7,
  Dependencies.Libraries.scalazC7,
  Dependencies.Libraries.snowplowTracker,
  // Scala (test only)
  Dependencies.Libraries.specs2
)

lazy val allSettings = BuildSettings.buildSettings ++
  BuildSettings.sbtAssemblySettings ++
  Seq(libraryDependencies ++= commonDependencies)

lazy val root = project.in(file("."))
  .settings(
    name        := "snowplow-elasticsearch-loader",
    version     := "0.8.0",
    description := "Load the contents of a Kinesis stream to Elasticsearch"
  )
  .settings(allSettings)
  .aggregate(core, http, tcp, tcp2x)

lazy val core = project
  .settings(moduleName := "snowplow-elasticsearch-loader-core")
  .settings(BuildSettings.buildSettings)
  .settings(libraryDependencies ++= commonDependencies)

lazy val http = project
  .settings(moduleName := "snowplow-elasticsearch-loader-http")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.elastic4sHttp,
    Dependencies.Libraries.elastic4sTest
  ))
  .dependsOn(core)

lazy val tcp = project
  .settings(moduleName := "snowplow-elasticsearch-loader-tcp")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.elastic4sTcp,
    Dependencies.Libraries.elastic4sTest
  ))
  .dependsOn(core)

lazy val tcp2x = project
  .settings(moduleName := "snowplow-elasticsearch-loader-tcp-2x")
  .settings(allSettings)
  .settings(libraryDependencies += Dependencies.Libraries.elasticsearch)
  .dependsOn(core)

shellPrompt := { _ => "elasticsearch-loader> " }
