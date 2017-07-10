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

lazy val root = project.in(file("."))
  .settings(
    name        := "snowplow-elasticsearch-sink",
    version     := "0.8.0",
    description := "Kinesis sink for Elasticsearch"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(libraryDependencies ++= commonDependencies)
  .aggregate(core, http, tcp)

lazy val core = project
  .settings(moduleName := "snowplow-elasticsearch-loader-core")
  .settings(BuildSettings.buildSettings)
  .settings(libraryDependencies ++= commonDependencies)

lazy val http = project
  .settings(moduleName := "snowplow-elasticsearch-loader-http")
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies += Dependencies.Libraries.elastic4sHttp)
  .dependsOn(core)

lazy val tcp = project
  .settings(moduleName := "snowplow-elasticsearch-loader-tcp")
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies += Dependencies.Libraries.elastic4sTcp)
  .dependsOn(core)

shellPrompt := { _ => "elasticsearch-sink> " }
