/**
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
  Dependencies.Libraries.log4jApi,
  Dependencies.Libraries.log4jCore,
  Dependencies.Libraries.kinesisClient,
  Dependencies.Libraries.kinesisConnector,
  Dependencies.Libraries.nsqClient,
  Dependencies.Libraries.netty,
  Dependencies.Libraries.jacksonCbor,
  Dependencies.Libraries.protobuf,
  // Scala
  Dependencies.Libraries.catsRetry,
  Dependencies.Libraries.circeOptics,
  Dependencies.Libraries.decline,
  Dependencies.Libraries.snowplowTracker,
  Dependencies.Libraries.snowplowTrackerId,
  Dependencies.Libraries.analyticsSDK,
  Dependencies.Libraries.awsSigner,
  Dependencies.Libraries.pureconfig,
  Dependencies.Libraries.pureconfigEnum,
  Dependencies.Libraries.badRows,
  // Scala (test only)
  Dependencies.Libraries.specs2,
  Dependencies.Libraries.circeLiteral
)

lazy val appDependencies = Seq(
  Dependencies.Libraries.elastic4sEsJava,
  Dependencies.Libraries.jacksonScala
)

lazy val buildSettings = Seq(
  organization := "com.snowplowanalytics",
  name := "snowplow-elasticsearch-loader",
  description := "Load the contents of a Kinesis stream or NSQ topic to Elasticsearch",
  scalaVersion := "2.12.14",
  scalacOptions := BuildSettings.compilerOptions,
  javacOptions := BuildSettings.javaCompilerOptions,
  resolvers += Resolver.jcenterRepo,
  shellPrompt := { _ =>
    "elasticsearch-loader> "
  }
)

lazy val allSettings = buildSettings ++
  BuildSettings.assemblySettings ++
  Seq(libraryDependencies ++= commonDependencies) ++
  BuildSettings.dynVerSettings ++
  BuildSettings.assemblySettings

lazy val root = project
  .in(file("."))
  .settings(allSettings)
  .aggregate(core, elasticsearch)

lazy val core = project
  .settings(moduleName := "snowplow-elasticsearch-loader-core")
  .settings(allSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(BuildSettings.addExampleConfToTestCp)

// project dealing with the ES
lazy val elasticsearch = project
  .settings(moduleName := "snowplow-elasticsearch-loader")
  .settings(allSettings)
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin)
  .settings(BuildSettings.dockerSettings)
  .settings(libraryDependencies ++= appDependencies)
  .dependsOn(core)

lazy val elasticsearchDistroless = project
  .in(file("distroless"))
  .settings(sourceDirectory := (elasticsearch / sourceDirectory).value)
  .settings(allSettings)
  .enablePlugins(SnowplowDistrolessDockerPlugin)
  .settings(BuildSettings.dockerSettings)
  .settings(libraryDependencies ++= appDependencies)
  .dependsOn(core)
