/**
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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
  // Scala
  Dependencies.Libraries.cats,
  Dependencies.Libraries.catsEffect,
  Dependencies.Libraries.catsRetry,
  Dependencies.Libraries.circeOptics,
  Dependencies.Libraries.decline,
  Dependencies.Libraries.snowplowTracker,
  Dependencies.Libraries.snowplowTrackerId,
  Dependencies.Libraries.analyticsSDK,
  Dependencies.Libraries.awsSigner,
  Dependencies.Libraries.pureconfig,
  Dependencies.Libraries.pureconfigEnum,
  // Scala (test only)
  Dependencies.Libraries.specs2,
  Dependencies.Libraries.circeLiteral
)

lazy val buildSettings = Seq(
  organization := "com.snowplowanalytics",
  name := "snowplow-elasticsearch-loader",
  version := "0.12.1",
  description := "Load the contents of a Kinesis stream or NSQ topic to Elasticsearch",
  scalaVersion := "2.12.10",
  scalacOptions := BuildSettings.compilerOptions,
  javacOptions := BuildSettings.javaCompilerOptions,
  resolvers += Resolver.jcenterRepo,
  shellPrompt := { _ =>
    "elasticsearch-loader> "
  },
  scalafmtOnCompile := true
)

lazy val allSettings = buildSettings ++
  BuildSettings.sbtAssemblySettings ++
  Seq(libraryDependencies ++= commonDependencies)

lazy val root = project
  .in(file("."))
  .settings(buildSettings)
  .aggregate(core, elasticsearch)

lazy val core = project
  .settings(moduleName := "snowplow-elasticsearch-loader-core")
  .settings(buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(libraryDependencies ++= commonDependencies)

// project dealing with the ES
lazy val elasticsearch = project
  .settings(moduleName := "snowplow-elasticsearch-loader")
  .settings(allSettings)
  .enablePlugins(JavaAppPackaging)
  .settings(BuildSettings.dockerSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.elastic4sHttp,
      Dependencies.Libraries.elastic4sTest
    ))
  .dependsOn(core)
