/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
  Dependencies.Libraries.sts,
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
