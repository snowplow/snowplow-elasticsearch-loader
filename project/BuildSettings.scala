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

// SBT
import sbt._
import Keys._

import com.typesafe.sbt.packager.Keys.{daemonUser, maintainer, packageName}
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._

object BuildSettings {

  lazy val compilerOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused-import",
    "-Xfuture",
    "-Xlint"
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  lazy val dockerSettings = Seq(
    Universal / sourceDirectory := new java.io.File((LocalRootProject / baseDirectory).value, "docker"),
    Docker / packageName := "elasticsearch-loader",
    dockerRepository := Some("snowplow-docker-registry.bintray.io"),
    dockerUsername := Some("snowplow"),
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.2.0",
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    Docker / daemonUser := "snowplow",
    dockerCmd := Seq("--help")
  )

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(
    Compile / sourceGenerators += Def.task {
      val dir = (Compile / sourceManaged).value
      val file = dir / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.stream.loader.generated
        |object Settings {
        |  val organization = "%s"
        |  val version = "%s"
        |  val name = "%s"
        |}
        |""".stripMargin.format(organization.value, version.value, moduleName.value))

      Seq(file)
    }.taskValue
  )

  // sbt-assembly settings for building an executable
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    assembly / assemblyJarName := { s"${moduleName.value}-${version.value}.jar" },
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case x if x.endsWith("module-info.class") => MergeStrategy.discard // not used by JDK8
      case "META-INF/io.netty.versions.properties" => MergeStrategy.first
      case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
}
