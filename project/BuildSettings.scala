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

// SBT
import sbt._
import Keys._

import com.typesafe.sbt.packager.Keys._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.{Docker, dockerExposedPorts, dockerUpdateLatest}
import sbtdynver.DynVerPlugin.autoImport._

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
    "-source", "11",
    "-target", "11"
  )

  lazy val dockerSettings =
    Seq(
      Docker / packageName := "elasticsearch-loader",
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
  lazy val assemblySettings = Seq(
    assembly / assemblyJarName := { s"${name.value}-${version.value}.jar" },
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

  lazy val dynVerSettings = Seq(
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-" // to be compatible with docker
    )


  lazy val addExampleConfToTestCp = Seq(
    Test / unmanagedClasspath += {
      baseDirectory.value.getParentFile / "config"
    }
  )
}
