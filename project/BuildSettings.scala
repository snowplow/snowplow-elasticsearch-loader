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

// SBT
import sbt._
import Keys._
import scala.io.Source

object BuildSettings {

  // Defines the ES Version to build for
  val ElasticsearchVersion = sys.env("ELASTICSEARCH_VERSION")

  // Basic settings for our app
  lazy val basicSettings = Seq(
    organization  := "com.snowplowanalytics",
    version       := "0.8.0",
    description   := "Kinesis sink for Elasticsearch",
    scalaVersion  := "2.11.11",
    scalacOptions := compilerOptions,
    javacOptions  := javaCompilerOptions
  )

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
    "-Xfuture",
    "-Xlint"
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(
    sourceGenerators in Compile += Def.task {
      val dir = (sourceManaged in Compile).value
      val file = dir / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch.generated
        |object Settings {
        |  val organization = "%s"
        |  val version = "%s"
        |  val name = "%s"
        |}
        |""".stripMargin.format(organization.value, version.value, name.value))

      // Dynamically load ElasticsearchClients
      val genDir = new java.io.File("").getAbsolutePath +
        "/src-compat/main/scala/com.snowplowanalytics.snowplow.storage.kinesis/elasticsearch/generated/"

      val esHttpClientFile = dir / "ElasticsearchSenderHTTP.scala"
      val esHttpClientLines = (if (ElasticsearchVersion.equals("1x")) {
        Source.fromFile(genDir + "ElasticsearchSenderHTTP_1x.scala")
      } else {
        Source.fromFile(genDir + "ElasticsearchSenderHTTP_2x.scala")
      })
      IO.write(esHttpClientFile, esHttpClientLines.mkString)

      val esTransportClientFile = dir / "ElasticsearchSenderTransport.scala"
      val esTransportClientLines = (if (ElasticsearchVersion.equals("1x")) {
        Source.fromFile(genDir + "ElasticsearchSenderTransport_1x.scala")
      } else {
        Source.fromFile(genDir + "ElasticsearchSenderTransport_2x.scala")
      })
      IO.write(esTransportClientFile, esTransportClientLines.mkString)

      Seq(
        file,
        esHttpClientFile,
        esTransportClientFile
      )
    }.taskValue
  )

  lazy val buildSettings = basicSettings ++ scalifySettings

  // sbt-assembly settings for building an executable
  import sbtassembly.Plugin._
  import AssemblyKeys._
  lazy val sbtAssemblySettings = assemblySettings ++ Seq(
    // Executable jarfile
    assemblyOption in assembly ~= { _.copy(prependShellScript = Some(defaultShellScript)) },
    // Name it as an executable
    jarName in assembly := { s"${name.value}-${version.value}-${ElasticsearchVersion}" },
    // Merge duplicate class in JodaTime and Elasticsearch 2.4
    mergeStrategy in assembly := {
      case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
      case x => (mergeStrategy in assembly).value(x)
    }
  )
}
