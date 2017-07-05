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
lazy val root = project.in(file("."))
  .settings(
    name        := "snowplow-elasticsearch-sink",
    version     := "0.8.0",
    description := "Kinesis sink for Elasticsearch"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(
    libraryDependencies ++= Dependencies.onVersion(
      all = Seq(
        // Java
        Dependencies.Libraries.config,
        Dependencies.Libraries.slf4j,
        Dependencies.Libraries.log4jOverSlf4j,
        Dependencies.Libraries.kinesisClient,
        Dependencies.Libraries.kinesisConnector,
        // Scala
        Dependencies.Libraries.scopt,
        Dependencies.Libraries.scalaz7,
        Dependencies.Libraries.snowplowTracker,
        Dependencies.Libraries.snowplowCommonEnrich,
        Dependencies.Libraries.igluClient,
        // Scala (test only)
        Dependencies.Libraries.specs2
      ),
      on1x = Seq(
        Dependencies.Libraries.jest._1x,
        Dependencies.Libraries.elasticsearch._1x
      ),
      on2x = Seq(
        Dependencies.Libraries.jest._2x,
        Dependencies.Libraries.elasticsearch._2x
      )
    )
  )

shellPrompt := { _ => "elasticsearch-sink> " }
