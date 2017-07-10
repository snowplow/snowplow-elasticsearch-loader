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

import sbt._

object Dependencies {

  object V {
    // Java
    val config           = "1.3.1"
    val slf4j            = "1.7.5"
    val kinesisClient    = "1.7.5"
    val kinesisConnector = "1.3.0"
    val validator        = "2.2.6"

    // Scala
    val scopt            = "3.6.0"
    val scalaz7          = "7.2.14"
    val snowplowTracker  = "0.3.0"
    val elastic4s        = "5.4.6"
    // Scala (test only)
    val specs2           = "3.9.2"
  }

  object Libraries {
    // Java
    val config           = "com.typesafe"           %  "config"                    % V.config
    val slf4j            = "org.slf4j"              %  "slf4j-simple"              % V.slf4j
    val log4jOverSlf4j   = "org.slf4j"              %  "log4j-over-slf4j"          % V.slf4j
    val kinesisClient    = "com.amazonaws"          %  "amazon-kinesis-client"     % V.kinesisClient
    val kinesisConnector = "com.amazonaws"          %  "amazon-kinesis-connectors" % V.kinesisConnector
    val validator        = "com.github.fge"         %  "json-schema-validator"     % V.validator

    // Scala
    val scopt            = "com.github.scopt"       %% "scopt"                     % V.scopt
    val scalaz7          = "org.scalaz"             %% "scalaz-core"               % V.scalaz7
    val scalazC7          = "org.scalaz"            %% "scalaz-concurrent"         % V.scalaz7
    val snowplowTracker  = "com.snowplowanalytics"  %% "snowplow-scala-tracker"    % V.snowplowTracker
    val elastic4sHttp    = "com.sksamuel.elastic4s" %% "elastic4s-http"            % V.elastic4s
    val elastic4sTcp     = "com.sksamuel.elastic4s" %% "elastic4s-tcp"             % V.elastic4s
    // Scala (test only)
    val specs2           = "org.specs2"             %% "specs2-core"               % V.specs2 % "test"
  }
}
