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
    val config           = "1.3.3"
    val slf4j            = "1.7.5"
    val kinesisClient    = "1.7.5"
    val kinesisConnector = "1.3.0"
    val validator        = "2.2.6"
    val elasticsearch    = "2.4.5"
    val nsqClient        = "1.1.0-rc1"
    // Scala
    val scopt            = "3.6.0"
    val scalaz7          = "7.2.14"
    val snowplowTracker  = "0.3.0"
    val analyticsSDK     = "0.2.0"
    val awsSigner        = "0.5.0"
    val elastic4s        = "5.4.6"
    val pureconfig       = "0.8.0"
    // Scala (test only)
    val specs2           = "3.9.2"
  }

  object Libraries {
    // Java
    val config           = "com.typesafe"            %  "config"                       % V.config
    val slf4j            = "org.slf4j"               %  "slf4j-simple"                 % V.slf4j
    val log4jOverSlf4j   = "org.slf4j"               %  "log4j-over-slf4j"             % V.slf4j
    val kinesisClient    = "com.amazonaws"           %  "amazon-kinesis-client"        % V.kinesisClient
    val kinesisConnector = "com.amazonaws"           %  "amazon-kinesis-connectors"    % V.kinesisConnector
    val validator        = "com.github.fge"          %  "json-schema-validator"        % V.validator
    val elasticsearch    = "org.elasticsearch"       %  "elasticsearch"                % V.elasticsearch
    val nsqClient        = "com.snowplowanalytics"   %  "nsq-java-client_2.10"         % V.nsqClient
    // Scala
    val scopt            = "com.github.scopt"        %% "scopt"                        % V.scopt
    val scalaz7          = "org.scalaz"              %% "scalaz-core"                  % V.scalaz7
    val scalazC7         = "org.scalaz"              %% "scalaz-concurrent"            % V.scalaz7
    val snowplowTracker  = "com.snowplowanalytics"   %% "snowplow-scala-tracker"       % V.snowplowTracker
    val analyticsSDK     = "com.snowplowanalytics"   %% "snowplow-scala-analytics-sdk" % V.analyticsSDK
    val awsSigner        = "io.ticofab"              %% "aws-request-signer"           % V.awsSigner
    val pureconfig       = "com.github.pureconfig"   %% "pureconfig"                   % V.pureconfig
    val elastic4sHttp    = "com.sksamuel.elastic4s"  %% "elastic4s-http"               % V.elastic4s
    val elastic4sTcp     = ("com.sksamuel.elastic4s" %% "elastic4s-tcp"                % V.elastic4s)
      .exclude("org.apache.logging.log4j", "log4j-1.2-api")
      .exclude("io.netty", "netty-all")
      .exclude("io.netty", "netty-buffer")
      .exclude("io.netty", "netty-codec")
      .exclude("io.netty", "netty-codec-http")
      .exclude("io.netty", "netty-common")
      .exclude("io.netty", "netty-handler")
      .exclude("io.netty", "netty-resolver")
      .exclude("io.netty", "netty-transport")
    // Scala (test only)
    val specs2           = "org.specs2"              %% "specs2-core"                  % V.specs2    % "test"
    val elastic4sTest    = "com.sksamuel.elastic4s"  %% "elastic4s-embedded"           % V.elastic4s % "test"
  }
}
