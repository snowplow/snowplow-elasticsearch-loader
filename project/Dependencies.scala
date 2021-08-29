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

import sbt._

object Dependencies {
  object V {
    // Java
    val config           = "1.4.1"
    val slf4j            = "1.7.32"
    val log4j            = "2.14.1"
    val kinesisClient    = "1.14.4"
    val kinesisConnector = "1.3.0"
    val elasticsearch    = "6.8.18"
    val nsqClient        = "1.1.0-rc1"
    // Scala
    val catsRetry        = "0.3.2"
    val circe            = "0.14.1"
    val decline          = "2.1.0"
    val snowplowTracker  = "1.0.0"
    val analyticsSDK     = "2.1.0"
    val awsSigner        = "0.5.2"
    val elastic4s        = "6.5.7"
    val pureconfig       = "0.9.1"
    val badRows          = "2.1.1"
    // Scala (test only)
    val specs2           = "4.1.0"
  }

  object Libraries {
    // Java
    val config           = "com.typesafe"                     %  "config"                       % V.config
    val slf4j            = "org.slf4j"                        %  "slf4j-simple"                 % V.slf4j
    val log4jOverSlf4j   = "org.slf4j"                        %  "log4j-over-slf4j"             % V.slf4j
    val log4jCore        = "org.apache.logging.log4j"         %  "log4j-core"                   % V.log4j
    val log4jApi         = "org.apache.logging.log4j"         %  "log4j-api"                    % V.log4j
    val kinesisClient    = "com.amazonaws"                    %  "amazon-kinesis-client"        % V.kinesisClient
    val kinesisConnector = "com.amazonaws"                    %  "amazon-kinesis-connectors"    % V.kinesisConnector
    val elasticsearch    = "org.elasticsearch"                %  "elasticsearch"                % V.elasticsearch
    val nsqClient        = "com.snowplowanalytics"            %  "nsq-java-client_2.10"         % V.nsqClient
    // Scala
    val catsRetry        = "com.github.cb372"                 %% "cats-retry-cats-effect"       % V.catsRetry
    val circeOptics      = "io.circe"                         %% "circe-optics"                 % V.circe
    val decline          = "com.monovore"                     %% "decline"                      % V.decline
    val snowplowTracker  = "com.snowplowanalytics"            %% "snowplow-scala-tracker-core"  % V.snowplowTracker
    val snowplowTrackerId = "com.snowplowanalytics"           %% "snowplow-scala-tracker-emitter-id" % V.snowplowTracker
    val analyticsSDK     = "com.snowplowanalytics"            %% "snowplow-scala-analytics-sdk" % V.analyticsSDK
    val awsSigner        = "io.ticofab"                       %% "aws-request-signer"           % V.awsSigner
    val pureconfig       = "com.github.pureconfig"            %% "pureconfig"                   % V.pureconfig
    val pureconfigEnum   = "com.github.pureconfig"            %% "pureconfig-enumeratum"        % V.pureconfig
    val elastic4sHttp    = "com.sksamuel.elastic4s"           %% "elastic4s-http"               % V.elastic4s
    val badRows          = "com.snowplowanalytics"            %% "snowplow-badrows"             % V.badRows
    // Scala (test only)
    val circeLiteral     = "io.circe"                         %% "circe-literal"                % V.circe     % Test
    val specs2           = "org.specs2"                       %% "specs2-core"                  % V.specs2    % Test
    val elastic4sTest    = "com.sksamuel.elastic4s"           %% "elastic4s-embedded"           % V.elastic4s % Test
  }
}
