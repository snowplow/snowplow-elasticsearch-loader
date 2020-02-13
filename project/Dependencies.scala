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
    val log4j            = "2.11.1"
    val kinesisClient    = "1.9.1"
    val kinesisConnector = "1.3.0"
    val validator        = "2.2.6"
    val elasticsearch    = "6.3.2"
    val nsqClient        = "1.1.0-rc1"
    val jackson          = "2.9.6"
    // TODO: consider removing paranamer version override when upgrading other libaries
    val paranamer        = "2.8"   // Jackson 2.9.6 uses version 2.8 of paranamer internally
                                   // but there is json4s-core_2.12 3.2.11 that overrides it back to 2.6

    // Scala
    val cats             = "1.6.1"
    val catsEffect       = "1.3.1"
    val catsRetry        = "0.2.5"
    val scopt            = "3.7.0"
    val snowplowTracker  = "0.5.0"
    val analyticsSDK     = "0.3.0"
    val awsSigner        = "0.5.0"
    val elastic4s        = "6.3.6"
    val pureconfig       = "0.9.1"
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
    val validator        = "com.github.fge"                   %  "json-schema-validator"        % V.validator
    val elasticsearch    = "org.elasticsearch"                %  "elasticsearch"                % V.elasticsearch
    val nsqClient        = "com.snowplowanalytics"            %  "nsq-java-client_2.10"         % V.nsqClient
    val paranamer        = "com.thoughtworks.paranamer"       %  "paranamer"                    % V.paranamer
    val jacksonCbor      = "com.fasterxml.jackson.dataformat" %  "jackson-dataformat-cbor"      % V.jackson
    val jacksonDatabind  = "com.fasterxml.jackson.core"       %  "jackson-databind"             % V.jackson
    // Scala
    val cats             = "org.typelevel"                    %% "cats-core"                    % V.cats
    val catsEffect       = "org.typelevel"                    %% "cats-effect"                  % V.catsEffect
    val catsRetry        = "com.github.cb372"                 %% "cats-retry-cats-effect"       % V.catsRetry
    val scopt            = "com.github.scopt"                 %% "scopt"                        % V.scopt
    val snowplowTracker  = "com.snowplowanalytics"            %% "snowplow-scala-tracker"       % V.snowplowTracker
    val analyticsSDK     = "com.snowplowanalytics"            %% "snowplow-scala-analytics-sdk" % V.analyticsSDK
    val awsSigner        = "io.ticofab"                       %% "aws-request-signer"           % V.awsSigner
    val pureconfig       = "com.github.pureconfig"            %% "pureconfig"                   % V.pureconfig
    val elastic4sHttp    = "com.sksamuel.elastic4s"           %% "elastic4s-http"               % V.elastic4s
    // Scala (test only)
    val specs2           = "org.specs2"                       %% "specs2-core"                  % V.specs2    % Test
    val elastic4sTest    = "com.sksamuel.elastic4s"           %% "elastic4s-embedded"           % V.elastic4s % Test
  }
}
