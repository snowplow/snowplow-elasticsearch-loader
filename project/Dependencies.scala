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

import sbt._

object Dependencies {
  object V {
    // Java
    val config           = "1.4.1"
    val slf4j            = "2.0.6"
    val log4j            = "2.17.1"
    val awsSdk           = "1.12.780"
    val kinesisClient    = "1.15.2"
    val kinesisConnector = "1.3.0"
    val nsqClient        = "1.3.0"
    val netty            = "4.1.117.Final" // Override provided version to fix security vulnerability
    val jackson          = "2.18.2"
    val protobuf         = "4.29.3"
    // Scala
    val catsRetry        = "2.1.1"
    val circe            = "0.14.1"
    val decline          = "2.1.0"
    val snowplowTracker  = "1.0.0"
    val analyticsSDK     = "2.1.0"
    val elastic4s        = "7.17.4"
    val pureconfig       = "0.16.0"
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
    val kinesisClient    = "com.amazonaws"                    %  "amazon-kinesis-client"        % V.kinesisClient exclude("com.fasterxml.jackson.dataformat", "jackson-dataformat-cbor")
    val kinesisConnector = "com.amazonaws"                    %  "amazon-kinesis-connectors"    % V.kinesisConnector exclude("com.fasterxml.jackson.dataformat", "jackson-dataformat-cbor")
    val sts              = "com.amazonaws"                    %  "aws-java-sdk-sts"             % V.awsSdk % Runtime
    val nsqClient        = "com.snowplowanalytics"            %  "nsq-java-client"              % V.nsqClient
    val netty            = "io.netty"                         %  "netty-all"                    % V.netty
    val jacksonCbor      = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"       % V.jackson // Override provided version to fix security vulnerability
    val protobuf         = "com.google.protobuf"              % "protobuf-java"                 % V.protobuf // Override provided version to fix security vulnerability
    // Scala
    val catsRetry        = "com.github.cb372"                 %% "cats-retry"                   % V.catsRetry
    val circeOptics      = "io.circe"                         %% "circe-optics"                 % V.circe
    val decline          = "com.monovore"                     %% "decline"                      % V.decline
    val jacksonScala     = "com.fasterxml.jackson.module"     %% "jackson-module-scala"         % V.jackson // Compatible version required for elastic4s
    val snowplowTracker  = "com.snowplowanalytics"            %% "snowplow-scala-tracker-core"  % V.snowplowTracker
    val snowplowTrackerId = "com.snowplowanalytics"           %% "snowplow-scala-tracker-emitter-id" % V.snowplowTracker
    val analyticsSDK     = "com.snowplowanalytics"            %% "snowplow-scala-analytics-sdk" % V.analyticsSDK
    val pureconfig       = "com.github.pureconfig"            %% "pureconfig"                   % V.pureconfig
    val pureconfigEnum   = "com.github.pureconfig"            %% "pureconfig-enumeratum"        % V.pureconfig
    val elastic4sEsJava  = "com.sksamuel.elastic4s"           %% "elastic4s-client-esjava"      % V.elastic4s
    val badRows          = "com.snowplowanalytics"            %% "snowplow-badrows"             % V.badRows
    // Scala (test only)
    val circeLiteral     = "io.circe"                         %% "circe-literal"                % V.circe     % Test
    val specs2           = "org.specs2"                       %% "specs2-core"                  % V.specs2    % Test
  }
}
