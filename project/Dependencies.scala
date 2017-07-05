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
    val config               = "1.3.1"
    val slf4j                = "1.7.5"
    val kinesisClient        = "1.7.5"
    val kinesisConnector     = "1.1.2"

    object jest {
      val _1x                = "1.0.3"
      val _2x                = "2.0.3"
    }

    object elasticsearch {
      val _1x                = "1.7.5"
      val _2x                = "2.4.0"
    }

    // Scala
    val scopt                = "3.6.0"
    val snowplowCommonEnrich = "0.22.0"
    val scalaz7              = "7.2.14"
    val snowplowTracker      = "0.3.0"
    // Scala (test only)
    val specs2               = "3.9.2"
  }

  object Libraries {
    // Java
    val config               = "com.typesafe"               %  "config"                    % V.config
    val slf4j                = "org.slf4j"                  %  "slf4j-simple"              % V.slf4j
    val log4jOverSlf4j       = "org.slf4j"                  %  "log4j-over-slf4j"          % V.slf4j
    val kinesisClient        = "com.amazonaws"              %  "amazon-kinesis-client"     % V.kinesisClient
    val kinesisConnector     = "com.amazonaws"              %  "amazon-kinesis-connectors" % V.kinesisConnector

    object jest {
      val _1x                = "io.searchbox"               %  "jest"                      % V.jest._1x
      val _2x                = "io.searchbox"               %  "jest"                      % V.jest._2x
    }

    object elasticsearch {
      val _1x                = "org.elasticsearch"          %  "elasticsearch"             % V.elasticsearch._1x
      val _2x                = "org.elasticsearch"          %  "elasticsearch"             % V.elasticsearch._2x
    }

    // Scala
    val scopt                = "com.github.scopt"           %% "scopt"                     % V.scopt
    val scalaz7              = "org.scalaz"                 %% "scalaz-core"               % V.scalaz7
    val snowplowTracker      = "com.snowplowanalytics"      %% "snowplow-scala-tracker"    % V.snowplowTracker
    val snowplowCommonEnrich = "com.snowplowanalytics"      % "snowplow-common-enrich"     % V.snowplowCommonEnrich
    // Scala (test only)
    val specs2               = "org.specs2"                 %% "specs2-core"               % V.specs2 % "test"
  }

  def onVersion[A](all: Seq[A] = Seq(), on1x: => Seq[A] = Seq(), on2x: => Seq[A] = Seq()) = {
    if (BuildSettings.ElasticsearchVersion.equals("1x")) {
      all ++ on1x
    } else {
      all ++ on2x
    }
  }
}
