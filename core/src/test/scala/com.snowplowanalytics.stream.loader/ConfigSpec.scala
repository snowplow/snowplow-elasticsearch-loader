/**
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.stream.loader

import java.nio.file.Paths

import cats.syntax.option._

import org.specs2.mutable.Specification

import pureconfig._

import com.snowplowanalytics.stream.loader.Config._

class ConfigSpec extends Specification {
  private val DefaultTestRegion = Region("ap-east-1")
  private val RealConfigReader =
    com.snowplowanalytics.stream.loader.Config.implicits().streamLoaderConfigReader
  private val TestConfigReader = com.snowplowanalytics.stream.loader.Config
    .implicits(
      new ConfigReader[Region] with ReadsMissingKeys {
        override def from(cur: ConfigCursor) =
          if (cur.isUndefined) Right(DefaultTestRegion)
          else cur.asString.map(Region)
      }
    )
    .streamLoaderConfigReader

  "Config.parseConfig" should {
    "accept example extended kinesis config" >> {
      val config = Paths.get(getClass.getResource("/config.kinesis.reference.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Kinesis(
          "test-kinesis-stream",
          "AT_TIMESTAMP",
          "2020-07-17T10:00:00Z".some,
          9999,
          Region("eu-central-1"),
          "test-app-name",
          "127.0.0.1".some,
          "http://localhost:4569".some,
          Source.Kinesis.Buffer(999999, 499, 499)
        ),
        Sink(
          Sink.GoodSink.Elasticsearch(
            Sink.GoodSink.Elasticsearch.ESClient(
              "localhost",
              9200,
              "es-user".some,
              "es-pass".some,
              "_yyyy-MM-dd".some,
              "derived_tstamp".some,
              59999,
              9999,
              5,
              true
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(true, Region("eu-central-1")),
            Sink.GoodSink.Elasticsearch.ESCluster("good", "good-doc".some),
            Sink.GoodSink.Elasticsearch.ESChunk(999999, 499)
          ),
          Sink.BadSink
            .Kinesis(
              "test-kinesis-bad-stream",
              Region("eu-central-1"),
              "127.0.0.1:7846".some,
              500,
              5242880
            )
        ),
        Purpose.Enriched,
        Monitoring(
          Monitoring.SnowplowMonitoring("localhost:14322", "test-app-id").some,
          Monitoring.Metrics(false)
        )
      )

      val result = testParseConfig(argv)
      result must beRight(expected)
    }

    "accept example minimal kinesis config" >> {
      val config = Paths.get(getClass.getResource("/config.kinesis.minimal.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Kinesis(
          "test-kinesis-stream",
          "LATEST",
          None,
          10000,
          DefaultTestRegion,
          "snowplow-elasticsearch-loader",
          None,
          None,
          Source.Kinesis.Buffer(1000000, 500, 500)
        ),
        Sink(
          Sink.GoodSink.Elasticsearch(
            Sink.GoodSink.Elasticsearch.ESClient(
              "localhost",
              9200,
              None,
              None,
              None,
              None,
              60000,
              10000,
              6,
              false
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(false, DefaultTestRegion),
            Sink.GoodSink.Elasticsearch.ESCluster("good", None),
            Sink.GoodSink.Elasticsearch.ESChunk(1000000, 500)
          ),
          Sink.BadSink.Kinesis("test-kinesis-bad-stream", DefaultTestRegion, None, 500, 5242880)
        ),
        Purpose.Enriched,
        Monitoring(
          None,
          Monitoring.Metrics(true)
        )
      )

      val result = testParseConfig(argv)
      result must beRight(expected)
    }

    "accept example extended nsq config" >> {
      val config = Paths.get(getClass.getResource("/config.nsq.reference.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Nsq(
          "test-nsq-stream",
          "test-nsq-channel-name",
          "127.0.0.1",
          34189,
          Source.Nsq.Buffer(499)
        ),
        Sink(
          Sink.GoodSink.Elasticsearch(
            Sink.GoodSink.Elasticsearch.ESClient(
              "localhost",
              9200,
              "es-user".some,
              "es-pass".some,
              "_yyyy-MM-dd".some,
              "derived_tstamp".some,
              59999,
              9999,
              5,
              true
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(true, Region("eu-central-1")),
            Sink.GoodSink.Elasticsearch.ESCluster("good", "good-doc".some),
            Sink.GoodSink.Elasticsearch.ESChunk(999999, 499)
          ),
          Sink.BadSink.Nsq("test-nsq-bad-stream", "127.0.0.1", 24509)
        ),
        Purpose.Enriched,
        Monitoring(
          Monitoring.SnowplowMonitoring("localhost:14322", "test-app-id").some,
          Monitoring.Metrics(false)
        )
      )

      val result = testParseConfig(argv)
      result must beRight(expected)
    }

    "accept example minimal nsq config" >> {
      val config = Paths.get(getClass.getResource("/config.nsq.minimal.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Nsq(
          "test-nsq-stream",
          "test-nsq-channel-name",
          "127.0.0.1",
          34189,
          Source.Nsq.Buffer(500)
        ),
        Sink(
          Sink.GoodSink.Elasticsearch(
            Sink.GoodSink.Elasticsearch.ESClient(
              "localhost",
              9200,
              None,
              None,
              None,
              None,
              60000,
              10000,
              6,
              false
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(false, DefaultTestRegion),
            Sink.GoodSink.Elasticsearch.ESCluster("good", None),
            Sink.GoodSink.Elasticsearch.ESChunk(1000000, 500)
          ),
          Sink.BadSink.Nsq("test-nsq-bad-stream", "127.0.0.1", 24509)
        ),
        Purpose.Json,
        Monitoring(
          None,
          Monitoring.Metrics(true)
        )
      )

      val result = testParseConfig(argv)
      result must beRight(expected)
    }

    "accept example extended stdin config" >> {
      val config = Paths.get(getClass.getResource("/config.stdin.reference.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Stdin,
        Sink(
          Sink.GoodSink.Stdout,
          Sink.BadSink.Stderr
        ),
        Purpose.Enriched,
        Monitoring(
          Monitoring.SnowplowMonitoring("localhost:14322", "test-app-id").some,
          Monitoring.Metrics(false)
        )
      )

      val result = testParseConfig(argv)
      result must beRight(expected)
    }

    "accept example minimal stdin config" >> {
      val config = Paths.get(getClass.getResource("/config.stdin.minimal.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Stdin,
        Sink(
          Sink.GoodSink.Stdout,
          Sink.BadSink.Stderr
        ),
        Purpose.Bad,
        Monitoring(
          None,
          Monitoring.Metrics(true)
        )
      )

      val result = testParseConfig(argv)
      result must beRight(expected)
    }

    "override default values" >> {
      val config = Paths.get(getClass.getResource("/config.test1.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Kinesis(
          "test-kinesis-stream",
          "LATEST",
          None,
          2000,
          Region("ca-central-1"),
          "test-app-name",
          None,
          None,
          Source.Kinesis.Buffer(201, 202, 203)
        ),
        Sink(
          Sink.GoodSink.Elasticsearch(
            Sink.GoodSink.Elasticsearch.ESClient(
              "localhost",
              9220,
              None,
              None,
              None,
              None,
              505,
              205,
              7,
              true
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(true, Region("ca-central-1")),
            Sink.GoodSink.Elasticsearch.ESCluster("testindex", None),
            Sink.GoodSink.Elasticsearch.ESChunk(206, 207)
          ),
          Sink.BadSink
            .Kinesis("test-kinesis-bad-stream", Region("ca-central-1"), None, 500, 5242880)
        ),
        Purpose.Bad,
        Monitoring(
          None,
          Monitoring.Metrics(false)
        )
      )

      val result = testParseConfig(argv)
      result must beRight(expected)
    }

    "set region correctly with test config reader" >> {
      val config = Paths.get(getClass.getResource("/config.test2.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Kinesis(
          "test-kinesis-stream",
          "LATEST",
          None,
          10000,
          Region("ca-central-1"),
          "snowplow-elasticsearch-loader",
          None,
          None,
          Source.Kinesis.Buffer(1000000, 500, 500)
        ),
        Sink(
          Sink.GoodSink.Elasticsearch(
            Sink.GoodSink.Elasticsearch.ESClient(
              "localhost",
              9200,
              None,
              None,
              None,
              None,
              60000,
              10000,
              6,
              false
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(false, DefaultTestRegion),
            Sink.GoodSink.Elasticsearch.ESCluster("good", None),
            Sink.GoodSink.Elasticsearch.ESChunk(1000000, 500)
          ),
          Sink.BadSink.Kinesis("test-kinesis-bad-stream", DefaultTestRegion, None, 500, 5242880)
        ),
        Purpose.Enriched,
        Monitoring(
          None,
          Monitoring.Metrics(true)
        )
      )

      val result = testParseConfig(argv)
      result must beRight(expected)
    }

    "set region correctly with real config reader" >> {
      val config = Paths.get(getClass.getResource("/config.test3.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val expected = StreamLoaderConfig(
        Source.Kinesis(
          "test-kinesis-stream",
          "LATEST",
          None,
          10000,
          Region("ca-central-1"),
          "snowplow-elasticsearch-loader",
          None,
          None,
          Source.Kinesis.Buffer(1000000, 500, 500)
        ),
        Sink(
          Sink.GoodSink.Elasticsearch(
            Sink.GoodSink.Elasticsearch.ESClient(
              "localhost",
              9200,
              None,
              None,
              None,
              None,
              60000,
              10000,
              6,
              false
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(true, Region("af-south-1")),
            Sink.GoodSink.Elasticsearch.ESCluster("good", None),
            Sink.GoodSink.Elasticsearch.ESChunk(1000000, 500)
          ),
          Sink.BadSink.Kinesis("test-kinesis-bad-stream", Region("eu-west-2"), None, 500, 5242880)
        ),
        Purpose.Enriched,
        Monitoring(
          None,
          Monitoring.Metrics(true)
        )
      )

      val result = testParseConfig(argv, RealConfigReader)
      result must beRight(expected)
    }

    "give error when unknown region given" >> {
      val config = Paths.get(getClass.getResource("/config.test4.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val result = testParseConfig(argv, RealConfigReader)
      result.fold(
        err => err.contains("unknown-region-1") && err.contains("unknown-region-1"),
        _ => false
      ) must beTrue
    }

    "give error when required fields are missing" >> {
      val config = Paths.get(getClass.getResource("/config.test5.hocon").toURI)
      val argv   = Array("--config", config.toString)

      val result = testParseConfig(argv)
      result.fold(
        err =>
          err.contains("streamName")
            && err.contains("endpoint")
            && err.contains("type"),
        _ => false
      ) must beTrue
    }
  }

  private def testParseConfig(
    args: Array[String],
    configReader: ConfigReader[StreamLoaderConfig] = TestConfigReader
  ) =
    Config.parseConfig(args, configReader)
}
