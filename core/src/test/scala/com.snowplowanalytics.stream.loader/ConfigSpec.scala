/**
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd.
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

import com.snowplowanalytics.stream.loader.Config._

class ConfigSpec extends Specification {

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
          "eu-central-1".some,
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
              9999,
              5,
              true
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(true, "eu-central-1".some),
            Sink.GoodSink.Elasticsearch.ESCluster("good", "good-doc".some),
            Sink.GoodSink.Elasticsearch.ESChunk(999999, 499)
          ),
          Sink.BadSink
            .Kinesis("test-kinesis-bad-stream", "eu-central-1".some, "127.0.0.1:7846".some)
        ),
        Purpose.Enriched,
        Monitoring(
          Monitoring.SnowplowMonitoring("localhost:14322", "test-app-id").some,
          Monitoring.Metrics(false)
        )
      )

      val result = Config.parseConfig(argv)
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
          "eu-central-1".some,
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
              10000,
              6,
              false
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(false, None),
            Sink.GoodSink.Elasticsearch.ESCluster("good", None),
            Sink.GoodSink.Elasticsearch.ESChunk(1000000, 500)
          ),
          Sink.BadSink.Kinesis("test-kinesis-bad-stream", "eu-central-1".some, None)
        ),
        Purpose.Bad,
        Monitoring(
          None,
          Monitoring.Metrics(true)
        )
      )

      val result = Config.parseConfig(argv)
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
              9999,
              5,
              true
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(true, "eu-central-1".some),
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

      val result = Config.parseConfig(argv)
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
              10000,
              6,
              false
            ),
            Sink.GoodSink.Elasticsearch.ESAWS(false, None),
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

      val result = Config.parseConfig(argv)
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

      val result = Config.parseConfig(argv)
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

      val result = Config.parseConfig(argv)
      result must beRight(expected)
    }
  }
}
