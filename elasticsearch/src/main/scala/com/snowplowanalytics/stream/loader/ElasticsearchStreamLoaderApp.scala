/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd.
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

// cats
import cats.data.Validated
import cats.syntax.validated._

// This project
import clients.BulkSender
import clients.ElasticsearchBulkSender
import transformers.{BadEventTransformer, EnrichedEventJsonTransformer, PlainJsonTransformer}
import executors.{KinesisSourceExecutor, NsqSourceExecutor}
import sinks.StdouterrSink
import model._

/** Main entry point for the Elasticsearch HTTP sink */
object ElasticsearchStreamLoaderApp extends StreamLoaderApp {
  lazy val bulkSender: BulkSender[EmitterJsonInput] = config.elasticsearch match {
    case None =>
      throw new RuntimeException("No configuration for Elasticsearch found.")
    case Some(esConfig) =>
      new ElasticsearchBulkSender(
        esConfig.client.endpoint,
        esConfig.client.port,
        esConfig.client.ssl,
        esConfig.aws.region,
        esConfig.aws.signing,
        esConfig.client.username,
        esConfig.client.password,
        esConfig.cluster.index,
        esConfig.cluster.documentType,
        esConfig.client.maxTimeout,
        CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey),
        tracker,
        esConfig.client.maxRetries
      )
  }

  lazy val goodSink = config.sink.good match {
    case "stdout"        => Some(new StdouterrSink)
    case "elasticsearch" => None
  }

  lazy val executor = (config.source, config.queue, config.elasticsearch, badSinkValidated) match {
    // Read records from Kinesis
    case ("kinesis", queue: Kinesis, Some(esConfig), Validated.Valid(badSink)) =>
      new KinesisSourceExecutor[ValidatedJsonRecord, EmitterJsonInput](
        config,
        queue,
        new KinesisPipeline(
          config.streamType,
          goodSink,
          badSink,
          bulkSender,
          esConfig.client.shardDateField,
          esConfig.client.shardDateFormat,
          config.streams.buffer.recordLimit,
          config.streams.buffer.byteLimit,
          tracker
        )
      ).valid

    // Read records from NSQ
    case ("nsq", queue: Nsq, Some(esConfig), Validated.Valid(badSink)) =>
      new NsqSourceExecutor(
        config.streamType,
        queue,
        config,
        goodSink,
        badSink,
        esConfig.client.shardDateField,
        esConfig.client.shardDateFormat,
        bulkSender).valid

    // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
    // TODO reduce code duplication
    case ("stdin", _, Some(esConfig), Validated.Valid(badSink)) =>
      new Runnable {
        val transformer = config.streamType match {
          case Good =>
            new EnrichedEventJsonTransformer(
              esConfig.client.shardDateField,
              esConfig.client.shardDateFormat)
          case PlainJson => new PlainJsonTransformer
          case Bad       => new BadEventTransformer
        }

        def run = for (ln <- scala.io.Source.stdin.getLines) {
          val emitterInput = transformer.consumeLine(ln)
          emitterInput._2.bimap(
            f => badSink.store(BadRow(emitterInput._1, f).toCompactJson, None, false),
            s =>
              goodSink match {
                case Some(gs) => gs.store(s.json.toString, None, true)
                case None     => bulkSender.send(List(ln -> s.valid))
            }
          )
        }
      }.valid
    case (_, _, _, Validated.Invalid(badSinkError)) =>
      s"badSink configuration is not correct: $badSinkError".invalid
    case _ => "Source must be set to 'stdin', 'kinesis' or 'nsq'".invalid
  }
  executor.fold(
    err => throw new RuntimeException(err),
    exec => {
      tracker foreach { t =>
        SnowplowTracking.initializeSnowplowTracking(t)
      }
      exec.run()

      // If the stream cannot be found, the KCL's "cw-metrics-publisher" thread will prevent the
      // application from exiting naturally so we explicitly call System.exit.
      // This does not apply to NSQ because NSQ consumer is non-blocking and fall here
      // right after consumer.start()
      config.source match {
        case "kinesis" => System.exit(1)
        case "stdin"   => System.exit(1)
        // do anything
        case "nsq" =>
      }
    }
  )

}
