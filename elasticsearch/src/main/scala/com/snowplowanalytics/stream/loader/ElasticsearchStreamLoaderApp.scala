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

// Scalaz
import scalaz._
import Scalaz._

// This project
import clients.BulkSender
import clients.ElasticsearchBulkSender
import transformers.{BadEventTransformer, EnrichedEventJsonTransformer, PlainJsonTransformer}
import executors.{KinesisSourceExecutor, NsqSourceExecutor}
import sinks.StdouterrSink
import model._

/** Main entry point for the Elasticsearch HTTP sink */
object ElasticsearchStreamLoaderApp extends StreamLoaderApp {
  val bulkSender: BulkSender[EmitterJsonInput] = config.elasticsearch match {
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
        esConfig.client.maxTimeout,
        CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey),
        tracker,
        esConfig.client.maxRetries
      )
  }

  val goodSink = config.sink.good match {
    case "stdout"        => Some(new StdouterrSink)
    case "elasticsearch" => None
  }

  val executor = (config.source, config.queue, config.elasticsearch, badSinkValidated) match {
    // Read records from Kinesis
    case ("kinesis", queue: Kinesis, Some(esConfig), Success(badSink)) =>
      new KinesisSourceExecutor[ValidatedJsonRecord, EmitterJsonInput](
        config,
        queue,
        new KinesisPipeline(
          config.streamType,
          esConfig.cluster.index,
          esConfig.cluster.documentType,
          goodSink,
          badSink,
          bulkSender,
          config.streams.buffer.recordLimit,
          config.streams.buffer.byteLimit,
          tracker
        )
      ).success

    // Read records from NSQ
    case ("nsq", queue: Nsq, Some(esConfig), Success(badSink)) =>
      new NsqSourceExecutor(
        config.streamType,
        esConfig.cluster.index,
        esConfig.cluster.documentType,
        queue,
        config,
        goodSink,
        badSink,
        bulkSender).success

    // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
    // TODO reduce code duplication
    case ("stdin", _, Some(esConfig), Success(badSink)) =>
      new Runnable {
        val transformer = config.streamType match {
          case Good =>
            new EnrichedEventJsonTransformer(esConfig.cluster.index, esConfig.cluster.documentType)
          case PlainJson =>
            new PlainJsonTransformer(esConfig.cluster.index, esConfig.cluster.documentType)
          case Bad =>
            new BadEventTransformer(esConfig.cluster.index, esConfig.cluster.documentType)
        }

        def run = for (ln <- scala.io.Source.stdin.getLines) {
          val emitterInput = transformer.consumeLine(ln)
          emitterInput._2.bimap(
            f => badSink.store(BadRow(emitterInput._1, f).toCompactJson, None, false),
            s =>
              goodSink match {
                case Some(gs) => gs.store(s.json.toString, None, true)
                case None     => bulkSender.send(List(ln -> s.success))
            }
          )
        }
      }.success
    case (_, _, _, Failure(badSinkError)) =>
      s"badSink configuration is not correct: $badSinkError".failure
    case _ => "Source must be set to 'stdin', 'kinesis' or 'nsq'".failure
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
