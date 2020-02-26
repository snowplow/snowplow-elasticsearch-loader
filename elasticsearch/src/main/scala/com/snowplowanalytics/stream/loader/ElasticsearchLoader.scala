/**
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd.
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

import cats.Id

import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.stream.loader.sinks.ISink
import com.snowplowanalytics.stream.loader.clients.{BulkSender, ElasticsearchBulkSender}
import com.snowplowanalytics.stream.loader.executors.{
  KinesisSourceExecutor,
  NsqSourceExecutor,
  StdinExecutor
}
import com.snowplowanalytics.stream.loader.Config._

/** Main entry point for the Elasticsearch HTTP sink */
object ElasticsearchLoader {

  def main(args: Array[String]): Unit = {
    val config: StreamLoaderConfig = Config.parseConfig(args)
    val tracker                    = config.monitoring.map(e => SnowplowTracking.initializeTracker(e.snowplow))
    val badSink                    = initBadSink(config)
    val bulkSender                 = ElasticsearchBulkSender(config, tracker)
    val goodSink = config.sink.good match {
      case GoodSink.Stdout        => Some(new sinks.StdouterrSink)
      case GoodSink.Elasticsearch => None
    }

    val executor = initExecutor(config, bulkSender, goodSink, badSink, tracker)

    SnowplowTracking.initializeSnowplowTracking(tracker)

    executor.run()

    // If the stream cannot be found, the KCL's "cw-metrics-publisher" thread will prevent the
    // application from exiting naturally so we explicitly call System.exit.
    // This does not apply to NSQ because NSQ consumer is non-blocking and fall here
    // right after consumer.start()
    config.source match {
      case Source.Kinesis => System.exit(1)
      case Source.Stdin   => System.exit(1)
      case Source.Nsq     => ()
    }
  }

  def initExecutor(
    config: StreamLoaderConfig,
    sender: BulkSender[EmitterJsonInput],
    goodSink: Option[ISink],
    badSink: ISink,
    tracker: Option[Tracker[Id]]): Runnable = {
    (config.source, config.queue) match {
      // Read records from Kinesis
      case (Source.Kinesis, queue: Queue.Kinesis) =>
        val pipeline = new KinesisPipeline(
          config.enabled,
          goodSink,
          badSink,
          sender,
          config.elasticsearch.client.shardDateField,
          config.elasticsearch.client.shardDateFormat,
          config.streams.buffer.recordLimit,
          config.streams.buffer.byteLimit
        )
        new KinesisSourceExecutor[ValidatedJsonRecord, EmitterJsonInput](config, queue, pipeline)

      // Read records from NSQ
      case (Source.Nsq, queue: Queue.Nsq) =>
        new NsqSourceExecutor(
          config.enabled,
          queue,
          config,
          goodSink,
          badSink,
          config.elasticsearch.client.shardDateField,
          config.elasticsearch.client.shardDateFormat,
          sender)

      // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
      case (Source.Stdin, _) => new StdinExecutor(config, sender, goodSink, badSink)
      case _                 => throw new RuntimeException("Source must be set to 'stdin', 'kinesis' or 'nsq'")
    }
  }

  def initBadSink(config: StreamLoaderConfig): ISink = {
    config.sink.bad match {
      case BadSink.Stderr => new sinks.StdouterrSink
      case BadSink.Nsq =>
        config.queue match {
          case queue: Queue.Nsq =>
            new sinks.NsqSink(queue.nsqdHost, queue.nsqdPort, config.streams.outStreamName)
          case _ =>
            System.err.println("queue config is not valid for Nsq")
            sys.exit(1)
        }
      case BadSink.None => new sinks.NullSink
      case BadSink.Kinesis =>
        config.queue match {
          case queue: Queue.Kinesis =>
            new sinks.KinesisSink(
              config.aws.accessKey,
              config.aws.secretKey,
              queue.endpoint,
              queue.region,
              config.streams.outStreamName)
          case _ =>
            System.err.println("queue config is not valid for Kinesis")
            sys.exit(1)
        }
    }
  }
}
