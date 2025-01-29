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
import com.snowplowanalytics.stream.loader.Config.Sink.{BadSink, GoodSink}

/** Main entry point for the Elasticsearch HTTP sink */
object ElasticsearchLoader {

  def main(args: Array[String]): Unit = {
    val configRes = Config.parseConfig(args)
    val config = configRes match {
      case Right(c) => c
      case Left(e) =>
        System.err.println(s"configuration error:\n$e")
        sys.exit(1)
    }
    val tracker = config.monitoring.snowplow.flatMap(SnowplowTracking.initializeTracker)
    val badSink = initBadSink(config)
    val goodSink = config.output.good match {
      case c: GoodSink.Elasticsearch => Right(ElasticsearchBulkSender(c, tracker))
      case GoodSink.Stdout           => Left(new sinks.StdouterrSink)
    }

    val executor = initExecutor(config, goodSink, badSink, tracker)

    SnowplowTracking.initializeSnowplowTracking(tracker)

    executor.run()

    // If the stream cannot be found, the KCL's "cw-metrics-publisher" thread will prevent the
    // application from exiting naturally so we explicitly call System.exit.
    // This does not apply to NSQ because NSQ consumer is non-blocking and fall here
    // right after consumer.start()
    config.input match {
      case Source.Stdin      => System.exit(1)
      case _: Source.Kinesis => System.exit(1)
      case _: Source.Nsq     => sys.addShutdownHook(executor.close())
    }
  }

  def initExecutor(
    config: StreamLoaderConfig,
    goodSink: Either[ISink, BulkSender[EmitterJsonInput]],
    badSink: ISink,
    tracker: Option[Tracker[Id]]
  ): Runnable with AutoCloseable = {
    val (shardDateField, shardDateFormat) = config.output.good match {
      case c: GoodSink.Elasticsearch => (c.client.shardDateField, c.client.shardDateFormat)
      case _                         => (None, None)
    }
    config.input match {
      // Read records from Kinesis
      case c: Source.Kinesis =>
        val pipeline = new KinesisPipeline(
          config.purpose,
          goodSink,
          badSink,
          shardDateField,
          shardDateFormat
        )
        new KinesisSourceExecutor[ValidatedJsonRecord, EmitterJsonInput](
          c,
          config.monitoring.metrics,
          pipeline
        )

      // Read records from NSQ
      case c: Source.Nsq =>
        new NsqSourceExecutor(
          config.purpose,
          c,
          goodSink,
          badSink,
          shardDateField,
          shardDateFormat
        )

      // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
      case Source.Stdin => new StdinExecutor(config, goodSink, badSink)
      case _            => throw new RuntimeException("Source must be set to 'stdin', 'kinesis' or 'nsq'")
    }
  }

  def initBadSink(config: StreamLoaderConfig): ISink = {
    config.output.bad match {
      case BadSink.None       => new sinks.NullSink
      case BadSink.Stderr     => new sinks.StdouterrSink
      case c: BadSink.Nsq     => new sinks.NsqSink(c)
      case c: BadSink.Kinesis => new sinks.KinesisSink(c)
    }
  }
}
