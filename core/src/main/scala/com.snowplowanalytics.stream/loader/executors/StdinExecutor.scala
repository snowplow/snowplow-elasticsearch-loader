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
package com.snowplowanalytics.stream.loader.executors

import cats.syntax.validated._
import com.snowplowanalytics.stream.loader.EmitterJsonInput
import com.snowplowanalytics.stream.loader.Config.{Purpose, StreamLoaderConfig}
import com.snowplowanalytics.stream.loader.Config.Sink.GoodSink
import com.snowplowanalytics.stream.loader.clients.BulkSender
import com.snowplowanalytics.stream.loader.sinks.ISink
import com.snowplowanalytics.stream.loader.transformers.{
  BadEventTransformer,
  EnrichedEventJsonTransformer,
  JsonTransformer
}
import com.snowplowanalytics.stream.loader.createBadRow

class StdinExecutor(
  config: StreamLoaderConfig,
  goodSink: Either[ISink, BulkSender[EmitterJsonInput]],
  badSink: ISink
) extends Runnable
    with AutoCloseable {

  val transformer = config.purpose match {
    case Purpose.Enriched =>
      config.output.good match {
        case c: GoodSink.Elasticsearch =>
          new EnrichedEventJsonTransformer(
            c.client.shardDateField,
            c.client.shardDateFormat
          )
        case GoodSink.Stdout => new EnrichedEventJsonTransformer(None, None)
      }
    case Purpose.Json => new JsonTransformer
    case Purpose.Bad  => new BadEventTransformer
  }

  def run = for (ln <- scala.io.Source.stdin.getLines) {
    val (line, result) = transformer.consumeLine(ln)
    result.bimap(
      f => badSink.store(List(createBadRow(line, f).compact), false),
      s =>
        goodSink match {
          case Left(gs)      => gs.store(List(s.json.toString), true)
          case Right(sender) => sender.send(List(ln -> s.valid))
        }
    )
  }

  override def close(): Unit = ()
}
