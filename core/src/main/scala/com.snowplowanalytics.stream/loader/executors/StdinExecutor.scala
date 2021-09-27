/**
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
) extends Runnable {

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
      f => badSink.store(createBadRow(line, f).compact, None, false),
      s =>
        goodSink match {
          case Left(gs)      => gs.store(s.json.toString, None, true)
          case Right(sender) => sender.send(List(ln -> s.valid))
        }
    )
  }
}
