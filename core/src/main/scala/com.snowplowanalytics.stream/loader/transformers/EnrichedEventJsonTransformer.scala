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
package transformers

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.model.Record

// Java
import java.nio.charset.StandardCharsets.UTF_8
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, DateTimeZone}

// Scala
import org.json4s.JsonAST.JString

// cats
import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.validated._
import cats.syntax.option._

// Snowplow
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer._

/**
 * Class to convert successfully enriched events to EmitterInputs
 *
 */
class EnrichedEventJsonTransformer(shardDateField: Option[String], shardDateFormat: Option[String])
    extends ITransformer[ValidatedJsonRecord, EmitterJsonInput]
    with StdinTransformer {

  private val dateFormatter = shardDateFormat match {
    case Some(format) => new SimpleDateFormat(format).some
    case _            => None
  }

  private val shardingField = shardDateField.getOrElse("derived_tstamp")

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedJsonRecord for the event
   */
  override def toClass(record: Record): ValidatedJsonRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    (recordString, toJsonRecord(recordString))
  }

  /**
   * Parses a string as a JsonRecord.
   * The -1 is necessary to prevent trailing empty strings from being discarded
   *
   * @param record the record to be parsed
   * @return the parsed JsonRecord or a list of failures
   */
  private def toJsonRecord(record: String): ValidatedNel[String, JsonRecord] =
    jsonifyGoodEvent(record.split("\t", -1)) match {
      case Left(h :: t) => NonEmptyList.of(h, t: _*).invalid
      case Left(Nil)    => "Empty list of failures but reported failure, should not happen".invalidNel
      case Right((_, json)) =>
        dateFormatter match {
          case Some(formatter) =>
            val shard = json \ shardingField match {
              case JString(timestampString) =>
                formatter
                  .format(
                    DateTime
                      .parse(timestampString)
                      .withZone(DateTimeZone.UTC)
                      .getMillis
                  )
                  .some
              case _ => None
            }
            JsonRecord(json, shard).validNel
          case None =>
            JsonRecord(json, None).validNel
        }
    }

  /**
   * Consume data from stdin rather than Kinesis
   *
   * @param line Line from stdin
   * @return Line as an EmitterJsonInput
   */
  def consumeLine(line: String): EmitterJsonInput =
    fromClass(line -> toJsonRecord(line))
}
