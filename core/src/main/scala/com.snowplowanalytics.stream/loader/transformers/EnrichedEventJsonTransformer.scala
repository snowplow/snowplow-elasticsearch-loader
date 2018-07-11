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

// Scalaz
import scalaz.Scalaz._
import scalaz._

// Snowplow
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer._

/**
 * Class to convert successfully enriched events to EmitterInputs
 *
 */
class EnrichedEventJsonTransformer
    extends ITransformer[ValidatedJsonRecord, EmitterJsonInput]
    with StdinTransformer {

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
  private def toJsonRecord(record: String): ValidationNel[String, JsonRecord] =
    jsonifyGoodEvent(record.split("\t", -1)) match {
      case Left(h :: t)     => NonEmptyList(h, t: _*).failure
      case Left(Nil)        => "Empty list of failures but reported failure, should not happen".failureNel
      case Right((_, json)) => JsonRecord(json).success
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
