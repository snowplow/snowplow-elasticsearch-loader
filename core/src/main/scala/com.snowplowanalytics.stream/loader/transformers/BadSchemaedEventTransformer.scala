/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.stream.loader.transformers

import java.nio.charset.StandardCharsets.UTF_8

import cats.syntax.either._
import cats.syntax.show._

import com.amazonaws.services.kinesis.model.Record

import org.json4s.jackson.parseJson

import io.circe.{DecodingFailure, Json}
import io.circe.parser.parse
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.stream.loader.{
  BadRowEncoders,
  EmitterJsonInput,
  JsonRecord,
  ValidatedJsonRecord
}

class BadSchemaedEventTransformer extends IJsonTransformer {

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of a bad row string
   * @return JsonRecord containing JSON string for the event and no event_id
   */
  override def toClass(record: Record): ValidatedJsonRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    val badRow = parse(recordString).flatMap(_.as[SelfDescribingData[Json]]) match {
      case Right(SelfDescribingData(schema, data)) =>
        SelfDescribingData(schema, BadRowEncoders.transform(data)).asJson.asRight
      case Left(error) => error.asLeft
    }
    val textBadRow = badRow.map(_.noSpaces).map(parseJson(_)).leftMap(_.show).toValidatedNel

    (recordString, textBadRow.map(row => JsonRecord(row, None)))
  }

  /**
   * Consume data from stdin rather than Kinesis
   *
   * @param line Line from stdin
   * @return Line as an EmitterJsonInput
   */
  def consumeLine(line: String): EmitterJsonInput = {
    val badRow = parse(line).map(_.noSpaces).map(parseJson(_)).leftMap(_.show).toValidatedNel
    fromClass((line, badRow.map(row => JsonRecord(row, None))))
  }

}
