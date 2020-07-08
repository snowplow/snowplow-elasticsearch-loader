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
package transformers

// Amazon
import com.amazonaws.services.kinesis.model.Record

// Java
import java.nio.charset.StandardCharsets.UTF_8

import io.circe.{Json, JsonObject}
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.optics.JsonPath.root

import cats.data.ValidatedNel
import cats.syntax.validated._

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

/**
 * Class to convert bad events to ElasticsearchObjects
 */
class BadEventTransformer extends IJsonTransformer {
  import BadEventTransformer._

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of a bad row string
   * @return JsonRecord containing JSON string for the event and no event_id
   */
  override def toClass(record: Record): ValidatedJsonRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    val badRowJson   = handleIgluJson(recordString)
    (recordString, badRowJson.map(json => JsonRecord(json, None)))
  }

  /**
   * Consume data from stdin rather than Kinesis
   *
   * @param line Line from stdin
   * @return Line as an EmitterJsonInput
   */
  def consumeLine(line: String): EmitterJsonInput =
    fromClass(line -> handleIgluJson(line).map(json => JsonRecord(json, None)))

}

object BadEventTransformer {

  /** All transformations to avoid union types within `iglu:com.snowplowanalytics.snowplow.badrows` */
  val fixes: List[Json => Json] = List(
    root.obj.modify(renameField("failure")),
    root.obj.modify(renameField("payload")),
    root.payload.raw.obj.modify(serializeField("parameters")),
    root.failure.obj.modify(renameField("error")),
    root.failure.obj.modify(renameField("message")),
    root.failure.messages.each.obj.modify(renameField("error")),
    root.failure.messages.each.obj.modify(serializeField("expectedMapping")),
    root.failure.messages.each.obj.modify(serializeField("json")),
    root.failure.messages.each.message.obj.modify(renameField("error")),
    root.failure_list.each.obj.modify(renameField("error")),
    root.failure_list.each.obj.modify(serializeField("value"))
  )

  /** Attempt to handle self-describing JSON and fix bad rows union types */
  def handleIgluJson(row: String): ValidatedNel[String, Json] =
    parse(row) match {
      case Right(json) =>
        json.as[SelfDescribingData[Json]] match {
          case Right(SelfDescribingData(schema, data)) if schema.vendor.contains("badrows") =>
            SelfDescribingData(schema, transform(data)).asJson.validNel
          case Right(_) =>
            json.validNel
          case Left(_) => json.validNel
        }
      case Left(_) => s"BadEventTransformer cannot parse $row as JSON".invalidNel
    }

  /**
   * Apply every known `BadRow` fix to a resulting JSON
   * @param badRow BadRow payload *without* self-describing meta
   * @return fixed JSON without union types
   */
  def transform(badRow: Json): Json =
    fixes.foldLeft(badRow) { (data, fix) =>
      fix.apply(data)
    }

  /** Append a type suffix to a key */
  def renameField(field: String)(jsonObject: JsonObject): JsonObject =
    jsonObject(field) match {
      case Some(value) if value.isString =>
        jsonObject.remove(field).add(field ++ "_str", value)
      case Some(value) if value.isArray =>
        jsonObject.remove(field).add(field ++ "_list", value)
      case _ => jsonObject
    }

  /** Stringify JSON value to avoid potential index explosion */
  def serializeField(field: String)(jsonObject: JsonObject): JsonObject =
    jsonObject(field) match {
      case Some(value) if !value.isString =>
        jsonObject.add(field, Json.fromString(value.noSpaces))
      case _ => jsonObject
    }
}
