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
package com.snowplowanalytics.snowplow.elasticsearch.core

import io.circe.{Json, JsonObject}
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.optics.JsonPath.root
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

/**
 * Transforms Snowplow bad row JSON for Elasticsearch ingestion.
 *
 * Elasticsearch cannot index a field that has different types across documents (e.g. 'failure'
 * being a String in one doc and an Object in another). This transformer renames fields by appending
 * a type suffix (_str / _list) and serialises certain nested values to strings to avoid index
 * mapping conflicts.
 */
object BadRowTransformer {

  private val fixes: List[Json => Json] = List(
    root.obj.modify(renameField("failure")),
    root.obj.modify(renameField("payload")),
    root.payload.raw.obj.modify(serializeField("parameters")),
    root.failure.obj.modify(renameField("error")),
    root.failure.obj.modify(renameField("errors")),
    root.failure.obj.modify(renameField("message")),
    root.failure.messages.each.obj.modify(renameField("error")),
    root.failure.messages.each.obj.modify(serializeField("expectedMapping")),
    root.failure.messages.each.obj.modify(serializeField("json")),
    root.failure.messages.each.message.obj.modify(renameField("error")),
    root.failure_list.each.obj.modify(renameField("error")),
    root.failure_list.each.obj.modify(serializeField("value"))
  )

  def handleIgluJson(row: String): Either[String, Json] =
    parse(row) match {
      case Right(json) =>
        json.as[SelfDescribingData[Json]] match {
          case Right(SelfDescribingData(schema, data)) if schema.vendor == "com.snowplowanalytics.snowplow.badrows" =>
            Right(SelfDescribingData(schema, transform(data)).asJson)
          case _ =>
            Right(json)
        }
      case Left(failure) =>
        Left(s"BadRowTransformer cannot parse row as JSON: ${failure.message}")
    }

  private def transform(badRow: Json): Json =
    fixes.foldLeft(badRow)((data, fix) => fix(data))

  private def renameField(field: String)(obj: JsonObject): JsonObject =
    obj(field) match {
      case Some(value) if value.isString => obj.remove(field).add(field + "_str", value)
      case Some(value) if value.isArray  => obj.remove(field).add(field + "_list", value)
      case _                             => obj
    }

  private def serializeField(field: String)(obj: JsonObject): JsonObject =
    obj(field) match {
      case Some(value) if !value.isString => obj.add(field, Json.fromString(value.noSpaces))
      case _                              => obj
    }
}
