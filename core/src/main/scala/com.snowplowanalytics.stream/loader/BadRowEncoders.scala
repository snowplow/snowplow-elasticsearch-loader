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
package com.snowplowanalytics.stream.loader

import io.circe.{Json, JsonObject}
import io.circe.optics.JsonPath.root

object BadRowEncoders {

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
    root.failure_list.each.obj.modify(renameField("value"))
  )

  /**
   *
   * @param badRow BadRow payload *without* self-describing meta
   * @return
   */
  def transform(badRow: Json): Json =
    fixes.foldLeft(badRow) { (data, fix) =>
      fix.apply(data)
    }

  def renameField(field: String)(jsonObject: JsonObject): JsonObject =
    jsonObject(field) match {
      case Some(value) if value.isString =>
        jsonObject.add(field ++ "_str", value)
      case Some(value) if value.isArray =>
        jsonObject.add(field ++ "_list", value)
      case _ => jsonObject
    }

  def serializeField(field: String)(jsonObject: JsonObject): JsonObject =
    jsonObject(field) match {
      case Some(value) if !value.isString =>
        jsonObject.add(field, Json.fromString(value.noSpaces))
      case _ => jsonObject
    }
}
