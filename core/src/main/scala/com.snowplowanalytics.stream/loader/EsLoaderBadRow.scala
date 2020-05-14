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

// cats
import cats.data.NonEmptyList

// json4s
import io.circe.Json
import io.circe.syntax._

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/** ES Loader rad row that could not be transformed by StdinTransformer */
case class EsLoaderBadRow(line: String, errors: NonEmptyList[String]) {
  import EsLoaderBadRow._

  private val tstamp = System.currentTimeMillis()
  // An ISO valid timestamp formatter
  private val tstampFormat = DateTimeFormat
    .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    .withZone(DateTimeZone.UTC)

  def toCompactJson =
    Json
      .obj(
        "line"           -> line.asJson,
        "errors"         -> errors.asJson,
        "failure_tstamp" -> getTstamp(tstamp, tstampFormat).asJson
      )
      .noSpaces
}

object EsLoaderBadRow {
  private def getTstamp(tstamp: Long, format: DateTimeFormatter): String = {
    val dt = new DateTime(tstamp)
    format.print(dt)
  }
}
