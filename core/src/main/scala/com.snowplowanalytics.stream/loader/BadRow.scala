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

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// JSON Schema
import com.github.fge.jsonschema.core.report.{LogLevel, ProcessingMessage}

case class BadRow(line: String, errors: NonEmptyList[String]) {
  import BadRow._

  private val tstamp = System.currentTimeMillis()
   // An ISO valid timestamp formatter
  private val tstampFormat = DateTimeFormat
    .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    .withZone(DateTimeZone.UTC)

  private val errs = errors.map(toProcessingMessage)

  def toCompactJson: String = compact(
    ("line"           -> line) ~
    ("errors"         -> errs.toList.map(e => fromJsonNode(e.asJson))) ~
    ("failure_tstamp" -> getTstamp(tstamp, tstampFormat))
  )
}

object BadRow {
  private def toProcessingMessage(s: String): ProcessingMessage =
    new ProcessingMessage()
      .setLogLevel(LogLevel.ERROR)
      .setMessage(s)

  private def getTstamp(tstamp: Long, format: DateTimeFormatter): String = {
    val dt = new DateTime(tstamp)
    format.print(dt)
  }
}
