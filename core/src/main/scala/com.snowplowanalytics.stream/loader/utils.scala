/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
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

// Scala
import scala.util.{Failure, Success, Try}
import org.json4s.JValue
import org.json4s.JsonAST.JString

// cats
import cats.syntax.option._

object utils {
  // to rm once 2.12 as well as the right projections
  def fold[A, B](t: Try[A])(ft: Throwable => B, fa: A => B): B = t match {
    case Success(a) => fa(a)
    case Failure(t) => ft(t)
  }

  /**
   * Extract the event_id field from an event JSON for use as a document ID
   * @param json object produced by the Snowplow Analytics SDK
   * @return Option boxing event_id
   */
  def extractEventId(json: JValue): Option[String] =
    extractField(json, "event_id")

  /**
   * Extract any field
   * @param json object produced by the Snowplow Analytics SDK
   * @return Option boxing event_id
   */
  def extractField(json: JValue, filedName: String): Option[String] = {
    json \ filedName match {
      case JString(eid) => eid.some
      case _            => None
    }
  }
}
