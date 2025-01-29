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
package com.snowplowanalytics.stream.loader

// Scala
import scala.util.{Failure, Success, Try}

import io.circe.Json

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
  def extractEventId(json: Json): Option[String] =
    extractField(json, "event_id")

  /**
   * Extract any field
   * @param json object produced by the Snowplow Analytics SDK
   * @return Option boxing event_id
   */
  def extractField(json: Json, filedName: String): Option[String] =
    for {
      obj    <- json.asObject
      value  <- obj.apply(filedName)
      string <- value.asString
    } yield string
}
