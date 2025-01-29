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
package com.snowplowanalytics.stream

import java.time.Instant

import cats.data.ValidatedNel
import cats.data.NonEmptyList

import com.snowplowanalytics.snowplow.badrows._

package object loader {

  /**
   * The original tab separated enriched event together with
   * a validated ElasticsearchObject created from it (or list of errors
   * if the creation process failed)
   */
  type ValidatedJsonRecord = (String, ValidatedNel[String, JsonRecord])

  /**
   * The input type for the ElasticsearchSender objects
   */
  type EmitterJsonInput = (String, ValidatedNel[String, JsonRecord])

  val processor = Processor(generated.Settings.name, generated.Settings.version)

  /** Create a generic bad row. */
  def createBadRow(line: String, errors: NonEmptyList[String]): BadRow.GenericError = {
    val payload   = Payload.RawPayload(line)
    val timestamp = Instant.now()
    val failure   = Failure.GenericFailure(timestamp, errors)
    BadRow.GenericError(processor, failure, payload)
  }
}
