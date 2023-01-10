/**
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd.
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
