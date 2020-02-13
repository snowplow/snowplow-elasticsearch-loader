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
package com.snowplowanalytics.stream

// cats
import cats.data.ValidatedNel

// json4s
import org.json4s._

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

  /**
   * Functions used to change a TSV pair to a JObject
   */
  type TsvToJsonConverter = (String, String) => ValidatedNel[String, JObject]
}
