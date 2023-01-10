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
package com.snowplowanalytics.stream.loader

import io.circe.Json

/**
 * Format in which Snowplow events are buffered
 *
 * @param json The JSON string for the event
 * @param shard optional shard to send the data
 */
case class JsonRecord(json: Json, shard: Option[String]) {
  override def toString: String = s"JsonRecord($json, $shard)"
}
