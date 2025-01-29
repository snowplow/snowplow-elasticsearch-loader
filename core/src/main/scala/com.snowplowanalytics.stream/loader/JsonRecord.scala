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
