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
package sinks

/**
 * Shared interface for all sinks
 */
trait ISink {
  def store(outputs: List[String], good: Boolean): Unit
}

/**
 * Sink which ignores all input
 */
class NullSink extends ISink {
  def store(outputs: List[String], good: Boolean): Unit = ()
}
