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
 * Stdout/err sink
 */
class StdouterrSink extends ISink {

  /**
   * Writes a string to stdout or stderr
   *
   * @param outputs The strings to write
   * @param good Whether to write to stdout or stderr
   */
  def store(outputs: List[String], good: Boolean): Unit =
    if (good)
      outputs.foreach(println(_)) // To stdout
    else
      outputs.foreach(Console.err.println(_)) // To stderr
}
