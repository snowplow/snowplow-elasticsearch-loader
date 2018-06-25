/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd.
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
package clients

import com.snowplowanalytics.snowplow.scalatracker.Tracker

trait ElasticsearchSender {
  val tracker: Option[Tracker]

  def sendToElasticsearch(records: List[EmitterInput]): List[EmitterInput]
  def close(): Unit
  def logClusterHealth(): Unit

  /**
   * Terminate the application in a way the KCL cannot stop, prevents shutdown hooks from running
   */
  protected def forceShutdown(): Unit = {
    tracker foreach {
      t =>
        // TODO: Instead of waiting a fixed time, use synchronous tracking or futures (when the tracker supports futures)
        SnowplowTracking.trackApplicationShutdown(t)
        sleep(5000)
    }

    Runtime.getRuntime.halt(1)
  }

  /**
   * Period between retrying sending events to Elasticsearch
   * @param sleepTime Length of time between tries
   */
  protected def sleep(sleepTime: Long): Unit =
    try {
      Thread.sleep(sleepTime)
    } catch {
      case e: InterruptedException => ()
    }
}
