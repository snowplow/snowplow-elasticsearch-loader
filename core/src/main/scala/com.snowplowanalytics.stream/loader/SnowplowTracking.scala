/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd.
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
package com.snowplowanalytics
package stream.loader

// json4s
import org.json4s._
import org.json4s.JsonDSL._

// Snowplow
import iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import snowplow.scalatracker.Tracker
import snowplow.scalatracker.emitters.AsyncEmitter

// This project
import model._

// Execution Context
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Functionality for sending Snowplow events for monitoring purposes
 */
object SnowplowTracking {

  private val HeartbeatInterval = 300000L

  /**
   * Configure a Tracker based on the configuration HOCON
   *
   * @param config The "monitoring" section of the HOCON
   * @return a new tracker instance
   */
  def initializeTracker(config: SnowplowMonitoringConfig): Tracker = {
    val endpoint = config.collectorUri
    val port     = config.collectorPort
    val appName  = config.appId
    // Not yet used
    val method = config.method
    val emitter =
      AsyncEmitter.createAndStart(endpoint, Some(port), config.ssl.getOrElse(false), None)
    new Tracker(List(emitter), com.snowplowanalytics.stream.loader.generated.Settings.name, appName)
  }

  /**
   * If a tracker has been configured, send a sink_write_failed event
   *
   * @param tracker a Tracker instance
   * @param lastRetryPeriod the backoff period between attempts
   * @param failureCount the number of consecutive failed writes
   * @param initialFailureTime Time of the first consecutive failed write
   * @param message What went wrong
   */
  def sendFailureEvent(
    tracker: Tracker,
    lastRetryPeriod: Long,
    failureCount: Long,
    initialFailureTime: Long,
    storageType: String,
    message: String): Unit = {

    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "storage_write_failed",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)),
        ("storage"              -> storageType) ~
          ("failureCount"       -> failureCount) ~
          ("initialFailureTime" -> initialFailureTime) ~
          ("lastRetryPeriod"    -> lastRetryPeriod) ~
          ("message"            -> message)
      ))
  }

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker a Tracker instance
   */
  def initializeSnowplowTracking(tracker: Tracker): Unit = {
    trackApplicationInitialization(tracker)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit =
        trackApplicationShutdown(tracker)
    })

    val heartbeatThread = new Thread {
      override def run(): Unit = {
        while (true) {
          trackApplicationHeartbeat(tracker, HeartbeatInterval)
          Thread.sleep(HeartbeatInterval)
        }
      }
    }

    heartbeatThread.start()
  }

  /**
   * Send an application_initialized unstructured event
   *
   * @param tracker a Tracker instance
   */
  private def trackApplicationInitialization(tracker: Tracker): Unit = {
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_initialized",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)),
        generated.Settings.name
      ))
  }

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker a Tracker instance
   */
  def trackApplicationShutdown(tracker: Tracker): Unit = {
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_shutdown",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)),
        JObject(Nil)
      ))
  }

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat(tracker: Tracker, heartbeatInterval: Long): Unit = {
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_heartbeat",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)),
        "interval" -> heartbeatInterval
      ))
  }
}
