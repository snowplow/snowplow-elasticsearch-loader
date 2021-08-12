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
package com.snowplowanalytics.stream.loader

import java.util.UUID

import cats.Id
import cats.data.NonEmptyList
import cats.effect.Clock

import scala.concurrent.duration.{MILLISECONDS, NANOSECONDS, TimeUnit}

// Snowplow
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.scalatracker.{Tracker, UUIDProvider}
import com.snowplowanalytics.snowplow.scalatracker.emitters.id.AsyncEmitter
import com.snowplowanalytics.snowplow.scalatracker.Emitter.EndpointParams

import io.circe.Json
import io.circe.syntax._

// This project
import Config._

// Execution Context
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Functionality for sending Snowplow events for monitoring purposes
 */
object SnowplowTracking {

  private val HeartbeatInterval = 300000L

  implicit val uuidProviderId: UUIDProvider[Id] = new UUIDProvider[Id] {
    override def generateUUID: Id[UUID] = UUID.randomUUID()
  }

  implicit val clockId: Clock[Id] = new Clock[Id] {
    override def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), MILLISECONDS)

    override def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), NANOSECONDS)
  }

  /**
   * Configure a Tracker based on the configuration HOCON
   *
   * @param config The "monitoring" section of the HOCON
   * @return a new tracker instance
   */
  def initializeTracker(config: SnowplowMonitoringConfig): Tracker[Id] = {
    val endpoint = config.collectorUri
    val port     = config.collectorPort
    val appName  = config.appId
    val emitter =
      AsyncEmitter.createAndStart(EndpointParams(endpoint, Some(port), config.ssl.getOrElse(false)))
    new Tracker[Id](
      NonEmptyList.of(emitter),
      com.snowplowanalytics.stream.loader.generated.Settings.name,
      appName)
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
    tracker: Tracker[Id],
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
        Json.obj(
          "storage"            -> storageType.asJson,
          "failureCount"       -> failureCount.asJson,
          "initialFailureTime" -> initialFailureTime.asJson,
          "lastRetryPeriod"    -> lastRetryPeriod.asJson,
          "message"            -> message.asJson
        )
      ))
  }

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker a Tracker instance
   */
  def initializeSnowplowTracking(tracker: Option[Tracker[Id]]): Unit = {
    tracker match {
      case Some(t) =>
        trackApplicationInitialization[Id](t)

        Runtime.getRuntime.addShutdownHook(new Thread() {
          override def run(): Unit =
            trackApplicationShutdown(t)
        })

        val heartbeatThread = new Thread {
          override def run(): Unit = {
            while (true) {
              trackApplicationHeartbeat(t, HeartbeatInterval)
              Thread.sleep(HeartbeatInterval)
            }
          }
        }

        heartbeatThread.start()
      case None => ()
    }
  }

  /**
   * Send an application_initialized unstructured event
   *
   * @param tracker a Tracker instance
   */
  private def trackApplicationInitialization[F[_]](tracker: Tracker[F]): Unit = {
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_initialized",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)),
        generated.Settings.name.asJson
      ))
  }

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker a Tracker instance
   */
  def trackApplicationShutdown[F[_]](tracker: Tracker[F]): Unit = {
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_shutdown",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)),
        Json.obj()
      ))
  }

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat[F[_]](
    tracker: Tracker[F],
    heartbeatInterval: Long): Unit = {
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_heartbeat",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)),
        Json.obj("interval" -> heartbeatInterval.asJson)
      ))
  }
}
