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

import java.util.UUID

import cats.Id
import cats.data.NonEmptyList
import cats.effect.Clock
import cats.syntax.option._

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
  def initializeTracker(config: Monitoring.SnowplowMonitoring): Option[Tracker[Id]] =
    config.collector match {
      case Collector((host, port)) =>
        val emitter =
          AsyncEmitter.createAndStart(EndpointParams(host, Some(port), port == 443))
        new Tracker[Id](
          NonEmptyList.of(emitter),
          com.snowplowanalytics.stream.loader.generated.Settings.name,
          config.appId
        ).some
      case _ => None
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
    message: String
  ): Unit = {

    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "storage_write_failed",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.obj(
          "storage"            -> storageType.asJson,
          "failureCount"       -> failureCount.asJson,
          "initialFailureTime" -> initialFailureTime.asJson,
          "lastRetryPeriod"    -> lastRetryPeriod.asJson,
          "message"            -> message.asJson
        )
      )
    )
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
          SchemaVer.Full(1, 0, 0)
        ),
        generated.Settings.name.asJson
      )
    )
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
          SchemaVer.Full(1, 0, 0)
        ),
        Json.obj()
      )
    )
  }

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat[F[_]](
    tracker: Tracker[F],
    heartbeatInterval: Long
  ): Unit = {
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_heartbeat",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.obj("interval" -> heartbeatInterval.asJson)
      )
    )
  }

  /**
   * Config helper functions
   */
  private object Collector {
    def isInt(s: String): Boolean = try { s.toInt; true }
    catch { case _: NumberFormatException => false }

    def unapply(hostPort: String): Option[(String, Int)] =
      hostPort.split(":").toList match {
        case host :: port :: Nil if isInt(port) => Some((host, port.toInt))
        case host :: Nil                        => Some((host, 80))
        case _                                  => None
      }
  }
}
