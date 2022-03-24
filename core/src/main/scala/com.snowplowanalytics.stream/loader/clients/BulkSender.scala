/**
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd.
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

// Java
import java.util.concurrent.TimeUnit

import org.slf4j.Logger

// Scala
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

// cats
import cats.effect.{ContextShift, IO}
import scala.concurrent.ExecutionContext
import cats.{Applicative, Id}

import retry.{PolicyDecision, RetryDetails, RetryPolicy}

// Snowplow
import com.snowplowanalytics.snowplow.scalatracker.Tracker

trait BulkSender[A] {
  import BulkSender._

  // With previous versions of ES there were hard limits regarding the size of the payload (32768
  // bytes) and since we don't really need the whole payload in those cases we cut it at 20k so that
  // it can be sent to a bad sink. This way we don't have to compute the size of the byte
  // representation of the utf-8 string.
  val maxSizeWhenReportingFailure = 20000

  val tracker: Option[Tracker[Id]]
  val log: Logger

  val maxConnectionWaitTimeMs: Long
  val maxAttempts: Int

  def send(records: List[A]): List[A]
  def close(): Unit
  def logHealth(): Unit

  /**
   * Terminate the application in a way the KCL cannot stop, prevents shutdown hooks from running
   */
  protected def forceShutdown(): Unit = {
    log.info("BulkSender force shutdown")
    tracker.foreach { t =>
      // TODO: Instead of waiting a fixed time, use synchronous tracking or futures (when the tracker supports futures)
      SnowplowTracking.trackApplicationShutdown(t)
      sleep(5000)
    }

    Runtime.getRuntime.halt(1)
  }
}

object BulkSender {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def delayPolicy[M[_]: Applicative](
    maxAttempts: Int,
    maxConnectionWaitTimeMs: Long
  ): RetryPolicy[M] =
    RetryPolicy.lift { status =>
      if (status.retriesSoFar >= maxAttempts) PolicyDecision.GiveUp
      else {
        val maxDelay                  = 2.milliseconds * Math.pow(2, status.retriesSoFar.toDouble).toLong
        val randomDelayNanos          = (maxDelay.toNanos * Random.nextDouble()).toLong
        val maxConnectionWaitTimeNano = maxConnectionWaitTimeMs * 1000
        val delayNanos =
          if (maxConnectionWaitTimeMs >= randomDelayNanos) maxConnectionWaitTimeNano
          else randomDelayNanos
        PolicyDecision.DelayAndRetry(new FiniteDuration(delayNanos, TimeUnit.NANOSECONDS))
      }
    }

  def onError(log: Logger, tracker: Option[Tracker[Id]], connectionAttemptStartTime: Long)(
    error: Throwable,
    details: RetryDetails
  ): IO[Unit] = {
    val duration = (error, details) match {
      case (error, RetryDetails.GivingUp(_, totalDelay)) =>
        IO(log.error("Storage threw an unexpected exception. Giving up ", error)).as(totalDelay)
      case (error, RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay)) =>
        IO(
          log.error(
            s"Storage threw an unexpected exception, after $retriesSoFar retries. Next attempt in $nextDelay ",
            error
          )
        ).as(cumulativeDelay)
    }

    duration.flatMap { delay =>
      IO {
        tracker.foreach { t =>
          SnowplowTracking.sendFailureEvent(
            t,
            delay.toMillis,
            0L,
            connectionAttemptStartTime,
            "elasticsearch",
            error.getMessage
          )
        }
      }
    }
  }

  /** Predicate about whether or not we should retry sending stuff to ES */
  def exPredicate: Throwable => Boolean = {
    case _: Exception => true
    case _            => false
  }

  def futureToTask[T](f: => Future[T]): IO[T] =
    IO.fromFuture(IO.delay(f))

  /**
   * Period between retrying sending events to Storage
   * @param sleepTime Length of time between tries
   */
  def sleep(sleepTime: Long): Unit =
    try Thread.sleep(sleepTime)
    catch {
      case _: InterruptedException => ()
    }
}
