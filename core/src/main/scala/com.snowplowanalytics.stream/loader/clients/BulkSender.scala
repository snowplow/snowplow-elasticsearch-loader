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
package clients

// Java
import java.util.concurrent.TimeUnit

import org.slf4j.Logger

// Scala
import scala.concurrent.Future
import scala.concurrent.duration._

// cats
import cats.effect.{ContextShift, IO}
import scala.concurrent.ExecutionContext
import cats.{Applicative, Id}

import retry.{RetryDetails, RetryPolicies, RetryPolicy}

// Snowplow
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import Config.Sink.GoodSink.Elasticsearch.ESChunk

trait BulkSender[A] {
  import BulkSender._

  implicit val contextShift: ContextShift[IO] = globalContextShift

  // With previous versions of ES there were hard limits regarding the size of the payload (32768
  // bytes) and since we don't really need the whole payload in those cases we cut it at 20k so that
  // it can be sent to a bad sink. This way we don't have to compute the size of the byte
  // representation of the utf-8 string.
  val maxSizeWhenReportingFailure = 20000

  val tracker: Option[Tracker[Id]]
  val log: Logger

  def send(records: List[A]): List[A]
  def close(): Unit
  def chunkConfig(): ESChunk

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

  val globalContextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def delayPolicy[M[_]: Applicative](
    maxAttempts: Int,
    maxConnectionWaitTimeMs: Long
  ): RetryPolicy[M] = {
    val basePolicy =
      RetryPolicies
        .fullJitter(20.milliseconds)
        .join(RetryPolicies.limitRetries(maxAttempts))
    RetryPolicies.limitRetriesByCumulativeDelay(
      FiniteDuration(maxConnectionWaitTimeMs, TimeUnit.MILLISECONDS),
      basePolicy
    )
  }

  def onError(log: Logger, tracker: Option[Tracker[Id]], connectionAttemptStartTime: Long)(
    error: Throwable,
    details: RetryDetails
  ): IO[Unit] = {
    val duration = details match {
      case RetryDetails.GivingUp(_, totalDelay) =>
        IO(log.error("Storage threw an unexpected exception. Giving up ", error)).as(totalDelay)
      case RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay) =>
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

  def futureToTask[T](f: => Future[T])(implicit cs: ContextShift[IO]): IO[T] =
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
