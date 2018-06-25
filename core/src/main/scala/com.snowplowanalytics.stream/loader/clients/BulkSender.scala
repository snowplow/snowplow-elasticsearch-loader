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

// Java
import org.slf4j.Logger

// Scala
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// Scalaz
import scalaz._
import Scalaz._
import scalaz.concurrent.{Strategy, Task}

// Snowplow
import com.snowplowanalytics.snowplow.scalatracker.Tracker

trait BulkSender[A] {
  val tracker: Option[Tracker]
  val log: Logger

  val maxConnectionWaitTimeMs: Long
  val maxAttempts: Int
  val delays: Seq[FiniteDuration] = 0.milliseconds +:
    Seq.fill(maxAttempts - 1)((maxConnectionWaitTimeMs / maxAttempts).milliseconds)

  // With previous versions of ES there were hard limits regarding the size of the payload (32768
  // bytes) and since we don't really need the whole payload in those cases we cut it at 20k so that
  // it can be sent to a bad sink. This way we don't have to compute the size of the byte
  // representation of the utf-8 string.
  val maxSizeWhenReportingFailure = 20000
  implicit val strategy           = Strategy.DefaultExecutorService

  def send(records: List[A]): List[A]
  def close(): Unit
  def logHealth(): Unit

  /**
   * Terminate the application in a way the KCL cannot stop, prevents shutdown hooks from running
   */
  protected def forceShutdown(): Unit = {
    tracker foreach { t =>
      // TODO: Instead of waiting a fixed time, use synchronous tracking or futures (when the tracker supports futures)
      SnowplowTracking.trackApplicationShutdown(t)
      sleep(5000)
    }

    Runtime.getRuntime.halt(1)
  }

  /**
   * Period between retrying sending events to Storage
   * @param sleepTime Length of time between tries
   */
  protected def sleep(sleepTime: Long): Unit =
    try {
      Thread.sleep(sleepTime)
    } catch {
      case e: InterruptedException => ()
    }

  /** Predicate about whether or not we should retry sending stuff to ES */
  def exPredicate(connectionStartTime: Long, storageType: String): (Throwable => Boolean) =
    _ match {
      case e: Exception =>
        log.error("Storage threw an unexpected exception ", e)
        tracker foreach { t =>
          SnowplowTracking.sendFailureEvent(
            t,
            delays.head.toMillis,
            0L,
            connectionStartTime,
            storageType,
            e.getMessage)
        }
        true
      case _ => false
    }

  /** Turn a scala.concurrent.Future into a scalaz.concurrent.Task */
  def futureToTask[T](f: => Future[T])(implicit ec: ExecutionContext, s: Strategy): Task[T] =
    Task.async { register =>
      f onComplete {
        case Success(v)  => s(register(v.right))
        case Failure(ex) => s(register(ex.left))
      }
    }

}
