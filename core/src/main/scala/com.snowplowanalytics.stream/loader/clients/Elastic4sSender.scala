/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplowanalytics.stream.loader
package clients

// Scala
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// Scalaz
import scalaz._
import Scalaz._
import scalaz.concurrent.{Strategy, Task}

// AWS SDK
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject

// SLF4j
import org.slf4j.Logger

trait Elastic4sSender extends ElasticsearchSender {
  val log: Logger

  val maxConnectionWaitTimeMs: Long
  val maxAttempts: Int
  val delays = 0.milliseconds +:
    Seq.fill(maxAttempts - 1)((maxConnectionWaitTimeMs / maxAttempts).milliseconds)

  // With previous versions of ES there were hard limits regarding the size of the payload (32768
  // bytes) and since we don't really need the whole payload in those cases we cut it at 20k so that
  // it can be sent to a bad sink. This way we don't have to compute the size of the byte
  // representation of the utf-8 string.
  val maxSizeWhenReportingFailure = 20000

  /**
   * Handle the response given for a bulk request, by producing a failure if we failed to insert
   * a given item.
   * @param error possible error
   * @param record associated to this item
   * @return a failure if an unforeseen error happened (e.g. not that the document already exists)
   */
  def handleResponse(
    error: Option[String],
    record: EmitterInput
  ): Option[EmitterInput] = {
    error.foreach(e => log.error(s"Record [$record] failed with message $e"))
    error.map { e =>
      if (e.contains("DocumentAlreadyExistsException") || e.contains("VersionConflictEngineException"))
        None
      else
        Some(record._1.take(maxSizeWhenReportingFailure) ->
          s"Elasticsearch rejected record with message $e".failureNel[ElasticsearchObject])
    }.getOrElse(None)
  }

  /** Predicate about whether or not we should retry sending stuff to ES */
  def exPredicate(connectionStartTime: Long): (Throwable => Boolean) = _ match {
    case e: Exception =>
      log.error("ElasticsearchSender threw an unexpected exception ", e)
      tracker foreach {
        t => SnowplowTracking.sendFailureEvent(t, delays.head.toMillis, 0L,
          connectionStartTime, e.getMessage)
      }
      true
    case _ => false
  }

  /** Turn a scala.concurrent.Future into a scalaz.concurrent.Task */
  def futureToTask[T](f: => Future[T])(implicit ec: ExecutionContext, s: Strategy): Task[T] =
    Task.async {
      register =>
        f onComplete {
          case Success(v) => s(register(v.right))
          case Failure(ex) => s(register(ex.left))
        }
    }
}
