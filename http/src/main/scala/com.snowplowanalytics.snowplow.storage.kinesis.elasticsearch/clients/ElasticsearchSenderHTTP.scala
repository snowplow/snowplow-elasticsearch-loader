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

package com.snowplowanalytics.elasticsearch.loader
package clients

// Scala
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Success => SSuccess, Failure => SFailure}

// Amazon
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject

// elastic4s
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.bulk.{BulkResponse, BulkResponseItem}

// Scalaz
import scalaz._
import scalaz.concurrent.Task
import Scalaz._

// SLF4j
import org.slf4j.LoggerFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

class ElasticsearchSenderHTTP(
  configuration: KinesisConnectorConfiguration,
  tracker: Option[Tracker] = None,
  maxConnectionWaitTimeMs: Long = 60000L,
  maxAttempts: Int = 6
) extends ElasticsearchSender {
  require(maxAttempts > 0)
  require(maxConnectionWaitTimeMs > 0)

  private val log = LoggerFactory.getLogger(getClass)

  private val endpoint = configuration.ELASTICSEARCH_ENDPOINT
  private val port = configuration.ELASTICSEARCH_PORT

  private val client = HttpClient(ElasticsearchClientUri(endpoint, port))

  private val delays = (1 to maxAttempts)
    .map(i => (maxConnectionWaitTimeMs / maxAttempts * i).milliseconds)

  override def close(): Unit = client.close()

  override def sendToElasticsearch(records: List[EmitterInput]): List[EmitterInput] = {
    val connectionAttemptStartTime = System.currentTimeMillis()

    val (successes, oldFailures) = records.partition(_._2.isSuccess)
    val successfulRecords = successes.collect { case (_, Success(record)) => record }
    val actions = successfulRecords
      .map(r => indexInto(r.getIndex / r.getType) id r.getId doc(r.getSource))

    val newFailures: List[EmitterInput] = futureToTask(client.execute(bulk(actions)))
      // we rety with linear backoff if an exception happened
      .retry(delays, exPredicate(connectionAttemptStartTime))
      .map { bulkResponse =>
        bulkResponse.items.zip(records)
          .map { case (bulkResponseItem, record) => handleResponse(bulkResponseItem, record) }
          .flatten
      }.unsafePerformSyncAttempt match {
        case \/-(s) => s.toList
        case -\/(f) =>
          // if the request failed more than it should have we force shutdown
          forceShutdown()
          Nil
      }

    log.info(s"Emitted ${successfulRecords.size - newFailures.size} records to Elasticseacrch")
    if (newFailures.nonEmpty) logClusterHealth()

    val allFailures = oldFailures ++ newFailures

    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")

    allFailures
  }

  /** Logs the cluster health */
  private def logClusterHealth(): Unit =
    client.execute(clusterHealth) onComplete {
      case SSuccess(health) => health.status match {
        case "green"  => log.info("Cluster health is green")
        case "yellow" => log.warn("Cluster health is yellow")
        case "red"    => log.error("Cluster health is red")
      }
      case SFailure(e) => log.error("Couldn't retrieve cluster health", e)
    }

  /**
   * Handle the response given for a bulk request, by producing a failure if we failed to insert
   * a given item.
   * @param bulkResponseItem part of a given bulk response specific to an item
   * @param record associated to this item
   * @return a failure if an unforeseen error happened (e.g. not that the document already exists)
   */
  private def handleResponse(
    bulkResponseItem: BulkResponseItem,
    record: EmitterInput
  ): Option[EmitterInput] = {
    val error = bulkResponseItem.error
    error.foreach(e => log.error(s"Record [$record] failed with message ${e.reason}"))
    error.map { e =>
      if (e.reason.contains("DocumentAlreadyExistsException") || e.reason.contains("VersionConflictEngineException"))
        None
      else 
        Some(record._1 -> s"Elasticsearch rejected record with message ${e.reason}".failureNel[ElasticsearchObject])
    }.getOrElse(None)
  }

  /** Predicate about whether or not we should retry sending stuff to ES */
  private def exPredicate(connectionStartTime: Long): (Throwable => Boolean) = _ match {
    case e: Exception =>
      log.error("ElasticsearchEmitter threw an unexpected exception ", e)
      tracker foreach {
        t => SnowplowTracking.sendFailureEvent(t, delays.head.toMillis, 0L,
          connectionStartTime, e.getMessage)
      }
      true
    case _ => false
  }

  /**
   * Terminate the application in a way the KCL cannot stop, prevents shutdown hooks from running
   */
  private def forceShutdown(): Unit = {
    log.error(s"Shutting down application as unable to connect to Elasticsearch for over $maxConnectionWaitTimeMs ms")
    tracker foreach {
      t =>
        // TODO: Instead of waiting a fixed time, use synchronous tracking or futures (when the tracker supports futures)
        SnowplowTracking.trackApplicationShutdown(t)
        sleep(5000)
    }

    Runtime.getRuntime.halt(1)
  }

  /** Turn a scala.concurrent.Future into a scalaz.concurrent.Task */
  private def futureToTask[T](f: => Future[T])(implicit ec: ExecutionContext): Task[T] =
    Task.async {
      register =>
        f onComplete {
          case SSuccess(v) => register(v.right)
          case SFailure(ex) => register(ex.left)
        }
    }
}