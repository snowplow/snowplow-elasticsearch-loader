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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure => SFailure, Success => SSuccess}

// elastic4s
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.bulk.{BulkResponse, BulkResponseItem}

// Scalaz
import scalaz._
import scalaz.concurrent.{Strategy, Task}
import Scalaz._

// SLF4j
import org.slf4j.LoggerFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

class ElasticsearchSenderHTTP(
  endpoint: String,
  port: Int,
  ssl: Boolean = false,
  override val tracker: Option[Tracker] = None,
  override val maxConnectionWaitTimeMs: Long = 60000L,
  override val maxAttempts: Int = 6
) extends Elastic4sSender {
  require(maxAttempts > 0)
  require(maxConnectionWaitTimeMs > 0)

  override val log = LoggerFactory.getLogger(getClass)

  private val uri =
    if (ssl) ElasticsearchClientUri(s"elasticsearch://$endpoint:$port?ssl=true")
    else ElasticsearchClientUri(endpoint, port)
  private val client = HttpClient(uri)

  implicit val strategy = Strategy.DefaultExecutorService

  override def close(): Unit = client.close()

  override def sendToElasticsearch(records: List[EmitterInput]): List[EmitterInput] = {
    val connectionAttemptStartTime = System.currentTimeMillis()

    val (successes, oldFailures) = records.partition(_._2.isSuccess)
    val successfulRecords = successes.collect { case (_, Success(record)) => record }
    val actions = successfulRecords
      .map(r => indexInto(r.getIndex / r.getType) id r.getId doc(r.getSource))

    val newFailures: List[EmitterInput] = if (actions.nonEmpty) {
      futureToTask(client.execute(bulk(actions)))
        // we rety with linear backoff if an exception happened
        .retry(delays, exPredicate(connectionAttemptStartTime))
        .map { bulkResponse =>
          bulkResponse.items.zip(records)
            .map { case (bulkResponseItem, record) =>
              handleResponse(bulkResponseItem.error.map(_.reason), record)
            }.flatten
        }.attempt.unsafePerformSync match {
          case \/-(s) => s.toList
          case -\/(f) =>
            log.error(s"Shutting down application as unable to connect to Elasticsearch for over $maxConnectionWaitTimeMs ms", f)
            // if the request failed more than it should have we force shutdown
            forceShutdown()
            Nil
        }
    } else Nil

    log.info(s"Emitted ${successfulRecords.size - newFailures.size} records to Elasticseacrch")
    if (newFailures.nonEmpty) logClusterHealth()

    val allFailures = oldFailures ++ newFailures

    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")

    allFailures
  }

  /** Logs the cluster health */
  override def logClusterHealth(): Unit =
    client.execute(clusterHealth) onComplete {
      case SSuccess(health) => health.status match {
        case "green"  => log.info("Cluster health is green")
        case "yellow" => log.warn("Cluster health is yellow")
        case "red"    => log.error("Cluster health is red")
      }
      case SFailure(e) => log.error("Couldn't retrieve cluster health", e)
    }
}