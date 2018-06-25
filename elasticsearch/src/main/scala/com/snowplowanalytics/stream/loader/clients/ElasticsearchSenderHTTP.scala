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
import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.sksamuel.elastic4s.http.{RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import org.apache.http.{Header, HttpHost}
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.RestClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure => SFailure, Success => SSuccess}

// elastic4s
import com.sksamuel.elastic4s.http.{HttpClient, NoOpHttpClientConfigCallback}
import com.sksamuel.elastic4s.http.ElasticDsl._

// Scalaz
import scalaz._
import Scalaz._
import scalaz.concurrent.Strategy

// AMZ
import com.amazonaws.auth.AWSCredentialsProvider

// SLF4j
import org.slf4j.LoggerFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

class ElasticsearchSenderHTTP(
                               endpoint: String,
                               port: Int,
                               username: Option[String],
                               password: Option[String],
                               credentialsProvider: AWSCredentialsProvider,
                               region: String,
                               ssl: Boolean = false,
                               awsSigning: Boolean = false,
                               override val tracker: Option[Tracker] = None,
                               override val maxConnectionWaitTimeMs: Long = 60000L,
                               override val maxAttempts: Int = 6
                             ) extends Elastic4sSender {
  require(maxAttempts > 0)
  require(maxConnectionWaitTimeMs > 0)

  override val log = LoggerFactory.getLogger(getClass)

  private val client = {
    val httpClientConfigCallback =
      if (awsSigning) new SignedHttpClientConfigCallback(credentialsProvider, region)
      else NoOpHttpClientConfigCallback
    val formedHost = new HttpHost(endpoint, port, if (ssl) "https" else "http")
    val headers: Array[Header] = (username, password) match {
      case (Some(u), Some(p)) =>
        val userpass = BaseEncoding.base64().encode(s"${username.get}:${password.get}".getBytes(Charsets.UTF_8))
        Array(new BasicHeader("Authorization", s"Basic $userpass"))
      case _ => Array.empty[Header]
    }
    val restClientBuilder = RestClient.builder(formedHost)
      .setHttpClientConfigCallback(httpClientConfigCallback)
      .setDefaultHeaders(headers)
    HttpClient.fromRestClient(restClientBuilder.build())
  }

  implicit val strategy = Strategy.DefaultExecutorService

  // do not close the es client, otherwise it will fail when resharding
  override def close(): Unit = ()

  override def sendToElasticsearch(records: List[EmitterInput]): List[EmitterInput] = {
    val connectionAttemptStartTime = System.currentTimeMillis()

    val (successes, oldFailures) = records.partition(_._2.isSuccess)
    val successfulRecords = successes.collect { case (_, Success(record)) => record }
    val actions = successfulRecords
      .map(r => indexInto(r.getIndex / r.getType) id r.getId doc(r.getSource))

    val newFailures: List[EmitterInput] = if (actions.nonEmpty) {
      futureToTask(client.execute(bulk(actions)))
        // we retry with linear back-off if an exception happened
        .retry(delays, exPredicate(connectionAttemptStartTime))
        .map {
          case Right(bulkResponseResponse) => bulkResponseResponse.result.items.zip(records)
            .map { case (bulkResponseItem, record) =>
              handleResponse(bulkResponseItem.error.map(_.reason), record)
            }.flatten.toList
          case Left(requestFailure) => List((requestFailure.error.reason, requestFailure.error.reason.failureNel)) // the bulk request failed
        }.attempt.unsafePerformSync match {
          case \/-(s) => s
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
  def logClusterHealth(): Unit =
    client.execute(clusterHealth) onComplete {
      case SSuccess(health) => health match {
        case Left(requestFailure) => log.error(s"Failure in cluster health request: ${requestFailure.error.reason}")
        case Right(response) =>
          response.result.status match {
            case "green"  => log.info("Cluster health is green")
            case "yellow" => log.warn("Cluster health is yellow")
            case "red"    => log.error("Cluster health is red")
          }
      }
      case SFailure(e) => log.error("Couldn't retrieve cluster health", e)
    }
}
