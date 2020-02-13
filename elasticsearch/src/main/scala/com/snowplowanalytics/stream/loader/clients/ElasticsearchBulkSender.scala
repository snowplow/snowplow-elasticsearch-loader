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

// AWS
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.auth.AWSCredentialsProvider

// Java
import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import org.slf4j.LoggerFactory

// Scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure => SFailure, Success => SSuccess}

import com.sksamuel.elastic4s.http.{ElasticClient, NoOpHttpClientConfigCallback}
import com.sksamuel.elastic4s.http.ElasticDsl._
import org.apache.http.{Header, HttpHost}
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.RestClient
import org.json4s.jackson.JsonMethods._

// cats
import cats.effect.{IO, Timer}
import cats.data.Validated
import cats.syntax.functor._
import cats.syntax.validated._

import retry.implicits._
import retry.RetryDetails
import retry.CatsEffect._

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

class ElasticsearchBulkSender(
  endpoint: String,
  port: Int,
  ssl: Boolean,
  region: String,
  awsSigning: Boolean,
  username: Option[String],
  password: Option[String],
  documentIndex: String,
  documentType: String,
  override val maxConnectionWaitTimeMs: Long = 60000L,
  credentialsProvider: AWSCredentialsProvider,
  override val tracker: Option[Tracker] = None,
  override val maxAttempts: Int = 6
) extends BulkSender[EmitterJsonInput] {
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
        val userpass =
          BaseEncoding.base64().encode(s"${username.get}:${password.get}".getBytes(Charsets.UTF_8))
        Array(new BasicHeader("Authorization", s"Basic $userpass"))
      case _ => Array.empty[Header]
    }
    val restClientBuilder = RestClient
      .builder(formedHost)
      .setHttpClientConfigCallback(httpClientConfigCallback)
      .setDefaultHeaders(headers)
    ElasticClient.fromRestClient(restClientBuilder.build())
  }

  // do not close the es client, otherwise it will fail when resharding
  override def close(): Unit = ()

  override def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {

    val connectionAttemptStartTime = System.currentTimeMillis()
    val (successes, oldFailures)   = records.partition(_._2.isValid)
    val successfulRecords = successes.collect {
      case (_, Validated.Valid(r)) =>
        val index = r.shard match {
          case Some(shardSuffix) => documentIndex + shardSuffix
          case None              => documentIndex
        }
        utils.extractEventId(r.json) match {
          case Some(id) =>
            new ElasticsearchObject(index, documentType, id, compact(render(r.json)))
          case None =>
            new ElasticsearchObject(index, documentType, compact(render(r.json)))
        }
    }
    val actions =
      successfulRecords.map(r => indexInto(r.getIndex / r.getType) id r.getId doc r.getSource)

    implicit def onError(error: Throwable, details: RetryDetails): IO[Unit] = {
      val duration = (error, details) match {
        case (error, RetryDetails.GivingUp(_, totalDelay)) =>
          IO(log.error("Storage threw an unexpected exception. Giving up ", error)).as(totalDelay)
        case (error, RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay)) =>
          IO(log.error(
            s"Storage threw an unexpected exception, after $retriesSoFar retries. Next attempt in $nextDelay ",
            error)).as(cumulativeDelay)
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
              error.getMessage)
          }
        }
      }
    }

    implicit val ioTimer: Timer[IO] = cats.effect.IO.timer(concurrent.ExecutionContext.global)

    val newFailures: List[EmitterJsonInput] = if (actions.nonEmpty) {
      futureToTask(client.execute(bulk(actions)))
        .retryingOnSomeErrors(exPredicate)
        .map { bulkResponseResponse =>
          bulkResponseResponse.result.items
            .zip(records)
            .flatMap {
              case (bulkResponseItem, record) =>
                handleResponse(bulkResponseItem.error.map(_.reason), record)
            }
            .toList
        }
        .attempt
        .unsafeRunSync() match {
        case Right(s) => s
        case Left(f) =>
          log.error(
            s"Shutting down application as unable to connect to Elasticsearch for over $maxConnectionWaitTimeMs ms",
            f)
          // if the request failed more than it should have we force shutdown
          forceShutdown()
          Nil
      }
    } else Nil

    log.info(s"Emitted ${successfulRecords.size - newFailures.size} records to Elasticseacrch")
    if (newFailures.nonEmpty) logHealth()

    val allFailures = oldFailures ++ newFailures

    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")

    allFailures
  }

  /** Logs the cluster health */
  override def logHealth(): Unit =
    client.execute(clusterHealth) onComplete {
      case SSuccess(health) =>
        health match {
          case response =>
            response.result.status match {
              case "green"  => log.info("Cluster health is green")
              case "yellow" => log.warn("Cluster health is yellow")
              case "red"    => log.error("Cluster health is red")
            }
        }
      case SFailure(e) => log.error("Couldn't retrieve cluster health", e)
    }

  /**
   * Handle the response given for a bulk request, by producing a failure if we failed to insert
   * a given item.
   * @param error possible error
   * @param record associated to this item
   * @return a failure if an unforeseen error happened (e.g. not that the document already exists)
   */
  private def handleResponse(
    error: Option[String],
    record: EmitterJsonInput
  ): Option[EmitterJsonInput] = {
    error.foreach(e => log.error(s"Record [$record] failed with message $e"))
    error
      .flatMap { e =>
        if (e.contains("DocumentAlreadyExistsException") || e.contains(
            "VersionConflictEngineException"))
          None
        else
          Some(
            record._1.take(maxSizeWhenReportingFailure) ->
              s"Elasticsearch rejected record with message $e".invalidNel)
      }
  }
}
