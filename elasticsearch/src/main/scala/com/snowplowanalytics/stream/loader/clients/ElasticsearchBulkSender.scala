/**
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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

import org.elasticsearch.client.RestClient

import com.sksamuel.elastic4s.IndexAndType
import com.sksamuel.elastic4s.indexes.IndexRequest
import com.sksamuel.elastic4s.http.{ElasticClient, NoOpHttpClientConfigCallback, Response}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.bulk.BulkResponse

import org.apache.http.{Header, HttpHost}
import org.apache.http.message.BasicHeader

import cats.Id
import cats.effect.{IO, Timer}
import cats.data.Validated
import cats.syntax.validated._

import retry.implicits._
import retry.{RetryDetails, RetryPolicy}
import retry.CatsEffect._

import com.snowplowanalytics.snowplow.scalatracker.Tracker

import com.snowplowanalytics.stream.loader.Config.StreamLoaderConfig

/**
 * Main ES component responsible for inserting data into a specific index,
 * data is passed here by [[Emitter]] */
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
  val maxConnectionWaitTimeMs: Long,
  credentialsProvider: AWSCredentialsProvider,
  val tracker: Option[Tracker[Id]],
  val maxAttempts: Int = 6
) extends BulkSender[EmitterJsonInput] {
  require(maxAttempts > 0)
  require(maxConnectionWaitTimeMs > 0)

  import ElasticsearchBulkSender._

  override val log = LoggerFactory.getLogger(getClass)

  private val client = {
    val httpClientConfigCallback =
      if (awsSigning) new SignedHttpClientConfigCallback(credentialsProvider, region)
      else NoOpHttpClientConfigCallback
    val formedHost = new HttpHost(endpoint, port, if (ssl) "https" else "http")
    val headers: Array[Header] = (username, password) match {
      case (Some(_), Some(_)) =>
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
  override def close(): Unit =
    log.info("Closing BulkSender")

  override def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {
    val connectionAttemptStartTime = System.currentTimeMillis()
    implicit def onErrorHandler: (Throwable, RetryDetails) => IO[Unit] =
      BulkSender.onError(log, tracker, connectionAttemptStartTime)
    implicit def retryPolicy: RetryPolicy[IO] =
      BulkSender.delayPolicy[IO](maxAttempts, maxConnectionWaitTimeMs)

    // oldFailures - failed at the transformation step
    val (successes, oldFailures) = records.partition(_._2.isValid)
    val esObjects = successes.collect {
      case (_, Validated.Valid(jsonRecord)) => composeObject(jsonRecord)
    }
    val actions = esObjects.map(composeRequest)

    // Sublist of records that could not be inserted
    val newFailures: List[EmitterJsonInput] = if (actions.nonEmpty) {
      BulkSender
        .futureToTask(client.execute(bulk(actions)))
        .retryingOnSomeErrors(BulkSender.exPredicate)
        .map(extractResult(records))
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

    log.info(s"Emitted ${esObjects.size - newFailures.size} records to Elasticsearch")
    if (newFailures.nonEmpty) logHealth()

    val allFailures = oldFailures ++ newFailures

    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")

    allFailures
  }

  /**
   * Get sublist of records that could not be inserted
   * @param records list of original records to send
   * @param response response with successful and failed results
   */
  def extractResult(records: List[EmitterJsonInput])(
    response: Response[BulkResponse]): List[EmitterJsonInput] =
    response.result.items
      .zip(records)
      .flatMap {
        case (bulkResponseItem, record) =>
          handleResponse(bulkResponseItem.error.map(_.reason), record)
      }
      .toList

  def composeObject(jsonRecord: JsonRecord): ElasticsearchObject = {
    val index = jsonRecord.shard match {
      case Some(shardSuffix) => documentIndex + shardSuffix
      case None              => documentIndex
    }
    utils.extractEventId(jsonRecord.json) match {
      case Some(id) =>
        new ElasticsearchObject(index, documentType, id, jsonRecord.json.noSpaces)
      case None =>
        new ElasticsearchObject(index, documentType, jsonRecord.json.noSpaces)
    }
  }

  /** Logs the cluster health */
  override def logHealth(): Unit =
    client.execute(clusterHealth).onComplete {
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
    record: EmitterJsonInput): Option[EmitterJsonInput] = {
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

object ElasticsearchBulkSender {
  implicit val ioTimer: Timer[IO] =
    IO.timer(concurrent.ExecutionContext.global)

  def composeRequest(obj: ElasticsearchObject): IndexRequest =
    indexInto(IndexAndType(obj.getIndex, obj.getType)).id(obj.getId).doc(obj.getSource)

  def apply(config: StreamLoaderConfig, tracker: Option[Tracker[Id]]): ElasticsearchBulkSender = {
    new ElasticsearchBulkSender(
      config.elasticsearch.client.endpoint,
      config.elasticsearch.client.port,
      config.elasticsearch.client.ssl,
      config.elasticsearch.aws.region,
      config.elasticsearch.aws.signing,
      config.elasticsearch.client.username,
      config.elasticsearch.client.password,
      config.elasticsearch.cluster.index,
      config.elasticsearch.cluster.documentType,
      config.elasticsearch.client.maxTimeout,
      CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey),
      tracker,
      config.elasticsearch.client.maxRetries
    )
  }
}
