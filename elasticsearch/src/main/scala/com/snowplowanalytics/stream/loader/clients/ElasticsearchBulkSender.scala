/**
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import org.slf4j.LoggerFactory

// Scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationLong

import org.elasticsearch.client.RestClient

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.{ElasticClient, ElasticRequest, Handler, Response}
import com.sksamuel.elastic4s.ElasticDsl.{BulkHandler => _, _}
import com.sksamuel.elastic4s.requests.indexes.IndexRequest
import com.sksamuel.elastic4s.http.{JavaClient, NoOpHttpClientConfigCallback}
import com.sksamuel.elastic4s.requests.bulk.{BulkRequest, BulkResponse}
import com.sksamuel.elastic4s.handlers.bulk.BulkHandlers

import org.apache.http.{Header, HttpHost}
import org.apache.http.message.BasicHeader

import cats.Id
import cats.effect.{IO, Timer}
import cats.data.Validated
import cats.implicits._

import retry.implicits._
import retry.{RetryDetails, RetryPolicy}
import retry._

import com.snowplowanalytics.snowplow.scalatracker.Tracker

import com.snowplowanalytics.stream.loader.Config.Region
import com.snowplowanalytics.stream.loader.Config.Sink.GoodSink
import com.snowplowanalytics.stream.loader.Config.Sink.GoodSink.Elasticsearch.ESChunk

/**
 * Main ES component responsible for inserting data into a specific index,
 * data is passed here by [[Emitter]]
 */
class ElasticsearchBulkSender(
  endpoint: String,
  port: Int,
  ssl: Boolean,
  awsSigning: Boolean,
  awsSigningRegion: Region,
  username: Option[String],
  password: Option[String],
  documentIndex: String,
  documentType: Option[String],
  indexTimeoutMs: Long,
  maxConnectionWaitTimeMs: Long,
  val tracker: Option[Tracker[Id]],
  maxAttempts: Int = 6,
  chunkConf: ESChunk
) extends BulkSender[EmitterJsonInput] {
  require(maxAttempts > 0)
  require(maxConnectionWaitTimeMs > 0)

  import ElasticsearchBulkSender._

  override val log = LoggerFactory.getLogger(getClass)

  private val client = {
    val httpClientConfigCallback =
      if (awsSigning) new SignedHttpClientConfigCallback(awsSigningRegion)
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
    ElasticClient(JavaClient.fromRestClient(restClientBuilder.build()))
  }

  /**
   * This BulkHandler is added to change endpoints of bulk api request to add
   * document type to them. This change is made in order to continue to support ES 6.x
   */
  implicit object CustomBulkHandler extends Handler[BulkRequest, BulkResponse] {
    override def build(t: BulkRequest): ElasticRequest = {
      val req = BulkHandlers.BulkHandler.build(t)
      documentType match {
        case None    => req
        case Some(t) => req.copy(endpoint = s"/$documentIndex/$t${req.endpoint}")
      }
    }
  }

  // do not close the es client, otherwise it will fail when resharding
  override def close(): Unit =
    log.info("Closing BulkSender")

  override def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {
    val connectionAttemptStartTime = System.currentTimeMillis()
    val onErrorHandler: (Throwable, RetryDetails) => IO[Unit] =
      BulkSender.onError(log, tracker, connectionAttemptStartTime)
    def onFailureHandler[A](res: Response[A], rd: RetryDetails): IO[Unit] =
      onErrorHandler(res.error.asException, rd)
    val retryPolicy: RetryPolicy[IO] =
      BulkSender.delayPolicy[IO](maxAttempts, maxConnectionWaitTimeMs)

    // oldFailures - failed at the transformation step
    val (successes, oldFailures) = records.partition(_._2.isValid)
    val esObjects = successes.collect { case (_, Validated.Valid(jsonRecord)) =>
      composeObject(jsonRecord)
    }
    val actions = esObjects.map(composeRequest)

    // Sublist of records that could not be inserted
    val newFailures: List[EmitterJsonInput] = if (actions.nonEmpty) {
      BulkSender
        .futureToTask(client.execute(bulk(actions)))
        .timeout(
          // see https://github.com/snowplow/snowplow-elasticsearch-loader/issues/242 for why this is needed
          indexTimeoutMs.millis
        )
        .retryingOnFailuresAndAllErrors(
          r => r.isSuccess,
          retryPolicy,
          onFailureHandler,
          onErrorHandler
        )
        .map(extractResult(records))
        .flatTap { failures =>
          IO.delay(log.info(s"Emitted ${esObjects.size - failures.size} records to Elasticsearch"))
        }
        .attempt
        .unsafeRunSync() match {
        case Right(s) => s
        case Left(f) =>
          log.error(
            s"Shutting down application as unable to connect to Elasticsearch for over $maxConnectionWaitTimeMs ms",
            f
          )
          // if the request failed more than it should have we force shutdown
          forceShutdown()
          Nil
      }
    } else Nil

    val allFailures = oldFailures ++ newFailures

    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")

    allFailures
  }

  override def chunkConfig(): ESChunk = chunkConf

  /**
   * Get sublist of records that could not be inserted
   * @param records list of original records to send
   * @param response response with successful and failed results
   */
  def extractResult(
    records: List[EmitterJsonInput]
  )(response: Response[BulkResponse]): List[EmitterJsonInput] =
    response.fold(records) { result =>
      result.items
        .zip(records)
        .flatMap { case (bulkResponseItem, record) =>
          handleResponse(bulkResponseItem.error.map(_.reason), record)
        }
        .toList
    }

  def composeObject(jsonRecord: JsonRecord): ElasticsearchObject = {
    val index = jsonRecord.shard match {
      case Some(shardSuffix) => documentIndex + shardSuffix
      case None              => documentIndex
    }
    val eventId = utils.extractEventId(jsonRecord.json)
    ElasticsearchObject(index, eventId, jsonRecord.json.noSpaces)
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
    error.foreach(e => log.error(s"Failed to index record in Elasticsearch: {}", e))
    error
      .flatMap { e =>
        if (
          e.contains("DocumentAlreadyExistsException") || e.contains(
            "VersionConflictEngineException"
          )
        )
          None
        else
          Some(
            record._1.take(maxSizeWhenReportingFailure) ->
              s"Elasticsearch rejected record with message $e".invalidNel
          )
      }
  }
}

object ElasticsearchBulkSender {
  implicit val ioTimer: Timer[IO] =
    IO.timer(concurrent.ExecutionContext.global)

  def composeRequest(obj: ElasticsearchObject): IndexRequest =
    indexInto(Index(obj.index)).id(obj.id.orNull).doc(obj.doc)

  def apply(
    config: GoodSink.Elasticsearch,
    tracker: Option[Tracker[Id]]
  ): ElasticsearchBulkSender = {
    new ElasticsearchBulkSender(
      config.client.endpoint,
      config.client.port,
      config.client.ssl,
      config.aws.signing,
      config.aws.region,
      config.client.username,
      config.client.password,
      config.cluster.index,
      config.cluster.documentType,
      config.client.indexTimeout,
      config.client.maxTimeout,
      tracker,
      config.client.maxRetries,
      config.chunk
    )
  }

  case class ElasticsearchObject(index: String, id: Option[String], doc: String)
}
