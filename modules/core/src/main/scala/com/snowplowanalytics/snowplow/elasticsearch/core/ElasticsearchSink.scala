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
package com.snowplowanalytics.snowplow.elasticsearch.core

import cats.effect.{Async, Ref, Resource}
import cats.syntax.all._
import com.sksamuel.elastic4s.{ElasticClient, ElasticRequest, Handler, Response}
import com.sksamuel.elastic4s.http4s.{Http4sClient => Elastic4sHttp4sClient}
import com.sksamuel.elastic4s.ElasticDsl.{BulkHandler => _, _}
import com.sksamuel.elastic4s.requests.bulk.{BulkRequest, BulkResponse, BulkResponseItem}
import com.sksamuel.elastic4s.handlers.bulk.BulkHandlers
import fs2.io.file.Files
import org.http4s.client.{Client => Http4sClient}
import org.http4s.BasicCredentials
import org.http4s.headers.Authorization
import org.http4s.blaze.client.BlazeClientBuilder
import scala.concurrent.duration._
import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.elasticsearch.core.Processing.IndexableRecord

trait ElasticsearchSink[F[_]] {
  def write(records: Vector[IndexableRecord]): F[ElasticsearchSink.IndexResult]
}

object ElasticsearchSink {

  private val BadRowErrorTypes: Set[String] = Set(
    "mapper_parsing_exception", // ES 6.x / 7.x / OpenSearch: field value incompatible with index mapping
    "document_parsing_exception", // ES 8.x: replacement for mapper_parsing_exception with type error cases and illegal_argument_exception with limit error cases
    "illegal_argument_exception" // ES 6.x / 7.x / OpenSearch: index mapping limit errors
  )

  case class FailedRecord(payload: String, errorMessage: String)

  case class IndexResult(failedRecords: Vector[FailedRecord])

  def build[F[_]: Async](
    config: Config.ElasticsearchSink,
    metrics: Metrics[F],
    appHealth: AppHealth.Interface[F, String, RuntimeService],
    retries: Config.Retries,
    uploadParallelism: Int
  ): Resource[F, ElasticsearchSink[F]] = {
    implicit val files: Files[F] = Files.forAsync[F]

    // This BulkHandler is added to change endpoints of bulk api request to add document type to
    // them. This change is made in order to continue to support ES 6.x
    implicit object CustomBulkHandler extends Handler[BulkRequest, BulkResponse] {
      override def build(t: BulkRequest): ElasticRequest = {
        val req = BulkHandlers.BulkHandler.build(t)
        config.documentType match {
          case None    => req
          case Some(t) => req.copy(endpoint = s"/${config.index}/$t${req.endpoint}")
        }
      }
    }
    for {
      http4sClient <- buildHttp4sClient[F](config, uploadParallelism)
      elasticClient <- Resource.make(
                         Async[F].delay(
                           ElasticClient(new Elastic4sHttp4sClient(http4sClient, config.url))
                         )
                       )(_.close())
    } yield new ElasticsearchSink[F] {
      def write(records: Vector[IndexableRecord]): F[IndexResult] =
        if (records.isEmpty) Async[F].pure(IndexResult(Vector.empty))
        else {
          for {
            retryRecordsRef <- Ref.of[F, Vector[IndexableRecord]](records)
            failedRecordsRef <- Ref.of[F, Vector[FailedRecord]](Vector.empty)
            // Elasticsearch Loader doesn't differentiate transient and setup errors therefore
            // placeholder values are given for configForSetup, toAlert and setupErrorCheck parameters.
            _ <- Retrying.withRetries(
                   appHealth,
                   retries.transientErrors,
                   Retrying.Config.ForSetup(1.minute),
                   RuntimeService.ElasticsearchSink,
                   _ => "",
                   PartialFunction.empty
                 ) { _ =>
                   retryRecordsRef.get.flatMap { retryRecords =>
                     val requests = retryRecords.map { r =>
                       indexInto(config.index + r.shardSuffix.getOrElse(""))
                         .id(r.id.orNull)
                         .source(r.record)
                     }
                     for {
                       start <- Async[F].realTime
                       response <- elasticClient.execute(bulk(requests).timeout(config.indexTimeout))
                       _ <- handleBulkResponse(retryRecordsRef, failedRecordsRef, config.additionalBadRowErrorTypes, metrics)(response)
                       finish <- Async[F].realTime
                       duration = finish - start
                       _ <- metrics.setElasticsearchLatency(duration)
                     } yield ()
                   }
                 }
            failedRecords <- failedRecordsRef.get
          } yield IndexResult(failedRecords)
        }
    }
  }

  private def buildHttp4sClient[F[_]: Async](config: Config.ElasticsearchSink, uploadParallelism: Int): Resource[F, Http4sClient[F]] = {
    val base = BlazeClientBuilder[F]
      .withMaxTotalConnections(uploadParallelism)
      .resource
    config.auth match {
      case Config.ElasticsearchSink.Auth.Basic(username, password) =>
        base.map(addBasicAuth(_, username, password))
      case Config.ElasticsearchSink.Auth.AWSSigning(serviceSigningName, region) =>
        base.flatMap(AwsSigningMiddleware(_, serviceSigningName, region))
      case Config.ElasticsearchSink.Auth.NoAuth => base
    }
  }

  private def addBasicAuth[F[_]: Async](
    client: Http4sClient[F],
    user: String,
    pass: String
  ): Http4sClient[F] =
    Http4sClient[F] { req =>
      client.run(req.putHeaders(Authorization(BasicCredentials(user, pass))))
    }

  /**
   * Processes the result of an ES bulk API call.
   *
   * Failures are split into two categories:
   *   - Bad rows (e.g. mapper_parsing_exception): the record cannot be fixed by retrying, so it is
   *     moved to failedRecordsRef and dropped from the retry cycle.
   *   - Transient errors (everything else): the record is kept in retryRecordsRef so that the
   *     caller's retry loop can attempt it again, and an exception is raised to trigger that loop.
   *
   * On success (or when all failures were bad rows), retryRecordsRef is cleared so the caller knows
   * there is nothing left to retry.
   */
  private[core] def handleBulkResponse[F[_]: Async](
    retryRecordsRef: Ref[F, Vector[IndexableRecord]],
    failedRecordsRef: Ref[F, Vector[FailedRecord]],
    additionalBadRowErrorTypes: Set[String],
    metrics: Metrics[F]
  )(
    response: Response[BulkResponse]
  ): F[Unit] =
    if (response.result.hasFailures) {
      retryRecordsRef.get.flatMap { records =>
        // Partition failures: bad rows are sent to the dead-letter sink; transient errors are retried.
        val (failedResponses, retryResponses) = response.result.failures
          .partition(shouldBeBadRow(additionalBadRowErrorTypes))
        val retryRecords = retryResponses.map(item => records(item.itemId)).toVector
        val failedRecords = failedResponses.map(i =>
          FailedRecord(records(i.itemId).record, i.error.map(e => s"${e.`type`}: ${e.reason}").getOrElse("unknown"))
        )
        val limitErrorCount = failedRecords.count(f => checkIfLimitError(f.errorMessage))

        metrics.addIndexLimitError(limitErrorCount) *>
          failedRecordsRef.update(v => v.appendedAll(failedRecords)) *> {
            if (retryRecords.isEmpty) {
              // All failures were bad rows — nothing left to retry.
              retryRecordsRef.set(Vector.empty)
            } else {
              // At least one transient error: update the retry ref and raise so the retry loop fires.
              // Error messages are capped at 10 to keep the message readable.
              val retryErrorMessages =
                retryResponses
                  .map(
                    _.error
                      .map(e => s"${e.`type`}: ${e.reason}")
                      .getOrElse("unknown")
                  )
                  .take(10)
                  .mkString(", \n")
              retryRecordsRef.set(retryRecords) *>
                Async[F].raiseError(
                  new RuntimeException(
                    s"${retryRecords.size} bulk operations failed: $retryErrorMessages"
                  )
                )
            }
          }
      }
    } else retryRecordsRef.set(Vector.empty)

  private[core] def shouldBeBadRow(additionalBadRowErrorTypes: Set[String])(response: BulkResponseItem): Boolean =
    response.error.exists(e => BadRowErrorTypes.contains(e.`type`) || additionalBadRowErrorTypes.contains(e.`type`))

  private val LimitErrorPattern = """.*Limit.*has been exceeded.*""".r

  private def checkIfLimitError(errorMsg: String): Boolean =
    LimitErrorPattern.matches(errorMsg)
}
