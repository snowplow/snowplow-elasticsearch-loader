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

import scala.concurrent.duration.FiniteDuration

import cats.effect.IO
import cats.effect.Ref
import com.sksamuel.elastic4s.{RequestSuccess, Response}
import com.sksamuel.elastic4s.requests.bulk.{BulkError, BulkResponse, BulkResponseItems, IndexBulkResponseItem}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import fs2.Stream
import Processing.IndexableRecord

class ElasticsearchSinkSpec extends Specification with CatsEffect {
  import ElasticsearchSinkSpec._

  def is = s2"""
  ElasticsearchSink.handleBulkResponse should
    return unit when there are no failures                               $e1
    treat mapper_parsing_exception as a bad row                          $e2
    raise an error and update retryRef for transient errors              $e3
    split mixed failures into bad rows and retry records                 $e4
    default error type to 'unknown' when error field is absent           $e5
    include only the first 10 error types in the retry exception message $e6
    treat additionalBadRowErrorTypes as bad rows                         $e7
    increment addIndexLimitError metric for limit error messages         $e8
    treat illegal_argument_exception as a bad row                        $e9
  """

  def e1 = {
    val records = Vector(IndexableRecord("record-0", None, None, None))
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      response = mkResponse(Seq(mkItem(0, 200, None)))
      _ <- ElasticsearchSink.handleBulkResponse[IO](retryRef, failedRef, Set.empty, noopMetrics)(response)
      failed <- failedRef.get
      retry <- retryRef.get
    } yield (failed must beEmpty)
      .and(retry must beEmpty)
  }

  def e2 = {
    val records = Vector(IndexableRecord("record-0", None, None, None))
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      response = mkResponse(Seq(mkItem(0, 400, Some("mapper_parsing_exception"))))
      _ <- ElasticsearchSink.handleBulkResponse[IO](retryRef, failedRef, Set.empty, noopMetrics)(response)
      failed <- failedRef.get
      retry <- retryRef.get
    } yield (failed must haveSize(1))
      .and(failed.head.errorMessage must beEqualTo("mapper_parsing_exception: reason"))
      .and(retry must beEmpty)
  }

  def e3 = {
    val records = Vector(IndexableRecord("record-0", None, None, None))
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      response = mkResponse(Seq(mkItem(0, 500, Some("es_rejected_execution_exception"))))
      result <- ElasticsearchSink.handleBulkResponse[IO](retryRef, failedRef, Set.empty, noopMetrics)(response).attempt
      failed <- failedRef.get
      retry <- retryRef.get
    } yield (result must beLeft[Throwable].like { case e => e must beAnInstanceOf[RuntimeException] })
      .and(retry must beEqualTo(records))
      .and(failed must beEmpty)
  }

  def e4 = {
    val records = Vector(
      IndexableRecord("record-0", None, None, None),
      IndexableRecord("record-1", None, None, None),
      IndexableRecord("record-2", None, None, None)
    )
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      response = mkResponse(
                   Seq(
                     mkItem(0, 400, Some("mapper_parsing_exception")),
                     mkItem(1, 500, Some("es_rejected_execution_exception")),
                     mkItem(2, 200, None)
                   )
                 )
      result <- ElasticsearchSink.handleBulkResponse[IO](retryRef, failedRef, Set.empty, noopMetrics)(response).attempt
      failed <- failedRef.get
      retry <- retryRef.get
    } yield (result must beLeft[Throwable])
      .and(failed must haveSize(1))
      .and(failed.head.errorMessage must beEqualTo("mapper_parsing_exception: reason"))
      .and(retry must haveSize(1))
      .and(retry.head must beEqualTo(IndexableRecord("record-1", None, None, None)))
  }

  def e5 = {
    val records = Vector(IndexableRecord("record-0", None, None, None))
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      response = mkResponse(Seq(mkItem(0, 500, None)))
      result <- ElasticsearchSink.handleBulkResponse[IO](retryRef, failedRef, Set.empty, noopMetrics)(response).attempt
      retry <- retryRef.get
    } yield (result must beLeft[Throwable].like { case e =>
      e.getMessage must contain("unknown")
    }).and(retry must beEqualTo(records))
  }

  def e6 = {
    val records = Vector.tabulate(12)(i => IndexableRecord(s""""record-$i"""", None, None, None))
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      response = mkResponse(records.indices.map(i => mkItem(i, 500, Some(s"transient_error_$i"))))
      result <- ElasticsearchSink.handleBulkResponse[IO](retryRef, failedRef, Set.empty, noopMetrics)(response).attempt
    } yield result must beLeft[Throwable].like { case e =>
      // message should contain types 0-9 but not type 10 or 11
      (e.getMessage must contain("transient_error_0"))
        .and(e.getMessage must contain("transient_error_9"))
        .and(e.getMessage must not(contain("transient_error_10")))
        .and(e.getMessage must not(contain("transient_error_11")))
    }
  }

  def e7 = {
    val records = Vector(
      IndexableRecord("record-0", None, None, None),
      IndexableRecord("record-1", None, None, None)
    )
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      response = mkResponse(
                   Seq(
                     mkItem(0, 400, Some("strict_dynamic_mapping_exception")),
                     mkItem(1, 500, Some("es_rejected_execution_exception"))
                   )
                 )
      result <- ElasticsearchSink
                  .handleBulkResponse[IO](retryRef, failedRef, Set("strict_dynamic_mapping_exception"), noopMetrics)(response)
                  .attempt
      failed <- failedRef.get
      retry <- retryRef.get
    } yield (result must beLeft[Throwable])
      .and(failed must haveSize(1))
      .and(failed.head.errorMessage must beEqualTo("strict_dynamic_mapping_exception: reason"))
      .and(retry must haveSize(1))
      .and(retry.head must beEqualTo(IndexableRecord("record-1", None, None, None)))
  }

  def e8 = {
    val limitReason = "Limit of total fields [1000] has been exceeded"
    val records = Vector(
      IndexableRecord("record-0", None, None, None),
      IndexableRecord("record-1", None, None, None),
      IndexableRecord("record-2", None, None, None),
      IndexableRecord("record-3", None, None, None)
    )
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      limitErrorCount <- Ref.of[IO, Int](0)
      response = mkResponse(
                   Seq(
                     mkItem(0, 400, Some("illegal_argument_exception"), limitReason),
                     mkItem(1, 400, Some("mapper_parsing_exception"), limitReason),
                     mkItem(2, 400, Some("illegal_argument_exception")),
                     mkItem(3, 500, Some("es_rejected_execution_exception"))
                   )
                 )
      _ <- ElasticsearchSink.handleBulkResponse[IO](retryRef, failedRef, Set.empty, countingMetrics(limitErrorCount))(response).attempt
      failed <- failedRef.get
      retry <- retryRef.get
      count <- limitErrorCount.get
    } yield (failed must haveSize(3))
      .and(retry must haveSize(1))
      .and[Any](count must beEqualTo(2))
  }
  def e9 = {
    val nonLimitReason = "Request size exceeded"
    val records        = Vector(IndexableRecord("record-0", None, None, None))
    for {
      retryRef <- Ref.of[IO, Vector[IndexableRecord]](records)
      failedRef <- Ref.of[IO, Vector[ElasticsearchSink.FailedRecord]](Vector.empty)
      limitErrorCount <- Ref.of[IO, Int](0)
      response = mkResponse(Seq(mkItem(0, 400, Some("illegal_argument_exception"), nonLimitReason)))
      _ <- ElasticsearchSink.handleBulkResponse[IO](retryRef, failedRef, Set.empty, countingMetrics(limitErrorCount))(response)
      failed <- failedRef.get
      retry <- retryRef.get
      count <- limitErrorCount.get
    } yield (failed must haveSize(1))
      .and(failed.head.errorMessage must beEqualTo(s"illegal_argument_exception: $nonLimitReason"))
      .and(retry must beEmpty)
      .and[Any](count must beEqualTo(0))
  }
}

object ElasticsearchSinkSpec {

  val noopMetrics: Metrics[IO] = new Metrics[IO] {
    def addGood(count: Int): IO[Unit]                        = IO.unit
    def addBad(count: Int): IO[Unit]                         = IO.unit
    def addIndexLimitError(count: Int): IO[Unit]             = IO.unit
    def setLatency(l: FiniteDuration): IO[Unit]              = IO.unit
    def setE2ELatency(l: FiniteDuration): IO[Unit]           = IO.unit
    def setElasticsearchLatency(l: FiniteDuration): IO[Unit] = IO.unit
    def scrape: IO[String]                                   = IO.pure("")
    def report: Stream[IO, Nothing]                          = Stream.never[IO]
  }

  def countingMetrics(limitErrorCount: Ref[IO, Int]): Metrics[IO] = new Metrics[IO] {
    def addGood(count: Int): IO[Unit]                        = IO.unit
    def addBad(count: Int): IO[Unit]                         = IO.unit
    def addIndexLimitError(count: Int): IO[Unit]             = limitErrorCount.update(_ + count)
    def setLatency(l: FiniteDuration): IO[Unit]              = IO.unit
    def setE2ELatency(l: FiniteDuration): IO[Unit]           = IO.unit
    def setElasticsearchLatency(l: FiniteDuration): IO[Unit] = IO.unit
    def scrape: IO[String]                                   = IO.pure("")
    def report: Stream[IO, Nothing]                          = Stream.never[IO]
  }

  def mkItem(
    id: Int,
    status: Int,
    errorType: Option[String],
    reason: String = "reason"
  ): BulkResponseItems =
    BulkResponseItems(
      index = Some(
        IndexBulkResponseItem(
          itemId        = id,
          id            = s"id-$id",
          index         = "idx",
          `type`        = "_doc",
          version       = 1L,
          forcedRefresh = false,
          seqNo         = 0L,
          primaryTerm   = 0L,
          found         = false,
          created       = status < 300,
          result        = if (status < 300) "indexed" else "error",
          status        = status,
          error         = errorType.map(t => BulkError(t, reason, "uuid", 0, "idx", None)),
          shards        = None
        )
      ),
      delete = None,
      update = None,
      create = None
    )

  def mkResponse(items: Seq[BulkResponseItems]): Response[BulkResponse] =
    RequestSuccess(
      status  = 200,
      body    = None,
      headers = Map.empty,
      result = BulkResponse(
        took   = 1L,
        errors = items.exists(_.index.exists(_.status >= 300)),
        _items = items
      )
    )
}
