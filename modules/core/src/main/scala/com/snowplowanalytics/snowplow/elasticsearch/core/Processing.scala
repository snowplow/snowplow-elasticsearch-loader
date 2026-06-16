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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{Instant, ZoneOffset}
import java.util.Base64

import scala.concurrent.duration.DurationLong

import io.circe.parser.parse

import cats.{Applicative, Foldable}
import cats.data.{NonEmptyList, Validated}
import cats.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, ParsingError}

import cats.effect.kernel.{Async, Sync, Unique}

import fs2.{Pipe, Stream}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}

import com.snowplowanalytics.snowplow.runtime.AppHealth
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, ListOfList, Sink}
import com.snowplowanalytics.snowplow.streams.compression.Decompression._

object Processing {

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] =
    env.source.decompressedStream(
      EventProcessingConfig(EventProcessingConfig.NoWindowing, env.metrics.setLatency),
      env.decompressionConfig,
      eventProcessor(env),
      env.badRowProcessor,
      decompressionBadRow(env.badRowProcessor)
    )

  case class ParseResult(
    events: List[IndexableRecord],
    parseFailures: List[BadRow],
    countBytes: Long,
    countItems: Int,
    token: Option[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  case class Batched(
    events: ListOfList[IndexableRecord],
    failures: ListOfList[BadRow],
    countBytes: Long,
    countItems: Int,
    tokens: Vector[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  case class IndexableRecord(
    record: String,
    id: Option[String],
    timestamp: Option[Instant],
    shardSuffix: Option[String] = None
  )

  private def eventProcessor[F[_]: Async](
    env: Environment[F]
  ): DecompressedEventProcessor[F] =
    _.through(parseAndTransform[F](env.cpuParallelism, env.purpose, env.sharding, env.badRowProcessor))
      .through(BatchUp.withTimeout(env.batching.maxBytes, env.batching.maxDelay))
      .through(writeToElasticsearch(env.elasticsearchSink, env.appHealth, env.uploadParallelism, env.badRowProcessor))
      .through(updateMetrics(env.metrics))
      .through(sendBadEvents(env.badSink, env.appHealth, env.badRowProcessor, env.badSinkMaxSize))
      .through(emitTokens)

  private def parseAndTransform[F[_]: Async](
    parallelism: Int,
    purpose: Config.Purpose,
    sharding: Option[Config.ElasticsearchSink.Sharding],
    processor: Processor
  ): Pipe[F, DecompressedTokenedEvents, ParseResult] = {
    val transform: (ByteBuffer, Processor) => Either[BadRow, IndexableRecord] = purpose match {
      case Config.Purpose.Json     => transformJson
      case Config.Purpose.Enriched => transformEnrichedEvent(sharding)
      case Config.Purpose.Bad      => transformBadRow
    }

    in =>
      in.parEvalMap(parallelism) { result =>
        val payloads = result.payloads
        for {
          numBytes <- Async[F].delay(payloads.foldLeft(0L)(_ + _.remaining()))
          (badRows, records) <- Foldable[List].traverseSeparateUnordered[F, ByteBuffer, BadRow, IndexableRecord](payloads) { bytes =>
                                  Sync[F].delay(transform(bytes, processor))
                                }
          earliestCollectorTstamp = records.flatMap(_.timestamp).minOption
        } yield ParseResult(
          records,
          badRows ::: result.bad,
          numBytes,
          payloads.size + result.bad.size,
          result.ack,
          earliestCollectorTstamp
        )
      }
  }

  private def transformJson(
    byteBuffer: ByteBuffer,
    processor: Processor
  ): Either[BadRow, IndexableRecord] = {
    val str = StandardCharsets.UTF_8.decode(byteBuffer.slice).toString
    parse(str)
      .map(_ => IndexableRecord(str, None, None, None))
      .leftMap(failure =>
        BadRow.LoaderParsingError(
          processor,
          createParsingError(s"Can't parse JSON: ${failure.message}"),
          Payload.RawPayload(byteBufferToBase64(byteBuffer))
        )
      )
  }

  private def transformBadRow(
    byteBuffer: ByteBuffer,
    processor: Processor
  ): Either[BadRow, IndexableRecord] =
    BadRowTransformer.handleIgluJson(
      StandardCharsets.UTF_8.decode(byteBuffer.slice).toString
    ) match {
      case Right(json) => Right(IndexableRecord(json.noSpaces, None, None, None))
      case Left(error) =>
        Left(
          BadRow.LoaderParsingError(
            processor,
            createParsingError(error),
            Payload.RawPayload(byteBufferToBase64(byteBuffer))
          )
        )
    }

  private[core] def transformEnrichedEvent(
    sharding: Option[Config.ElasticsearchSink.Sharding]
  )(
    byteBuffer: ByteBuffer,
    processor: Processor
  ): Either[BadRow, IndexableRecord] =
    Event.parseBytes(byteBuffer.slice) match {
      case Validated.Valid(event) =>
        val jsonString = event.toJson(lossy = true).noSpaces
        val timestamp  = Some(event.collector_tstamp)
        val shardSuffix = sharding.flatMap { s =>
          event.atomic
            .get(s.dateField)
            .flatMap(_.asString)
            .flatMap { rawTimestamp =>
              Either.catchNonFatal {
                s.dateFormat.format(Instant.parse(rawTimestamp).atZone(ZoneOffset.UTC))
              }.toOption
            }
        }
        Right(IndexableRecord(jsonString, event.event_id.toString.some, timestamp, shardSuffix))

      case Validated.Invalid(error) =>
        Left(
          BadRow.LoaderParsingError(
            processor,
            error,
            Payload.RawPayload(byteBufferToBase64(byteBuffer))
          )
        )
    }

  private def chooseEarliestTstamp(o1: Option[Instant], o2: Option[Instant]): Option[Instant] =
    (o1, o2)
      .mapN { case (t1, t2) =>
        if (t1.isBefore(t2)) t1 else t2
      }
      .orElse(o1)
      .orElse(o2)

  private def decompressionBadRow(processor: Processor)(err: DecompressionError): BadRow =
    BadRow.LoaderParsingError(
      processor,
      createParsingError(err.message),
      Payload.RawPayload(err.payload)
    )

  private def createParsingError(errorMsg: String): ParsingError =
    ParsingError.RowDecodingError(
      NonEmptyList.of(
        ParsingError.RowDecodingErrorInfo.UnhandledRowDecodingError(errorMsg)
      )
    )

  private def byteBufferToBase64(byteBuffer: ByteBuffer): String =
    StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(byteBuffer.slice)).toString

  private def writeToElasticsearch[F[_]: Async](
    sink: ElasticsearchSink[F],
    appHealth: AppHealth.Interface[F, String, RuntimeService],
    parallelism: Int,
    processor: Processor
  ): Pipe[F, Batched, Batched] =
    _.parEvalMap(parallelism) { batch =>
      for {
        writeResult <- sink
                         .write(batch.events.asIterable.toVector)
                         .onError { _ =>
                           appHealth.beUnhealthyForRuntimeService(RuntimeService.ElasticsearchSink)
                         }
        now <- Async[F].realTimeInstant
        sinkBadRows = writeResult.failedRecords
                        .map { r =>
                          BadRow.GenericError(
                            processor,
                            Failure.GenericFailure(
                              now,
                              NonEmptyList.one(s"Failed to write to Elasticsearch: ${r.errorMessage}")
                            ),
                            Payload.RawPayload(r.payload)
                          )
                        }
      } yield batch.copy(failures = batch.failures.prepend(sinkBadRows.toList))
    }

  private def updateMetrics[F[_]: Sync](metrics: Metrics[F]): Pipe[F, Batched, Batched] =
    _.evalTap { batch =>
      val badCount  = batch.failures.asIterable.size
      val goodCount = batch.countItems - badCount
      val updateE2ELatency = batch.earliestCollectorTstamp.fold(Sync[F].unit) { timestamp =>
        for {
          now <- Sync[F].realTime
          e2eLatency = now - timestamp.toEpochMilli.millis
          _ <- metrics.setE2ELatency(e2eLatency)
        } yield ()
      }
      metrics.addGood(goodCount) >> metrics.addBad(badCount) >> updateE2ELatency
    }

  private def sendBadEvents[F[_]: Sync](
    badSink: Sink[F],
    appHealth: AppHealth.Interface[F, String, RuntimeService],
    processor: Processor,
    maxSize: Int
  ): Pipe[F, Batched, Batched] =
    _.evalTap { batch =>
      if (batch.failures.nonEmpty) {
        val serialized =
          batch.failures.mapUnordered(badRow => BadRowsSerializer.withMaxSize(badRow, processor, maxSize))
        badSink
          .sinkSimple(serialized)
          .onError { _ =>
            appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)
          }
      } else Applicative[F].unit
    }

  private def emitTokens[F[_]]: Pipe[F, Batched, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private implicit def batchable: BatchUp.Batchable[ParseResult, Batched] =
    new BatchUp.Batchable[ParseResult, Batched] {
      def combine(pending: Batched, parseResult: ParseResult): Batched =
        Batched(
          pending.events.prepend(parseResult.events),
          pending.failures.prepend(parseResult.parseFailures),
          pending.countBytes + parseResult.countBytes,
          pending.countItems + parseResult.countItems,
          parseResult.token.fold(pending.tokens)(pending.tokens :+ _),
          chooseEarliestTstamp(pending.earliestCollectorTstamp, parseResult.earliestCollectorTstamp)
        )

      def single(a: ParseResult): Batched =
        Batched(
          ListOfList.of(List(a.events)),
          ListOfList.of(List(a.parseFailures)),
          a.countBytes,
          a.countItems,
          a.token.toVector,
          a.earliestCollectorTstamp
        )

      def weightOf(a: ParseResult): Long =
        a.countBytes
    }
}
