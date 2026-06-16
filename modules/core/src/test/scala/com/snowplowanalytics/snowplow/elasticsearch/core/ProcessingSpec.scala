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
import java.time.Instant

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Unique
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import cats.syntax.traverse._

import fs2.{Chunk, Stream}

import MockEnvironment._
import ProcessingSpec._

import org.specs2.Specification

import com.github.luben.zstd.ZstdOutputStream

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.streams.TokenedEvents
import com.snowplowanalytics.snowplow.streams.compression.{Compressor, GzipCompressor, ZstdCompressor}

class ProcessingSpec extends Specification with CatsEffect {

  def is = s2"""
  The elasticsearch loader should
    write enriched events to Elasticsearch and checkpoint                   $writesEnrichedEventsAndCheckpoints
    emit bad rows for badly formatted events                                $emitsBadRowsForBadlyFormattedEvents
    handle a mix of good and bad events in the same batch                   $handlesMixOfGoodAndBadEvents
    emit separate batches per token after maxDelay timeout                  $emitsSeparateBatchesAfterMaxDelay
    batch events based on the maxBytes limit                                $batchesEventsBasedOnMaxBytes
    write JSON events to Elasticsearch under the Json purpose               $writesJsonEventsUnderJsonPurpose
    write bad row events to Elasticsearch under the Bad purpose             $writesBadRowsUnderBadPurpose
    emit bad rows for non-JSON input under the Bad purpose                  $emitsBadRowsForNonJsonInputUnderBadPurpose
    emit bad row from Elasticsearch correctly                               $emitsBadRowFromElasticsearch
    mark app as unhealthy when Elasticsearch write throws a runtime error   $marksUnhealthyWhenElasticsearchThrows
    mark app as unhealthy when the bad-row sink throws                      $marksUnhealthyWhenBadSinkThrows
    decompress and write zstd-compressed enriched events                    $writesZstdCompressedEvents
    emit bad row for corrupt zstd-compressed input                          $emitsBadRowForCorruptZstd
    decompress and write gzip-compressed enriched events                    $writesGzipCompressedEvents
    decompress and write mixed plain + zstd + gzip events                   $writesMixedCompressedEvents
  """

  def writesEnrichedEventsAndCheckpoints = {
    // collectorTstamp = Instant.EPOCH so that toEpochMilli = 0.
    // Sleep 42123ms before the stream so virtual time = 42123ms when updateMetrics runs.
    // e2eLatency = 42123ms - 0ms = 42123ms.
    val latency = 42123.millis
    val io = for {
      inputs <- inputEvents(2, goodEnriched(Some(Instant.EPOCH)))
      control <- MockEnvironment.build(Stream.emits(inputs))
      _ <- IO.sleep(latency)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(4),
        Action.AddedGoodCountMetric(4),
        Action.AddedBadCountMetric(0),
        Action.SetE2ELatencyMetric(latency),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
      )
    )
    TestControl.executeEmbed(io)
  }

  def emitsBadRowsForBadlyFormattedEvents = {
    val io = for {
      inputs <- inputEvents(3, badlyFormatted)
      control <- MockEnvironment.build(Stream.emits(inputs))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.AddedGoodCountMetric(0),
        Action.AddedBadCountMetric(6),
        Action.SentToBad(6),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
      )
    )
    TestControl.executeEmbed(io)
  }

  def handlesMixOfGoodAndBadEvents = {
    val toInputs = for {
      bads <- inputEvents(3, badlyFormatted)
      goods <- inputEvents(3, goodEnriched(Some(Instant.EPOCH)))
    } yield bads.zip(goods).map { case (bad, good) =>
      TokenedEvents(bad.events ++ good.events, good.ack)
    }
    val latency = 42123.millis

    val io = for {
      inputs <- toInputs
      control <- MockEnvironment.build(Stream.emits(inputs))
      _ <- IO.sleep(latency)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(6),
        Action.AddedGoodCountMetric(6),
        Action.AddedBadCountMetric(6),
        Action.SetE2ELatencyMetric(latency),
        Action.SentToBad(6),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
      )
    )
    TestControl.executeEmbed(io)
  }

  def emitsSeparateBatchesAfterMaxDelay = {
    val io = for {
      inputs <- inputEvents(2, goodEnriched(Some(Instant.EPOCH)))
      input = Stream.emits(inputs).covary[IO].spaced(3.seconds)
      control <- MockEnvironment.build(input, maxDelay = 2.seconds)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(0),
        Action.SetE2ELatencyMetric(2.seconds),
        Action.Checkpointed(List(inputs(0).ack)),
        Action.WroteToElasticsearch(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(0),
        Action.SetE2ELatencyMetric(5.seconds),
        Action.Checkpointed(List(inputs(1).ack))
      )
    )
    TestControl.executeEmbed(io)
  }
  def writesJsonEventsUnderJsonPurpose =
    for {
      inputs <- inputEvents(1, validJson)
      control <- MockEnvironment.build(
                   Stream.emits(inputs),
                   purpose = Config.Purpose.Json
                 )
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(0),
        Action.Checkpointed(List(inputs(0).ack))
      )
    )

  def writesBadRowsUnderBadPurpose =
    for {
      inputs <- inputEvents(1, validIgluBadRow)
      control <- MockEnvironment.build(
                   Stream.emits(inputs),
                   purpose = Config.Purpose.Bad
                 )
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(0),
        Action.Checkpointed(List(inputs(0).ack))
      )
    )

  def emitsBadRowsForNonJsonInputUnderBadPurpose =
    for {
      inputs <- inputEvents(1, badlyFormatted)
      control <- MockEnvironment.build(
                   Stream.emits(inputs),
                   purpose = Config.Purpose.Bad
                 )
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.AddedGoodCountMetric(0),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.Checkpointed(List(inputs(0).ack))
      )
    )

  def emitsBadRowFromElasticsearch = {
    // 1 TokenedEvents with 2 enriched events; ES returns 1 FailedRecord.
    // goodCount = 2 items - 1 failure = 1.
    val mocks = Mocks.default.copy(
      elasticsearchResponses = List(
        Right(
          ElasticsearchSink.IndexResult(
            Vector(ElasticsearchSink.FailedRecord("null", "error"))
          )
        )
      )
    )
    val latency = 42123.millis

    val io = for {
      inputs <- inputEvents(1, goodEnriched(Some(Instant.EPOCH)))
      control <- MockEnvironment.build(Stream.emits(inputs), mocks)
      _ <- IO.sleep(latency)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(2),
        Action.AddedGoodCountMetric(1),
        Action.AddedBadCountMetric(1),
        Action.SetE2ELatencyMetric(latency),
        Action.SentToBad(1),
        Action.Checkpointed(List(inputs(0).ack))
      )
    )
    TestControl.executeEmbed(io)
  }

  def marksUnhealthyWhenElasticsearchThrows = {
    val mocks = Mocks.default.copy(
      elasticsearchResponses = List(
        Left(new RuntimeException("ES is unavailable"))
      )
    )

    val io = for {
      inputs <- inputEvents(1, goodEnriched())
      control <- MockEnvironment.build(Stream.emits(inputs), mocks)
      _ <- Processing.stream(control.environment).compile.drain.voidError
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.BecameUnhealthy(RuntimeService.ElasticsearchSink)
      )
    )
    TestControl.executeEmbed(io)
  }

  def marksUnhealthyWhenBadSinkThrows = {
    val mocks = Mocks.default.copy(
      badSinkResponse = Left(new RuntimeException("bad sink unavailable"))
    )

    val io = for {
      inputs <- inputEvents(1, badlyFormatted)
      control <- MockEnvironment.build(Stream.emits(inputs), mocks)
      _ <- Processing.stream(control.environment).compile.drain.voidError
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.AddedGoodCountMetric(0),
        Action.AddedBadCountMetric(2),
        Action.BecameUnhealthy(RuntimeService.BadSink)
      )
    )
    TestControl.executeEmbed(io)
  }

  def writesZstdCompressedEvents = {
    val latency = 42123.millis
    val io = for {
      inputs <- inputEvents(2, goodZstdCompressed(Some(Instant.EPOCH)))
      control <- MockEnvironment.build(Stream.emits(inputs))
      _ <- IO.sleep(latency)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(4),
        Action.AddedGoodCountMetric(4),
        Action.AddedBadCountMetric(0),
        Action.SetE2ELatencyMetric(latency),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
      )
    )
    TestControl.executeEmbed(io)
  }

  def emitsBadRowForCorruptZstd =
    for {
      inputs <- inputEvents(1, corruptZstdCompressed)
      control <- MockEnvironment.build(Stream.emits(inputs))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.AddedGoodCountMetric(0),
        Action.AddedBadCountMetric(1),
        Action.SentToBad(1),
        Action.Checkpointed(List(inputs(0).ack))
      )
    )

  def writesGzipCompressedEvents = {
    val latency = 42123.millis
    val io = for {
      inputs <- inputEvents(2, goodGzipCompressed(Some(Instant.EPOCH)))
      control <- MockEnvironment.build(Stream.emits(inputs))
      _ <- IO.sleep(latency)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(4),
        Action.AddedGoodCountMetric(4),
        Action.AddedBadCountMetric(0),
        Action.SetE2ELatencyMetric(latency),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
      )
    )
    TestControl.executeEmbed(io)
  }

  def writesMixedCompressedEvents = {
    val latency = 42123.millis
    val io = for {
      inputs <- inputEvents(2, goodMixed(Some(Instant.EPOCH)))
      control <- MockEnvironment.build(Stream.emits(inputs))
      _ <- IO.sleep(latency)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(6),
        Action.AddedGoodCountMetric(6),
        Action.AddedBadCountMetric(0),
        Action.SetE2ELatencyMetric(latency),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
      )
    )
    TestControl.executeEmbed(io)
  }

  def batchesEventsBasedOnMaxBytes = {
    // Compute the byte weight of a single TokenedEvents so maxBytes can be calibrated.
    val io = for {
      inputs <- inputEvents(3, goodEnriched(Some(Instant.EPOCH)))
      weights  = inputs.map(_.events.foldLeft(0L)((acc, bb) => acc + bb.remaining()))
      maxBytes = weights(0) + weights(1) + 1
      control <- MockEnvironment.build(Stream.emits(inputs), maxBytes = maxBytes)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteToElasticsearch(4),
        Action.AddedGoodCountMetric(4),
        Action.AddedBadCountMetric(0),
        Action.SetE2ELatencyMetric(0.millis),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack)),
        Action.WroteToElasticsearch(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(0),
        Action.SetE2ELatencyMetric(0.millis),
        Action.Checkpointed(List(inputs(2).ack))
      )
    )
    TestControl.executeEmbed(io)
  }
}

object ProcessingSpec {

  private val zstdFactory = ZstdCompressor.factory(3)
  private val gzipFactory = GzipCompressor.factory(6)

  private def compress(factory: Compressor.Factory, tsvBytes: List[Array[Byte]]): ByteBuffer = {
    val compressor = factory.buildAndInitialize(1000000, 1)
    tsvBytes.foreach { bytes =>
      val _ = compressor.addRecord(bytes, 0, bytes.length)
    }
    compressor.result
  }

  private def mkEvents(n: Int, optCollectorTstamp: Option[Instant]): IO[(Unique.Token, List[Array[Byte]])] =
    for {
      ack <- IO.unique
      ids <- List.fill(n)(IO.randomUUID).sequence
      now <- IO.realTimeInstant
      collectorTstamp = optCollectorTstamp.getOrElse(now)
    } yield {
      val bytes = ids.map(id => Event.minimal(id, collectorTstamp, "0.0.0", "0.0.0").toTsv.getBytes(StandardCharsets.UTF_8))
      (ack, bytes)
    }

  def inputEvents(count: Int, source: IO[TokenedEvents]): IO[List[TokenedEvents]] =
    Stream.eval(source).repeat.take(count.toLong).compile.toList

  def goodEnriched(optCollectorTstamp: Option[Instant] = None): IO[TokenedEvents] =
    mkEvents(2, optCollectorTstamp).map { case (ack, bytes) =>
      TokenedEvents(Chunk.from(bytes.map(ByteBuffer.wrap)), ack)
    }

  def badlyFormatted: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token)
    }

  def validJson: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("""{"key":"value1"}""", """{"key":"value2"}""").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token)
    }

  def validIgluBadRow: IO[TokenedEvents] =
    IO.unique.map { token =>
      val row =
        """{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0","data":{"failure":"something went wrong","payload":{}}}"""
      val serialized = Chunk(row, row).map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token)
    }

  def goodZstdCompressed(optCollectorTstamp: Option[Instant] = None): IO[TokenedEvents] =
    mkEvents(2, optCollectorTstamp).map { case (ack, bytes) =>
      TokenedEvents(Chunk(compress(zstdFactory, bytes)), ack)
    }

  def goodGzipCompressed(optCollectorTstamp: Option[Instant] = None): IO[TokenedEvents] =
    mkEvents(2, optCollectorTstamp).map { case (ack, bytes) =>
      TokenedEvents(Chunk(compress(gzipFactory, bytes)), ack)
    }

  def goodMixed(optCollectorTstamp: Option[Instant] = None): IO[TokenedEvents] =
    mkEvents(3, optCollectorTstamp).map { case (ack, bytes) =>
      val plain          = ByteBuffer.wrap(bytes(0))
      val zstdCompressed = compress(zstdFactory, List(bytes(1)))
      val gzipCompressed = compress(gzipFactory, List(bytes(2)))
      TokenedEvents(Chunk(plain, zstdCompressed, gzipCompressed), ack)
    }

  def corruptZstdCompressed: IO[TokenedEvents] =
    IO.unique.map { token =>
      val baos = new java.io.ByteArrayOutputStream()
      val zstd = new ZstdOutputStream(baos)
      zstd.write(1)
      zstd.write(1)
      val sizeBytes = ByteBuffer.allocate(4)
      sizeBytes.order(java.nio.ByteOrder.BIG_ENDIAN)
      sizeBytes.putInt(10)
      zstd.write(sizeBytes.array())
      zstd.write(Array[Byte](1, 2, 3))
      zstd.close()
      TokenedEvents(Chunk(ByteBuffer.wrap(baos.toByteArray)), token)
    }
}
