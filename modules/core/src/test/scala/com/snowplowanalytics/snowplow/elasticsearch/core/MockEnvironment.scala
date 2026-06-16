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

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}

import fs2.Stream

import org.http4s.client.Client

import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo}
import com.snowplowanalytics.snowplow.streams.{
  EventProcessingConfig,
  EventProcessor,
  ListOfList,
  Sink,
  Sinkable,
  SourceAndAck,
  TokenedEvents
}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class WroteToElasticsearch(count: Int) extends Action
    case class SentToBad(count: Int) extends Action
    case class AddedGoodCountMetric(count: Int) extends Action
    case class AddedBadCountMetric(count: Int) extends Action
    case class SetE2ELatencyMetric(latency: FiniteDuration) extends Action
    case class SetElasticsearchLatencyMetric(latency: FiniteDuration) extends Action
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class BecameUnhealthy(service: RuntimeService) extends Action
  }

  case class Mocks(
    elasticsearchResponses: List[Either[Throwable, ElasticsearchSink.IndexResult]],
    badSinkResponse: Either[Throwable, Unit]
  )
  object Mocks {
    val default: Mocks = Mocks(
      elasticsearchResponses = List.fill(10)(Right(ElasticsearchSink.IndexResult(Vector.empty))),
      badSinkResponse        = Right(())
    )
  }

  val testAppInfo: AppInfo = new AppInfo {
    def name        = "elasticsearch-loader"
    def version     = "0.0.0"
    def dockerAlias = s"snowplow/$name:$version"
    def cloud       = "OnPrem"
  }

  def build(
    input: Stream[IO, TokenedEvents],
    mocks: Mocks             = Mocks.default,
    purpose: Config.Purpose  = Config.Purpose.Enriched,
    maxBytes: Long           = 64 * 1024 * 1024,
    maxDelay: FiniteDuration = 10.seconds
  ): IO[MockEnvironment] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      esResponses <- Ref[IO].of(mocks.elasticsearchResponses)
      env = Environment(
              appInfo             = testAppInfo,
              source              = testSourceAndAck(input, state),
              elasticsearchSink   = testElasticsearchSink(state, esResponses),
              appHealth           = testAppHealth(state),
              badSink             = testBadSink(state, mocks.badSinkResponse),
              httpClient          = testHttpClient,
              metrics             = testMetrics(state),
              purpose             = purpose,
              sharding            = None,
              batching            = Config.Batching(maxBytes = maxBytes, maxDelay = maxDelay),
              cpuParallelism      = 1,
              uploadParallelism   = 1,
              badSinkMaxSize      = Int.MaxValue,
              decompressionConfig = DecompressionConfig(maxBytesInBatch = 5242880, maxBytesSinglePayload = 10000000)
            )
    } yield MockEnvironment(state, env)

  private def testSourceAndAck(
    input: Stream[IO, TokenedEvents],
    state: Ref[IO, Vector[Action]]
  ): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(
        config: EventProcessingConfig[IO],
        processor: EventProcessor[IO]
      ): Stream[IO, Nothing] =
        input
          .through(processor)
          .chunks
          .evalMap(tokens => state.update(_ :+ Action.Checkpointed(tokens.toList)))
          .drain

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] = IO.pure(None)
    }

  private def testElasticsearchSink(
    state: Ref[IO, Vector[Action]],
    responsesRef: Ref[IO, List[Either[Throwable, ElasticsearchSink.IndexResult]]]
  ): ElasticsearchSink[IO] =
    new ElasticsearchSink[IO] {
      def write(records: Vector[Processing.IndexableRecord]): IO[ElasticsearchSink.IndexResult] =
        if (records.isEmpty)
          IO.pure(ElasticsearchSink.IndexResult(Vector.empty))
        else
          responsesRef
            .modify {
              case Nil          => (Nil, Right(ElasticsearchSink.IndexResult(Vector.empty)))
              case head :: tail => (tail, head)
            }
            .flatMap {
              case Right(result) => state.update(_ :+ Action.WroteToElasticsearch(records.size)) *> IO.pure(result)
              case Left(e)       => IO.raiseError(e)
            }
    }

  private def testAppHealth(
    state: Ref[IO, Vector[Action]]
  ): AppHealth.Interface[IO, String, RuntimeService] =
    new AppHealth.Interface[IO, String, RuntimeService] {
      def beHealthyForSetup: IO[Unit]                                   = IO.unit
      def beUnhealthyForSetup(alert: String): IO[Unit]                  = IO.unit
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] = IO.unit
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameUnhealthy(service))
    }

  private def testBadSink(
    state: Ref[IO, Vector[Action]],
    response: Either[Throwable, Unit]
  ): Sink[IO] =
    new Sink[IO] {
      def sink(batch: ListOfList[Sinkable]): IO[Unit] =
        response match {
          case Right(_) =>
            state.update(_ :+ Action.SentToBad(batch.asIterable.size))
          case Left(e) =>
            IO.raiseError(e)
        }
      def isHealthy: IO[Boolean] = IO.pure(true)
    }

  private def testMetrics(state: Ref[IO, Vector[Action]]): Metrics[IO] =
    new Metrics[IO] {
      def addGood(count: Int): IO[Unit] =
        state.update(_ :+ Action.AddedGoodCountMetric(count))
      def addBad(count: Int): IO[Unit] =
        state.update(_ :+ Action.AddedBadCountMetric(count))
      def setLatency(latency: FiniteDuration): IO[Unit] = IO.unit
      def setE2ELatency(latency: FiniteDuration): IO[Unit] =
        state.update(_ :+ Action.SetE2ELatencyMetric(latency))
      def setElasticsearchLatency(latency: FiniteDuration): IO[Unit] =
        state.update(_ :+ Action.SetElasticsearchLatencyMetric(latency))
      def scrape: IO[String]          = IO.pure("")
      def report: Stream[IO, Nothing] = Stream.never[IO]
    }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

}
