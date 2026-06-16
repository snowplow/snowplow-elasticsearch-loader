/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.elasticsearch.it

import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

import scala.concurrent.duration._

import cats.Id
import cats.syntax.all._
import cats.effect.IO
import cats.effect.std.Queue

import fs2.Stream

import io.circe.{JsonObject, parser}

import org.http4s.{Method, Request, Uri}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.MediaType
import org.http4s.headers.`Content-Type`

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, ListOfList}
import com.snowplowanalytics.snowplow.streams.kinesis.{
  BackoffPolicy,
  KinesisFactory,
  KinesisHttpSourceConfig,
  KinesisSinkConfigM,
  KinesisSourceConfig
}

object TestHelpers {

  private val PollTimeoutSeconds = 300L

  def putEvents(
    kinesisEndpoint: String,
    streamName: String,
    payloads: List[Array[Byte]]
  ): IO[Unit] = {
    val config = KinesisSinkConfigM[Id](
      streamName             = streamName,
      throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
      recordLimit            = 500,
      byteLimit              = 5 * 1024 * 1024,
      customEndpoint         = Some(URI.create(kinesisEndpoint)),
      maxRetries             = 10
    )
    KinesisFactory.resource[IO].use { factory =>
      factory.sink(config).use { sink =>
        payloads.traverse_ { payload =>
          sink.sinkSimple(ListOfList.ofItems(payload))
        }
      }
    }
  }

  def putEvents(
    kinesisEndpoint: String,
    streamName: String,
    events: List[Event]
  )(implicit ev: DummyImplicit
  ): IO[Unit] =
    putEvents(kinesisEndpoint, streamName, events.map(_.toTsv.getBytes(StandardCharsets.UTF_8)))

  def collectBadRows(queue: Queue[IO, JsonObject]): EventProcessor[IO] =
    _.evalMap { tokenedEvents =>
      tokenedEvents.events.toList
        .traverse_ { buf =>
          parser
            .parse(StandardCharsets.UTF_8.decode(buf.slice()).toString)
            .toOption
            .flatMap(_.asObject)
            .traverse_(queue.offer)
        }
        .as(tokenedEvents.ack)
    }

  def awaitBadRows(queue: Queue[IO, JsonObject], expectedCount: Int): IO[List[JsonObject]] =
    Stream
      .fromQueueUnterminated(queue)
      .take(expectedCount.toLong)
      .compile
      .toList

  def pollForBadRows(
    kinesisEndpoint: String,
    streamName: String,
    expectedCount: Int
  ): IO[List[JsonObject]] = {
    val endpoint = URI.create(kinesisEndpoint)
    val sourceConfig = KinesisHttpSourceConfig(
      kinesis = KinesisSourceConfig(
        appName                          = UUID.randomUUID().toString,
        streamName                       = streamName,
        workerIdentifier                 = "test-worker",
        initialPosition                  = KinesisSourceConfig.InitialPosition.TrimHorizon,
        retrievalMode                    = KinesisSourceConfig.Retrieval.Polling(500, 200.millis),
        customEndpoint                   = Some(endpoint),
        dynamodbCustomEndpoint           = Some(endpoint),
        cloudwatchCustomEndpoint         = Some(endpoint),
        leaseDuration                    = 5.seconds,
        maxLeasesToStealAtOneTimeFactor  = BigDecimal(2),
        checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
        debounceCheckpoints              = 1.second,
        maxRetries                       = 10,
        apiCallAttemptTimeout            = 15.seconds
      ),
      http = None
    )

    for {
      queue <- Queue.unbounded[IO, JsonObject]
      results <- KinesisFactory.resource[IO].use { factory =>
                   val kinesisStream: Stream[IO, Nothing] = Stream
                     .resource(factory.source(sourceConfig))
                     .flatMap { source =>
                       source.stream(
                         EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit),
                         collectBadRows(queue)
                       )
                     }
                   kinesisStream.compile.drain.background.use { _ =>
                     IO.race(
                       awaitBadRows(queue, expectedCount),
                       IO.sleep(PollTimeoutSeconds.seconds)
                     ).flatMap {
                       case Left(rows) => IO.pure(rows)
                       case Right(_) =>
                         IO.raiseError(
                           new RuntimeException(
                             s"Timed out waiting for $expectedCount bad rows in stream '$streamName'"
                           )
                         )
                     }
                   }
                 }
    } yield results
  }

  // Uses a raw http4s request + circe instead of the elastic4s typed client. elastic4s 7.x
  // deserializes hits.total into a Total case class expecting the ES 7+ object format
  // {"value": N, "relation": "eq"}, but ES 6 returns it as a plain integer. The raw approach
  // navigates directly to hits.hits[*]._source and ignores the total field entirely, making
  // it compatible with all ES and OpenSearch versions.
  def pollForDocs(
    esUrl: String,
    index: String,
    expectedCount: Int
  ): IO[List[JsonObject]] =
    BlazeClientBuilder[IO].resource.use { http4sClient =>
      val uri = Uri.unsafeFromString(s"$esUrl/$index/_search?size=1000")

      def poll(deadline: Instant): IO[List[JsonObject]] =
        for {
          now <- IO.realTimeInstant
          response <- http4sClient
                        .run(Request[IO](Method.GET, uri))
                        .use { response =>
                          if (response.status.isSuccess) response.as[String]
                          else IO.pure("")
                        }
                        .attempt
          result <- response match {
                      case Left(_) =>
                        IO.sleep(2.seconds) >> poll(deadline)
                      case Right("") =>
                        IO.sleep(2.seconds) >> poll(deadline)
                      case Right(body) =>
                        val docs = (for {
                          json <- parser.parse(body).toOption
                          hitsArr <- json.hcursor.downField("hits").downField("hits").as[List[io.circe.Json]].toOption
                        } yield hitsArr.flatMap(_.hcursor.downField("_source").as[JsonObject].toOption))
                          .getOrElse(Nil)

                        if (docs.size >= expectedCount)
                          IO.pure(docs)
                        else if (now.isAfter(deadline))
                          IO.raiseError(
                            new RuntimeException(
                              s"Timed out waiting for $expectedCount docs in index '$index' (found ${docs.size})"
                            )
                          )
                        else
                          IO.sleep(2.seconds) >> poll(deadline)
                    }
        } yield result

      IO.realTimeInstant.flatMap(d => poll(d.plusSeconds(PollTimeoutSeconds)))
    }

  def createIndexWithFieldLimit(
    esUrl: String,
    index: String,
    fieldLimit: Int,
    mappingType: Option[String] = None
  ): IO[Unit] =
    BlazeClientBuilder[IO].resource.use { http4sClient =>
      val uri            = Uri.unsafeFromString(s"$esUrl/$index")
      val propertiesJson = """{"se_action":{"type":"long"}}"""
      val mappingsJson = mappingType match {
        case Some(t) => s""""$t":{"properties":$propertiesJson}"""
        case None    => s""""properties":$propertiesJson"""
      }
      val body =
        s"""{
           |  "settings":{"index.mapping.total_fields.limit":$fieldLimit},
           |  "mappings":{$mappingsJson}
           |}
           |""".stripMargin
      val request = Request[IO](Method.PUT, uri)
        .withEntity(body)
        .putHeaders(`Content-Type`(MediaType.application.json))
      http4sClient
        .run(request)
        .use { response =>
          if (response.status.isSuccess) IO.unit
          else IO.raiseError(new RuntimeException(s"Failed to create index '$index': HTTP ${response.status.code}"))
        }
    }

  def docsToEventMap(docs: List[JsonObject]): Map[String, JsonObject] =
    docs.map { doc =>
      val eventId = doc("event_id")
        .flatMap(_.asString)
        .getOrElse(throw new RuntimeException(s"Document missing event_id field: ${doc.toJson.noSpaces}"))
      (eventId, doc)
    }.toMap
}
