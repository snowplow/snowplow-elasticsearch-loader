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
import com.comcast.ip4s.Port
import cats.implicits._
import org.http4s.{ParseFailure, Uri}
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import java.time.format.DateTimeFormatter
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, HttpClient, Metrics => CommonMetrics, Retrying, Sentry, Telemetry}
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

case class Config[+Factory, +Source, +BadSink](
  license: AcceptedLicense,
  input: Source,
  output: Config.Output[BadSink],
  streams: Factory,
  purpose: Config.Purpose,
  batching: Config.Batching,
  retries: Config.Retries,
  cpuParallelismFactor: BigDecimal,
  uploadParallelismFactor: BigDecimal,
  decompression: DecompressionConfig,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring,
  http: Config.Http
)

object Config {

  case class Output[+BadSink](
    good: ElasticsearchSink,
    bad: SinkWithMetadata[BadSink]
  )

  case class SinkWithMetadata[+BadSink](
    sink: BadSink,
    maxRecordSize: Int
  )

  case class SinkMetadata(
    maxRecordSize: Int
  )

  case class ElasticsearchSink(
    url: Uri,
    index: String,
    documentType: Option[String],
    auth: ElasticsearchSink.Auth,
    sharding: Option[ElasticsearchSink.Sharding],
    indexTimeout: FiniteDuration,
    additionalBadRowErrorTypes: Set[String]
  )
  object ElasticsearchSink {
    case class Sharding(
      dateFormat: DateTimeFormatter,
      dateField: String
    )

    sealed trait Auth
    object Auth {
      case object NoAuth extends Auth
      case class Basic(username: String, password: String) extends Auth
      case class AWSSigning(serviceSigningName: String, region: String) extends Auth
    }
  }

  case class Batching(
    maxBytes: Long,
    maxDelay: FiniteDuration
  )

  case class Retries(
    transientErrors: Retrying.Config.ForTransient
  )

  sealed trait Purpose
  object Purpose {
    case object Enriched extends Purpose
    case object Bad extends Purpose
    case object Json extends Purpose
  }

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry.Config],
    healthProbe: HealthProbe
  )
  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig],
    prometheus: CommonMetrics.PrometheusConfig
  )

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  case class Http(client: HttpClient.Config)

  implicit def decoder[Factory: Decoder, Source: Decoder, BadSink: Decoder]: Decoder[Config[Factory, Source, BadSink]] = {
    implicit val configuration: Configuration = Configuration.default.withDiscriminator("type")

    implicit val licenseDecoder: Decoder[AcceptedLicense] = AcceptedLicense.decoder(
      AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.1/")
    )

    implicit val http4sUriDecoder: Decoder[Uri] =
      Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))

    implicit val dateFormatterDecoder: Decoder[DateTimeFormatter] =
      Decoder[String].emap(s => Either.catchNonFatal(DateTimeFormatter.ofPattern(s)).leftMap(_.toString))

    implicit val sinkWithMetadataDecoder = for {
      sink <- Decoder[BadSink]
      metadata <- deriveConfiguredDecoder[SinkMetadata]
    } yield SinkWithMetadata(sink, metadata.maxRecordSize)

    // ElasticsearchOutput decoders (innermost first)
    implicit val esShardingDecoder: Decoder[ElasticsearchSink.Sharding] = deriveConfiguredDecoder[ElasticsearchSink.Sharding]
      .emap { s =>
        val validDateFields = Set(
          "etl_tstamp",
          "collector_tstamp",
          "dvce_created_tstamp",
          "dvce_sent_tstamp",
          "refr_dvce_tstamp",
          "derived_tstamp",
          "true_tstamp"
        )
        if (validDateFields.contains(s.dateField))
          Right(s)
        else
          Left(s"'${s.dateField}' is not a valid sharding dateField. Must be one of: ${validDateFields.toSeq.sorted.mkString(", ")}")
      }
    implicit val esAuthDecoder: Decoder[ElasticsearchSink.Auth] = deriveConfiguredDecoder
    implicit val esSinkDecoder: Decoder[ElasticsearchSink]      = deriveConfiguredDecoder
    implicit val outputDecoder: Decoder[Output[BadSink]]        = deriveConfiguredDecoder

    implicit val batchingDecoder: Decoder[Batching] = deriveConfiguredDecoder

    implicit val retriesDecoder: Decoder[Retries] = deriveConfiguredDecoder

    implicit val purposeDecoder: Decoder[Purpose] = Decoder.decodeString.emap {
      case "ENRICHED_EVENTS" => Right(Purpose.Enriched)
      case "BAD_ROWS"        => Right(Purpose.Bad)
      case "JSON"            => Right(Purpose.Json)
      case other             => Left(s"Unknown purpose: $other")
    }

    implicit val metricsDecoder: Decoder[Metrics]         = deriveConfiguredDecoder
    implicit val healthProbeDecoder: Decoder[HealthProbe] = deriveConfiguredDecoder
    implicit val monitoringDecoder: Decoder[Monitoring]   = deriveConfiguredDecoder
    implicit val httpDecoder: Decoder[Http]               = deriveConfiguredDecoder[Http]

    deriveConfiguredDecoder[Config[Factory, Source, BadSink]]
  }
}
