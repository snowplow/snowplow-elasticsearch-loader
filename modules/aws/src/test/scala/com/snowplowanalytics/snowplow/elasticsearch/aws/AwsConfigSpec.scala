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
package com.snowplowanalytics.snowplow.elasticsearch.aws

import java.nio.file.Paths

import scala.concurrent.duration.DurationInt

import org.specs2.Specification

import cats.Id

import cats.effect.{ExitCode, IO}

import cats.effect.testing.specs2.CatsEffect

import com.comcast.ip4s.Port

import org.http4s.Uri

import com.snowplowanalytics.snowplow.runtime.Metrics.{PrometheusConfig, StatsdConfig}
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, ConfigParser, HttpClient, Retrying, Sentry, Telemetry}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

import com.snowplowanalytics.snowplow.streams.kinesis.{
  BackoffPolicy,
  KinesisHttpSourceConfig,
  KinesisSinkConfig,
  KinesisSinkConfigM,
  KinesisSourceConfig
}

import com.snowplowanalytics.snowplow.elasticsearch.core.Config

class AwsConfigSpec extends Specification with CatsEffect {

  def is = s2"""
  Config parse should be able to parse
    minimal aws config  $minimal
    reference aws config $reference
    config with AWS signing auth $awsSigningDefault
    config with AWS service signing name $awsSigningServiceName
  """

  private def minimal =
    assert(
      resource       = "/config.aws.minimal.hocon",
      expectedResult = Right(AwsConfigSpec.minimalConfig)
    )

  private def reference =
    assert(
      resource       = "/config.aws.reference.hocon",
      expectedResult = Right(AwsConfigSpec.referenceConfig)
    )

  private def awsSigningDefault =
    assert(
      resource = "/config.awssigning.test.hocon",
      expectedResult = Right(
        AwsConfigSpec.minimalConfig.copy(output =
          AwsConfigSpec.minimalConfig.output.copy(good =
            AwsConfigSpec.minimalConfig.output.good.copy(auth = Config.ElasticsearchSink.Auth.AWSSigning("es", "eu-central-1"))
          )
        )
      )
    )

  private def awsSigningServiceName =
    assert(
      resource = "/config.awssigning.servicename.test.hocon",
      expectedResult = Right(
        AwsConfigSpec.minimalConfig.copy(output =
          AwsConfigSpec.minimalConfig.output.copy(good =
            AwsConfigSpec.minimalConfig.output.good.copy(auth = Config.ElasticsearchSink.Auth.AWSSigning("aoss", "eu-central-1"))
          )
        )
      )
    )

  private def assert(
    resource: String,
    expectedResult: Either[ExitCode, Config[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig]]
  ) = {
    val path = Paths.get(getClass.getResource(resource).toURI)
    ConfigParser.configFromFile[IO, Config[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig]](path).value.map { result =>
      result must beEqualTo(expectedResult)
    }
  }
}

object AwsConfigSpec {

  private val defaultKinesisSource = KinesisSourceConfig(
    appName                          = "snowplow-elasticsearch-loader",
    streamName                       = "snowplow-enriched",
    workerIdentifier                 = "testWorkerId",
    initialPosition                  = KinesisSourceConfig.InitialPosition.Latest,
    retrievalMode                    = KinesisSourceConfig.Retrieval.Polling(750, 1500.millis),
    customEndpoint                   = None,
    dynamodbCustomEndpoint           = None,
    cloudwatchCustomEndpoint         = None,
    leaseDuration                    = 10.seconds,
    maxLeasesToStealAtOneTimeFactor  = BigDecimal(2),
    checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
    debounceCheckpoints              = 10.seconds,
    maxRetries                       = 10,
    apiCallAttemptTimeout            = 15.seconds
  )

  private val defaultBadSink = KinesisSinkConfigM[Id](
    streamName             = "snowplow-bad",
    throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
    recordLimit            = 500,
    byteLimit              = 5242880,
    customEndpoint         = None,
    maxRetries             = 10
  )

  private val defaultRetries =
    Config.Retries(Retrying.Config.ForTransient(delay = 1.second, attempts = 5))

  private val defaultMonitoring = Config.Monitoring(
    metrics     = Config.Metrics(statsd = None, prometheus = PrometheusConfig(Map.empty)),
    sentry      = None,
    healthProbe = Config.HealthProbe(port = Port.fromInt(8000).get, unhealthyLatency = 2.minutes)
  )

  private val minimalConfig = Config[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig](
    license = AcceptedLicense(),
    input   = KinesisHttpSourceConfig(kinesis = defaultKinesisSource, http = None),
    output = Config.Output(
      good = Config.ElasticsearchSink(
        url                        = Uri.unsafeFromString("http://localhost:9200"),
        index                      = "snowplow",
        documentType               = None,
        auth                       = Config.ElasticsearchSink.Auth.Basic("elastic", "changeme"),
        sharding                   = None,
        indexTimeout               = 1.minute,
        additionalBadRowErrorTypes = Set()
      ),
      bad = Config.SinkWithMetadata(
        sink          = defaultBadSink,
        maxRecordSize = 1000000
      )
    ),
    streams                 = EmptyConfig(),
    purpose                 = Config.Purpose.Enriched,
    batching                = Config.Batching(maxBytes = 10000000L, maxDelay = 1.second),
    retries                 = defaultRetries,
    cpuParallelismFactor    = BigDecimal(1),
    uploadParallelismFactor = BigDecimal(4),
    decompression           = DecompressionConfig(maxBytesInBatch = 5242880, maxBytesSinglePayload = 10000000),
    monitoring              = defaultMonitoring,
    telemetry = Telemetry.Config(
      disable         = false,
      collectorUri    = Uri.unsafeFromString("https://collector-g.snowplowanalytics.com"),
      userProvidedId  = None,
      autoGeneratedId = None,
      instanceId      = None,
      moduleName      = None,
      moduleVersion   = None
    ),
    http = Config.Http(HttpClient.Config(maxConnectionsPerServer = 4))
  )

  private val referenceConfig = Config[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig](
    license = AcceptedLicense(),
    input = KinesisHttpSourceConfig(
      kinesis = defaultKinesisSource.copy(
        initialPosition = KinesisSourceConfig.InitialPosition.TrimHorizon
      ),
      http = None
    ),
    output = Config.Output(
      good = Config.ElasticsearchSink(
        url                        = Uri.unsafeFromString("http://localhost:9200"),
        index                      = "snowplow",
        documentType               = None,
        auth                       = Config.ElasticsearchSink.Auth.NoAuth,
        sharding                   = None,
        indexTimeout               = 1.minute,
        additionalBadRowErrorTypes = Set("strict_dynamic_mapping_exception")
      ),
      bad = Config.SinkWithMetadata(
        sink          = defaultBadSink,
        maxRecordSize = 1000000
      )
    ),
    streams                 = EmptyConfig(),
    purpose                 = Config.Purpose.Enriched,
    batching                = Config.Batching(maxBytes = 10000000L, maxDelay = 1.second),
    retries                 = defaultRetries,
    cpuParallelismFactor    = BigDecimal(1),
    uploadParallelismFactor = BigDecimal(4),
    decompression           = DecompressionConfig(maxBytesInBatch = 5242880, maxBytesSinglePayload = 10000000),
    monitoring = Config.Monitoring(
      metrics = Config.Metrics(
        statsd = Some(
          StatsdConfig(
            hostname = "127.0.0.1",
            port     = 8125,
            tags     = Map("env" -> "prod"),
            period   = 1.minute,
            prefix   = "snowplow.elasticsearch.loader.aws"
          )
        ),
        prometheus = PrometheusConfig(Map("env" -> "prod"))
      ),
      sentry      = Some(Sentry.ConfigM[Id](dsn = "https://public@sentry.example.com/1", tags = Map("myTag" -> "xyz"), environment = None)),
      healthProbe = Config.HealthProbe(port = Port.fromInt(8000).get, unhealthyLatency = 2.minutes)
    ),
    telemetry = Telemetry.Config(
      disable         = false,
      collectorUri    = Uri.unsafeFromString("https://collector-g.snowplowanalytics.com"),
      userProvidedId  = Some("my_pipeline"),
      autoGeneratedId = Some("hfy67e5ydhtrd"),
      instanceId      = Some("665bhft5u6udjf"),
      moduleName      = Some("elasticsearch-loader-vmss"),
      moduleVersion   = Some("1.0.0")
    ),
    http = Config.Http(HttpClient.Config(maxConnectionsPerServer = 4))
  )
}
