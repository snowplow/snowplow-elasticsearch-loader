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

import cats.implicits._
import cats.effect.kernel.{Async, Resource}
import org.http4s.client.Client
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe, HttpClient, Sentry}
import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  elasticsearchSink: ElasticsearchSink[F],
  appHealth: AppHealth.Interface[F, String, RuntimeService],
  badSink: Sink[F],
  httpClient: Client[F],
  metrics: Metrics[F],
  purpose: Config.Purpose,
  sharding: Option[Config.ElasticsearchSink.Sharding],
  batching: Config.Batching,
  cpuParallelism: Int,
  uploadParallelism: Int,
  badSinkMaxSize: Int,
  decompressionConfig: DecompressionConfig
) {
  def badRowProcessor = BadRowProcessor(appInfo.name, appInfo.version)
}

object Environment {
  def fromConfig[F[_]: Async, FactoryConfig, SourceConfig, BadSinkConfig](
    config: Config[FactoryConfig, SourceConfig, BadSinkConfig],
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, BadSinkConfig]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- Sentry.enable[F](appInfo, config.monitoring.sentry)
      httpClient <- HttpClient.resource[F](config.http.client)
      cpuParallelism    = chooseCpuParallelism(config)
      uploadParallelism = chooseUploadParallelism(config)
      factory <- toFactory(config.streams)
      sourceAndAck <- factory.source(config.input)
      sourceReporter = sourceAndAck.isHealthy(config.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, String, RuntimeService](List(sourceReporter)))
      metrics <- Metrics.build[F](config.monitoring.metrics, sourceAndAck)
      _ <- HealthProbe.resource(config.monitoring.healthProbe.port, appHealth, metrics.scrape)
      elasticsearchSink <- ElasticsearchSink.build[F](config.output.good, metrics, appHealth, config.retries, uploadParallelism)
      badSink <- factory.sink(config.output.bad.sink).onError { case _ =>
                   Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink))
                 }
      _ <- Resource.eval(appHealth.beHealthyForSetup)
    } yield Environment(
      appInfo,
      sourceAndAck,
      elasticsearchSink,
      appHealth,
      badSink,
      httpClient,
      metrics,
      config.purpose,
      config.output.good.sharding,
      config.batching,
      cpuParallelism,
      uploadParallelism,
      config.output.bad.maxRecordSize,
      config.decompression
    )

  /**
   * See the description of `cpuParallelism` on the [[Environment]] class
   *
   * For bigger instances (more cores) we want more parallelism, so that cpu-intensive steps can
   * take advantage of all the cores.
   */
  private def chooseCpuParallelism(config: Config[_, _, _]): Int =
    (Runtime.getRuntime.availableProcessors * config.cpuParallelismFactor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

  /**
   * See the description of `uploadParallelism` on the [[Environment]] class
   */
  private def chooseUploadParallelism(config: Config[_, _, _]): Int =
    (Runtime.getRuntime.availableProcessors * config.uploadParallelismFactor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt
}
