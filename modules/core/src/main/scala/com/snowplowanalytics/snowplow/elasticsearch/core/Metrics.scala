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

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.effect.kernel.{Async, Resource}
import cats.implicits._

import fs2.Stream

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}
import com.snowplowanalytics.snowplow.runtime.Metrics.Destination.PrometheusAndStatsd

trait Metrics[F[_]] {
  def addGood(count: Int): F[Unit]
  def addBad(count: Int): F[Unit]
  def addIndexLimitError(count: Int): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(latency: FiniteDuration): F[Unit]
  def setElasticsearchLatency(latency: FiniteDuration): F[Unit]

  def scrape: F[String]
  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics, sourceAndAck: SourceAndAck[F]): Resource[F, Metrics[F]] =
    CommonMetrics.build[F](config.statsd, config.prometheus).evalMap { entries =>
      for {
        good <- entries.counter("events_good", PrometheusAndStatsd)
        bad <- entries.counter("events_bad", PrometheusAndStatsd)
        indexLimitError <- entries.counter("es_index_limit_error", PrometheusAndStatsd)
        latency <- entries.lagGauge("latency", sourceAndAck.currentStreamLatency, PrometheusAndStatsd)
        e2eLatency <- entries.timer("e2e_latency", 1.second, PrometheusAndStatsd)
        elasticsearchLatency <- entries.timer("elasticsearch_latency", 1.second, PrometheusAndStatsd)
      } yield new Metrics[F] {
        def addGood(count: Int): F[Unit]                        = good.add(count.toLong)
        def addBad(count: Int): F[Unit]                         = bad.add(count.toLong)
        def addIndexLimitError(count: Int): F[Unit]             = indexLimitError.add(count.toLong)
        def setLatency(l: FiniteDuration): F[Unit]              = latency.record(l)
        def setE2ELatency(l: FiniteDuration): F[Unit]           = e2eLatency.record(l)
        def setElasticsearchLatency(l: FiniteDuration): F[Unit] = elasticsearchLatency.record(l)

        def scrape: F[String]          = entries.scrape
        def report: Stream[F, Nothing] = entries.report
      }
    }
}
