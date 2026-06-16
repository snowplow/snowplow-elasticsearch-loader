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

import java.nio.file.Path
import cats.data.EitherT
import cats.implicits._
import cats.effect.ExitCode
import cats.effect.kernel.{Async, Resource, Sync}
import io.circe.Decoder
import com.monovore.decline.Opts
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.runtime.{AppInfo, ConfigParser, LogUtils, Telemetry}
import com.snowplowanalytics.snowplow.streams.Factory

object Run {
  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def fromCli[F[_]: Async, FactoryConfig: Decoder, SourceConfig: Decoder, BadSinkConfig: Decoder](
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, BadSinkConfig]]
  ): Opts[F[ExitCode]] =
    Opts.option[Path]("config", help = "path to config file").map { configPath =>
      fromConfigPath(appInfo, toFactory, configPath)
    }

  def fromConfigPath[F[_]: Async, FactoryConfig: Decoder, SourceConfig: Decoder, BadSinkConfig: Decoder](
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, BadSinkConfig]],
    pathToConfig: Path
  ): F[ExitCode] = {
    val eitherT = for {
      config <- ConfigParser.configFromFile[F, Config[FactoryConfig, SourceConfig, BadSinkConfig]](pathToConfig)
      _ <- EitherT.right[String](fromConfig(appInfo, toFactory, config))
    } yield ExitCode.Success

    eitherT
      .leftSemiflatMap { s =>
        Logger[F].error(s).as(ExitCode.Error)
      }
      .merge
      .handleErrorWith { e =>
        Logger[F].error(e)("Exiting") >>
          LogUtils.prettyLogException[F](e).as(ExitCode.Error)
      }
  }

  private def fromConfig[F[_]: Async, FactoryConfig, SourceConfig, BadSinkConfig](
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, BadSinkConfig]],
    config: Config[FactoryConfig, SourceConfig, BadSinkConfig]
  ): F[ExitCode] =
    Environment.fromConfig(config, appInfo, toFactory).use { env =>
      Processing
        .stream(env)
        .concurrently(Telemetry.stream(config.telemetry, env.appInfo, env.httpClient))
        .concurrently(env.metrics.report)
        .compile
        .drain
        .as(ExitCode.Success)
    }
}
