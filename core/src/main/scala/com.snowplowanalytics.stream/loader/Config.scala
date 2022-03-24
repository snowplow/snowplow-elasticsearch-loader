/**
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.stream.loader

import java.nio.file.{Files, Path}
import java.text.SimpleDateFormat

import scala.util.Try

import com.monovore.decline.{Command, Opts}

import cats.syntax.either._
import cats.syntax.validated._

import com.amazonaws.regions.{DefaultAwsRegionProviderChain, Regions}

import com.typesafe.config.ConfigOrigin

import pureconfig._
import pureconfig.generic.{FieldCoproductHint, ProductHint}
import pureconfig.generic.semiauto._
import pureconfig.error._

object Config {

  implicit def hint[T]: ProductHint[T]               = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val queueHint: FieldCoproductHint[Source] = new FieldCoproductHint[Source]("type")

  final case class StreamLoaderConfig(
    input: Source,
    output: Sink,
    purpose: Purpose,
    monitoring: Monitoring
  )

  sealed trait Source extends Product with Serializable
  object Source {
    final case object Stdin extends Source

    final case class Nsq(
      streamName: String,
      channelName: String,
      nsqlookupdHost: String,
      nsqlookupdPort: Int,
      buffer: Nsq.Buffer
    ) extends Source

    object Nsq {
      final case class Buffer(recordLimit: Long)
    }

    final case class Kinesis(
      streamName: String,
      initialPosition: String,
      initialTimestamp: Option[String],
      maxRecords: Long,
      region: Region,
      appName: String,
      customEndpoint: Option[String],
      dynamodbCustomEndpoint: Option[String],
      buffer: Kinesis.Buffer
    ) extends Source {
      val timestampEither = initialTimestamp
        .toRight("An initial timestamp needs to be provided when choosing AT_TIMESTAMP")
        .right
        .flatMap { s =>
          val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
          utils.fold(Try(format.parse(s)))(t => Left(t.getMessage), Right(_))
        }
      require(
        initialPosition != "AT_TIMESTAMP" || timestampEither.isRight,
        timestampEither.left.getOrElse("")
      )

      val timestamp = timestampEither.right.toOption
    }

    object Kinesis {
      final case class Buffer(byteLimit: Long, recordLimit: Long, timeLimit: Long)
    }
  }

  final case class Sink(good: Sink.GoodSink, bad: Sink.BadSink)
  object Sink {
    sealed trait GoodSink extends Product with Serializable
    object GoodSink {

      final case object Stdout extends GoodSink

      final case class Elasticsearch(
        client: Elasticsearch.ESClient,
        aws: Elasticsearch.ESAWS,
        cluster: Elasticsearch.ESCluster,
        chunk: Elasticsearch.ESChunk
      ) extends GoodSink

      object Elasticsearch {

        final case class ESClient(
          endpoint: String,
          port: Int,
          username: Option[String],
          password: Option[String],
          shardDateFormat: Option[String],
          shardDateField: Option[String],
          maxTimeout: Long,
          maxRetries: Int,
          ssl: Boolean
        )

        final case class ESAWS(signing: Boolean, region: Region)

        final case class ESCluster(index: String, documentType: Option[String])

        final case class ESChunk(byteLimit: Long, recordLimit: Long)
      }
    }

    sealed trait BadSink extends Product with Serializable
    object BadSink {
      final case object None extends BadSink

      final case object Stderr extends BadSink

      final case class Nsq(
        streamName: String,
        nsqdHost: String,
        nsqdPort: Int
      ) extends BadSink

      final case class Kinesis(
        streamName: String,
        region: Region,
        customEndpoint: Option[String]
      ) extends BadSink
    }
  }

  sealed trait Purpose extends Product with Serializable
  object Purpose {
    final case object Enriched extends Purpose
    final case object Bad      extends Purpose
    final case object Json     extends Purpose

    implicit val purposeConfigReader: ConfigReader[Purpose] = ConfigReader.fromString { str =>
      str.toLowerCase match {
        case "enriched_events" => Purpose.Enriched.asRight
        case "bad_rows"        => Purpose.Bad.asRight
        case "json"            => Purpose.Json.asRight
        case other             => RawFailureReason(s"Cannot parse $other into supported purpose type").asLeft
      }
    }
  }

  final case class Monitoring(
    snowplow: Option[Monitoring.SnowplowMonitoring],
    metrics: Monitoring.Metrics
  )
  object Monitoring {
    final case class SnowplowMonitoring(
      collector: String,
      appId: String
    )

    final case class Metrics(cloudWatch: Boolean)
  }

  final case class Region(name: String)

  final case class RawFailureReason(description: String) extends FailureReason
  final case class RawConfigReaderFailure(description: String, origin: Option[ConfigOrigin] = None)
      extends ConfigReaderFailure

  /**
   * All config implicits are put into case class because we want to make region config reader
   * changeable to write unit tests for config parsing.
   * Region config reader is special config reader because it allows Region types to be missing.
   * If they are missing, it tries to retrieve region with "DefaultAwsRegionProviderChain".
   * If it is also unsuccessful, it throws error. In the tests, it is changed with dummy config
   * reader in order to not use "DefaultAwsRegionProviderChain" during tests.
   */
  case class implicits(
    regionConfigReader: ConfigReader[Region] with ReadsMissingKeys = new ConfigReader[Region]
      with ReadsMissingKeys {
      override def from(cur: ConfigCursor) =
        if (cur.isUndefined)
          Config.getRegion.toRight(
            ConfigReaderFailures(
              RawConfigReaderFailure(
                "Region can not be resolved, needs to be passed explicitly"
              )
            )
          )
        else
          cur.asString.flatMap { r =>
            val region = Region(r)
            checkRegion(region).leftMap(e => ConfigReaderFailures(RawConfigReaderFailure(e)))
          }
    }
  ) {
    implicit val implRegionConfigReader: ConfigReader[Region] = regionConfigReader
    implicit val streamLoaderConfigReader: ConfigReader[StreamLoaderConfig] =
      deriveReader[StreamLoaderConfig]
    implicit val sourceConfigReader: ConfigReader[Source] =
      deriveReader[Source]
    implicit val sourceStdinConfigReader: ConfigReader[Source.Stdin.type] =
      deriveReader[Source.Stdin.type]
    implicit val sourceNsqConfigReader: ConfigReader[Source.Nsq] =
      deriveReader[Source.Nsq]
    implicit val sourceNsqBufferConfigReader: ConfigReader[Source.Nsq.Buffer] =
      deriveReader[Source.Nsq.Buffer]
    implicit val sourceKinesisConfigReader: ConfigReader[Source.Kinesis] =
      deriveReader[Source.Kinesis]
    implicit val sourceKinesisConfigBufferReader: ConfigReader[Source.Kinesis.Buffer] =
      deriveReader[Source.Kinesis.Buffer]
    implicit val sinkConfigReader: ConfigReader[Sink] =
      deriveReader[Sink]
    implicit val sinkGoodConfigReader: ConfigReader[Sink.GoodSink] =
      deriveReader[Sink.GoodSink]
    implicit val sinkGoodStdoutConfigReader: ConfigReader[Sink.GoodSink.Stdout.type] =
      deriveReader[Sink.GoodSink.Stdout.type]
    implicit val sinkGoodESConfigReader: ConfigReader[Sink.GoodSink.Elasticsearch] =
      deriveReader[Sink.GoodSink.Elasticsearch]
    implicit val sinkGoodESClientConfigReader: ConfigReader[Sink.GoodSink.Elasticsearch.ESClient] =
      deriveReader[Sink.GoodSink.Elasticsearch.ESClient]
    implicit val sinkGoodESClusterConfigReader: ConfigReader[
      Sink.GoodSink.Elasticsearch.ESCluster
    ] =
      deriveReader[Sink.GoodSink.Elasticsearch.ESCluster]
    implicit val sinkGoodESAWSConfigReader: ConfigReader[Sink.GoodSink.Elasticsearch.ESAWS] =
      deriveReader[Sink.GoodSink.Elasticsearch.ESAWS]
    implicit val sinkGoodESChunkConfigReader: ConfigReader[Sink.GoodSink.Elasticsearch.ESChunk] =
      deriveReader[Sink.GoodSink.Elasticsearch.ESChunk]
    implicit val sinkBadSinkConfigReader: ConfigReader[Sink.BadSink] =
      deriveReader[Sink.BadSink]
    implicit val sinkBadKinesisConfigReader: ConfigReader[Sink.BadSink.Kinesis] =
      deriveReader[Sink.BadSink.Kinesis]
    implicit val sinkBadNoneConfigReader: ConfigReader[Sink.BadSink.None.type] =
      deriveReader[Sink.BadSink.None.type]
    implicit val sinkBadStderrConfigReader: ConfigReader[Sink.BadSink.Stderr.type] =
      deriveReader[Sink.BadSink.Stderr.type]
    implicit val sinkBadNsqConfigReader: ConfigReader[Sink.BadSink.Nsq] =
      deriveReader[Sink.BadSink.Nsq]
    implicit val monitoringConfigReader: ConfigReader[Monitoring] =
      deriveReader[Monitoring]
    implicit val snowplowMonitoringConfig: ConfigReader[Monitoring.SnowplowMonitoring] =
      deriveReader[Monitoring.SnowplowMonitoring]
    implicit val metricsConfigReader: ConfigReader[Monitoring.Metrics] =
      deriveReader[Monitoring.Metrics]
  }

  val config = Opts
    .option[Path]("config", "Path to a HOCON configuration file")
    .mapValidated { path =>
      if (Files.exists(path) && Files.isRegularFile(path))
        path.valid
      else
        s"Configuration file $path does not exist".invalidNel
    }
    .orNone

  val command = Command("snowplow-stream-loader", generated.Settings.version, true)(config)

  def parseConfig(arguments: Array[String]): Either[String, StreamLoaderConfig] = {
    val configImplicits = com.snowplowanalytics.stream.loader.Config.implicits()
    parseConfig(arguments, configImplicits.streamLoaderConfigReader)
  }

  def parseConfig(
    arguments: Array[String],
    configReader: ConfigReader[StreamLoaderConfig]
  ): Either[String, StreamLoaderConfig] = {
    implicit val implConfigReader: ConfigReader[StreamLoaderConfig] = configReader
    for {
      path <- command.parse(arguments).leftMap(_.toString)
      source = path.fold(ConfigSource.empty)(ConfigSource.file)
      c = namespaced(
        ConfigSource.default(namespaced(source.withFallback(namespaced(ConfigSource.default))))
      )
      parsed <- c.load[StreamLoaderConfig].leftMap(showFailures)
    } yield parsed
  }

  /** Optionally give precedence to configs wrapped in a "esloader" block. To help avoid polluting config namespace */
  private def namespaced(configObjSource: ConfigObjectSource): ConfigObjectSource =
    ConfigObjectSource {
      for {
        configObj <- configObjSource.value()
        conf = configObj.toConfig
      } yield {
        if (conf.hasPath(Namespace))
          conf.getConfig(Namespace).withFallback(conf.withoutPath(Namespace))
        else
          conf
      }
    }

  private def showFailures(failures: ConfigReaderFailures): String = {
    val failureStrings = failures.toList.map { failure =>
      val location = failure.origin.map(o => s" at ${o.lineNumber}").getOrElse("")
      s"${failure.description}$location"
    }
    failureStrings.mkString("\n")
  }

  private def getRegion: Option[Region] =
    Either.catchNonFatal((new DefaultAwsRegionProviderChain).getRegion).toOption.map(Region)

  private def checkRegion(region: Region): Either[String, Region] = {
    val allRegions = Regions.values().toList.map(_.getName)
    if (allRegions.contains(region.name)) region.asRight
    else s"Region ${region.name} is unknown, choose from [${allRegions.mkString(", ")}]".asLeft
  }

  // Used as an option prefix when reading system properties.
  val Namespace = "snowplow"
}
