/**
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd.
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

import enumeratum.{Enum, EnumEntry}

import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader, ConfigSource}
import pureconfig.generic.{FieldCoproductHint, ProductHint}
import pureconfig.generic.semiauto._
import pureconfig.error.ConfigReaderFailures
import pureconfig.module.enumeratum._

object Config {

  implicit def hint[T]: ProductHint[T]              = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val queueHint: FieldCoproductHint[Queue] = new FieldCoproductHint[Queue]("enabled")

  sealed trait StreamType extends EnumEntry with EnumEntry.Hyphencase
  object StreamType extends Enum[StreamType] {
    case object Good      extends StreamType
    case object Bad       extends StreamType
    case object PlainJson extends StreamType

    val values = findValues
  }

  sealed trait BadSink extends EnumEntry with EnumEntry.Hyphencase
  object BadSink extends Enum[BadSink] {
    case object Stderr  extends BadSink
    case object Nsq     extends BadSink
    case object None    extends BadSink
    case object Kinesis extends BadSink

    val values = findValues
  }

  sealed trait GoodSink extends EnumEntry with EnumEntry.Hyphencase
  object GoodSink extends Enum[GoodSink] {
    case object Elasticsearch extends GoodSink
    case object Stdout        extends GoodSink

    val values = findValues

    implicit val goodSinkReader: ConfigReader[GoodSink] = deriveEnumerationReader[GoodSink]
  }

  case class SinkConfig(good: GoodSink, bad: BadSink)

  object SinkConfig {
    implicit val sinkConfigReader: ConfigReader[SinkConfig] = deriveReader[SinkConfig]
  }

  case class AWSConfig(accessKey: String, secretKey: String)

  object AWSConfig {
    implicit val awsConfigReader: ConfigReader[AWSConfig] = deriveReader[AWSConfig]
  }

  sealed trait Queue
  object Queue {
    final case class Nsq(
      channelName: String,
      nsqdHost: String,
      nsqdPort: Int,
      nsqlookupdHost: String,
      nsqlookupdPort: Int
    ) extends Queue

    object Nsq {
      implicit val nsqReader: ConfigReader[Nsq] = deriveReader[Nsq]
    }

    final case class Kinesis(
      initialPosition: String,
      initialTimestamp: Option[String],
      maxRecords: Long,
      region: String,
      appName: String,
      customEndpoint: Option[String],
      dynamodbCustomEndpoint: Option[String],
      disableCloudWatch: Option[Boolean]
    ) extends Queue {
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

      val endpoint = customEndpoint.getOrElse(region match {
        case cn @ "cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
        case _                 => s"https://kinesis.$region.amazonaws.com"
      })

      val dynamodbEndpoint = dynamodbCustomEndpoint.getOrElse(region match {
        case cn @ "cn-north-1" => s"https://dynamodb.$cn.amazonaws.com.cn"
        case _                 => s"https://dynamodb.$region.amazonaws.com"
      })
    }

    object Kinesis {
      implicit val kinesisReader: ConfigReader[Kinesis] = deriveReader[Kinesis]
    }

    implicit val queueReader: ConfigReader[Queue] = deriveReader[Queue]
  }

  case class BufferConfig(byteLimit: Long, recordLimit: Long, timeLimit: Long)

  object BufferConfig {
    implicit val bufferReader: ConfigReader[BufferConfig] = deriveReader[BufferConfig]
  }

  case class StreamsConfig(
    inStreamName: String,
    outStreamName: String,
    buffer: BufferConfig
  )

  object StreamsConfig {
    implicit val streamsConfigReader: ConfigReader[StreamsConfig] = deriveReader[StreamsConfig]
  }

  case class ESClientConfig(
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

  object ESClientConfig {
    implicit val esClientConfigReader: ConfigReader[ESClientConfig] = deriveReader[ESClientConfig]
  }

  case class ESAWSConfig(signing: Boolean, region: String)

  object ESAWSConfig {
    implicit val esAWSConfigReader: ConfigReader[ESAWSConfig] = deriveReader[ESAWSConfig]
  }

  case class ESClusterConfig(name: String, index: String, documentType: Option[String])

  object ESClusterConfig {
    implicit val esClusterConfigReader: ConfigReader[ESClusterConfig] =
      deriveReader[ESClusterConfig]
  }

  case class ESConfig(
    client: ESClientConfig,
    aws: ESAWSConfig,
    cluster: ESClusterConfig
  )

  object ESConfig {
    implicit val esConfigReader: ConfigReader[ESConfig] = deriveReader[ESConfig]
  }

  case class SnowplowMonitoringConfig(
    collectorUri: String,
    collectorPort: Int,
    ssl: Option[Boolean],
    appId: String,
    method: String
  )

  object SnowplowMonitoringConfig {
    implicit val snowplowMonitoringConfig: ConfigReader[SnowplowMonitoringConfig] =
      deriveReader[SnowplowMonitoringConfig]
  }

  case class MonitoringConfig(snowplow: SnowplowMonitoringConfig)
  object MonitoringConfig {
    implicit val monitoringConfigReader: ConfigReader[MonitoringConfig] =
      deriveReader[MonitoringConfig]
  }

  sealed trait Source extends EnumEntry with EnumEntry.Hyphencase
  object Source extends Enum[Source] {
    case object Kinesis extends Source
    case object Nsq     extends Source
    case object Stdin   extends Source

    val values = findValues

    implicit val sourceConfigReader: ConfigReader[Source] = deriveEnumerationReader[Source]
  }

  case class StreamLoaderConfig(
    source: Source,
    sink: SinkConfig,
    enabled: StreamType,
    aws: AWSConfig,
    queue: Queue,
    streams: StreamsConfig,
    elasticsearch: ESConfig,
    monitoring: Option[MonitoringConfig]
  )

  object StreamLoaderConfig {

    implicit val streamLoaderConfigReader: ConfigReader[StreamLoaderConfig] =
      deriveReader[StreamLoaderConfig]

  }

  val config = Opts
    .option[Path]("config", "Path to a HOCON configuration file")
    .mapValidated { path =>
      if (Files.exists(path) && Files.isRegularFile(path))
        path.valid
      else
        s"Configuration file $path does not exist".invalidNel
    }

  val command = Command("snowplow-stream-loader", generated.Settings.version, true)(config)

  def parseConfig(arguments: Array[String]): StreamLoaderConfig = {
    val result = for {
      path <- command.parse(arguments).leftMap(_.toString)
      source = ConfigSource.file(path.toFile)
      parsed <- source.load[StreamLoaderConfig].leftMap(showFailures(_))
    } yield parsed

    result match {
      case Right(c) => c
      case Left(e) =>
        System.err.println(s"configuration error:\n$e")
        sys.exit(1)
    }
  }

  private def showFailures(failures: ConfigReaderFailures): String = {
    val failureStrings = failures.toList.map { failure =>
      val location = failure.origin.map(o => s" at ${o.lineNumber}").getOrElse("")
      s"${failure.description}$location"
    }
    failureStrings.mkString("\n")
  }

}
