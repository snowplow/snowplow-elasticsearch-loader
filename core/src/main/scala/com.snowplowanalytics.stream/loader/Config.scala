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

  implicit def hint[T]: ProductHint[T]               = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val queueHint: FieldCoproductHint[Source] = new FieldCoproductHint[Source]("type")

  case class Monitoring(snowplow: Monitoring.SnowplowMonitoring)
  object Monitoring {
    implicit val monitoringConfigReader: ConfigReader[Monitoring] =
      deriveReader[Monitoring]

    case class SnowplowMonitoring(
      collector: String,
      appId: String
    )

    object SnowplowMonitoring {
      implicit val snowplowMonitoringConfig: ConfigReader[SnowplowMonitoring] =
        deriveReader[SnowplowMonitoring]
    }
  }

  sealed trait Source extends Product with Serializable
  object Source {
    case class Stdin() extends Source
    object Stdin {
      implicit val stdinReader: ConfigReader[Stdin] = deriveReader[Stdin]
    }

    final case class Nsq(
      streamName: String,
      channelName: String,
      nsqlookupdHost: String,
      nsqlookupdPort: Int,
      buffer: Nsq.Buffer
    ) extends Source

    object Nsq {
      implicit val nsqReader: ConfigReader[Nsq] = deriveReader[Nsq]

      case class Buffer(recordLimit: Long)
      object Buffer {
        implicit val nsqBufferReader: ConfigReader[Buffer] = deriveReader[Buffer]
      }
    }

    final case class Kinesis(
      streamName: String,
      initialPosition: String,
      initialTimestamp: Option[String],
      maxRecords: Long,
      region: String,
      appName: String,
      customEndpoint: Option[String],
      dynamodbCustomEndpoint: Option[String],
      disableCloudWatch: Option[Boolean],
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

      val endpoint = getKinesisEndpoint(region, customEndpoint)

      val dynamodbEndpoint = dynamodbCustomEndpoint.getOrElse(region match {
        case cn @ "cn-north-1" => s"https://dynamodb.$cn.amazonaws.com.cn"
        case _                 => s"https://dynamodb.$region.amazonaws.com"
      })
    }

    object Kinesis {
      implicit val kinesisReader: ConfigReader[Kinesis] = deriveReader[Kinesis]

      case class Buffer(byteLimit: Long, recordLimit: Long, timeLimit: Long)
      object Buffer {
        implicit val kinesisBufferReader: ConfigReader[Buffer] = deriveReader[Buffer]
      }
    }

    implicit val sourceReader: ConfigReader[Source] = deriveReader[Source]
  }

  case class Sink(good: Sink.GoodSink, bad: Sink.BadSink)
  object Sink {
    implicit val sinkConfigReader: ConfigReader[Sink] = deriveReader[Sink]

    sealed trait GoodSink extends Product with Serializable
    object GoodSink {

      case class Stdout() extends GoodSink
      object Stdout {
        implicit val stdoutConfigReader: ConfigReader[Stdout] = deriveReader[Stdout]
      }

      case class Elasticsearch(
        client: Elasticsearch.ESClient,
        aws: Elasticsearch.ESAWS,
        cluster: Elasticsearch.ESCluster,
        chunk: Elasticsearch.ESChunk
      ) extends GoodSink

      object Elasticsearch {
        implicit val esConfigReader: ConfigReader[Elasticsearch] = deriveReader[Elasticsearch]

        case class ESClient(
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

        object ESClient {
          implicit val esClientConfigReader: ConfigReader[ESClient] = deriveReader[ESClient]
        }

        case class ESAWS(signing: Boolean, region: String)

        object ESAWS {
          implicit val esAWSConfigReader: ConfigReader[ESAWS] = deriveReader[ESAWS]
        }

        case class ESCluster(index: String, documentType: Option[String])

        object ESCluster {
          implicit val esClusterConfigReader: ConfigReader[ESCluster] =
            deriveReader[ESCluster]
        }

        case class ESChunk(byteLimit: Long, recordLimit: Long)

        object ESChunk {
          implicit val chunkConfigReader: ConfigReader[ESChunk] = deriveReader[ESChunk]
        }
      }

      implicit val goodSinkConfigReader: ConfigReader[GoodSink] = deriveReader[GoodSink]
    }

    sealed trait BadSink extends Product with Serializable
    object BadSink {
      case class None() extends BadSink
      object None {
        implicit val badSinkNoneConfigReader: ConfigReader[None] = deriveReader[None]
      }

      case class Stderr() extends BadSink
      object Stderr {
        implicit val badSinkStderrConfigReader: ConfigReader[Stderr] = deriveReader[Stderr]
      }

      case class Nsq(
        streamName: String,
        nsqdHost: String,
        nsqdPort: Int
      ) extends BadSink
      object Nsq {
        implicit val badSinkNsqConfigReader: ConfigReader[Nsq] = deriveReader[Nsq]
      }

      case class Kinesis(
        streamName: String,
        region: String,
        customEndpoint: Option[String]
      ) extends BadSink {
        val endpoint = getKinesisEndpoint(region, customEndpoint)
      }
      object Kinesis {
        implicit val badSinkKinesisConfigReader: ConfigReader[Kinesis] = deriveReader[Kinesis]
      }

      implicit val badSinkConfigReader: ConfigReader[BadSink] = deriveReader[BadSink]
    }
  }

  sealed trait Purpose extends EnumEntry with EnumEntry.Hyphencase
  object Purpose extends Enum[Purpose] {
    case object Good      extends Purpose
    case object Bad       extends Purpose
    case object PlainJson extends Purpose

    val values = findValues

    implicit val purposeReader: ConfigReader[Purpose] = deriveEnumerationReader[Purpose]
  }

  case class StreamLoaderConfig(
    input: Source,
    output: Sink,
    purpose: Purpose,
    monitoring: Option[Monitoring]
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

  def parseConfig(arguments: Array[String]): Either[String, StreamLoaderConfig] =
    for {
      path <- command.parse(arguments).leftMap(_.toString)
      source = ConfigSource.file(path.toFile)
      parsed <- source.load[StreamLoaderConfig].leftMap(showFailures(_))
    } yield parsed

  private def showFailures(failures: ConfigReaderFailures): String = {
    val failureStrings = failures.toList.map { failure =>
      val location = failure.origin.map(o => s" at ${o.lineNumber}").getOrElse("")
      s"${failure.description}$location"
    }
    failureStrings.mkString("\n")
  }

  private def getKinesisEndpoint(region: String, customEndpoint: Option[String]): String =
    customEndpoint.getOrElse(region match {
      case cn @ "cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _                 => s"https://kinesis.$region.amazonaws.com"
    })
}
