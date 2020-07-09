/**
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd.
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

import com.typesafe.config.ConfigFactory

import com.monovore.decline.{Command, Opts}

import cats.syntax.either._
import cats.syntax.validated._

import enumeratum.{Enum, EnumEntry}

import pureconfig._
import pureconfig.error.ConfigReaderFailures
import pureconfig.module.enumeratum._

object Config {

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
  }

  case class SinkConfig(good: GoodSink, bad: BadSink)

  case class AWSConfig(accessKey: String, secretKey: String)

  sealed trait Queue
  object Queue {
    final case class Nsq(
      channelName: String,
      nsqdHost: String,
      nsqdPort: Int,
      nsqlookupdHost: String,
      nsqlookupdPort: Int)
        extends Queue

    final case class Kinesis(
      initialPosition: String,
      initialTimestamp: Option[String],
      maxRecords: Long,
      region: String,
      appName: String)
        extends Queue {
      val timestampEither = initialTimestamp
        .toRight("An initial timestamp needs to be provided when choosing AT_TIMESTAMP")
        .right
        .flatMap { s =>
          val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
          utils.fold(Try(format.parse(s)))(t => Left(t.getMessage), Right(_))
        }
      require(
        initialPosition != "AT_TIMESTAMP" || timestampEither.isRight,
        timestampEither.left.getOrElse(""))

      val timestamp = timestampEither.right.toOption

      val endpoint = region match {
        case cn @ "cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
        case _                 => s"https://kinesis.$region.amazonaws.com"
      }
    }
  }

  case class BufferConfig(byteLimit: Long, recordLimit: Long, timeLimit: Long)

  case class StreamsConfig(
    inStreamName: String,
    outStreamName: String,
    buffer: BufferConfig
  )

  case class MariadbColumnStoreConfig(
    database: String,
    table: String,
    mapping_file: String,
    columnstore_file: String,
    delimiter: String,
    date_format: String,
    enclose_by_character: String,
    escape_character: String,
    read_cache_size: Int,
    header: Boolean,
    ignore_malformed_csv: Boolean,
    err_log: String
  )
 /**
mcsimport database table input_file [-m mapping_file] [-c Columnstore.xml] [-d delimiter]
[-n null_option] [-df date_format] [-default_non_mapped] [-E enclose_by_character]
[-C escape_character] [-rc read_cache_size] [-header] [-ignore_malformed_csv] [-err_log]
*/
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

  case class ESAWSConfig(signing: Boolean, region: String)

  case class ESClusterConfig(name: String, index: String, documentType: String)

  case class ESConfig(
    client: ESClientConfig,
    aws: ESAWSConfig,
    cluster: ESClusterConfig
  )
  case class SnowplowMonitoringConfig(
    collectorUri: String,
    collectorPort: Int,
    ssl: Option[Boolean],
    appId: String,
    method: String
  )

  case class MonitoringConfig(snowplow: SnowplowMonitoringConfig)

  sealed trait Source extends EnumEntry with EnumEntry.Hyphencase
  object Source extends Enum[Source] {
    case object Kinesis extends Source
    case object Nsq     extends Source
    case object Stdin   extends Source

    val values = findValues
  }

  case class StreamLoaderConfig(
    source: Source,
    sink: SinkConfig,
    enabled: StreamType,
    aws: AWSConfig,
    queue: Queue,
    streams: StreamsConfig,
    elasticsearch: ESConfig,
    monitoring: Option[MonitoringConfig])

  implicit def hint[T]: ProductHint[T]              = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val queueHint: FieldCoproductHint[Queue] = new FieldCoproductHint[Queue]("enabled")

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
      config = ConfigFactory.parseFile(path.toFile).resolve()
      parsed <- loadConfig[StreamLoaderConfig](config).leftMap(showFailures)
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
      val location = failure.location.map(l => s" at ${l.lineNumber}").getOrElse("")
      s"${failure.description}$location"
    }
    failureStrings.mkString("\n")
  }

}
