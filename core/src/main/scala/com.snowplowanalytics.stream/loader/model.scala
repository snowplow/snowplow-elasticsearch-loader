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

import java.text.SimpleDateFormat

import scala.util.Try

package model {
  sealed trait StreamType
  case object Good      extends StreamType
  case object Bad       extends StreamType
  case object PlainJson extends StreamType

  case class SinkConfig(good: String, bad: String)
  case class AWSConfig(accessKey: String, secretKey: String)
  sealed trait Queue
  final case class Nsq(
    channelName: String,
    nsqdHost: String,
    nsqdPort: Int,
    nsqlookupdHost: String,
    nsqlookupdPort: Int
  ) extends Queue
  final case class Kinesis(
    initialPosition: String,
    initialTimestamp: Option[String],
    maxRecords: Long,
    region: String,
    appName: String
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
      timestampEither.left.getOrElse(""))

    val timestamp = timestampEither.right.toOption

    val endpoint = region match {
      case cn @ "cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _                 => s"https://kinesis.$region.amazonaws.com"
    }
  }
  case class BufferConfig(byteLimit: Long, recordLimit: Long, timeLimit: Long)
  case class StreamsConfig(
    inStreamName: String,
    outStreamName: String,
    buffer: BufferConfig
  )
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
  case class StreamLoaderConfig(
    source: String,
    sink: SinkConfig,
    enabled: String,
    aws: AWSConfig,
    queue: Queue,
    streams: StreamsConfig,
    elasticsearch: Option[ESConfig],
    monitoring: Option[MonitoringConfig]
  ) {
    val streamType = enabled match {
      case "good"       => Good
      case "bad"        => Bad
      case "plain-json" => PlainJson
      case _ =>
        throw new IllegalArgumentException(
          "\"enabled\" must be set to \"good\", \"bad\" or \"plain-json\" ")
    }
  }
}
