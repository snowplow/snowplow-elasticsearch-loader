/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.elasticsearch.loader

package model {
  sealed trait StreamType
  case object Good extends StreamType
  case object Bad extends StreamType
  case object PlainJson extends StreamType

  case class SinkConfig(good: String, bad: String)
  case class AWSConfig(accessKey: String, secretKey: String)
  case class NSQConfig(
    channelName: String,
    host: String,
    port: Int,
    lookupPort: Int
  )
  case class KinesisConfig(
    initialPosition: String,
    maxRecords: Long,
    region: String,
    appName: String
  ) {
    val endpoint = region match {
      case cn@"cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    }
  }
  case class BufferConfig(byteLimit: Long, recordLimit: Long, timeLimit: Long)
  case class StreamsConfig(
    streamNameIn: String,
    streamNameOut: String,
    buffer: BufferConfig
  )
  case class ESClientConfig(
    endpoint: String,
    port: Int,
    maxTimeout: Long,
    ssl: Boolean
  )
  case class ESAWSConfig(signing: Boolean, region: String)
  case class ESClusterConfig(name: String, index: String, clusterType: String)
  case class ESConfig(
    client: ESClientConfig,
    aws: ESAWSConfig,
    cluster: ESClusterConfig
  )
  case class SnowplowMonitoringConfig(
    collectorUri: String,
    collectorPort: Int,
    appId: String,
    method: String
  )
  case class MonitoringConfig(snowplow: SnowplowMonitoringConfig)
  case class ESLoaderConfig(
    source: String,
    sink: SinkConfig,
    enabled: String,
    aws: AWSConfig,
    nsq: NSQConfig,
    kinesis: KinesisConfig,
    streams: StreamsConfig,
    elasticsearch: ESConfig,
    monitoring: Option[MonitoringConfig]
  ) {
    val streamType = enabled match {
      case "good" => Good
      case "bad" => Bad
      case "plain-json" => PlainJson
      case _ => throw new IllegalArgumentException("\"enabled\" must be set to \"good\", \"bad\" or \"plain-json\" ")
    }
  }
}