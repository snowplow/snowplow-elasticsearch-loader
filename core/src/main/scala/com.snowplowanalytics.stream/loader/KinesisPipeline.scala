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
package com.snowplowanalytics
package stream.loader

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.interfaces.{IEmitter, IKinesisConnectorPipeline}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{AllPassFilter, BasicMemoryBuffer}

// This project
import sinks._
import model._
import transformers.{BadEventTransformer, EnrichedEventJsonTransformer, PlainJsonTransformer}
import clients.BulkSender

// Tracker
import snowplow.scalatracker.Tracker

/**
 * KinesisElasticsearchPipeline class sets up the Emitter/Buffer/Transformer/Filter
 *
 * @param streamType the type of stream, good, bad or plain-json
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param bulkSender The Client to use
 * @param tracker a Tracker instance
 */
class KinesisPipeline(
  streamType: StreamType,
  goodSink: Option[ISink],
  badSink: ISink,
  bulkSender: BulkSender[EmitterJsonInput],
  shardDateField: Option[String],
  shardDateFormat: Option[String],
  bufferRecordLimit: Long,
  bufferByteLimit: Long,
  tracker: Option[Tracker] = None
) extends IKinesisConnectorPipeline[ValidatedJsonRecord, EmitterJsonInput] {

  override def getEmitter(
    configuration: KinesisConnectorConfiguration): IEmitter[EmitterJsonInput] =
    new Emitter(bulkSender, goodSink, badSink, bufferRecordLimit, bufferByteLimit)

  override def getBuffer(configuration: KinesisConnectorConfiguration) =
    new BasicMemoryBuffer[ValidatedJsonRecord](configuration)

  override def getTransformer(c: KinesisConnectorConfiguration) = streamType match {
    case Good      => new EnrichedEventJsonTransformer(shardDateField, shardDateFormat)
    case PlainJson => new PlainJsonTransformer
    case Bad       => new BadEventTransformer

  }

  override def getFilter(c: KinesisConnectorConfiguration) =
    new AllPassFilter[ValidatedJsonRecord]()
}
