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
package com.snowplowanalytics
package stream.loader

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.interfaces.{
  IBuffer,
  IEmitter,
  IKinesisConnectorPipeline,
  ITransformer
}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{AllPassFilter, BasicMemoryBuffer}

// This project
import com.snowplowanalytics.stream.loader.sinks._
import com.snowplowanalytics.stream.loader.Config._
import com.snowplowanalytics.stream.loader.transformers.{
  BadEventTransformer,
  EnrichedEventJsonTransformer,
  JsonTransformer
}
import com.snowplowanalytics.stream.loader.clients.BulkSender

/**
 * KinesisElasticsearchPipeline class sets up the Emitter/Buffer/Transformer/Filter,
 * orchestrating the whole records flow
 *
 * @param purpose kind of data stored, good, bad or plain-json
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 */
class KinesisPipeline(
  purpose: Purpose,
  goodSink: Either[ISink, BulkSender[EmitterJsonInput]],
  badSink: ISink,
  shardDateField: Option[String],
  shardDateFormat: Option[String]
) extends IKinesisConnectorPipeline[ValidatedJsonRecord, EmitterJsonInput] {

  def getEmitter(configuration: KinesisConnectorConfiguration): IEmitter[EmitterJsonInput] =
    new Emitter(goodSink, badSink)

  def getBuffer(configuration: KinesisConnectorConfiguration): IBuffer[ValidatedJsonRecord] =
    new BasicMemoryBuffer[ValidatedJsonRecord](configuration)

  def getTransformer(
    c: KinesisConnectorConfiguration
  ): ITransformer[ValidatedJsonRecord, EmitterJsonInput] =
    purpose match {
      case Purpose.Enriched => new EnrichedEventJsonTransformer(shardDateField, shardDateFormat)
      case Purpose.Json     => new JsonTransformer
      case Purpose.Bad      => new BadEventTransformer
    }

  def getFilter(c: KinesisConnectorConfiguration) =
    new AllPassFilter[ValidatedJsonRecord]()
}
