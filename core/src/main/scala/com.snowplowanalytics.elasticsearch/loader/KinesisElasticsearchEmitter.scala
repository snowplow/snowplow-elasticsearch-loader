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

package com.snowplowanalytics
package elasticsearch.loader

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  UnmodifiableBuffer
}

// Java
import java.io.IOException
import java.util.{List => JList}

// Scala
import scala.collection.JavaConverters._

// Scalaz
import scalaz._
import Scalaz._

// Tracker
import snowplow.scalatracker.Tracker

// This project
import sinks._
import clients._

/**
 * Class to send valid records to Elasticsearch and invalid records to Kinesis
 *
 * @param configuration the KCL configuration
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param elasticsearchSender ES Client to use
 * @param tracker a Tracker instance
 */
class KinesisElasticsearchEmitter(
  configuration: KinesisConnectorConfiguration,
  goodSink: Option[ISink],
  badSink: ISink,
  elasticsearchSender: ElasticsearchSender,
  tracker: Option[Tracker] = None
) extends IEmitter[EmitterInput] {

  // ElasticsearchEmitter instance
  private val elasticsearchEmitter = new ElasticsearchEmitter(elasticsearchSender, 
                                                              goodSink, 
                                                              badSink, 
                                                              configuration.BUFFER_RECORD_COUNT_LIMIT,  
                                                              configuration.BUFFER_BYTE_SIZE_LIMIT)
  /**
   * Emits good records to stdout or Elasticsearch.
   * All records which Elasticsearch rejects and all records which failed transformation
   * get sent to to stderr or Kinesis.
   *
   * @param buffer BasicMemoryBuffer containing EmitterInputs
   * @return list of inputs which failed transformation or which Elasticsearch rejected
   */
  @throws[IOException]
  override def emit(buffer: UnmodifiableBuffer[EmitterInput]): JList[EmitterInput] = {
    val records = buffer.getRecords.asScala.toList
    elasticsearchEmitter.attemptEmit(records).asJava
  }

  /**
   * Handles records rejected by the SnowplowElasticsearchTransformer or by Elasticsearch
   *
   * @param records List of failed records
   */
  override def fail(records: JList[EmitterInput]): Unit =
    elasticsearchEmitter.fail(records.asScala.toList)

  /**
   * Closes the Elasticsearch client when the KinesisConnectorRecordProcessor is shut down
   */
  override def shutdown(): Unit = elasticsearchSender.close

}
