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
package executors

// NSQ
import com.snowplowanalytics.client.nsq.NSQConsumer
import com.snowplowanalytics.client.nsq.lookup.DefaultNSQLookup
import com.snowplowanalytics.client.nsq.NSQMessage
import com.snowplowanalytics.client.nsq.NSQConfig
import com.snowplowanalytics.client.nsq.callbacks.NSQMessageCallback
import com.snowplowanalytics.client.nsq.callbacks.NSQErrorCallback
import com.snowplowanalytics.client.nsq.exceptions.NSQException

//Java
import java.nio.charset.StandardCharsets.UTF_8

// Scala
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

// Logging
import org.slf4j.LoggerFactory

// This project
import sinks._
import clients._
import model._
import transformers.{BadEventTransformer, EnrichedEventJsonTransformer, PlainJsonTransformer}

/**
 * NSQSource executor
 *
 * @param streamType the type of stream, good, bad or plain-json
 * @param nsq Nsq NsqConfig
 * @param config ESLoader Configuration
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param bulkSender function for sending to storage
 */
class NsqSourceExecutor(
  streamType: StreamType,
  nsq: Nsq,
  config: StreamLoaderConfig,
  goodSink: Option[ISink],
  badSink: ISink,
  shardDateField: Option[String],
  shardDateFormat: Option[String],
  bulkSender: BulkSender[EmitterJsonInput]
) extends Runnable {

  lazy val log = LoggerFactory.getLogger(getClass())

  // nsq messages will be buffered in msgBuffer until buffer size become equal to nsqBufferSize
  private val msgBuffer = new ListBuffer[EmitterJsonInput]()
  // ElasticsearchEmitter instance
  private val emitter = new Emitter(
    bulkSender,
    goodSink,
    badSink,
    config.streams.buffer.recordLimit,
    config.streams.buffer.byteLimit)
  private val transformer = streamType match {
    case Good      => new EnrichedEventJsonTransformer(shardDateField, shardDateFormat)
    case PlainJson => new PlainJsonTransformer
    case Bad       => new BadEventTransformer
  }

  /**
   * Consumer will be started to wait new message.
   */
  override def run(): Unit = {
    val nsqCallback = new NSQMessageCallback {
      val nsqBufferSize = config.streams.buffer.recordLimit

      override def message(msg: NSQMessage): Unit = {
        val msgStr = new String(msg.getMessage(), UTF_8)
        msgBuffer.synchronized {
          val emitterInput = transformer.consumeLine(msgStr)
          msgBuffer += emitterInput
          msg.finished()

          if (msgBuffer.size == nsqBufferSize) {
            val rejectedRecords = emitter.emit(msgBuffer.toList)
            emitter.fail(rejectedRecords.asJava)
            msgBuffer.clear()
          }
        }
      }
    }

    val errorCallback = new NSQErrorCallback {
      override def error(e: NSQException): Unit =
        log.error(s"Exception while consuming topic ${config.streams.inStreamName}", e)
    }

    // use NSQLookupd
    val lookup = new DefaultNSQLookup
    lookup.addLookupAddress(nsq.nsqlookupdHost, nsq.nsqlookupdPort)
    val consumer = new NSQConsumer(
      lookup,
      config.streams.inStreamName,
      nsq.channelName,
      nsqCallback,
      new NSQConfig(),
      errorCallback)
    consumer.start()
  }
}
