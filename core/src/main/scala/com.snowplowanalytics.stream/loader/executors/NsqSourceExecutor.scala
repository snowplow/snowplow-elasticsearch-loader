/**
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd.
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
import java.util.concurrent.{Executors, TimeUnit}

// Scala
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

// Logging
import org.slf4j.LoggerFactory

// This project
import sinks._
import clients._
import com.snowplowanalytics.stream.loader.Config._
import transformers.{BadEventTransformer, EnrichedEventJsonTransformer, JsonTransformer}

/**
 * NSQSource executor
 *
 * @param purpose kind of data stored, good, bad or plain-json
 * @param nsq Nsq NsqConfig
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 */
class NsqSourceExecutor(
  purpose: Purpose,
  nsq: Source.Nsq,
  goodSink: Either[ISink, BulkSender[EmitterJsonInput]],
  badSink: ISink,
  shardDateField: Option[String],
  shardDateFormat: Option[String]
) extends Runnable
    with AutoCloseable {

  lazy val log = LoggerFactory.getLogger(getClass())

  // nsq messages will be buffered in msgBuffer until buffer size become equal to nsqBufferSize
  private val msgBuffer      = new ListBuffer[EmitterJsonInput]()
  private var msgBufferBytes = 0L
  // ElasticsearchEmitter instance
  private val emitter =
    new Emitter(
      goodSink,
      badSink
    )
  private val transformer = purpose match {
    case Purpose.Enriched => new EnrichedEventJsonTransformer(shardDateField, shardDateFormat)
    case Purpose.Json     => new JsonTransformer
    case Purpose.Bad      => new BadEventTransformer
  }

  val executorService = Executors.newSingleThreadScheduledExecutor

  /**
   * Consumer will be started to wait new message.
   */
  val nsqCallback: NSQMessageCallback = new NSQMessageCallback {
    val nsqBufferSize = nsq.buffer.recordLimit

    override def message(msg: NSQMessage): Unit = {
      val bytes  = msg.getMessage
      val msgStr = new String(bytes, UTF_8)
      msgBuffer.synchronized {
        val emitterInput = transformer.consumeLine(msgStr)
        msgBuffer += emitterInput
        msgBufferBytes += bytes.size
        msg.finished()

        if (msgBuffer.size == nsqBufferSize || msgBufferBytes > nsq.buffer.byteLimit) {
          flush()
        }
      }
    }
  }

  def flush(): Unit = msgBuffer.synchronized {
    if (msgBuffer.nonEmpty) {
      val rejectedRecords = emitter.attemptEmit(msgBuffer.toList)
      emitter.fail(rejectedRecords.asJava)
      msgBuffer.clear()
      msgBufferBytes = 0
    }
  }

  val errorCallback = new NSQErrorCallback {
    override def error(e: NSQException): Unit =
      log.error(s"Exception while consuming topic ${nsq.streamName}", e)
  }

  // use NSQLookupd
  val lookup = new DefaultNSQLookup
  lookup.addLookupAddress(nsq.nsqlookupdHost, nsq.nsqlookupdPort)
  val consumer =
    new NSQConsumer(
      lookup,
      nsq.streamName,
      nsq.channelName,
      nsqCallback,
      new NSQConfig(),
      errorCallback
    )

  override def run(): Unit = {
    val flusher = new Runnable {
      def run(): Unit = flush()
    }
    executorService.scheduleWithFixedDelay(
      flusher,
      nsq.buffer.timeLimit,
      nsq.buffer.timeLimit,
      TimeUnit.MILLISECONDS
    )
    consumer.start()
  }

  override def close(): Unit = {
    consumer.close()
    executorService.shutdown()
  }
}
