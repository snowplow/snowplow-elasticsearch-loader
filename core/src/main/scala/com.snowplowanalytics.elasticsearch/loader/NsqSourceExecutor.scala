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

// Scalaz
import scalaz._

// Logging
import org.slf4j.LoggerFactory

// This project
import sinks._
import clients._
import StreamType._

/**
 * NSQSource executor
 *
 * @param streamType the type of stream, good/bad
 * @param documentIndex the elasticsearch index name
 * @param documentType the elasticsearch index type
 * @param config the NSQ configuration
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param elasticsearchSender function for sending to elasticsearch
 */
class NsqSourceExecutor(
  streamType: StreamType,
  documentIndex: String,
  documentType: String,
  config: ElasticsearchSinkNsqConfig,
  goodSink: Option[ISink],
  badSink: ISink,
  elasticsearchSender: ElasticsearchSender
) extends Runnable {

  lazy val log = LoggerFactory.getLogger(getClass())

  val transformer = streamType match {
    case StreamType.Good => new SnowplowElasticsearchTransformer(documentIndex, documentType)
    case StreamType.Bad => new BadEventTransformer(documentIndex, documentType)
  }

  val topicName = config.nsqSourceTopicName
  val channelName = config.nsqSourceChannelName

 /**
   * Consumer will be started to wait new message.
   */
  override def run(): Unit = {
    val nsqCallback = new  NSQMessageCallback {
      override def message(msg: NSQMessage): Unit = {
        val msgStr = new String(msg.getMessage(), UTF_8)
        val emitterInput = transformer.consumeLine(msgStr)
        emitterInput._2.bimap(
          f => badSink.store(BadRow(emitterInput._1, f).toCompactJson, None, false),          
          s => goodSink match {
            case Some(gs) => gs.store(s.getSource, None, true)
            case None => elasticsearchSender.sendToElasticsearch(List(msgStr -> s.success))
          }
        )
        msg.finished()
      }
    }

    val errorCallback = new NSQErrorCallback {
      override def error(e: NSQException): Unit = 
        log.error(s"Exception while consuming topic $topicName", e)
    }

    // use NSQLookupd
    val lookup = new DefaultNSQLookup
    lookup.addLookupAddress(config.nsqHost, config.nsqlookupPort)
    val consumer = new NSQConsumer(lookup,
                                   topicName,
                                   channelName,
                                   nsqCallback,
                                   new NSQConfig(),
                                   errorCallback)
    consumer.start() 
  }   
}
