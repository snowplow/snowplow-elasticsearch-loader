/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd.
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
package sinks

//Java
import java.nio.charset.StandardCharsets.UTF_8

// NSQ
import com.github.brainlag.nsq.NSQProducer

/**
 * NSQ sink
 * @param config Configuration for NSQ
 */
class NsqSink(config: ElasticsearchSinkNsqConfig) extends ISink {

  private val producer = new NSQProducer().addAddress(config.nsqHost, config.nsqPort).start();

  /**
   * Writes a string to NSQ
   *
   * @param output The string to write
   * @param key Unused parameter which exists to implement ISink
   * @param good Whether to write to GoodTopic or BadTopic
   */
  override def store(output: String, key: Option[String], good: Boolean): Unit = {
    val topicName = good match {
      case true => config.nsqGoodSinkTopicName
      case false => config.nsqBadSinkTopicName
    }
    producer.produce(topicName, output.getBytes(UTF_8))      
  }
}

