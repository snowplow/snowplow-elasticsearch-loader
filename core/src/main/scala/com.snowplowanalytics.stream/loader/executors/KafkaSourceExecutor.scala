/**
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd.
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

// Kafka
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.nio.charset.StandardCharsets.UTF_8

//Java
import java.time.Duration
import java.util
import java.util.Properties

// Scala
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

// Logging
import org.slf4j.LoggerFactory

// This project
import com.snowplowanalytics.stream.loader.Config._
import com.snowplowanalytics.stream.loader.clients._
import com.snowplowanalytics.stream.loader.sinks._
import com.snowplowanalytics.stream.loader.transformers.{
  BadEventTransformer,
  EnrichedEventJsonTransformer,
  JsonTransformer
}

/**
 * NSQSource executor
 *
 * @param purpose kind of data stored, good, bad or plain-json
 * @param kafka Kafka KafkaConfig
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 */
class KafkaSourceExecutor(
  purpose: Purpose,
  kafka: Source.Kafka,
  goodSink: Either[ISink, BulkSender[EmitterJsonInput]],
  badSink: ISink,
  shardDateField: Option[String],
  shardDateFormat: Option[String]
) extends Runnable {

  private lazy val log = LoggerFactory.getLogger(getClass)

  // nsq messages will be buffered in msgBuffer until buffer size become equal to nsqBufferSize
  private val msgBuffer = new ListBuffer[EmitterJsonInput]()
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

  /**
   * Creates a new Kafka Producer with the given
   * configuration options
   *
   * @return a new Kafka Producer
   */
  private def createConsumer: KafkaConsumer[String, Array[Byte]] = {

    log.info(s"Connect Kafka Consumer to brokers: ${kafka.brokers}")

    val props = new Properties()
    props.put("bootstrap.servers", kafka.brokers)
    props.put("group.id", kafka.groupId)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    kafka.sourceConf.getOrElse(Map()).foreach { case (k, v) => props.setProperty(k, v) }

    new KafkaConsumer[String, Array[Byte]](props)
  }

  /** Never-ending processing loop over source stream. */
  override def run(): Unit = {
    log.info(s"Running Kafka consumer group: ${kafka.groupId}.")
    log.info(s"Processing raw input Kafka topic: ${kafka.topicName}")
    val kafkaBufferSize = kafka.buffer.recordLimit
    val consumer        = createConsumer

    consumer.subscribe(util.Collections.singletonList(kafka.topicName))

    while (true) {
      val recordValues = consumer
        .poll(Duration.ofMillis(100)) // Wait 100 ms if data is not available
        .asScala
        .toList
        .map(_.value)
      msgBuffer.synchronized {
        for (record <- recordValues) {
          val msgStr       = new String(record, UTF_8)
          val emitterInput = transformer.consumeLine(msgStr)
          msgBuffer += emitterInput

          if (msgBuffer.size == kafkaBufferSize) {
            val rejectedRecords = emitter.attemptEmit(msgBuffer.toList)
            emitter.fail(rejectedRecords.asJava)
            msgBuffer.clear()
          }
        }
      }
    }
  }
}
