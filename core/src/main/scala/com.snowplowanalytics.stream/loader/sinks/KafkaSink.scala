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
package sinks

// Kafka
import org.apache.kafka.clients.producer._

//Java
import java.util.Properties
import org.slf4j.LoggerFactory

//Scala
import scala.util.Random
import java.nio.charset.StandardCharsets.UTF_8

//This project
import com.snowplowanalytics.stream.loader.Config.Sink.BadSink.{Kafka => KafkaSinkConfig}

/**
 * Kafka Sink
 */
class KafkaSink(conf: KafkaSinkConfig) extends ISink {
  private lazy val log = LoggerFactory.getLogger(getClass)

  private val kafkaProducer = createProducer

  /**
   * Creates a new Kafka Producer with the given
   * configuration options
   *
   * @return a new Kafka Producer
   */
  private def createProducer: KafkaProducer[String, Array[Byte]] = {

    log.info(s"Create Kafka Producer to brokers: ${conf.brokers}")

    val props = new Properties()
    props.setProperty("bootstrap.servers", conf.brokers)
    props.setProperty("acks", "all")
    props.setProperty("retries", conf.retries.toString)
    props.setProperty("buffer.memory", conf.buffer.byteLimit.toString)
    props.setProperty("linger.ms", conf.buffer.timeLimit.toString)
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty(
      "value.serializer",
      "org.apache.kafka.common.serialization.ByteArraySerializer")

    conf.producerConf.getOrElse(Map()).foreach { case (k, v) => props.setProperty(k, v) }

    new KafkaProducer[String, Array[Byte]](props)
  }

  /**
   * Writes a string to Kafka
   *
   * @param output The string to write
   * @param key A hash of the key determines to which shard the
   *            record is assigned. Defaults to a random string.
   * @param good Unused parameter which exists to extend ISink
   */
  override def store(output: String, key: Option[String], good: Boolean): Unit = {
    kafkaProducer.send(
      new ProducerRecord(
        conf.topicName,
        key.getOrElse(Random.nextInt.toString),
        output.getBytes(UTF_8)),
      new Callback {
        override def onCompletion(metadata: RecordMetadata, e: Exception): Unit =
          if (e != null) log.error(s"Sending event failed: ${e.getMessage}")
      }
    )
    Nil
  }
}
