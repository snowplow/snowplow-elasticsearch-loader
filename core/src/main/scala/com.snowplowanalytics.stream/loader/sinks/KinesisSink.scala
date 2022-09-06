/**
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd.
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

// Java
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

// SLF4j
import org.slf4j.LoggerFactory

// Scala
import scala.util.{Failure, Random, Success, Try}

// Amazon
import com.amazonaws.services.kinesis.model._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import com.snowplowanalytics.stream.loader.Config.Sink.BadSink.{Kinesis => KinesisSinkConfig}

/**
 * Kinesis Sink
 *
 * @param conf Config for Kinesis sink
 */
class KinesisSink(conf: KinesisSinkConfig) extends ISink {

  private lazy val log = LoggerFactory.getLogger(getClass)

  // Explicitly create a client so we can configure the end point
  val client = AmazonKinesisClientBuilder
    .standard()
    .withCredentials(new DefaultAWSCredentialsProviderChain())
    // Region should be set in the EndpointConfiguration when custom endpoint is used
    .condWith(conf.customEndpoint.isEmpty, _.withRegion(conf.region.name))
    .optWith[String](
      conf.customEndpoint,
      b => e => b.withEndpointConfiguration(new EndpointConfiguration(e, conf.region.name))
    )
    .build()

  require(
    streamExists(conf.streamName),
    s"Stream ${conf.streamName} doesn't exist or is neither active nor updating (deleted or creating)"
  )

  /**
   * Checks if a stream exists.
   *
   * @param name Name of the stream to look for
   * @return Whether the stream both exists and is active
   */
  def streamExists(name: String): Boolean =
    try {
      val describeStreamResult = client.describeStream(name)
      val status               = describeStreamResult.getStreamDescription.getStreamStatus
      status == "ACTIVE" || status == "UPDATING"
    } catch {
      case rnfe: ResourceNotFoundException => false
    }

  private def put(name: String, data: ByteBuffer, key: String): PutRecordResult = {
    val putRecordRequest = {
      val p = new PutRecordRequest()
      p.setStreamName(name)
      p.setData(data)
      p.setPartitionKey(key)
      p
    }
    client.putRecord(putRecordRequest)
  }

  /**
   * Write a record to the Kinesis stream
   *
   * @param output The string record to write
   * @param key A hash of the key determines to which shard the
   *            record is assigned. Defaults to a random string.
   * @param good Unused parameter which exists to extend ISink
   */
  def store(output: String, key: Option[String], good: Boolean): Unit =
    Try {
      put(
        conf.streamName,
        ByteBuffer.wrap(output.getBytes(UTF_8)),
        key.getOrElse(Random.nextInt.toString)
      )
    } match {
      case Success(result) =>
        log.info("Writing successful")
        log.info(s"  + ShardId: ${result.getShardId}")
        log.info(s"  + SequenceNumber: ${result.getSequenceNumber}")
      case Failure(f) =>
        log.error("Writing failed")
        log.error("  + " + f.getMessage)
    }

  implicit class AwsKinesisClientBuilderExtensions(builder: AmazonKinesisClientBuilder) {
    def optWith[A](
      opt: Option[A],
      f: AmazonKinesisClientBuilder => A => AmazonKinesisClientBuilder
    ): AmazonKinesisClientBuilder =
      opt.map(f(builder)).getOrElse(builder)
    def condWith[A](
      cond: => Boolean,
      f: AmazonKinesisClientBuilder => AmazonKinesisClientBuilder
    ): AmazonKinesisClientBuilder =
      if (cond) f(builder) else builder
  }
}
