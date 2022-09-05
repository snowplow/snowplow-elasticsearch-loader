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
import scala.collection.JavaConverters._

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
  import KinesisSink._

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

  private def put(name: String, keyedData: List[KeyedData]): PutRecordsResult = {
    val prres = keyedData.map { case (key, data) =>
      new PutRecordsRequestEntry()
        .withPartitionKey(key)
        .withData(ByteBuffer.wrap(data))
    }
    val putRecordsRequest =
      new PutRecordsRequest()
        .withStreamName(name)
        .withRecords(prres.asJava)
    client.putRecords(putRecordsRequest)
  }

  /**
   * Write records to the Kinesis stream
   *
   * @param outputs The string records to write
   * @param good Unused parameter which exists to extend ISink
   */
  def store(outputs: List[String], good: Boolean): Unit =
    groupOutputs(conf.recordLimit, conf.byteLimit) {
      outputs.map(s => Random.nextInt.toString -> s.getBytes(UTF_8))
    }.foreach { keyedData =>
      Try {
        put(
          conf.streamName,
          keyedData
        )
      } match {
        case Success(result) =>
          log.info("Writing successful")
          result.getRecords.asScala.foreach { record =>
            log.debug(s"  + ShardId: ${record.getShardId}")
            log.debug(s"  + SequenceNumber: ${record.getSequenceNumber}")
          }
        case Failure(f) =>
          log.error("Writing to Kinesis failed", f)
      }
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

object KinesisSink {

  // Represents a partition key and the serialized record content
  private type KeyedData = (String, Array[Byte])

  /**
   *  Takes a list of records and splits it into several lists, where each list is as big as
   *  possible with respecting the record limit and the size limit.
   */
  def groupOutputs(recordLimit: Int, byteLimit: Int)(
    keyedData: List[KeyedData]
  ): List[List[KeyedData]] = {
    case class Batch(size: Int, count: Int, keyedData: List[KeyedData])

    keyedData
      .foldLeft(List.empty[Batch]) { case (acc, (key, data)) =>
        val recordSize = data.length + key.getBytes(UTF_8).length
        acc match {
          case head :: tail =>
            if (head.count + 1 > recordLimit || head.size + recordSize > byteLimit)
              List(Batch(recordSize, 1, List(key -> data))) ++ List(head) ++ tail
            else
              List(
                Batch(head.size + recordSize, head.count + 1, (key -> data) :: head.keyedData)
              ) ++ tail
          case Nil =>
            List(Batch(recordSize, 1, List(key -> data)))
        }
      }
      .map(_.keyedData.reverse)
      .reverse
  }

}
