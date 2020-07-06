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
package emitters

// Amazon
import cats.data.NonEmptyList
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter
import com.snowplowanalytics.stream.loader.clients.BulkSender
import com.snowplowanalytics.stream.loader.sinks.ISink
import com.snowplowanalytics.stream.loader.EsLoaderBadRow

import scala.collection.mutable.ListBuffer

// Java
import java.io.IOException
import java.util.{List => List}

// cats
import cats.data.Validated

// Scala
import scala.collection.JavaConverters._
import scala.collection.immutable.{List => SList}

// This project

/**
 * Emitter class for any sort of BulkSender Extension
 *
 * @param bulkSender        The bulkSender Client to use for the sink
 * @param badSink           the configured BadSink
 * @param bufferRecordLimit record limit for buffer
 * @param bufferByteLimit   byte limit for buffer
 */
class Emitter[T](
  bulkSender: BulkSender[T],
  badSink: ISink,
  bufferRecordLimit: Long,
  bufferByteLimit: Long
) extends IEmitter[T] {

  /**
   * This function is called from AWS library.
   * Emits good records to Kinesis, Postgres, S3 or Elasticsearch.
   * All records which Elasticsearch rejects and all records which failed transformation
   * get sent to to stderr or Kinesis.
   *
   * @param buffer list containing EmitterInputs
   * @return list of inputs which failed transformation or which Elasticsearch rejected
   */
  @throws[IOException]
  def emit(buffer: UnmodifiableBuffer[T]): List[T] =
    if (buffer.getRecords.asScala.isEmpty) {
      null
    } else {
      // Send all valid records to bulk sender and returned rejected/unvalidated ones.
      sliceAndSend(buffer.getRecords.asScala.toList)
    }.asJava

  /**
   * This is called from NsqSourceExecutor
   * Emits good records to Sink and bad records to Kinesis.
   * All valid records in the buffer get sent to the sink in a bulk request.
   * All invalid requests and all requests which failed transformation get sent to Kinesis.
   *
   * @param records List of records to send
   * @return List of inputs which the sink rejected
   */
  def emitList(records: SList[T]): SList[T] =
    for {
      recordSlice <- splitBuffer(records, bufferByteLimit, bufferRecordLimit)
      result      <- bulkSender.send(recordSlice)
    } yield result

  /**
   * Emits good records to Elasticsearch and bad records to Kinesis.
   * All valid records in the buffer get sent to Elasticsearch in a bulk request.
   * All invalid requests and all requests which failed transformation get sent to Kinesis.
   *
   * @param records List of records to send to Elasticsearch
   * @return List of inputs which Elasticsearch rejected
   */
  def sliceAndSend(records: SList[T]): SList[T] = {
    val failures: SList[SList[T]] = for {
      recordSlice <- splitBuffer(records, bufferByteLimit, bufferRecordLimit)
    } yield bulkSender.send(recordSlice)
    failures.flatten
  }

  /**
   * Splits the buffer into emittable chunks based on the
   * buffer settings defined in the config
   *
   * @param records     The records to split
   * @param byteLimit   emitter byte limit
   * @param recordLimit emitter record limit
   * @return a list of buffers
   */
  def splitBuffer(
    records: SList[T],
    byteLimit: Long,
    recordLimit: Long
  ): SList[SList[T]] = {
    // partition the records in
    val remaining: ListBuffer[T]      = records.to[ListBuffer]
    val buffers: ListBuffer[SList[T]] = new ListBuffer
    val curBuffer: ListBuffer[T]      = new ListBuffer
    var runningByteCount: Long        = 0L

    while (remaining.nonEmpty) {
      val record = remaining.remove(0)

      val byteCount: Long = record match {
        case (_, Validated.Valid(obj)) => obj.toString.getBytes("UTF-8").length.toLong
        case (_, Validated.Invalid(_)) => 0L // This record will be ignored in the sender
      }

      if ((curBuffer.length + 1) > recordLimit || (runningByteCount + byteCount) > byteLimit) {
        // add this buffer to the output and start a new one with this record
        // (if the first record is larger than the byte limit the buffer will be empty)
        if (curBuffer.nonEmpty) {
          buffers += curBuffer.toList
          curBuffer.clear()
        }
        curBuffer += record
        runningByteCount = byteCount
      } else {
        curBuffer += record
        runningByteCount += byteCount
      }
    }

    // add any remaining items to the final buffer
    if (curBuffer.nonEmpty) buffers += curBuffer.toList

    buffers.toList
  }

  /**
   * Closes the Sink client when the KinesisConnectorRecordProcessor is shut down
   */
  override def shutdown(): Unit = {
    println("Shutting down emitter")
    bulkSender.close()
  }

  /**
   * Handles records rejected by the JsonTransformer or by Sink
   *
   * @param records List of failed records
   */
  override def fail(records: List[T]): Unit = {
    records.asScala.foreach {
      case (r: String, Validated.Invalid(fs: NonEmptyList[String])) =>
        val output = EsLoaderBadRow(r, fs).toCompactJson
        badSink.store(output, None, false)
      case (_, Validated.Valid(_)) => ()
    }
  }

}
