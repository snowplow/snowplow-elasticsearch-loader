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
package com.snowplowanalytics.stream.loader

// Amazon
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

// Java
import java.io.IOException
import java.util.{List => List}

// cats
import cats.data.Validated

// Scala
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.{List => SList}
import scala.collection.JavaConverters._

// This project
import sinks.ISink
import clients.BulkSender

/**
 * Emitter class for any sort of BulkSender Extension
 *
 * @param bulkSender        The bulkSender Client to use for the sink
 * @param goodSink          the configured GoodSink
 * @param badSink           the configured BadSink
 * @param bufferRecordLimit record limit for buffer
 * @param bufferByteLimit   byte limit for buffer
 */
class Emitter(
  bulkSender: BulkSender[EmitterJsonInput],
  goodSink: Option[ISink],
  badSink: ISink,
  bufferRecordLimit: Long,
  bufferByteLimit: Long
) extends IEmitter[EmitterJsonInput] {

  @throws[IOException]
  override def emit(buffer: UnmodifiableBuffer[EmitterJsonInput]): List[EmitterJsonInput] =
    attemptEmit(buffer.getRecords.asScala.toList).asJava

  /**
   * Emits good records to stdout or sink.
   * All records which sink rejects and all records which failed transformation
   * get sent to to stderr or Kinesis.
   *
   * @param records list containing EmitterJsonInputs
   * @return list of inputs which failed transformation or which the sink rejected
   */
  @throws[IOException]
  private def attemptEmit(records: SList[EmitterJsonInput]): SList[EmitterJsonInput] = {
    if (records.isEmpty) {
      null
    } else {
      val (validRecords: SList[EmitterJsonInput], invalidRecords: SList[EmitterJsonInput]) =
        records.partition(_._2.isValid)
      // Send all valid records to stdout / Sink and return those rejected by it
      val rejects = goodSink match {
        case Some(s) =>
          validRecords.foreach {
            case (_, record) => record.map(r => s.store(r.json.toString, None, true))
          }
          Nil
        case None if validRecords.isEmpty => Nil
        case _                            => emit(validRecords)
      }
      invalidRecords ++ rejects
    }
  }

  /**
   * Emits good records to Sink and bad records to Kinesis.
   * All valid records in the buffer get sent to the sink in a bulk request.
   * All invalid requests and all requests which failed transformation get sent to Kinesis.
   *
   * @param records List of records to send
   * @return List of inputs which the sink rejected
   */
  def emit(records: SList[EmitterJsonInput]): SList[EmitterJsonInput] = {
    val failures: SList[SList[EmitterJsonInput]] = for {
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
  private def splitBuffer(
    records: SList[EmitterJsonInput],
    byteLimit: Long,
    recordLimit: Long
  ): SList[SList[EmitterJsonInput]] = {
    // partition the records in
    val remaining: ListBuffer[EmitterJsonInput]      = records.to[ListBuffer]
    val buffers: ListBuffer[SList[EmitterJsonInput]] = new ListBuffer
    val curBuffer: ListBuffer[EmitterJsonInput]      = new ListBuffer
    var runningByteCount: Long                       = 0L

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
  override def shutdown(): Unit = bulkSender.close()

  /**
   * Handles records rejected by the JsonTransformer or by Sink
   *
   * @param records List of failed records
   */
  override def fail(records: List[EmitterJsonInput]): Unit = {
    records.asScala.foreach {
      case (r: String, Validated.Invalid(fs)) =>
        val output = BadRow(r, fs).toCompactJson
        badSink.store(output, None, false)
      case (_, Validated.Valid(_)) => ()
    }
  }

}
