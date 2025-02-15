/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.stream.loader

import java.util.Properties
import org.slf4j.Logger

// Scala
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

// AWS libs
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{KinesisConnectorConfiguration, UnmodifiableBuffer}
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer

// cats
import cats.syntax.validated._

import io.circe.Json

// Specs2
import org.specs2.mutable.Specification

// This project
import sinks._
import clients._
import Config.Sink.GoodSink.Elasticsearch.ESChunk

case class MockElasticsearchSender(chunkConf: ESChunk) extends BulkSender[EmitterJsonInput] {
  var sentRecords: List[EmitterJsonInput]       = List.empty
  var callCount: Int                            = 0
  val calls: ListBuffer[List[EmitterJsonInput]] = new ListBuffer
  override val log: Logger                      = null
  override def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {
    sentRecords = sentRecords ::: records
    callCount += 1
    calls += records
    List.empty
  }
  override def close() = {}
  override def chunkConfig(): ESChunk = chunkConf
  override val tracker                = None
}

class EmitterSpec extends Specification {
  val documentType = "enriched"

  "The emitter" should {
    "return all invalid records" in {

      val fakeSender: BulkSender[EmitterJsonInput] = new BulkSender[EmitterJsonInput] {
        override def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = List.empty
        override def close(): Unit                                                 = ()
        override def chunkConfig(): ESChunk                                        = ESChunk(1L, 1L)
        override val tracker                                                       = None
        override val log: Logger                                                   = null
      }

      val kcc =
        new KinesisConnectorConfiguration(new Properties, new DefaultAWSCredentialsProviderChain)
      val eem = new Emitter(Right(fakeSender), new StdouterrSink)

      val validInput: EmitterJsonInput   = "good" -> JsonRecord(Json.obj(), None).valid
      val invalidInput: EmitterJsonInput = "bad"  -> "malformed event".invalidNel

      val input = List(validInput, invalidInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub  = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      val actual = eem.emit(ub)

      actual must_== List(invalidInput).asJava
    }

    "send multiple records in seperate requests where single record size > buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1000")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = MockElasticsearchSender(ESChunk(1000L, 1L))
      val eem = new Emitter(Right(ess), new StdouterrSink)

      val validInput: EmitterJsonInput = "good" -> JsonRecord(Json.obj(), None).valid

      val input = List.fill(50)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub  = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 50
      forall(ess.calls) { c =>
        c.length mustEqual 1
      }
    }

    "send a single record in 1 request where record size > buffer bytes size " in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1000")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = MockElasticsearchSender(ESChunk(1000L, 1L))
      val eem = new Emitter(Right(ess), new StdouterrSink)

      val validInput: EmitterJsonInput = "good" -> JsonRecord(Json.obj(), None).valid

      val input = List(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub  = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall(ess.calls) { c =>
        c.length mustEqual 1
      }
    }

    "send multiple records in 1 request where total byte size < buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1048576")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = MockElasticsearchSender(ESChunk(1048576L, 100L))
      val eem = new Emitter(Right(ess), new StdouterrSink)

      val validInput: EmitterJsonInput = "good" -> JsonRecord(Json.obj(), None).valid

      val input = List.fill(50)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub  = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall(ess.calls) { c =>
        c.length mustEqual 50
      }
    }

    "send a single record in 1 request where single record size < buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1048576")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = MockElasticsearchSender(ESChunk(1048576L, 1L))
      val eem = new Emitter(Right(ess), new StdouterrSink)

      val validInput: EmitterJsonInput = "good" -> JsonRecord(Json.obj(), None).valid

      val input = List(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub  = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall(ess.calls) { c =>
        c.length mustEqual 1
      }
    }

    "send multiple records in batches where single record byte size < buffer size and total byte size > buffer size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "200")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = MockElasticsearchSender(ESChunk(200L, 2L))
      val eem = new Emitter(Right(ess), new StdouterrSink)

      // record size is 95 bytes
      val validInput: EmitterJsonInput = "good" -> JsonRecord(Json.obj(), None).valid

      val input = List.fill(20)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub  = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 10 // 10 buffers of 2 records each
      forall(ess.calls) { c =>
        c.length mustEqual 2
      }
    }
  }

}
