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
package com.snowplowanalytics.stream.loader.clients

import scala.concurrent.ExecutionContext.Implicits.global

// elastic4s
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient

import cats.syntax.validated._

import io.circe.literal._

import com.snowplowanalytics.stream.loader.{EmitterJsonInput, JsonRecord}
import com.snowplowanalytics.stream.loader.Config.Sink.GoodSink.Elasticsearch.ESChunk
import com.snowplowanalytics.stream.loader.Config.Region

// specs2
import org.specs2.mutable.Specification

class ElasticsearchBulkSenderSpec extends Specification {
  val elasticHost = "127.0.0.1"
  val elasticPort = 28875
  val client      = ElasticClient(JavaClient(ElasticProperties(s"http://$elasticHost:$elasticPort")))
  val index       = "idx"
  val sender = new ElasticsearchBulkSender(
    elasticHost,
    elasticPort,
    false,
    false,
    Region("region"),
    None,
    None,
    index,
    None,
    30000L,
    10000L,
    None,
    6,
    ESChunk(1L, 1L)
  )

  client.execute(createIndex(index)).await

  "send" should {
    "successfully send stuff" in {

      val validInput: EmitterJsonInput = "good" -> JsonRecord(json"""{"s":"json"}""", None).valid
      val input                        = List(validInput)

      sender.send(input) must_== List.empty
      // eventual consistency
      Thread.sleep(1000)
      client
        .execute(search(index))
        .await
        .result
        .hits
        .hits
        .head
        .sourceAsString must_== """{"s":"json"}"""
    }

    "report old failures" in {
      val data = List(("a", "f".invalidNel))
      sender.send(data) must_== List(("a", "f".invalidNel))
    }
  }
}
