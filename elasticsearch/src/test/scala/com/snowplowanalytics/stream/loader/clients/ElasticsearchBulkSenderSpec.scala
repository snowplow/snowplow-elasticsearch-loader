/**
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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
    Some("region"),
    None,
    None,
    index,
    None,
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
