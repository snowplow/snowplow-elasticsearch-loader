/**
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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

// elastic4s
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.embedded.LocalNode

import cats.syntax.validated._

import io.circe.literal._

import com.snowplowanalytics.stream.loader.{CredentialsLookup, EmitterJsonInput, JsonRecord}

// specs2
import org.specs2.mutable.Specification

class ElasticsearchBulkSenderSpec extends Specification {
  def init() = {
    val node = LocalNode("es", System.getProperty("java.io.tmpdir"))
    node.start()
    val client       = node.elastic4sclient()
    val creds        = CredentialsLookup.getCredentialsProvider("a", "s")
    val documentType = "enriched"
    val index        = "idx"
    val sender = new ElasticsearchBulkSender(
      node.ip,
      node.port,
      false,
      "region",
      false,
      None,
      None,
      index,
      documentType,
      10000L,
      creds,
      None)

//    client.execute(createIndex(index)).await

    (sender, index, client)
  }

  val (sender, index, client) = init()

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
