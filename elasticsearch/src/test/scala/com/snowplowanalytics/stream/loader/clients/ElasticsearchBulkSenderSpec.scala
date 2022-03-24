/**
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.ElasticDsl._

import cats.syntax.validated._

import io.circe.literal._

import com.snowplowanalytics.stream.loader.{CredentialsLookup, EmitterJsonInput, JsonRecord}

// specs2
import org.specs2.mutable.Specification

class ElasticsearchBulkSenderSpec extends Specification {
  val node = LocalNode("es", System.getProperty("java.io.tmpdir"))
  node.start()
  val client       = node.client(true)
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
    None
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
