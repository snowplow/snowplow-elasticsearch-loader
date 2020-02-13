/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.stream.loader

// Scala
import org.json4s.jackson.JsonMethods._

// cats
import cats.syntax.validated._

// Specs2
import org.specs2.mutable.Specification

// This project
import transformers.BadEventTransformer

/**
 * Tests BadEventTransformer
 */
class BadEventTransformerSpec extends Specification {

  val documentIndex = "snowplow"
  val documentType  = "enriched"

  "The from method" should {
    "successfully convert a bad event JSON to an ElasticsearchObject" in {
      val input =
        """{"line":"failed","errors":["Record does not match Thrift SnowplowRawEvent schema"]}"""
      val result =
        new BadEventTransformer().fromClass(input -> JsonRecord(parse(input), None).valid)
      val json: String = compact(
        render(result._2.getOrElse(throw new RuntimeException("Json failed transformation")).json))
      json.toString must_== input
    }
  }

}
