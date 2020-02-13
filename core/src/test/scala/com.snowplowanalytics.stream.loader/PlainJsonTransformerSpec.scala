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
package com.snowplowanalytics.stream.loader

// Scala
import org.json4s.jackson.JsonMethods.{compact, parse, render}

// Scalaz
import cats.syntax.validated._

// Specs2
import org.specs2.mutable.Specification

// This project
import transformers.PlainJsonTransformer

/**
 * Tests PlainJsonTransformer
 */
class PlainJsonTransformerSpec extends Specification {

  val documentIndex = "snowplow"
  val documentType  = "enriched"

  "The from method" should {
    "successfully convert a  plain JSON to an JsonRecord" in {
      val input = """{"key1":"value1","key2":"value2","key3":"value3"}"""

      val result =
        new PlainJsonTransformer().fromClass(input -> JsonRecord(parse(input), None).valid)
      val json: String = compact(
        render(
          result._2.getOrElse(throw new RuntimeException("Plain Json failed transformation")).json))
      json.toString must_== input
    }
  }

}
