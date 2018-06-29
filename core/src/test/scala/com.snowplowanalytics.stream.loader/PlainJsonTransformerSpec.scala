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

// Specs2
import org.specs2.mutable.Specification

/**
 * Tests PlainJsonTransformer
 */
class PlainJsonTransformerSpec extends Specification {

  "The from method" should {
    "successfully convert a  plain JSON to an ElasticsearchObject" in {
      val input  = """{"key1":"value1", "key2":"value2", "key3":"value3"}"""
      val result = new PlainJsonTransformer("snowplow", "PlainJson").consumeLine(input)
      val elasticsearchObject =
        result._2.getOrElse(throw new RuntimeException("Plain Json failed transformation"))
      elasticsearchObject.getIndex must_== "snowplow"
      elasticsearchObject.getType must_== "PlainJson"
      elasticsearchObject.getSource must_== input
    }
  }

}
