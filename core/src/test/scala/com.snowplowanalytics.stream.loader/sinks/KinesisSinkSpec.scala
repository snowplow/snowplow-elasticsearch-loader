/**
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.stream.loader.sinks

import com.snowplowanalytics.stream.loader.Config.Sink.BadSink.{Kinesis => KinesisSinkConfig}
import com.snowplowanalytics.stream.loader.Config

import org.specs2.mutable.Specification
import org.specs2.matcher.MatchResult

class KinesisSinkSpec extends Specification {

  val baseConfig: KinesisSinkConfig =
    KinesisSinkConfig("name", Config.Region("ap-east-1"), None, 0, 0)

  "group outputs" should {
    "leave a list untouched if its number of records and its size are below the limits" in {
      val config = baseConfig.copy(recordLimit = 100, byteLimit = 1000)
      val inputs = List(
        ("abc" -> "foo".getBytes),
        ("def" -> "bar".getBytes)
      )
      val result = KinesisSink.groupOutputs(config)(inputs)

      checkEquality(result, List(inputs))
    }

    "split a list if it reaches recordLimit" in {
      val config = baseConfig.copy(recordLimit = 2, byteLimit = 1000)
      val inputs = List(
        ("abc" -> "foo".getBytes),
        ("def" -> "bar".getBytes),
        ("ghi" -> "baz".getBytes)
      )
      val result = KinesisSink.groupOutputs(config)(inputs)
      val expected = List(
        List(
          ("abc" -> "foo".getBytes),
          ("def" -> "bar".getBytes)
        ),
        List(
          ("ghi" -> "baz".getBytes)
        )
      )

      checkEquality(result, expected)
    }

    "split a list if it reaches sizeLimit" in {
      val config = baseConfig.copy(recordLimit = 100, byteLimit = 12)
      val inputs = List(
        ("abc" -> "foo".getBytes),
        ("def" -> "bar".getBytes),
        ("ghi" -> "baz".getBytes)
      )
      val result = KinesisSink.groupOutputs(config)(inputs)
      val expected = List(
        List(
          ("abc" -> "foo".getBytes),
          ("def" -> "bar".getBytes)
        ),
        List(
          ("ghi" -> "baz".getBytes)
        )
      )

      checkEquality(result, expected)
    }

    "split a list if it reaches recordLimit and then sizeLimit" in {
      val config = baseConfig.copy(recordLimit = 2, byteLimit = 12)
      val inputs = List(
        ("abc"    -> "foo".getBytes),
        ("def"    -> "bar".getBytes),
        ("ghi"    -> "baz".getBytes),
        ("abcdef" -> "foobarbaz".getBytes)
      )
      val result = KinesisSink.groupOutputs(config)(inputs)
      val expected = List(
        List(
          ("abc" -> "foo".getBytes),
          ("def" -> "bar".getBytes)
        ),
        List(
          ("ghi" -> "baz".getBytes)
        ),
        List(
          ("abcdef" -> "foobarbaz".getBytes)
        )
      )
      checkEquality(result, expected)
    }

  }

  def checkEquality(
    result: List[List[(String, Array[Byte])]],
    expected: List[List[(String, Array[Byte])]]
  ): MatchResult[Any] = {
    val resultFixed   = result.map(_.map { case (key, bytes) => key -> bytes.toSeq })
    val expectedFixed = result.map(_.map { case (key, bytes) => key -> bytes.toSeq })
    resultFixed must_== expectedFixed
  }

}
