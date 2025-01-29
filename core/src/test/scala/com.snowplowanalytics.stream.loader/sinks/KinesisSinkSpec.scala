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
      val recordLimit = 100
      val byteLimit   = 1000
      val inputs = List(
        ("abc" -> "foo".getBytes),
        ("def" -> "bar".getBytes)
      )
      val result = KinesisSink.groupOutputs(recordLimit, byteLimit)(inputs)

      checkEquality(result, List(inputs))
    }

    "split a list if it reaches recordLimit" in {
      val recordLimit = 2
      val byteLimit   = 1000
      val inputs = List(
        ("abc" -> "foo".getBytes),
        ("def" -> "bar".getBytes),
        ("ghi" -> "baz".getBytes)
      )
      val result = KinesisSink.groupOutputs(recordLimit, byteLimit)(inputs)
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
      val recordLimit = 100
      val byteLimit   = 12
      val inputs = List(
        ("abc" -> "foo".getBytes),
        ("def" -> "bar".getBytes),
        ("ghi" -> "baz".getBytes)
      )
      val result = KinesisSink.groupOutputs(recordLimit, byteLimit)(inputs)
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
      val recordLimit = 2
      val byteLimit   = 12
      val inputs = List(
        ("abc"    -> "foo".getBytes),
        ("def"    -> "bar".getBytes),
        ("ghi"    -> "baz".getBytes),
        ("abcdef" -> "foobarbaz".getBytes)
      )
      val result = KinesisSink.groupOutputs(recordLimit, byteLimit)(inputs)
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
