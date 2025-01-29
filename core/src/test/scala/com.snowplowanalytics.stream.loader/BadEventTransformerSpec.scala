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

// cats
import cats.syntax.validated._

import io.circe.literal._

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

  val processor = json"""{"artifact": "snowplow-rdb-shredder", "version": "0.11.0"}"""

  "fromClass" should {
    "successfully convert a bad event JSON to an ElasticsearchObject" in {
      val input =
        json"""{"line":"failed","errors":["Record does not match Thrift SnowplowRawEvent schema"]}"""
      val result =
        new BadEventTransformer().fromClass(input.noSpaces -> JsonRecord(input, None).valid)
      val json =
        result._2.getOrElse(throw new RuntimeException("Json failed transformation")).json
      json must_== input
    }
  }

  "transform" should {
    "fix payload string" >> {
      val input    = json"""{"processor": $processor, "failure": {}, "payload": "foo"}"""
      val expected = json"""{"processor": $processor, "failure": {}, "payload_str": "foo"}"""
      val result   = transformers.BadEventTransformer.transform(input)
      result must beEqualTo(expected)
    }
  }

}
