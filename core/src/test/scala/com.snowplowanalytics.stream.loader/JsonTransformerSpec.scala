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
import transformers.JsonTransformer

/**
 * Tests PlainJsonTransformer
 */
class JsonTransformerSpec extends Specification {

  val documentIndex = "snowplow"
  val documentType  = "enriched"

  "The from method" should {
    "successfully convert a  plain JSON to an JsonRecord" in {
      val input = json"""{"key1":"value1","key2":"value2","key3":"value3"}"""

      val result =
        new JsonTransformer().fromClass(input.noSpaces -> JsonRecord(input, None).valid)
      val json =
        result._2.getOrElse(throw new RuntimeException("Plain Json failed transformation")).json
      json must_== input
    }
  }

}
