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
package transformers

// Java
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

// Amazon
import com.amazonaws.services.kinesis.model.Record

import cats.data.ValidatedNel
import cats.syntax.show._
import cats.syntax.validated._

import io.circe.JsonObject
import io.circe.syntax._
import io.circe.parser.parse

/**
 * Class to convert plain JSON to EmitterInputs
 */
class JsonTransformer extends IJsonTransformer {

  /**
   * Convert an Amazon Kinesis record to a json string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedRecord for the event
   */
  override def toClass(record: Record): ValidatedJsonRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    (recordString, toJsonRecord(recordString))
  }

  /**
   * Parses a json string as a JsonRecord.
   *
   * @param jsonString the JSON string to be parsed
   * @return the parsed JsonRecord
   */
  private def toJsonRecord(jsonString: String): ValidatedNel[String, JsonRecord] =
    parse(jsonString).flatMap(_.as[JsonObject]) match {
      case Right(json) =>
        JsonRecord(json.add("id", UUID.randomUUID().asJson).asJson, None).validNel
      case Left(error) => s"Json parsing error, ${error.show}".invalidNel
    }

  /**
   * Consume data from stdin/NSQ rather than Kinesis
   *
   * @param line Line from stdin/NSQ
   * @return Line as an EmitterInput
   */
  def consumeLine(line: String): EmitterJsonInput =
    fromClass(line -> toJsonRecord(line))

}
