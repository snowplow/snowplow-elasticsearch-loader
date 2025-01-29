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

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.model.Record

// Java
import java.nio.charset.StandardCharsets.UTF_8
import java.text.SimpleDateFormat

import org.joda.time.{DateTime, DateTimeZone}

// cats
import cats.data.{Validated, ValidatedNel}
import cats.syntax.validated._
import cats.syntax.option._

import io.circe.syntax._

// Snowplow
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

/** Class to convert successfully enriched events to EmitterInputs */
class EnrichedEventJsonTransformer(shardDateField: Option[String], shardDateFormat: Option[String])
    extends ITransformer[ValidatedJsonRecord, EmitterJsonInput]
    with StdinTransformer {

  private val dateFormatter = shardDateFormat match {
    case Some(format) => new SimpleDateFormat(format).some
    case _            => None
  }

  private val shardingField = shardDateField.getOrElse("derived_tstamp")

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedJsonRecord for the event
   */
  override def toClass(record: Record): ValidatedJsonRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    (recordString, toJsonRecord(recordString))
  }

  /**
   * Parses a string as a JsonRecord.
   * The -1 is necessary to prevent trailing empty strings from being discarded
   *
   * @param record the record to be parsed
   * @return the parsed JsonRecord or a list of failures
   */
  private def toJsonRecord(record: String): ValidatedNel[String, JsonRecord] =
    Event.parse(record) match {
      case Validated.Invalid(error) => error.asJson.noSpaces.invalidNel
      case Validated.Valid(event) =>
        val json = event.toJson(true)
        dateFormatter match {
          case Some(formatter) =>
            val shard = event.atomic.get(shardingField).flatMap(_.asString) match {
              case Some(timestampString) =>
                formatter
                  .format(
                    DateTime
                      .parse(timestampString)
                      .withZone(DateTimeZone.UTC)
                      .getMillis
                  )
                  .some
              case _ => None
            }
            JsonRecord(json, shard).validNel
          case None =>
            JsonRecord(json, None).validNel
        }
    }

  /**
   * Consume data from stdin rather than Kinesis
   *
   * @param line Line from stdin
   * @return Line as an EmitterJsonInput
   */
  def consumeLine(line: String): EmitterJsonInput =
    fromClass(line -> toJsonRecord(line))
}
