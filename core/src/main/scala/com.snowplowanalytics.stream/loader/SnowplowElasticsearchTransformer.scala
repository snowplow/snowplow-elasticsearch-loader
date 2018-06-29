/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.stream.loader

// Java
import java.nio.charset.StandardCharsets.UTF_8

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.model.Record

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer._

/**
 * Class to convert successfully enriched events to EmitterInputs
 *
 * @param documentIndex the elasticsearch index name
 * @param documentType the elasticsearch index type
 */
class SnowplowElasticsearchTransformer(documentIndex: String, documentType: String)
    extends ITransformer[ValidatedRecord, EmitterInput]
    with StdinTransformer {

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedRecord for the event
   */
  override def toClass(record: Record): ValidatedRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    (recordString, toJsonRecord(recordString))
  }

  /**
   * Convert a buffered event JSON to an EmitterInput
   *
   * @param record ValidatedRecord containing a good event JSON
   * @return An EmitterInput
   */
  override def fromClass(record: ValidatedRecord): EmitterInput =
    record.map(_.map(r =>
      r.id match {
        case Some(id) => new ElasticsearchObject(documentIndex, documentType, id, r.json)
        case None     => new ElasticsearchObject(documentIndex, documentType, r.json)
    }))

  /**
   * Parses a string as a JsonRecord.
   * The -1 is necessary to prevent trailing empty strings from being discarded
   * @param record the record to be parsed
   * @return the parsed JsonRecord or a list of failures
   */
  private def toJsonRecord(record: String): ValidationNel[String, JsonRecord] =
    jsonifyGoodEvent(record.split("\t", -1)) match {
      case Left(h :: t)     => NonEmptyList(h, t: _*).failure
      case Left(Nil)        => "Empty list of failures but reported failure, should not happen".failureNel
      case Right((_, json)) => JsonRecord(compact(render(json)), extractEventId(json)).success
    }

  /**
   * Extract the event_id field from an event JSON for use as a document ID
   * @param json object produced by the Snowplow Analytics SDK
   * @return Option boxing event_id
   */
  private def extractEventId(json: JValue): Option[String] = {
    json \ "event_id" match {
      case JString(eid) => eid.some
      case _            => None
    }
  }

  /**
   * Consume data from stdin rather than Kinesis
   *
   * @param line Line from stdin
   * @return Line as an EmitterInput
   */
  def consumeLine(line: String): EmitterInput =
    fromClass(line -> toJsonRecord(line))

}
