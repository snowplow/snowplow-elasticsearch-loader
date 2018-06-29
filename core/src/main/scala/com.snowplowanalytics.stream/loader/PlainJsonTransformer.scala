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
import java.util.UUID

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

/**
 * Class to convert plain JSON to EmitterInputs
 *
 * @param documentIndex the elasticsearch index name
 * @param documentType the elasticsearch index type
 */
class PlainJsonTransformer(documentIndex: String, documentType: String)
    extends ITransformer[ValidatedRecord, EmitterInput]
    with StdinTransformer {

  /**
   * Convert an Amazon Kinesis record to a json string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedRecord for the event
   */
  override def toClass(record: Record): ValidatedRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    (recordString, toJsonRecord(recordString))
  }

  /**
   * Convert plain json to an EmitterInput
   *
   * @param record ValidatedRecord containing plain JSON
   * @return An EmitterInput
   */
  override def fromClass(record: ValidatedRecord): EmitterInput = {
    val uuid = UUID.randomUUID().toString // generate unique UUID for every Json document
    record.map(_.map(r => new ElasticsearchObject(documentIndex, documentType, uuid, r.json)))
  }

  /**
   * Parses a json string as a JsonRecord.
   *
   * @param jsonString the JSON string to be parsed
   * @return the parsed JsonRecord
   */
  private def toJsonRecord(jsonString: String): ValidationNel[String, JsonRecord] = {
    parseOpt(jsonString) match {
      case Some(jvalue) => JsonRecord(jsonString, None).success
      case None         => "Json parsing error".failureNel
    }
  }

  /**
   * Consume data from stdin/NSQ rather than Kinesis
   *
   * @param line Line from stdin/NSQ
   * @return Line as an EmitterInput
   */
  override def consumeLine(line: String): EmitterInput =
    fromClass(line -> toJsonRecord(line))

}
