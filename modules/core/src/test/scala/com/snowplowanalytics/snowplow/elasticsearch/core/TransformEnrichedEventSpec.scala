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
package com.snowplowanalytics.snowplow.elasticsearch.core

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.{Base64, UUID}
import io.circe.Json
import io.circe.parser.{parse => parseJson}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}
import org.specs2.Specification

class TransformEnrichedEventSpec extends Specification {
  import TransformEnrichedEventSpec._

  def is = s2"""
  transformEnrichedEvent should
    parse a valid TSV enriched event to JSON                         $parsesValidTsvToJson
    include unstruct_event, contexts as shredded JSON fields         $parsesUnstructAndContextsToJsonFields
    extract collector_tstamp as the IndexableRecord timestamp        $extractsCollectorTstampAsTimestamp
    set event_id as the IndexableRecord document id                  $setsEventIdAsDocumentId
    return a BadRow for an invalid TSV line                          $returnsBadRowForInvalidTsv
    compute shardSuffix when sharding is configured                  $computesShardSuffixWhenShardingConfigured
    compute shardSuffix from a dateField other than collector_tstamp $computesShardSuffixFromNonCollectorTstampDateField
    return None shardSuffix when sharding is not configured          $returnsNoneShardSuffixWhenNoSharding
  """

  def parsesValidTsvToJson = {
    val bytes  = toByteBuffer(testEvent)
    val result = Processing.transformEnrichedEvent(None)(bytes, processor)
    result must beRight { r: Processing.IndexableRecord =>
      val json = parseJson(r.record).getOrElse(Json.Null)
      (json.hcursor.get[String]("v_collector") must beRight("v_collector"))
        .and(json.hcursor.get[String]("v_etl") must beRight("v_etl"))
    }
  }

  def parsesUnstructAndContextsToJsonFields = {
    val unstructJson = Json.obj("targetUrl" -> Json.fromString("https://example.com"))
    val contextJson  = Json.obj("id" -> Json.fromString("abc-123"))
    val derivedJson  = Json.obj("useragent" -> Json.fromString("Mozilla/5.0"))
    val event = testEvent.copy(
      unstruct_event = UnstructEvent(
        Some(SelfDescribingData(SchemaKey("com.example", "link_click", "jsonschema", SchemaVer.Full(1, 0, 0)), unstructJson))
      ),
      contexts =
        Contexts(List(SelfDescribingData(SchemaKey("com.example", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0)), contextJson))),
      derived_contexts =
        Contexts(List(SelfDescribingData(SchemaKey("com.example", "user_agent", "jsonschema", SchemaVer.Full(1, 0, 0)), derivedJson)))
    )
    val result = Processing.transformEnrichedEvent(None)(toByteBuffer(event), processor)
    result must beRight { r: Processing.IndexableRecord =>
      val cursor = parseJson(r.record).getOrElse(Json.Null).hcursor
      (cursor.downField("unstruct_event_com_example_link_click_1").focus must beSome(unstructJson))
        .and(
          cursor.downField("contexts_com_example_web_page_1").focus must beSome(
            Json.arr(contextJson.deepMerge(Json.obj("_schema_version" -> Json.fromString("iglu:com.example/web_page/jsonschema/1-0-0"))))
          )
        )
        .and(
          cursor.downField("contexts_com_example_user_agent_1").focus must beSome(
            Json.arr(derivedJson.deepMerge(Json.obj("_schema_version" -> Json.fromString("iglu:com.example/user_agent/jsonschema/1-0-0"))))
          )
        )
    }
  }

  def extractsCollectorTstampAsTimestamp = {
    val bytes  = toByteBuffer(testEvent)
    val result = Processing.transformEnrichedEvent(None)(bytes, processor)
    result must beRight { r: Processing.IndexableRecord =>
      r.timestamp must beSome(now)
    }
  }

  def setsEventIdAsDocumentId = {
    val bytes  = toByteBuffer(testEvent)
    val result = Processing.transformEnrichedEvent(None)(bytes, processor)
    result must beRight { r: Processing.IndexableRecord =>
      r.id must beSome(testEvent.event_id.toString)
    }
  }

  def returnsBadRowForInvalidTsv = {
    val bytes           = ByteBuffer.wrap("not-a-valid-tsv".getBytes(StandardCharsets.UTF_8))
    val result          = Processing.transformEnrichedEvent(None)(bytes, processor)
    val expectedPayload = StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(bytes.slice)).toString
    result must beLeft[BadRow].like { case BadRow.LoaderParsingError(_, _, payload) =>
      payload.event == expectedPayload
    }
  }

  def computesShardSuffixWhenShardingConfigured = {
    val sharding = Config.ElasticsearchSink.Sharding(
      dateField  = "collector_tstamp",
      dateFormat = DateTimeFormatter.ofPattern("_yyyy-MM-dd")
    )
    val timestamp = Instant.parse("2026-03-10T10:15:30.00Z")
    val bytes     = toByteBuffer(testEvent.copy(collector_tstamp = timestamp))
    val result    = Processing.transformEnrichedEvent(Some(sharding))(bytes, processor)
    result must beRight { r: Processing.IndexableRecord =>
      r.shardSuffix must beSome("_2026-03-10")
    }
  }

  def computesShardSuffixFromNonCollectorTstampDateField = {
    val sharding = Config.ElasticsearchSink.Sharding(
      dateField  = "derived_tstamp",
      dateFormat = DateTimeFormatter.ofPattern("_yyyy-MM-dd")
    )
    val collectorTstamp = Instant.parse("2026-03-10T10:15:30.00Z")
    val derivedTstamp   = Instant.parse("2026-03-12T08:00:00.00Z")
    val bytes           = toByteBuffer(testEvent.copy(collector_tstamp = collectorTstamp, derived_tstamp = Some(derivedTstamp)))
    val result          = Processing.transformEnrichedEvent(Some(sharding))(bytes, processor)
    result must beRight { r: Processing.IndexableRecord =>
      r.shardSuffix must beSome("_2026-03-12")
    }
  }

  def returnsNoneShardSuffixWhenNoSharding = {
    val bytes  = toByteBuffer(testEvent)
    val result = Processing.transformEnrichedEvent(None)(bytes, processor)
    result must beRight { r: Processing.IndexableRecord =>
      r.shardSuffix must beNone
    }
  }
}

object TransformEnrichedEventSpec {
  val processor: Processor = Processor("test", "0.0.0")
  val now: Instant         = Instant.now()
  val testEvent = Event.minimal(
    UUID.randomUUID(),
    now,
    "v_collector",
    "v_etl"
  )

  def toByteBuffer(event: Event): ByteBuffer =
    ByteBuffer.wrap(event.toTsv.getBytes(StandardCharsets.UTF_8))
}
