/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.elasticsearch.it

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import io.circe.Json
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}
import org.specs2.matcher.MatchResult

// Mutable specs2 `in` returns Fragment via side effect; `-Wnonunit-statement` suppressed intentionally
@annotation.nowarn("msg=unused value of type org.specs2.specification.core.Fragment")
abstract class EnrichedSpec extends BaseSpec {

  private val IndexName = "snowplow"

  val FieldLimitErrorIdentifier = "Limit of total fields"

  "index an enriched event" in withResource { infra =>
    val collectorTstamp = "2026-03-10T10:15:30Z"
    val testEventsMap: Map[UUID, Event] = (1 to 10).map { i =>
      val eventId = UUID.randomUUID()
      (
        eventId,
        Event
          .minimal(eventId, Instant.parse(collectorTstamp), s"v_collector-$i", s"v_etl-$i")
          .copy(app_id = Some("my-app-üöä"))
      )
    }.toMap

    TestHelpers.putEvents(infra.kinesisEndpoint, infra.streamGood, testEventsMap.values.toList) >>
      TestHelpers.pollForDocs(infra.esUrl, IndexName, testEventsMap.size).map { docs =>
        val resultEventsMap = TestHelpers.docsToEventMap(docs)
        (resultEventsMap must haveSize(testEventsMap.size)) and
          forall(testEventsMap) { case (eventId, event) =>
            val doc = resultEventsMap(eventId.toString)
            (doc("app_id").flatMap(_.asString) must beEqualTo(event.app_id)) and
              (doc("collector_tstamp").flatMap(_.asString) must beSome(event.collector_tstamp.toString)) and
              (doc("v_collector").flatMap(_.asString) must beSome(event.v_collector)) and
              (doc("v_etl").flatMap(_.asString) must beSome(event.v_etl))
          }
      }
  }

  "write bad rows to bad stream" in withResource { infra =>
    val eventCount                  = 10
    val malformedEvents             = createMalformedEvents(eventCount)
    val oversizedEvents             = createEventsWithOversizedEntities(eventCount)
    val eventsWithIncompatibleTypes = createEventsWithIncompatibleTypes(eventCount)
    val allEvents                   = malformedEvents ++ oversizedEvents ++ eventsWithIncompatibleTypes

    TestHelpers.putEvents(infra.kinesisEndpoint, infra.streamGood, allEvents) >>
      TestHelpers.pollForBadRows(infra.kinesisEndpoint, infra.streamBad, allEvents.size).map { badRows =>
        val badRowsSizeCheck: MatchResult[Any] = badRows must haveSize(allEvents.size)
        val malformedEventsCheck: MatchResult[Any] = badRows.count { b =>
          b("schema").flatMap(_.asString).exists(_.startsWith("iglu:com.snowplowanalytics.snowplow.badrows")) &&
          b("data").map(_.noSpaces).exists(_.contains("NotTSV"))
        } must beEqualTo(eventCount)
        val oversizedEventsCheck: MatchResult[Any] = badRows.count { b =>
          b("schema").flatMap(_.asString).exists(_.startsWith("iglu:com.snowplowanalytics.snowplow.badrows")) &&
          b("data").map(_.noSpaces).exists(_.contains(FieldLimitErrorIdentifier))
        } must beEqualTo(eventCount)
        val incompatibleTypesCheck: MatchResult[Any] = badRows.count { b =>
          b("schema").flatMap(_.asString).exists(_.startsWith("iglu:com.snowplowanalytics.snowplow.badrows")) &&
          b("data").map(_.noSpaces).exists(_.contains("failed to parse field [se_action] of type [long]"))
        } must beEqualTo(eventCount)

        badRowsSizeCheck and malformedEventsCheck and oversizedEventsCheck and incompatibleTypesCheck
      }
  }

  private def createMalformedEvents(count: Int) = (1 to count).toList.map(i => s"not-valid-tsv-$i".getBytes(StandardCharsets.UTF_8))

  private def createEventsWithOversizedEntities(count: Int) = {
    val oversizedJson = Json.fromFields((1 to 200).map(i => (s"key-$i", Json.fromString(s"value-$i"))))
    val unstructEvent = SnowplowEvent.UnstructEvent(
      Some(SelfDescribingData(SchemaKey("com.example", "field_limit", "jsonschema", SchemaVer.Full(1, 0, 0)), oversizedJson))
    )
    (1 to count).toList
      .map { i =>
        Event
          .minimal(UUID.randomUUID(), Instant.now(), s"v_collector-$i", s"v_etl-$i")
          .copy(unstruct_event = unstructEvent)
      }
      .map(_.toTsv.getBytes(StandardCharsets.UTF_8))
  }

  private def createEventsWithIncompatibleTypes(count: Int) =
    (1 to count).toList
      .map { _ =>
        Event
          .minimal(UUID.randomUUID(), Instant.now(), s"v_collector", s"v_etl")
          .copy(se_action = Some("se_action"))
      }
      .map(_.toTsv.getBytes(StandardCharsets.UTF_8))
}
