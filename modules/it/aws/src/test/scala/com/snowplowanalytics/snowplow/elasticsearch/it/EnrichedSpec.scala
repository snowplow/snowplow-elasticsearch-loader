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

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

// Mutable specs2 `in` returns Fragment via side effect; `-Wnonunit-statement` suppressed intentionally
@annotation.nowarn("msg=unused value of type org.specs2.specification.core.Fragment")
abstract class EnrichedSpec extends BaseSpec {

  private val IndexName = "snowplow"

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

  "write bad row to bad stream for malformed event" in withResource { infra =>
    val malformedEvents = (1 to 10).toList.map(i => s"not-valid-tsv-$i".getBytes(StandardCharsets.UTF_8))

    TestHelpers.putEvents(infra.kinesisEndpoint, infra.streamGood, malformedEvents) >>
      TestHelpers.pollForBadRows(infra.kinesisEndpoint, infra.streamBad, malformedEvents.size).map { badRows =>
        (badRows must haveSize(malformedEvents.size)) and
          forall(badRows) { b =>
            (b("schema").flatMap(_.asString) must beSome(startWith("iglu:com.snowplowanalytics.snowplow.badrows"))) and
              (b("data").map(_.noSpaces) must beSome(contain("NotTSV")))
          }
      }
  }
}
