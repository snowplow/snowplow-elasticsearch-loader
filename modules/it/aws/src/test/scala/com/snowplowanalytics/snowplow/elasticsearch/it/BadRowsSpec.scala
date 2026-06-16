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

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}

import Containers.TestInfrastructure

// Mutable specs2 `in` returns Fragment via side effect; `-Wnonunit-statement` suppressed intentionally
@annotation.nowarn("msg=unused value of type org.specs2.specification.core.Fragment")
class BadRowsSpec extends BaseSpec {

  override val resource: Resource[IO, TestInfrastructure] =
    Containers.allContainers(
      esFactory     = Containers.elasticsearch(Containers.Images.elasticsearch7),
      loaderPurpose = "BAD_ROWS"
    )

  "index a bad row JSON document in Elasticsearch" in withResource { infra =>
    val badRows = (1 to 10).toList.map(_ =>
      BadRow
        .GenericError(
          Processor("elasticsearch-loader-it", "1.0.0"),
          Failure.GenericFailure(Instant.now(), NonEmptyList.one("test bad row")),
          Payload.RawPayload("test-payload")
        )
        .compact
        .getBytes(StandardCharsets.UTF_8)
    )

    TestHelpers.putEvents(infra.kinesisEndpoint, infra.streamGood, badRows) >>
      TestHelpers
        .pollForDocs(infra.esUrl, "snowplow", badRows.size)
        .map { docs =>
          (docs must haveSize(badRows.size)) and
            forall(docs) { b =>
              (b("schema").flatMap(_.asString) must beSome(startWith("iglu:com.snowplowanalytics.snowplow.badrows"))) and
                (b("data").map(_.noSpaces) must beSome(contain("test bad row")))
            }
        }
  }

  "write to bad stream when input is not valid JSON" in withResource { infra =>
    val invalidJson = List("not-valid-json".getBytes(StandardCharsets.UTF_8))

    TestHelpers.putEvents(infra.kinesisEndpoint, infra.streamGood, invalidJson) >>
      TestHelpers
        .pollForBadRows(infra.kinesisEndpoint, infra.streamBad, 1)
        .map { badRows =>
          (badRows must haveSize(1)) and
            (badRows.head("schema").flatMap(_.asString) must beSome(startWith("iglu:com.snowplowanalytics.snowplow.badrows")))
        }
  }
}
