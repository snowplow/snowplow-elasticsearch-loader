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

import io.circe.Json
import io.circe.syntax._
import org.specs2.Specification
import java.time.Instant
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}
import com.snowplowanalytics.snowplow.elasticsearch.core.TransformEnrichedEventSpec.testEvent

class BadRowTransformerSpec extends Specification {
  import BadRowTransformerSpec._

  def is = s2"""
  BadRowTransformer should
    pass through non-self-describing JSON unchanged                                $passThroughNonSelfDescribingJson
    rename string 'failure' field to 'failure_str'                                 $renameStringFailureToFailureStr
    rename array 'failure' field to 'failure_list'                                 $renameArrayFailureToFailureList
    leave object 'failure' field unchanged                                         $leaveObjectFailureUnchanged
    serialize non-string 'parameters' to a string                                  $serializeNonStringParameters
    handle JSON that is not a badrows schema unchanged                             $passThroughNonBadrowsSchema
    rename string 'payload' field to 'payload_str'                                 $renameStringPayloadToPayloadStr
    rename array 'payload' field to 'payload_list'                                 $renameArrayPayloadToPayloadList
    rename string 'error' inside failure object to 'error_str'                     $renameStringErrorInFailureToErrorStr
    rename string 'errors' inside failure object to 'errors_str'                   $renameStringErrorsInFailureToErrorsStr
    rename string 'message' inside failure object to 'message_str'                 $renameStringMessageInFailureToMessageStr
    rename string 'error' inside failure.messages items to 'error_str'             $renameStringErrorInFailureMessagesToErrorStr
    serialize non-string 'expectedMapping' inside failure.messages items to string $serializeExpectedMappingInFailureMessages
    serialize non-string 'json' inside failure.messages items to string            $serializeJsonInFailureMessages
    rename string 'error' inside failure.messages[*].message object to 'error_str' $renameStringErrorInFailureMessagesMessageToErrorStr
    rename string 'error' inside failure_list items to 'error_str'                 $renameStringErrorInFailureListToErrorStr
    serialize non-string 'value' inside failure_list items to string               $serializeValueInFailureList
  """

  // non-SDJ: handleIgluJson passes plain JSON through unchanged
  def passThroughNonSelfDescribingJson = {
    val json = Json.obj("foo" -> Json.fromString("bar"))
    BadRowTransformer.handleIgluJson(json.noSpaces) must beRight(json)
  }

  // fix 1: root.obj.modify(renameField("failure")) — string case
  def renameStringFailureToFailureStr = {
    val badRow = BadRow.LoaderRuntimeError(
      processor,
      "some error",
      Payload.LoaderPayload(testEvent)
    )
    val result = BadRowTransformer.handleIgluJson(badRow.compact)
    result must beRight { j: Json =>
      j.hcursor.downField("data").get[String]("failure_str") must beRight("some error")
    }
  }

  // fix 1: root.obj.modify(renameField("failure")) — array case
  def renameArrayFailureToFailureList = {
    val data   = Json.obj("failure" -> Json.arr(Json.fromString("e1"), Json.fromString("e2")))
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure_list").as[List[Json]] must beRight
    }
  }

  // fix 1: root.obj.modify(renameField("failure")) — object case (no rename)
  def leaveObjectFailureUnchanged = {
    val failure = Failure.SizeViolation(Instant.now, 1, 2, "expectation")
    val badRow  = BadRow.SizeViolation(processor, failure, Payload.RawPayload("payload"))
    val result  = BadRowTransformer.handleIgluJson(badRow.compact)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure").as[Json] must beRight(failure.asJson)
    }
  }

  // fix 3: root.payload.raw.obj.modify(serializeField("parameters"))
  def serializeNonStringParameters = {
    val params = Json.obj("key" -> Json.fromString("value"))
    val data   = Json.obj("payload" -> Json.obj("raw" -> Json.obj("parameters" -> params)))
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      val paramsField = j.hcursor
        .downField("data")
        .downField("payload")
        .downField("raw")
        .get[String]("parameters")
      paramsField must beRight(params.noSpaces)
    }
  }

  // non-badrows schema: handleIgluJson passes self-describing JSON from other vendors through unchanged
  def passThroughNonBadrowsSchema = {
    val json = io.circe.parser
      .parse(
        """{"schema":"iglu:com.example/event/jsonschema/1-0-0","data":{"failure":"oops"}}"""
      )
      .toOption
      .get
    BadRowTransformer.handleIgluJson(json.noSpaces) must beRight(json)
  }

  // fix 2: root.obj.modify(renameField("payload"))
  def renameStringPayloadToPayloadStr = {
    val data   = Json.obj("payload" -> Json.fromString("raw payload string"))
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").get[String]("payload_str") must beRight("raw payload string")
    }
  }

  def renameArrayPayloadToPayloadList = {
    val data   = Json.obj("payload" -> Json.arr(Json.fromString("p1"), Json.fromString("p2")))
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("payload_list").as[List[Json]] must beRight
    }
  }

  // fix 4: root.failure.obj.modify(renameField("error"))
  def renameStringErrorInFailureToErrorStr = {
    val data   = Json.obj("failure" -> Json.obj("error" -> Json.fromString("some error")))
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure").get[String]("error_str") must beRight("some error")
    }
  }

  // fix 5: root.failure.obj.modify(renameField("errors"))
  def renameStringErrorsInFailureToErrorsStr = {
    val data   = Json.obj("failure" -> Json.obj("errors" -> Json.fromString("some errors")))
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure").get[String]("errors_str") must beRight("some errors")
    }
  }

  // fix 6: root.failure.obj.modify(renameField("message"))
  def renameStringMessageInFailureToMessageStr = {
    val data   = Json.obj("failure" -> Json.obj("message" -> Json.fromString("some message")))
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure").get[String]("message_str") must beRight("some message")
    }
  }

  // fix 7: root.failure.messages.each.obj.modify(renameField("error"))
  def renameStringErrorInFailureMessagesToErrorStr = {
    val data = Json.obj(
      "failure" -> Json.obj(
        "messages" -> Json.arr(Json.obj("error" -> Json.fromString("msg error")))
      )
    )
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure").downField("messages").downArray.get[String]("error_str") must beRight("msg error")
    }
  }

  // fix 8: root.failure.messages.each.obj.modify(serializeField("expectedMapping"))
  def serializeExpectedMappingInFailureMessages = {
    val mapping = Json.obj("type" -> Json.fromString("text"))
    val data = Json.obj(
      "failure" -> Json.obj(
        "messages" -> Json.arr(Json.obj("expectedMapping" -> mapping))
      )
    )
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure").downField("messages").downArray.get[String]("expectedMapping") must beRight(
        mapping.noSpaces
      )
    }
  }

  // fix 9: root.failure.messages.each.obj.modify(serializeField("json"))
  def serializeJsonInFailureMessages = {
    val jsonField = Json.obj("key" -> Json.fromString("value"))
    val data = Json.obj(
      "failure" -> Json.obj(
        "messages" -> Json.arr(Json.obj("json" -> jsonField))
      )
    )
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure").downField("messages").downArray.get[String]("json") must beRight(jsonField.noSpaces)
    }
  }

  // fix 10: root.failure.messages.each.message.obj.modify(renameField("error"))
  def renameStringErrorInFailureMessagesMessageToErrorStr = {
    val data = Json.obj(
      "failure" -> Json.obj(
        "messages" -> Json.arr(
          Json.obj("message" -> Json.obj("error" -> Json.fromString("nested error")))
        )
      )
    )
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor
        .downField("data")
        .downField("failure")
        .downField("messages")
        .downArray
        .downField("message")
        .get[String]("error_str") must beRight("nested error")
    }
  }

  // fix 11: root.failure_list.each.obj.modify(renameField("error"))
  // failure_list exists after an array-valued failure field is renamed by fix 1
  def renameStringErrorInFailureListToErrorStr = {
    val data = Json.obj(
      "failure" -> Json.arr(Json.obj("error" -> Json.fromString("list error")))
    )
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure_list").downArray.get[String]("error_str") must beRight("list error")
    }
  }

  // fix 12: root.failure_list.each.obj.modify(serializeField("value"))
  def serializeValueInFailureList = {
    val value = Json.obj("key" -> Json.fromString("val"))
    val data = Json.obj(
      "failure" -> Json.arr(Json.obj("value" -> value))
    )
    val sdj    = mkBadRowSdj(data)
    val result = BadRowTransformer.handleIgluJson(sdj.noSpaces)
    result must beRight { j: Json =>
      j.hcursor.downField("data").downField("failure_list").downArray.get[String]("value") must beRight(value.noSpaces)
    }
  }
}

object BadRowTransformerSpec {
  val processor = Processor("test", "test")

  private def mkBadRowSdj(data: Json): Json =
    Json.obj(
      "schema" -> Json.fromString(
        "iglu:com.snowplowanalytics.snowplow.badrows/tracker_protocol_violation/jsonschema/1-0-0"
      ),
      "data" -> data
    )
}
