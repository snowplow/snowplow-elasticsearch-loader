/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.elasticsearch.it.elasticsearch

import cats.effect.{IO, Resource}
import com.snowplowanalytics.snowplow.elasticsearch.it.Containers.TestInfrastructure
import com.snowplowanalytics.snowplow.elasticsearch.it.{Containers, EnrichedSpec}

class ElasticsearchV7Spec extends EnrichedSpec {

  override val resource: Resource[IO, TestInfrastructure] =
    Containers.allContainers(Containers.elasticsearch(Containers.Images.elasticsearch7))

  // Elasticsearch 7.x doesn't mention field limit error in the error message.
  override val FieldLimitErrorIdentifier: String = "unstruct_event_com_example_field_limit_1"
}
