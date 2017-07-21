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

package com.snowplowanalytics.elasticsearch.loader

import clients.{ElasticsearchSender, ElasticsearchSenderHTTP}

/** Main entry point for the Elasticsearch HTTP sink */
object ElasticsearchHTTPSinkApp extends App with ElasticsearchSinkApp {
  override lazy val arguments = args

  val conf = parseConfig()
  val finalConfig = convertConfig(conf)
  val region = conf.getString("elasticsearch.aws.region")
  val signing = conf.getBoolean("elasticsearch.aws.signing")
  val ssl = conf.getBoolean("elasticsearch.client.ssl")
  val maxConnectionTime = conf.getLong("elasticsearch.client.max-timeout")

  override lazy val elasticsearchSender: ElasticsearchSender =
    new ElasticsearchSenderHTTP(
      finalConfig.ELASTICSEARCH_ENDPOINT,
      finalConfig.ELASTICSEARCH_PORT,
      finalConfig.AWS_CREDENTIALS_PROVIDER,
      region, ssl, signing, getTracker(conf), maxConnectionTime)

  run(conf)
}
