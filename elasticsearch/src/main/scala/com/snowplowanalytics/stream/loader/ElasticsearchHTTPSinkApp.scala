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

import clients.{ElasticsearchSender, ElasticsearchSenderHTTP}

/** Main entry point for the Elasticsearch HTTP sink */
object ElasticsearchHTTPSinkApp extends App with ElasticsearchSinkApp {
  override lazy val arguments = args

  val config = parseConfig().get
  val esEndpoint = config.elasticsearch.client.endpoint
  val esPort = config.elasticsearch.client.port
  val esUsername = config.elasticsearch.client.username
  val esPassword = config.elasticsearch.client.password
  val region = config.elasticsearch.aws.region
  val signing = config.elasticsearch.aws.signing
  val ssl = config.elasticsearch.client.ssl
  val maxConnectionTime = config.elasticsearch.client.maxTimeout
  val credentials = CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey)
  val tracker = config.monitoring.map(e => SnowplowTracking.initializeTracker(e.snowplow))

  override lazy val elasticsearchSender: ElasticsearchSender =
    new ElasticsearchSenderHTTP(esEndpoint, esPort, esUsername, esPassword, credentials, region, ssl, signing, tracker, maxConnectionTime)

  run(config)
}
