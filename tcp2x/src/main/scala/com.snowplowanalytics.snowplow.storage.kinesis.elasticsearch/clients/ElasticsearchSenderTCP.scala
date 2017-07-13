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
package clients

// Amazon
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject

// Elasticsearch
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.client.transport.{NoNodeAvailableException, TransportClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

// Scala
import scala.annotation.tailrec

// Scalaz
import scalaz._
import Scalaz._

// SLF4j
import org.slf4j.LoggerFactory

// Java
import java.net.InetAddress

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

class ElasticsearchSenderTCP(
  configuration: KinesisConnectorConfiguration,
  override val tracker: Option[Tracker] = None,
  maxConnectionWaitTimeMs: Long = 60000
) extends ElasticsearchSender {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * The settings key for the cluster name.
   * 
   * Defaults to elasticsearch.
   */
  private val ElasticsearchClusterNameKey = "cluster.name"

  /**
   * The settings key for transport client sniffing. If set to true, this instructs the TransportClient to
   * find all nodes in the cluster, providing robustness if the original node were to become unavailable.
   * 
   * Defaults to false.
   */
  private val ElasticsearchClientTransportSniffKey = "client.transport.sniff"

  /**
   * The settings key for ignoring the cluster name. Set to true to ignore cluster name validation
   * of connected nodes.
   * 
   * Defaults to false.
   */
  private val ElasticsearchClientTransportIgnoreClusterNameKey = "client.transport.ignore_cluster_name"

  /**
   * The settings key for ping timeout. The time to wait for a ping response from a node.
   * 
   * Default to 5s.
   */
  private val ElasticsearchClientTransportPingTimeoutKey = "client.transport.ping_timeout"

  /**
   * The settings key for node sampler interval. How often to sample / ping the nodes listed and connected.
   * 
   * Defaults to 5s
   */
  private val ElasticsearchClientTransportNodesSamplerIntervalKey = "client.transport.nodes_sampler_interval"

  private val settings = Settings.settingsBuilder
    .put(ElasticsearchClusterNameKey,                         configuration.ELASTICSEARCH_CLUSTER_NAME)
    .put(ElasticsearchClientTransportSniffKey,                configuration.ELASTICSEARCH_TRANSPORT_SNIFF)
    .put(ElasticsearchClientTransportIgnoreClusterNameKey,    configuration.ELASTICSEARCH_IGNORE_CLUSTER_NAME)
    .put(ElasticsearchClientTransportPingTimeoutKey,          configuration.ELASTICSEARCH_PING_TIMEOUT)
    .put(ElasticsearchClientTransportNodesSamplerIntervalKey, configuration.ELASTICSEARCH_NODE_SAMPLER_INTERVAL)
    .build

  /**
   * The Elasticsearch client.
   */
  private val elasticsearchClient = TransportClient.builder().settings(settings).build()

  /**
   * The Elasticsearch endpoint.
   */
  private val elasticsearchEndpoint = configuration.ELASTICSEARCH_ENDPOINT

  /**
   * The Elasticsearch port.
   */
  private val elasticsearchPort = configuration.ELASTICSEARCH_PORT

  /**
   * The amount of time to wait in between unsuccessful index requests (in milliseconds).
   * 10 seconds = 10 * 1000 = 10000
   */
  private val BackoffPeriod = 10000L

  elasticsearchClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticsearchEndpoint), elasticsearchPort))

  log.info(s"ElasticsearchSender using elasticsearch endpoint $elasticsearchEndpoint:$elasticsearchPort")

  // With previous versions of ES there were hard limits regarding the size of the payload (32768
  // bytes) and since we don't really need the whole payload in those cases we cut it at 20k so that
  // it can be sent to a bad sink. This way we don't have to compute the size of the byte
  // representation of the utf-8 string.
  private val maxSizeWhenReportingFailure = 20000

  /**
   * Emits good records to Elasticsearch and bad records to Kinesis.
   * All valid records in the buffer get sent to Elasticsearch in a bulk request.
   * All invalid requests and all requests which failed transformation get sent to Kinesis.
   *
   * @param records List of records to send to Elasticsearch
   * @return List of inputs which Elasticsearch rejected
   */
  override def sendToElasticsearch(records: List[EmitterInput]): List[EmitterInput] = {

    val bulkRequest = elasticsearchClient.prepareBulk()

    records.foreach(recordTuple => recordTuple.map(record => record.map(validRecord => {
      val indexRequestBuilder = {
        val irb =
          elasticsearchClient.prepareIndex(validRecord.getIndex, validRecord.getType, validRecord.getId)
        irb.setSource(validRecord.getSource)
        val version = validRecord.getVersion
        if (version != null) {
          irb.setVersion(version)
        }
        val ttl = validRecord.getTtl
        if (ttl != null) {
          irb.setTTL(ttl)
        }
        val create = validRecord.getCreate
        if (create != null) {
          irb.setCreate(create)
        }
        irb
      }
      bulkRequest.add(indexRequestBuilder)
    })))

    val connectionAttemptStartTime = System.currentTimeMillis()

    /**
     * Keep attempting to execute the buldRequest until it succeeds
     *
     * @return List of inputs which Elasticsearch rejected
     */
    @tailrec def attemptEmit(attemptNumber: Long = 1): List[EmitterInput] = {

      if (attemptNumber > 1 && System.currentTimeMillis() - connectionAttemptStartTime > maxConnectionWaitTimeMs) {
        log.error(s"Shutting down application as unable to connect to Elasticsearch for over $maxConnectionWaitTimeMs ms")
        forceShutdown()
      }

      try {
        val bulkResponse = bulkRequest.execute.actionGet
        val responses = bulkResponse.getItems

        val allFailures = responses.toList.zip(records).filter(_._1.isFailed).map(pair => {
          val (response, record) = pair
          val failure = response.getFailure

          log.error(s"Record failed with message: ${response.getFailureMessage}")
          
          if (failure.getMessage.contains("DocumentAlreadyExistsException") || failure.getMessage.contains("VersionConflictEngineException")) {
            None
          } else {
            Some(record._1.take(maxSizeWhenReportingFailure) ->
              s"Elasticsearch rejected record with message: ${failure.getMessage}"
                .failureNel[ElasticsearchObject])
          }
        })

        val numberOfSkippedRecords = allFailures.count(_.isEmpty)
        val failures = allFailures.flatten

        log.info(s"Emitted ${records.size - failures.size - numberOfSkippedRecords} records to Elasticsearch")

        if (!failures.isEmpty) {
          logClusterHealth()
          log.warn(s"Returning ${failures.size} records as failed")
        }

        failures
      } catch {
        case nnae: NoNodeAvailableException => {
          log.error(s"No nodes found at $elasticsearchEndpoint:$elasticsearchPort. Retrying in $BackoffPeriod milliseconds", nnae)
          sleep(BackoffPeriod)
          tracker foreach {
            t => SnowplowTracking.sendFailureEvent(t, BackoffPeriod, attemptNumber, connectionAttemptStartTime, nnae.toString)
          }
          attemptEmit(attemptNumber + 1)
        }
        case e: Exception => {
          log.error("Unexpected exception ", e)
          
          sleep(BackoffPeriod)
          tracker foreach {
            t => SnowplowTracking.sendFailureEvent(t, BackoffPeriod, attemptNumber, connectionAttemptStartTime, e.toString)
          }
          attemptEmit(attemptNumber + 1)
        }
      }
    }

    attemptEmit()
  }

  /**
   * Shuts the client down
   */
  override def close(): Unit = elasticsearchClient.close

  /**
   * Logs the Elasticsearch cluster's health
   */
  override def logClusterHealth(): Unit = {
    val healthRequestBuilder = elasticsearchClient.admin.cluster.prepareHealth()
    val response = healthRequestBuilder.execute.actionGet
    if (response.getStatus.equals(ClusterHealthStatus.RED)) {
      log.error("Cluster health is RED. Indexing ability will be limited")
    } else if (response.getStatus.equals(ClusterHealthStatus.YELLOW)) {
      log.warn("Cluster health is YELLOW.")
    } else if (response.getStatus.equals(ClusterHealthStatus.GREEN)) {
      log.info("Cluster health is GREEN.")
    }
  }
}
