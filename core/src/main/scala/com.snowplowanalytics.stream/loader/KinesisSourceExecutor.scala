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

package com.snowplowanalytics
package stream.loader

// Logging
import org.slf4j.LoggerFactory

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  KinesisConnectorExecutorBase,
  KinesisConnectorRecordProcessorFactory
}

// AWS Client Library
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory

// Java
import java.util.Date

// Tracker
import snowplow.scalatracker.Tracker

// This project
import clients.ElasticsearchSender
import sinks._
import model._

/**
  * Boilerplate class for Kinesis Conenector
  *
  * @param streamType the type of stream, good, bad or plain-json
  * @param documentIndex the elasticsearch index name
  * @param documentType the elasticsearch index type
  * @param config the KCL configuration
  * @param initialPosition initial position for kinesis stream
  * @param initialTimestamp timestamp for "AT_TIMESTAMP" initial position
  * @param goodSink the configured GoodSink
  * @param badSink the configured BadSink
  * @param elasticsearchSender function for sending to elasticsearch
  * @param tracker a Tracker instance
  */
class KinesisSourceExecutor(
                             streamType: StreamType,
                             documentIndex: String,
                             documentType: String,
                             config: KinesisConnectorConfiguration,
                             initialPosition: String,
                             initialTimestamp: Option[Date],
                             goodSink: Option[ISink],
                             badSink: ISink,
                             elasticsearchSender: ElasticsearchSender,
                             tracker: Option[Tracker] = None
                           ) extends KinesisConnectorExecutorBase[ValidatedRecord, EmitterInput] {

  val LOG = LoggerFactory.getLogger(getClass)

  def getKCLConfig(initialPosition: String, timestamp: Option[Date], kcc: KinesisConnectorConfiguration): KinesisClientLibConfiguration = {
    val cfg = new KinesisClientLibConfiguration(
      kcc.APP_NAME,
      kcc.KINESIS_INPUT_STREAM,
      kcc.AWS_CREDENTIALS_PROVIDER,
      kcc.WORKER_ID).withKinesisEndpoint(kcc.KINESIS_ENDPOINT)
      .withFailoverTimeMillis(kcc.FAILOVER_TIME)
      .withMaxRecords(kcc.MAX_RECORDS)
      .withIdleTimeBetweenReadsInMillis(kcc.IDLE_TIME_BETWEEN_READS)
      .withCallProcessRecordsEvenForEmptyRecordList(KinesisConnectorConfiguration.DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST)
      .withCleanupLeasesUponShardCompletion(kcc.CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY)
      .withParentShardPollIntervalMillis(kcc.PARENT_SHARD_POLL_INTERVAL)
      .withShardSyncIntervalMillis(kcc.SHARD_SYNC_INTERVAL)
      .withTaskBackoffTimeMillis(kcc.BACKOFF_INTERVAL)
      .withMetricsBufferTimeMillis(kcc.CLOUDWATCH_BUFFER_TIME)
      .withMetricsMaxQueueSize(kcc.CLOUDWATCH_MAX_QUEUE_SIZE)
      .withUserAgent(kcc.APP_NAME + ","
        + kcc.CONNECTOR_DESTINATION + ","
        + KinesisConnectorConfiguration.KINESIS_CONNECTOR_USER_AGENT)
      .withRegionName(kcc.REGION_NAME)

    timestamp.filter(_ => initialPosition == "AT_TIMESTAMP")
      .map(cfg.withTimestampAtInitialPositionInStream(_))
      .getOrElse(cfg.withInitialPositionInStream(kcc.INITIAL_POSITION_IN_STREAM))
  }

  /**
    * Initialize the Amazon Kinesis Client Library configuration and worker with metrics factory
    *
    * @param kinesisConnectorConfiguration Amazon Kinesis connector configuration
    * @param metricFactory would be used to emit metrics in Amazon Kinesis Client Library
    */
  override def initialize(kinesisConnectorConfiguration: KinesisConnectorConfiguration, metricFactory: IMetricsFactory): Unit = {
    val kinesisClientLibConfiguration = getKCLConfig(initialPosition, initialTimestamp, kinesisConnectorConfiguration)

    if (!kinesisConnectorConfiguration.CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST) {
      LOG.warn("The false value of callProcessRecordsEvenForEmptyList will be ignored. It must be set to true for the bufferTimeMillisecondsLimit to work correctly.")
    }

    if (kinesisConnectorConfiguration.IDLE_TIME_BETWEEN_READS > kinesisConnectorConfiguration.BUFFER_MILLISECONDS_LIMIT) {
      LOG.warn("idleTimeBetweenReads is greater than bufferTimeMillisecondsLimit. For best results, ensure that bufferTimeMillisecondsLimit is more than or equal to idleTimeBetweenReads ")
    }

    val workerBuilder = new Worker.Builder()
      .recordProcessorFactory(getKinesisConnectorRecordProcessorFactory())
      .config(kinesisClientLibConfiguration)

    // If a metrics factory was specified, use it.
    if (metricFactory != null) {
      workerBuilder.metricsFactory(metricFactory)
    }

    worker = workerBuilder.build()

    LOG.info(getClass().getSimpleName() + " worker created")
  }

  initialize(config, null)

  override def getKinesisConnectorRecordProcessorFactory = {
    new KinesisConnectorRecordProcessorFactory[ValidatedRecord, EmitterInput](
      new KinesisElasticsearchPipeline(streamType, documentIndex, documentType, goodSink, badSink, elasticsearchSender, tracker), config)
  }
}
