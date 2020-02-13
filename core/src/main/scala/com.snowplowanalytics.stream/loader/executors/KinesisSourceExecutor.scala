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
package executors

// Java
import java.util.Date
import java.util.Properties

// Logging
import org.slf4j.LoggerFactory

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  KinesisConnectorExecutorBase,
  KinesisConnectorRecordProcessorFactory
}
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline

// AWS Client Library
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory

// This project
import model._

/**
 * Boilerplate class for Kinesis Conenector
 * @param streamLoaderConfig streamLoaderConfig
 * @param kinesis  queue settings
 * @param kinesisConnectorPipeline kinesisConnectorPipeline

 */
class KinesisSourceExecutor[A, B](
  streamLoaderConfig: StreamLoaderConfig,
  kinesis: Kinesis,
  kinesisConnectorPipeline: IKinesisConnectorPipeline[A, B]
) extends KinesisConnectorExecutorBase[A, B] {

  val LOG = LoggerFactory.getLogger(getClass)

  def getKCLConfig(
    initialPosition: String,
    timestamp: Option[Date],
    kcc: KinesisConnectorConfiguration): KinesisClientLibConfiguration = {
    val cfg = new KinesisClientLibConfiguration(
      kcc.APP_NAME,
      kcc.KINESIS_INPUT_STREAM,
      kcc.AWS_CREDENTIALS_PROVIDER,
      kcc.WORKER_ID)
      .withKinesisEndpoint(kcc.KINESIS_ENDPOINT)
      .withFailoverTimeMillis(kcc.FAILOVER_TIME)
      .withMaxRecords(kcc.MAX_RECORDS)
      .withIdleTimeBetweenReadsInMillis(kcc.IDLE_TIME_BETWEEN_READS)
      .withCallProcessRecordsEvenForEmptyRecordList(
        KinesisConnectorConfiguration.DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST)
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

    timestamp
      .filter(_ => initialPosition == "AT_TIMESTAMP")
      .map(cfg.withTimestampAtInitialPositionInStream(_))
      .getOrElse(cfg.withInitialPositionInStream(kcc.INITIAL_POSITION_IN_STREAM))
  }

  /**
   * Builds a KinesisConnectorConfiguration
   *
   * @param config the configuration HOCON
   * @param queue queue configuration
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(config: StreamLoaderConfig, queue: Kinesis): KinesisConnectorConfiguration = {
    val props = new Properties
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, queue.endpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, queue.appName.trim)
    props.setProperty(
      KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM,
      queue.initialPosition)
    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, queue.maxRecords.toString)

    // So that the region of the DynamoDB table is correct
    props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, queue.region)

    props.setProperty(
      KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM,
      config.streams.inStreamName)

    props.setProperty(
      KinesisConnectorConfiguration.PROP_ELASTICSEARCH_ENDPOINT,
      config.elasticsearch.get.client.endpoint)
    props.setProperty(
      KinesisConnectorConfiguration.PROP_ELASTICSEARCH_CLUSTER_NAME,
      config.elasticsearch.get.cluster.name)
    props.setProperty(
      KinesisConnectorConfiguration.PROP_ELASTICSEARCH_PORT,
      config.elasticsearch.get.client.port.toString)

    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
      config.streams.buffer.byteLimit.toString)
    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT,
      config.streams.buffer.recordLimit.toString)
    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
      config.streams.buffer.timeLimit.toString)

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "elasticsearch")
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    new KinesisConnectorConfiguration(
      props,
      CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey))
  }

  /**
   * Initialize the Amazon Kinesis Client Library configuration and worker with metrics factory
   *
   * @param kinesisConnectorConfiguration Amazon Kinesis connector configuration
   * @param metricFactory would be used to emit metrics in Amazon Kinesis Client Library
   */
  override def initialize(
    kinesisConnectorConfiguration: KinesisConnectorConfiguration,
    metricFactory: IMetricsFactory): Unit = {
    val kinesisClientLibConfiguration =
      getKCLConfig(kinesis.initialPosition, kinesis.timestamp, kinesisConnectorConfiguration)

    if (!kinesisConnectorConfiguration.CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST) {
      LOG.warn(
        "The false value of callProcessRecordsEvenForEmptyList will be ignored. It must be set to true for the bufferTimeMillisecondsLimit to work correctly.")
    }

    if (kinesisConnectorConfiguration.IDLE_TIME_BETWEEN_READS > kinesisConnectorConfiguration.BUFFER_MILLISECONDS_LIMIT) {
      LOG.warn(
        "idleTimeBetweenReads is greater than bufferTimeMillisecondsLimit. For best results, ensure that bufferTimeMillisecondsLimit is more than or equal to idleTimeBetweenReads ")
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

  def getKinesisConnectorRecordProcessorFactory =
    new KinesisConnectorRecordProcessorFactory[A, B](
      kinesisConnectorPipeline,
      convertConfig(streamLoaderConfig, kinesis))

  initialize(convertConfig(streamLoaderConfig, kinesis), null)

}
