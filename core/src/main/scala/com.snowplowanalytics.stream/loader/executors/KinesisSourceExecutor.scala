/**
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd.
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
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

// This project
import com.snowplowanalytics.stream.loader.Config._

/**
 * Boilerplate class for Kinesis Connector
 * @param kinesis  queue settings
 * @param metrics metrics settings
 * @param kinesisConnectorPipeline kinesisConnectorPipeline
 */
class KinesisSourceExecutor[A, B](
  kinesis: Source.Kinesis,
  metrics: Monitoring.Metrics,
  kinesisConnectorPipeline: IKinesisConnectorPipeline[A, B]
) extends KinesisConnectorExecutorBase[A, B] {

  val LOG = LoggerFactory.getLogger(getClass)

  def getKCLConfig(
    initialPosition: String,
    timestamp: Option[Date],
    kcc: KinesisConnectorConfiguration
  ): KinesisClientLibConfiguration = {
    val cfg = new KinesisClientLibConfiguration(
      kcc.APP_NAME,
      kcc.KINESIS_INPUT_STREAM,
      kcc.AWS_CREDENTIALS_PROVIDER,
      kcc.WORKER_ID
    )
      .withFailoverTimeMillis(kcc.FAILOVER_TIME)
      .withMaxRecords(kcc.MAX_RECORDS)
      .withIdleTimeBetweenReadsInMillis(kcc.IDLE_TIME_BETWEEN_READS)
      .withCallProcessRecordsEvenForEmptyRecordList(
        KinesisConnectorConfiguration.DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST
      )
      .withCleanupLeasesUponShardCompletion(
        KinesisClientLibConfiguration.DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION
      )
      .withParentShardPollIntervalMillis(kcc.PARENT_SHARD_POLL_INTERVAL)
      .withShardSyncIntervalMillis(kcc.SHARD_SYNC_INTERVAL)
      .withTaskBackoffTimeMillis(kcc.BACKOFF_INTERVAL)
      .withMetricsBufferTimeMillis(kcc.CLOUDWATCH_BUFFER_TIME)
      .withMetricsMaxQueueSize(kcc.CLOUDWATCH_MAX_QUEUE_SIZE)
      .withUserAgent(
        kcc.APP_NAME + ","
          + kcc.CONNECTOR_DESTINATION + ","
          + KinesisConnectorConfiguration.KINESIS_CONNECTOR_USER_AGENT
      )
      .withRegionName(kcc.REGION_NAME)
      .condWith(kinesis.customEndpoint.isDefined, _.withKinesisEndpoint(kcc.KINESIS_ENDPOINT))
      .condWith(
        kinesis.dynamodbCustomEndpoint.isDefined,
        _.withDynamoDBEndpoint(kcc.DYNAMODB_ENDPOINT)
      )

    timestamp
      .filter(_ => initialPosition == "AT_TIMESTAMP")
      .map(cfg.withTimestampAtInitialPositionInStream)
      .getOrElse(cfg.withInitialPositionInStream(kcc.INITIAL_POSITION_IN_STREAM))
  }

  implicit class KinesisClientLibConfigurationExtension(c: KinesisClientLibConfiguration) {
    def condWith(
      cond: => Boolean,
      f: KinesisClientLibConfiguration => KinesisClientLibConfiguration
    ): KinesisClientLibConfiguration =
      if (cond) f(c) else c
  }

  /**
   * Builds a KinesisConnectorConfiguration
   */
  def convertConfig: KinesisConnectorConfiguration = {
    val props = new Properties
    kinesis.customEndpoint.foreach(
      props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, _)
    )
    kinesis.dynamodbCustomEndpoint.foreach(
      props.setProperty(KinesisConnectorConfiguration.PROP_DYNAMODB_ENDPOINT, _)
    )
    // So that the region of the DynamoDB table is correct
    props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, kinesis.region.name)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, kinesis.appName.trim)
    props.setProperty(
      KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM,
      kinesis.initialPosition
    )
    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, kinesis.maxRecords.toString)

    props.setProperty(
      KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM,
      kinesis.streamName
    )

    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
      kinesis.buffer.byteLimit.toString
    )
    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT,
      kinesis.buffer.recordLimit.toString
    )
    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
      kinesis.buffer.timeLimit.toString
    )

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "elasticsearch")
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    new KinesisConnectorConfiguration(
      props,
      new DefaultAWSCredentialsProviderChain()
    )
  }

  /**
   * Initialize the Amazon Kinesis Client Library configuration and worker with metrics factory
   *
   * @param kinesisConnectorConfiguration Amazon Kinesis connector configuration
   * @param metricFactory would be used to emit metrics in Amazon Kinesis Client Library
   */
  override def initialize(
    kinesisConnectorConfiguration: KinesisConnectorConfiguration,
    metricFactory: IMetricsFactory
  ): Unit = {
    val kinesisClientLibConfiguration =
      getKCLConfig(kinesis.initialPosition, kinesis.timestamp, kinesisConnectorConfiguration)

    if (!kinesisConnectorConfiguration.CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST) {
      LOG.warn(
        "The false value of callProcessRecordsEvenForEmptyList will be ignored. It must be set to true for the bufferTimeMillisecondsLimit to work correctly."
      )
    }

    if (
      kinesisConnectorConfiguration.IDLE_TIME_BETWEEN_READS > kinesisConnectorConfiguration.BUFFER_MILLISECONDS_LIMIT
    ) {
      LOG.warn(
        "idleTimeBetweenReads is greater than bufferTimeMillisecondsLimit. For best results, ensure that bufferTimeMillisecondsLimit is more than or equal to idleTimeBetweenReads "
      )
    }

    worker = if (metrics.cloudWatch) {
      new Worker.Builder()
        .recordProcessorFactory(getKinesisConnectorRecordProcessorFactory())
        .config(kinesisClientLibConfiguration)
        .build()
    } else {
      new Worker.Builder()
        .recordProcessorFactory(getKinesisConnectorRecordProcessorFactory())
        .config(kinesisClientLibConfiguration)
        .metricsFactory(new NullMetricsFactory())
        .build()
    }

    LOG.info(getClass.getSimpleName + " worker created")
  }

  def getKinesisConnectorRecordProcessorFactory =
    new KinesisConnectorRecordProcessorFactory[A, B](
      kinesisConnectorPipeline,
      convertConfig
    )

  initialize(convertConfig, null)

}
