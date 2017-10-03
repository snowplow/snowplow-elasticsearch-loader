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

// Java
import java.io.File
import java.util.Properties

// Config
import com.typesafe.config.ConfigFactory

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

// Scalaz
import scalaz._
import Scalaz._

// Pureconfig
import pureconfig._

// This project
import sinks._
import clients._
import model._

/**
 * Main entry point for the Elasticsearch sink
 */
trait ElasticsearchSinkApp {
  def arguments: Array[String]
  def elasticsearchSender: ElasticsearchSender

  def parseConfig(): Option[ESLoaderConfig] = {
    val projectName = "snowplow-elasticsearch-loader"
    case class FileConfig(config: File = new File("."))
    val parser = new scopt.OptionParser[FileConfig](projectName) {
      head(projectName, generated.Settings.version)
      help("help")
      version("version")
      opt[File]("config").required().valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(config = f))
        .validate(f =>
          if (f.exists) success
          else failure(s"Configurationfile $f does not exist")
        )
    }

    val conf = parser.parse(arguments, FileConfig()) match {
      case Some(c) => ConfigFactory.parseFile(c.config).resolve()
      case None    => ConfigFactory.empty()
    }

    if (conf.isEmpty()) {
      System.err.println("Empty configuration file")
      System.exit(1)
    }

    implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    val esLoaderConf = loadConfig[ESLoaderConfig](conf) match {
      case Left(e) =>
        System.err.println(s"configuration error: $e")
        System.exit(1)
        None
       case Right(c) => Some(c)
    }

    esLoaderConf
  }

/**
  * Builds a KinesisConnectorConfiguration
  *
  * @param config the configuration HOCON
  * @return A KinesisConnectorConfiguration
  */
  def convertConfig(config: ESLoaderConfig): KinesisConnectorConfiguration = {
    val props = new Properties

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM,
      config.streams.inStreamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT,
      config.kinesis.endpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME,
      config.kinesis.appName.trim)
    props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM,
      config.kinesis.initialPosition)
    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS,
      config.kinesis.maxRecords.toString)

    // So that the region of the DynamoDB table is correct
    props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME,
      config.kinesis.region)

    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_ENDPOINT,
      config.elasticsearch.client.endpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_CLUSTER_NAME,
      config.elasticsearch.cluster.name)
    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_PORT,
      config.elasticsearch.client.port.toString)

    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
      config.streams.buffer.byteLimit.toString)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT,
      config.streams.buffer.recordLimit.toString)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
      config.streams.buffer.timeLimit.toString)

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "elasticsearch")
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    new KinesisConnectorConfiguration(props, CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey))
  }

  def run(conf: ESLoaderConfig): Unit = {
    val streamType = conf.streamType
    val documentIndex = conf.elasticsearch.cluster.index
    val documentType = conf.elasticsearch.cluster.clusterType

    val credentials = CredentialsLookup.getCredentialsProvider(conf.aws.accessKey, conf.aws.secretKey)
    val tracker = conf.monitoring.map(e => SnowplowTracking.initializeTracker(e.snowplow))

    val goodSink = conf.sink.good match {
      case "stdout" => Some(new StdouterrSink)
      case "elasticsearch" => None
    }

    val badSink = conf.sink.bad match {
      case "stderr" => new StdouterrSink
      case "nsq" => new NsqSink(conf)
      case "none" => new NullSink
      case "kinesis" => {
        val kinesisSinkName = conf.streams.outStreamName
        val kinesisSinkRegion = conf.kinesis.region
        val kinesisSinkEndpoint = conf.kinesis.endpoint
        new KinesisSink(credentials, kinesisSinkEndpoint, kinesisSinkRegion, kinesisSinkName)
      }
    }

    val executor = conf.source match {

      // Read records from Kinesis
      case "kinesis" =>
        new KinesisSourceExecutor(streamType,
                                  documentIndex,
                                  documentType,
                                  convertConfig(conf),
                                  conf.kinesis.initialPosition,
                                  conf.kinesis.timestamp,
                                  goodSink,
                                  badSink,
                                  elasticsearchSender,
                                  tracker
                                 ).success

      // Read records from NSQ
      case "nsq" =>
        new NsqSourceExecutor(streamType,
                              documentIndex,
                              documentType,
                              conf,
                              goodSink,
                              badSink,
                              elasticsearchSender
                             ).success

      // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
      // TODO reduce code duplication
      case "stdin" => new Runnable {
        val transformer = streamType match {
          case Good => new SnowplowElasticsearchTransformer(documentIndex, documentType)
          case Bad => new BadEventTransformer(documentIndex, documentType)
          case PlainJson => new PlainJsonTransformer(documentIndex, documentType)
        }

        def run = for (ln <- scala.io.Source.stdin.getLines) {
          val emitterInput = transformer.consumeLine(ln)
          emitterInput._2.bimap(
            f => badSink.store(BadRow(emitterInput._1, f).toCompactJson, None, false),
            s => goodSink match {
              case Some(gs) => gs.store(s.getSource, None, true)
              case None => elasticsearchSender.sendToElasticsearch(List(ln -> s.success))
            }
          )
        }
      }.success

      case _ => "Source must be set to 'stdin', 'kinesis' or 'nsq'".failure
    }

    executor.fold(
      err => throw new RuntimeException(err),
      exec => {
        tracker foreach {
          t => SnowplowTracking.initializeSnowplowTracking(t)
        }
        exec.run()

        // If the stream cannot be found, the KCL's "cw-metrics-publisher" thread will prevent the
        // application from exiting naturally so we explicitly call System.exit.
        // This does not apply to NSQ because NSQ consumer is non-blocking and fall here
        // right after consumer.start()
        conf.source match {
          case "kinesis" => System.exit(1)
          case "stdin" => System.exit(1)
          // do anything
          case "nsq" =>
        }
      }
    )
  }

}