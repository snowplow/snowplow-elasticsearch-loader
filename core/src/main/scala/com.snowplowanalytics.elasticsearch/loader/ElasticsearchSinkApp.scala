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
import com.typesafe.config.{Config, ConfigFactory}

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

// Scalaz
import scalaz._
import Scalaz._

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project
import sinks._
import clients._

// Whether the input stream contains enriched events or bad events
object StreamType extends Enumeration {
  type StreamType = Value
  val Good, Bad = Value
}

/**
 * Main entry point for the Elasticsearch sink
 */
trait ElasticsearchSinkApp {
  def arguments: Array[String]
  def elasticsearchSender: ElasticsearchSender

  def parseConfig(): Config = {
    val projectName = "snowplow-elasticsearch-loader"
    case class FileConfig(config: File = new File("."))
    val parser = new scopt.OptionParser[FileConfig](projectName) {
      head(projectName, generated.Settings.version)
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
    conf
  }


  def run(conf: Config): Unit = {
    val streamType = conf.getString("stream-type") match {
      case "good" => StreamType.Good
      case "bad" => StreamType.Bad
      case _ => throw new RuntimeException("\"stream-type\" must be set to \"good\" or \"bad\"")
    }

    val elasticsearch = conf.getConfig("elasticsearch")
    val esCluster = elasticsearch.getConfig("cluster")
    val documentIndex = esCluster.getString("index")
    val documentType = esCluster.getString("type")

    val finalConfig = convertConfig(conf)
    val nsqConfig = new ElasticsearchSinkNsqConfig(conf)

    val goodSink = conf.getString("sink.good") match {
      case "stdout" => Some(new StdouterrSink)
      case "elasticsearch" => None
      case "NSQ" => Some(new NsqSink(nsqConfig))
    }

    val badSink = conf.getString("sink.bad") match {
      case "stderr" => new StdouterrSink
      case "NSQ" => new NsqSink(nsqConfig)
      case "none" => new NullSink
      case "kinesis" => {
        val kinesis = conf.getConfig("kinesis")
        val kinesisSink = kinesis.getConfig("out")
        val kinesisSinkName = kinesisSink.getString("stream-name")
        val kinesisSinkRegion = kinesis.getString("region")
        val kinesisSinkEndpoint = getKinesisEndpoint(kinesisSinkRegion)
        new KinesisSink(finalConfig.AWS_CREDENTIALS_PROVIDER,
          kinesisSinkEndpoint, kinesisSinkRegion, kinesisSinkName)
      }
    }

    val tracker = getTracker(conf)

    val executor = conf.getString("source") match {

      // Read records from Kinesis
      case "kinesis" => new KinesisSourceExecutor(streamType, documentIndex, documentType, finalConfig, goodSink, badSink, elasticsearchSender, tracker).success

      // Read records from NSQ
      case "NSQ" => new NsqSourceExecutor(streamType, documentIndex, documentType, nsqConfig, goodSink, badSink, elasticsearchSender).success

      // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
      // TODO reduce code duplication
      case "stdin" => new Runnable {
        val transformer = streamType match {
          case StreamType.Good => new SnowplowElasticsearchTransformer(documentIndex, documentType)
          case StreamType.Bad => new BadEventTransformer(documentIndex, documentType)
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

      case _ => "Source must be set to 'stdin', 'kinesis' or 'NSQ'".failure
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
        // This did not applied for NSQ because NSQ consumer is non-blocking and fall here 
        // right after consumer.start()
        conf.getString("source") match { 
          case "kinesis" => System.exit(1)
          case "stdin" => System.exit(1)
          // do anything
          case "NSQ" =>    
        }
      }
    )
  }

  /**
   * Builds a KinesisConnectorConfiguration from the "connector" field of the configuration HOCON
   *
   * @param connector The "connector" field of the configuration HOCON
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(connector: Config): KinesisConnectorConfiguration = {

    val aws = connector.getConfig("aws")
    val accessKey = aws.getString("access-key")
    val secretKey = aws.getString("secret-key")

    val elasticsearch = connector.getConfig("elasticsearch")
    val esClient = elasticsearch.getConfig("client")
    val esCluster = elasticsearch.getConfig("cluster")
    val elasticsearchEndpoint = esClient.getString("endpoint")
    val elasticsearchPort = esClient.getString("port")
    val clusterName = esCluster.getString("name")

    val kinesis = connector.getConfig("kinesis")
    val kinesisIn = kinesis.getConfig("in")
    val streamRegion = kinesis.getString("region")
    val appName = kinesis.getString("app-name").trim
    val initialPosition = kinesisIn.getString("initial-position")
    val maxRecords = if (kinesisIn.hasPath("maxRecords")) {
      kinesisIn.getInt("maxRecords")
    } else {
      10000
    }
    val streamName = kinesisIn.getString("stream-name")
    val streamEndpoint = getKinesisEndpoint(streamRegion)

    val buffer = connector.getConfig("buffer")
    val byteLimit = buffer.getString("byte-limit")
    val recordLimit = buffer.getString("record-limit")
    val timeLimit = buffer.getString("time-limit")

    val props = new Properties

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, streamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, streamEndpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, appName)
    props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM, initialPosition)
    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, maxRecords.toString)

    // So that the region of the DynamoDB table is correct
    props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, streamRegion)

    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_ENDPOINT, elasticsearchEndpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_CLUSTER_NAME, clusterName)
    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_PORT, elasticsearchPort)

    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, byteLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, recordLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, timeLimit)

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "elasticsearch")
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    new KinesisConnectorConfiguration(props, CredentialsLookup.getCredentialsProvider(accessKey, secretKey))
  }

  def getTracker(conf: Config): Option[Tracker] =
    if (conf.hasPath("monitoring.snowplow")) {
      SnowplowTracking.initializeTracker(conf.getConfig("monitoring.snowplow")).some
    } else {
      None
    }

  private def getKinesisEndpoint(region: String): String =
    region match {
      case cn@"cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    }
}

/**
  * Rigidly load the configuration of the NSQ here to error.
  */
class ElasticsearchSinkNsqConfig(config: Config) { 
  private val nsq = config.getConfig("NSQ")
  val nsqGoodSourceTopicName = nsq.getString("good-source-topic")
  val nsqGoodSourceChannelName = nsq.getString("good-source-channel")    
  val nsqGoodSinkTopicName = nsq.getString("good-sink")
  val nsqBadSourceTopicName = nsq.getString("bad-source-topic")
  val nsqBadSourceChannelName = nsq.getString("bad-source-channel")
  val nsqBadSinkTopicName = nsq.getString("bad-sink")
  val nsqHost = nsq.getString("host")
  val nsqPort = nsq.getInt("port")
  val nsqlookupPort = nsq.getInt("lookup-port")
}
