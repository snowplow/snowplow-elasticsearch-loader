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

// Java
import java.io.File

// Config
import com.typesafe.config.ConfigFactory

// Scala
import scopt.OptionParser

// cats
import cats.data.Validated
import cats.syntax.validated._

// Pureconfig
import pureconfig._

// This project
import sinks._
import model._

/**
 * Main entry point for the Elasticsearch sink
 */
trait StreamLoaderApp extends App {

  val executor: Validated[String, Runnable]
  lazy val arguments = args

  private def parseConfig(): Option[StreamLoaderConfig] = {
    val projectName = "snowplow-stream-loader"
    case class FileConfig(config: File = new File("."))
    val parser: OptionParser[FileConfig] = new scopt.OptionParser[FileConfig](projectName) {
      head(projectName, com.snowplowanalytics.stream.loader.generated.Settings.version)
      help("help")
      version("version")
      opt[File]("config")
        .required()
        .valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(config = f))
        .validate(f =>
          if (f.exists) success
          else failure(s"Configurationfile $f does not exist"))
    }

    val config = parser.parse(arguments, FileConfig()) match {
      case Some(c) => ConfigFactory.parseFile(c.config).resolve()
      case None    => ConfigFactory.empty()
    }

    if (config.isEmpty) {
      System.err.println("Empty configuration file")
      System.exit(1)
    }

    implicit def hint[T]   = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    implicit val queueHint = new FieldCoproductHint[Queue]("enabled")

    val streamLoaderConf = loadConfig[StreamLoaderConfig](config) match {
      case Left(e) =>
        System.err.println(s"configuration error: $e")
        System.exit(1)
        None
      case Right(c) => Some(c)
    }

    streamLoaderConf
  }

  lazy val config: StreamLoaderConfig = parseConfig().get

  lazy val tracker = config.monitoring.map(e => SnowplowTracking.initializeTracker(e.snowplow))

  lazy val badSinkValidated = config.sink.bad match {
    case "stderr" => (new StdouterrSink).valid
    case "nsq" =>
      config.queue match {
        case queue: Nsq =>
          new NsqSink(queue.nsqdHost, queue.nsqdPort, config.streams.outStreamName).valid
        case _ => "queue config is not valid for Nsq".invalid
      }
    case "none" => (new NullSink).valid
    case "kinesis" =>
      config.queue match {
        case queue: Kinesis =>
          new KinesisSink(
            config.aws.accessKey,
            config.aws.secretKey,
            queue.endpoint,
            queue.region,
            config.streams.outStreamName).valid
        case _ => "queue config is not valid for Kinesis".invalid
      }
  }

}
