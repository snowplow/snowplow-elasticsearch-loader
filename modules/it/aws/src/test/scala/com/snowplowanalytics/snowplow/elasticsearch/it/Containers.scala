/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.elasticsearch.it

import java.net.URI
import java.util.UUID

import scala.concurrent.duration._

import cats.effect.{IO, Resource}

import org.testcontainers.containers.{BindMode, Network}
import org.testcontainers.containers.wait.strategy.Wait

import com.dimafeng.testcontainers.GenericContainer

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._

import com.snowplowanalytics.snowplow.elasticsearch.aws.BuildInfo

object Containers {

  object Images {
    val localstack     = sys.env.getOrElse("LOCALSTACK_IMAGE", "localstack/localstack:3.8.1")
    val elasticsearch6 = sys.env.getOrElse("ES6_IMAGE", "docker.elastic.co/elasticsearch/elasticsearch:6.8.23")
    val elasticsearch7 = sys.env.getOrElse("ES7_IMAGE", "docker.elastic.co/elasticsearch/elasticsearch:7.17.27")
    val elasticsearch8 = sys.env.getOrElse("ES8_IMAGE", "docker.elastic.co/elasticsearch/elasticsearch:8.17.0")
    val elasticsearch9 = sys.env.getOrElse("ES9_IMAGE", "docker.elastic.co/elasticsearch/elasticsearch:9.0.1")
    val openSearch1    = sys.env.getOrElse("OS1_IMAGE", "opensearchproject/opensearch:1.3.19")
    val openSearch2    = sys.env.getOrElse("OS2_IMAGE", "opensearchproject/opensearch:2.18.0")
    val openSearch3    = sys.env.getOrElse("OS3_IMAGE", "opensearchproject/opensearch:3.0.0")
  }

  /** Pre-resolved host-side URLs and stream names needed by tests. */
  case class TestInfrastructure(
    kinesisEndpoint: String, // e.g. "http://localhost:4566" — for writing events from test code
    esUrl: String, // e.g. "http://localhost:32768" — for polling ES from test code
    streamGood: String, // e.g. "good-<uuid>"
    streamBad: String // e.g. "bad-<uuid>"
  )

  /** ES/OpenSearch backend — returned by the per-spec container factory. */
  case class EsContainer(
    internalUrl: String, // used by Loader container env var, e.g. "http://elasticsearch:9200"
    externalUrl: String // used by test polling, e.g. "http://localhost:32768"
  )

  /**
   * Start all containers on a shared network and return TestInfrastructure. Containers are started
   * once per spec class.
   *
   * @param esFactory
   *   Creates and starts the ES or OpenSearch container on the given network, returning an
   *   EsContainer with internal and external URLs.
   */
  def allContainers(
    esFactory: Network => Resource[IO, EsContainer],
    optConfigPath: Option[String] = None,
    loaderPurpose: String         = "ENRICHED_EVENTS",
    mappingType: Option[String]   = None
  ): Resource[IO, TestInfrastructure] = {
    val uuid       = UUID.randomUUID().toString
    val streamGood = s"good-$uuid"
    val streamBad  = s"bad-$uuid"
    val appName    = s"it-loader-$uuid"
    val configPath = optConfigPath.getOrElse(getClass.getClassLoader.getResource("config/loader.hocon").getPath)

    for {
      network <- Resource.fromAutoCloseable(IO(Network.newNetwork()))
      kinesisEndpoint <- localstack(network, streamGood, streamBad)
      esContainer <- esFactory(network)
      _ <- Resource.eval(TestHelpers.createIndexWithFieldLimit(esContainer.externalUrl, "snowplow", 200, mappingType))
      _ <- loader(network, esContainer.internalUrl, streamGood, streamBad, appName, configPath, loaderPurpose)
    } yield TestInfrastructure(kinesisEndpoint, esContainer.externalUrl, streamGood, streamBad)
  }

  def elasticsearch(image: String)(network: Network): Resource[IO, EsContainer] = {
    val alias        = "elasticsearch"
    val internalPort = 9200
    val container = GenericContainer(
      dockerImage = image,
      env = Map(
        "xpack.security.enabled" -> "false",
        "xpack.security.http.ssl.enabled" -> "false",
        "discovery.type" -> "single-node",
        "ES_JAVA_OPTS" -> "-Xms256m -Xmx512m", // ES 6/7
        "CLI_JAVA_OPTS" -> "-Xms256m -Xmx512m" // ES 8+ (ES_JAVA_OPTS deprecated)
      ),
      exposedPorts = Seq(internalPort),
      waitStrategy = Wait.forHttp("/").forPort(internalPort).forStatusCode(200)
    )
    Resource
      .make {
        IO {
          container.container.withNetwork(network)
          container.container.withNetworkAliases(alias)
        } >> IO.blocking(container.start()).as(container)
      }(c => IO.blocking(c.stop()))
      .map { c =>
        EsContainer(
          internalUrl = s"http://$alias:$internalPort",
          externalUrl = s"http://localhost:${c.container.getMappedPort(internalPort)}"
        )
      }
  }

  def openSearch(image: String)(network: Network): Resource[IO, EsContainer] = {
    val alias        = "opensearch"
    val internalPort = 9200
    val container = GenericContainer(
      dockerImage = image,
      env = Map("DISABLE_SECURITY_PLUGIN" -> "true", "discovery.type" -> "single-node", "OPENSEARCH_JAVA_OPTS" -> "-Xms256m -Xmx512m"),
      exposedPorts = Seq(internalPort),
      waitStrategy = Wait.forHttp("/").forPort(internalPort).forStatusCode(200)
    )
    Resource
      .make {
        IO {
          container.container.withNetwork(network)
          container.container.withNetworkAliases(alias)
        } >> IO.blocking(container.start()).as(container)
      }(c => IO.blocking(c.stop()))
      .map { c =>
        EsContainer(
          internalUrl = s"http://$alias:$internalPort",
          externalUrl = s"http://localhost:${c.container.getMappedPort(internalPort)}"
        )
      }
  }

  private def makeKinesisClient(endpoint: String): KinesisAsyncClient =
    KinesisAsyncClient
      .builder()
      .endpointOverride(URI.create(endpoint))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar"))
      )
      .region(Region.EU_CENTRAL_1)
      .build()

  private def localstack(
    network: Network,
    streamGood: String,
    streamBad: String
  ): Resource[IO, String] = {
    val alias        = "localstack"
    val internalPort = 4566
    val container = GenericContainer(
      dockerImage  = Images.localstack,
      env          = Map("AWS_ACCESS_KEY_ID" -> "foo", "AWS_SECRET_ACCESS_KEY" -> "bar"),
      exposedPorts = Seq(internalPort),
      waitStrategy = Wait.forLogMessage(".*Ready\\..*", 1)
    )
    Resource
      .make {
        IO {
          container.container.withNetwork(network)
          container.container.withNetworkAliases(alias)
        } >> IO.blocking(container.start()).as(container)
      }(c => IO.blocking(c.stop()))
      .evalMap { c =>
        val mappedPort      = c.container.getMappedPort(internalPort)
        val kinesisEndpoint = s"http://localhost:$mappedPort"
        Resource
          .fromAutoCloseable(IO(makeKinesisClient(kinesisEndpoint)))
          .use { kinesisClient =>
            createStream(kinesisClient, streamGood) >>
              createStream(kinesisClient, streamBad) >>
              waitForActive(kinesisClient, streamGood) >>
              waitForActive(kinesisClient, streamBad)
          }
          .as(kinesisEndpoint)
      }
  }

  private def createStream(client: KinesisAsyncClient, name: String): IO[Unit] =
    IO.fromCompletableFuture(
      IO(client.createStream(CreateStreamRequest.builder().streamName(name).shardCount(1).build()))
    ).void
      .handleErrorWith {
        case _: ResourceInUseException => IO.unit // stream already exists — safe to proceed
        case e                         => IO.raiseError(e)
      }

  private def waitForActive(
    client: KinesisAsyncClient,
    name: String,
    retriesLeft: Int = 60
  ): IO[Unit] = {
    val check: IO[StreamStatus] =
      IO.fromCompletableFuture(
        IO(client.describeStream(DescribeStreamRequest.builder().streamName(name).build()))
      ).map(_.streamDescription().streamStatus())
    check.flatMap {
      case StreamStatus.ACTIVE  => IO.unit
      case _ if retriesLeft > 0 => IO.sleep(1.seconds) >> waitForActive(client, name, retriesLeft - 1)
      case status =>
        IO.raiseError(new RuntimeException(s"Timed out waiting for stream '$name' to become ACTIVE (last status: $status)"))
    }
  }

  private def loader(
    network: Network,
    esUrl: String,
    streamGood: String,
    streamBad: String,
    appName: String,
    configPath: String,
    purpose: String
  ): Resource[IO, GenericContainer] = {
    val container = GenericContainer(
      dockerImage = BuildInfo.dockerAlias,
      env = Map(
        "AWS_REGION" -> "eu-central-1",
        "AWS_ACCESS_KEY_ID" -> "foo",
        "AWS_SECRET_ACCESS_KEY" -> "bar",
        "STREAM_GOOD" -> streamGood,
        "STREAM_BAD" -> streamBad,
        "APP_NAME" -> appName,
        "LOCALSTACK_ENDPOINT" -> "http://localstack:4566",
        "ES_URL" -> esUrl,
        "PURPOSE" -> purpose
      ),
      fileSystemBind = Seq(
        GenericContainer.FileSystemBind(configPath, "/snowplow/config/loader.hocon", BindMode.READ_ONLY)
      ),
      command      = Seq("--config", "/snowplow/config/loader.hocon"),
      exposedPorts = Seq(8000),
      waitStrategy = Wait.forHttp("/health").forPort(8000).forStatusCode(200)
    )
    Resource.make {
      IO(container.container.withNetwork(network)) >>
        IO.blocking(container.start()).as(container)
    }(c => IO.blocking(c.stop()))
  }
}
