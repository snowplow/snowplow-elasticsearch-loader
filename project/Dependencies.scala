import sbt._

object Dependencies {

  object V {
    // Snowplow libraries
    val commonStreams = "0.29.0"
    val badRows       = "2.3.0"
    val analyticsSdk  = "3.2.0"

    // Scala and FP core
    val decline     = "2.1.0"
    val circeOptics = "0.14.1"

    // Elasticsearch
    val elastic4s = "9.1.1"

    // Cloud SDKs
    val awsSdk = "2.54.18"

    // Logging
    val slf4j = "2.0.17"

    // Versions held above what something upstream asks for, to keep a vulnerable copy out of the
    // images. See securityOverrides below for where each one applies.
    val jackson = "2.22.4"
    val guava   = "33.7.2-jre"

    // Test only
    val catsEffect          = "3.6.1"
    val specs2              = "4.21.0"
    val specs2CE            = "1.6.0"
    val testcontainersScala = "0.44.1"

    // Compiler plugins
    val betterMonadicFor = "0.3.1"
  }

  // Snowplow libraries
  val streamsCore  = "com.snowplowanalytics" %% "streams-core"                 % V.commonStreams
  val runtime      = "com.snowplowanalytics" %% "runtime-common"               % V.commonStreams
  val kinesis      = "com.snowplowanalytics" %% "kinesis"                      % V.commonStreams
  val badRows      = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badRows
  val analyticsSdk = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk

  // Scala and FP core
  val declineEffect = "com.monovore" %% "decline-effect" % V.decline
  val circeOptics   = "io.circe"     %% "circe-optics"   % V.circeOptics

  // Elasticsearch
  val elastic4sClientHttp4s = "nl.gn0s1s" %% "elastic4s-client-http4s" % V.elastic4s
  val elastic4sJsonCirce    = "nl.gn0s1s" %% "elastic4s-json-circe"    % V.elastic4s

  // Cloud SDKs
  val awsAuth     = "software.amazon.awssdk" % "auth"          % V.awsSdk
  val awsHttpAuth = "software.amazon.awssdk" % "http-auth-aws" % V.awsSdk
  val sts         = "software.amazon.awssdk" % "sts"           % V.awsSdk % Runtime

  // Logging
  val slf4j = "org.slf4j" % "slf4j-simple" % V.slf4j

  // Artifacts named only to force a version up; grouped into securityOverrides below
  val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % V.jackson
  val guava           = "com.google.guava"           % "guava"            % V.guava

  // Named to force a version up for binary compatibility rather than security.
  // elastic4s brings an older jackson-module-scala, which refuses to load against the newer
  // jackson-databind that runtime-common brings, so it is kept on the same version as jackson-databind.
  val jacksonModuleScala = "com.fasterxml.jackson.module" %% "jackson-module-scala" % V.jackson

  // Test only
  val catsEffectTestkit  = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect          % Test
  val specs2             = "org.specs2"    %% "specs2-core"                % V.specs2              % Test
  val specs2CE           = "org.typelevel" %% "cats-effect-testing-specs2" % V.specs2CE            % Test
  val testcontainersCore = "com.dimafeng"  %% "testcontainers-scala-core"  % V.testcontainersScala % Test

  // Compiler plugins
  val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % V.betterMonadicFor

  // Security overrides. An entry is needed only while it still evicts something: once the upstream
  // catches up, delete it. To check, drop the line and compare the resolved runtime classpath
  // before and after; reading the dependency tree for eviction markers is unreliable, because sbt
  // truncates long lines. Appended to coreDependencies, so they also cover the aws modules, which
  // depend on core.
  //
  // runtime-common brings an older jackson-databind, and iglu-scala-client (via runtime-common) and
  // elastic4s bring older ones still. Naming it pulls jackson-core up alongside. jackson-module-scala
  // above shares V.jackson and would carry jackson-databind up on its own, so dropping only this line
  // shows no change; to check it, compare against what runtime-common asks for instead.
  //
  // amazon-kinesis-client (via kinesis) brings guava 32.1.1-jre (CVE-2026-102554).
  val securityOverrides = Seq(
    jacksonDatabind,
    guava
  )

  val coreDependencies = Seq(
    slf4j,
    awsAuth,
    awsHttpAuth,
    declineEffect,
    badRows,
    analyticsSdk,
    circeOptics,
    streamsCore,
    runtime,
    elastic4sClientHttp4s,
    elastic4sJsonCirce,
    jacksonModuleScala,
    catsEffectTestkit,
    specs2,
    specs2CE
  ) ++ securityOverrides

  val awsDependencies = Seq(
    kinesis,
    sts,
    catsEffectTestkit,
    specs2,
    specs2CE
  )

  val itDependencies = Seq(
    testcontainersCore,
    catsEffectTestkit,
    specs2,
    specs2CE
  )
}
