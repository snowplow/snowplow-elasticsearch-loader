import sbt._

object Dependencies {
  object V {
    // Java
    val slf4j     = "2.0.17"
    val elastic4s = "9.1.1"
    val awsSdk    = "2.42.41"

    // Scala
    val betterMonadicFor = "0.3.1"
    val decline          = "2.1.0"
    val badRows          = "2.3.0"
    val analyticsSdk     = "3.2.0"
    val circeOptics      = "0.14.1"

    // Snowplow common-streams
    val commonStreams = "0.24.0"

    // CVE overrides
    val jacksonCore  = "2.21.2" // Fix CVE SNYK-JAVA-COMFASTERXMLJACKSONCORE-15907551, -15365924
    val commonsLang3 = "3.18.0" // Fix CVE SNYK-JAVA-ORGAPACHECOMMONS-10734078
    val netty        = "4.1.132.Final" // Fix CVE SNYK-JAVA-IONETTY-15789756, -15789758

    // Tests
    val catsEffect          = "3.6.1"
    val specs2              = "4.21.0"
    val specs2CE            = "1.6.0"
    val testcontainersScala = "0.41.4"
  }

  // Java
  val slf4j       = "org.slf4j"              % "slf4j-simple"  % V.slf4j
  val sts         = "software.amazon.awssdk" % "sts"           % V.awsSdk % Runtime
  val awsAuth     = "software.amazon.awssdk" % "auth"          % V.awsSdk
  val awsHttpAuth = "software.amazon.awssdk" % "http-auth-aws" % V.awsSdk

  // Scala
  val betterMonadicFor      = "com.olegpy"            %% "better-monadic-for"           % V.betterMonadicFor
  val declineEffect         = "com.monovore"          %% "decline-effect"               % V.decline
  val badRows               = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badRows
  val analyticsSdk          = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val circeOptics           = "io.circe"              %% "circe-optics"                 % V.circeOptics
  val elastic4sClientHttp4s = "nl.gn0s1s"             %% "elastic4s-client-http4s"      % V.elastic4s
  val elastic4sJsonCirce    = "nl.gn0s1s"             %% "elastic4s-json-circe"         % V.elastic4s

  // Snowplow common-streams
  val streamsCore = "com.snowplowanalytics" %% "streams-core"   % V.commonStreams
  val runtime     = "com.snowplowanalytics" %% "runtime-common" % V.commonStreams
  val kinesis     = "com.snowplowanalytics" %% "kinesis"        % V.commonStreams

  // Tests
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect % Test
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2     % Test
  val specs2CE          = "org.typelevel" %% "cats-effect-testing-specs2" % V.specs2CE   % Test

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
    catsEffectTestkit,
    specs2,
    specs2CE
  )

  val awsDependencies = Seq(
    kinesis,
    sts,
    catsEffectTestkit,
    specs2,
    specs2CE
  )

  // IT
  val testcontainersCore = "com.dimafeng" %% "testcontainers-scala-core" % V.testcontainersScala % Test

  val itDependencies = Seq(
    testcontainersCore,
    catsEffectTestkit,
    specs2,
    specs2CE
  )

  // Transitive CVE overrides — force safe versions that parents haven't yet adopted
  val cveOverrides = Seq(
    "com.fasterxml.jackson.core" % "jackson-core"      % V.jacksonCore,
    "org.apache.commons"         % "commons-lang3"     % V.commonsLang3,
    "io.netty"                   % "netty-codec-http"  % V.netty,
    "io.netty"                   % "netty-codec-http2" % V.netty
  )
}
