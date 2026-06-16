/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.elasticsearch.core

import cats.effect.IO
import cats.effect.Ref
import cats.effect.testing.specs2.CatsEffect
import org.http4s._
import org.http4s.client.{Client => Http4sClient}
import org.specs2.Specification
import org.typelevel.ci._
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

class AwsSigningMiddlewareSpec extends Specification with CatsEffect {

  def is = s2"""
  AwsSigningMiddleware should
    add Authorization header to signed request       $e1
    add x-amz-date header                            $e2
    preserve the request body after signing          $e3
    include signing region in Authorization scope    $e4
  """

  private val testCredentials: StaticCredentialsProvider = StaticCredentialsProvider.create(
    AwsBasicCredentials.create("EXAMPLEID", "EXAMPLEKEY")
  )
  private val signer      = AwsV4HttpSigner.create()
  private val testRegion  = "us-east-1"
  private val testService = "es"

  // A client that captures both the request headers and the consumed body bytes.
  // Body bytes are read eagerly inside the handler before the resource scope closes.
  private def capturingClient: IO[(Http4sClient[IO], IO[(Headers, Array[Byte])])] =
    Ref.of[IO, Option[(Headers, Array[Byte])]](None).map { ref =>
      val client = Http4sClient.fromHttpApp[IO](HttpApp[IO] { req =>
        req.body.compile.toVector.flatMap { bodyBytes =>
          ref.set(Some((req.headers, bodyBytes.toArray))).as(Response[IO](Status.Ok))
        }
      })
      val getCapture = ref.get.flatMap(IO.fromOption(_)(new RuntimeException("no request captured")))
      (client, getCapture)
    }

  def e1 =
    for {
      (underlying, getCapture) <- capturingClient
      signingClient = AwsSigningMiddleware.withCredentials(underlying, testService, testRegion, testCredentials, signer)
      req           = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("https://search-test.us-east-1.es.amazonaws.com/_search"))
      _ <- signingClient.run(req).use(_ => IO.unit)
      (headers, _) <- getCapture
    } yield {
      val authHeader = headers.get(ci"Authorization")
      (authHeader must beSome) and
        (authHeader.get.head.value must startWith("AWS4-HMAC-SHA256"))
    }

  def e2 =
    for {
      (underlying, getCapture) <- capturingClient
      signingClient = AwsSigningMiddleware.withCredentials(underlying, testService, testRegion, testCredentials, signer)
      req           = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("https://search-test.us-east-1.es.amazonaws.com/_search"))
      _ <- signingClient.run(req).use(_ => IO.unit)
      (headers, _) <- getCapture
    } yield headers.get(ci"x-amz-date") must beSome

  def e3 = {
    val body = """{"query": {"match_all": {}}}"""
    for {
      (underlying, getCapture) <- capturingClient
      signingClient = AwsSigningMiddleware.withCredentials(underlying, testService, testRegion, testCredentials, signer)
      req = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("https://search-test.us-east-1.es.amazonaws.com/_search"))
              .withEntity(body)
      _ <- signingClient.run(req).use(_ => IO.unit)
      (_, bodyBytes) <- getCapture
    } yield new String(bodyBytes, "UTF-8") must beEqualTo(body)
  }

  def e4 =
    for {
      (underlying, getCapture) <- capturingClient
      signingClient = AwsSigningMiddleware.withCredentials(underlying, testService, "eu-west-1", testCredentials, signer)
      req           = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("https://search-test.eu-west-1.es.amazonaws.com/_search"))
      _ <- signingClient.run(req).use(_ => IO.unit)
      (headers, _) <- getCapture
    } yield {
      val auth = headers.get(ci"Authorization").get.head.value
      auth must contain("eu-west-1")
    }
}
