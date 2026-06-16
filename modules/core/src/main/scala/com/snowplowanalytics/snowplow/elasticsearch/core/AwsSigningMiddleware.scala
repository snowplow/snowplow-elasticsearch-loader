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

import cats.effect.Async
import cats.effect.Resource
import fs2.Stream
import org.http4s.client.{Client => Http4sClient}
import org.http4s.{Header, Request}
import org.typelevel.ci.CIString
import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}
import software.amazon.awssdk.http.auth.aws.signer.{AwsV4FamilyHttpSigner, AwsV4HttpSigner}
import software.amazon.awssdk.http.auth.spi.signer.{SignRequest, SignerProperty}
import software.amazon.awssdk.http.{ContentStreamProvider, SdkHttpFullRequest, SdkHttpMethod, SdkHttpRequest}
import java.net.URI
import scala.jdk.CollectionConverters._

object AwsSigningMiddleware {

  private val serviceSigningNameProp: SignerProperty[String] = AwsV4FamilyHttpSigner.SERVICE_SIGNING_NAME
  private val regionNameProp: SignerProperty[String]         = AwsV4HttpSigner.REGION_NAME

  /**
   * Wraps an http4s client with AWS SigV4 request signing.
   *
   * Uses DefaultCredentialsProvider — picks up credentials from env vars, instance profile, ECS
   * task role, etc. in the standard AWS chain. Returns a Resource so the provider's background
   * threads are cleaned up on release.
   */
  def apply[F[_]: Async](
    client: Http4sClient[F],
    serviceSigningName: String,
    region: String
  ): Resource[F, Http4sClient[F]] =
    for {
      credentialsProvider <- Resource.fromAutoCloseable(Async[F].delay(DefaultCredentialsProvider.builder().build()))
      signer = AwsV4HttpSigner.create()
    } yield withCredentials(client, serviceSigningName, region, credentialsProvider, signer)

  private[core] def withCredentials[F[_]: Async](
    client: Http4sClient[F],
    serviceSigningName: String,
    region: String,
    credentialsProvider: AwsCredentialsProvider,
    signer: AwsV4HttpSigner
  ): Http4sClient[F] =
    Http4sClient[F] { req =>
      for {
        bodyBytes <- Resource.eval(req.body.compile.to(Array))
        payload = ContentStreamProvider.fromByteArray(bodyBytes)
        credentials <- Resource.eval(Async[F].blocking(credentialsProvider.resolveCredentials()))
        sdkReq = buildSdkRequest(req)
        signedResult = signer.sign(
                         SignRequest
                           .builder(credentials)
                           .request(sdkReq)
                           .payload(payload)
                           .putProperty(serviceSigningNameProp, serviceSigningName)
                           .putProperty(regionNameProp, region)
                           .build()
                       )
        // The original body stream was consumed above to compute the payload hash.
        // withBodyStream restores the body from the buffered bytes so the underlying
        // client can still read it when sending the signed request.
        signedReq = applySignedHeaders(req, signedResult.request())
                      .withBodyStream(Stream.emits(bodyBytes))
        response <- client.run(signedReq)
      } yield response
    }

  private def buildSdkRequest[F[_]](req: Request[F]): SdkHttpFullRequest = {
    val builder = SdkHttpFullRequest
      .builder()
      .method(SdkHttpMethod.fromValue(req.method.name))
      .uri(URI.create(req.uri.renderString))

    req.headers.headers.foreach { h =>
      builder.appendHeader(h.name.toString, h.value)
    }

    builder.build()
  }

  private def applySignedHeaders[F[_]](
    req: Request[F],
    signedSdkReq: SdkHttpRequest
  ): Request[F] = {
    val newHeaders: List[Header.ToRaw] = signedSdkReq
      .headers()
      .asScala
      .flatMap { case (name, values) =>
        values.asScala.map(v => Header.Raw(CIString(name), v): Header.ToRaw)
      }
      .toList
    req.putHeaders(newHeaders: _*)
  }
}
