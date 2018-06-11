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

package com.snowplowanalytics.elasticsearch.loader.clients

// java
import java.time.{LocalDateTime, ZoneId}

// Apache HTTP
import org.apache.http.{HttpEntityEnclosingRequest, HttpRequest, HttpRequestInterceptor}
import org.apache.http.client.methods.HttpRequestWrapper
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.protocol.HttpContext

// ES
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback

// AMZ
import com.amazonaws.util.IOUtils
import com.amazonaws.auth.AWSCredentialsProvider

// aws signer
import io.ticofab.AwsSigner

// Scala
import scala.util.Try

/**
 * Signs outgoing HTTP requests to AWS Elasticsearch service
 * @param credentialsProvider AWS credentials provider
 * @param region in which to sign the requests
 */
class SignedHttpClientConfigCallback(
  credentialsProvider: AWSCredentialsProvider,
  region: String
) extends HttpClientConfigCallback {
  private def clock(): LocalDateTime = LocalDateTime.now(ZoneId.of("UTC"))
  private val service = "es"
  private val signer = AwsSigner(credentialsProvider, region, service, () => SignedHttpClientConfigCallback.this.clock())

  /** Add the signed headers to outgoing requests */
  override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder =
    httpClientBuilder.addInterceptorLast(new HttpRequestInterceptor {
      override def process(request: HttpRequest, context: HttpContext): Unit = {
        Try(request.asInstanceOf[HttpRequestWrapper]).foreach { rw =>
          // build the signed headers from the body, exists headers, params, etc
          signer.getSignedHeaders(
            Try(rw.getURI.getRawPath).getOrElse(""),
            Option(rw.getMethod).getOrElse("GET"),
            params(rw),
            headers(rw),
            body(rw)
          ).foreach { case (name, value) => request.setHeader(name, value) }
        }
      }
    })

  private def body(request: HttpRequestWrapper): Option[Array[Byte]] =
    Try(request.getOriginal)
      .flatMap(original => Try(original.asInstanceOf[HttpEntityEnclosingRequest]))
      .flatMap(enclosingReq => Try(enclosingReq.getEntity.getContent))
      .map(IOUtils.toByteArray)
      .toOption

  private def params(rw: HttpRequestWrapper): Map[String, String] =
    Try(rw.getURI.getQuery)
      .map(splitQueryString)
      .getOrElse(Map.empty)

  private def splitQueryString(s: String): Map[String, String] =
    s.split("&")
      .map(_.split("="))
      .collect { case Array(k, v) => k -> v }
      .toMap

  private def headers(rw: HttpRequestWrapper): Map[String, String] =
    Option(rw.getAllHeaders)
      .map(_.map(h => h.getName -> h.getValue).toMap)
      .getOrElse(Map.empty)
      .map {
        // Removing the port in the headers for the signed request
        case ("Host", url) => "Host" -> url.replaceFirst(":[0-9]+", "")
        case t => t
      }
}