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

import cats.effect.IO
import cats.effect.testing.specs2.CatsResource

import com.snowplowanalytics.snowplow.elasticsearch.it.Containers.TestInfrastructure

import org.specs2.execute.{Failure, Skipped}
import org.specs2.mutable.SpecificationLike

import scala.concurrent.duration._

abstract class BaseSpec extends CatsResource[IO, TestInfrastructure] with SpecificationLike {

  override protected val ResourceTimeout: Duration = 2.minutes
  override protected val Timeout: Duration         = 10.minutes

  // specs2 converts timeouts to Skipped("TIMEOUT") rather than failures, which causes timed-out
  // tests to be silently reported as successes in CI. Mapping Skipped → Failure ensures a timeout
  // always fails the build.
  override def is = super.is.map(_.map(_.updateExecution(_.mapFinalResult {
    case s: Skipped => Failure(s.message)
    case other      => other
  })))
}
