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
package com.snowplowanalytics.stream.loader
package sinks

//Java
import java.nio.charset.StandardCharsets.UTF_8

// NSQ
import com.snowplowanalytics.client.nsq.NSQProducer

// Scala
import scala.collection.JavaConverters._

import com.snowplowanalytics.stream.loader.Config.Sink.BadSink.{Nsq => NsqSinkConfig}

/**
 * NSQ sink
 *
 * @param conf config for NSQ sink
 */
class NsqSink(conf: NsqSinkConfig) extends ISink {

  private val producer = new NSQProducer().addAddress(conf.nsqdHost, conf.nsqdPort).start()

  /**
   * Writes a string to NSQ
   *
   * @param outputs The strings to write
   * @param good Unused parameter which exists to extend ISink
   */
  override def store(outputs: List[String], good: Boolean): Unit =
    producer.produceMulti(conf.streamName, outputs.map(_.getBytes(UTF_8)).asJava)
}
