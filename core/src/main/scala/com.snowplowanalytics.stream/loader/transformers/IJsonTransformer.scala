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
package com.snowplowanalytics.stream.loader.transformers

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}

trait IJsonTransformer
    extends ITransformer[ValidatedJsonRecord, EmitterJsonInput]
    with StdinTransformer
