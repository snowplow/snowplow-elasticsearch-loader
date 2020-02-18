package com.snowplowanalytics.stream.loader.transformers

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}

trait IJsonTransformer
    extends ITransformer[ValidatedJsonRecord, EmitterJsonInput]
    with StdinTransformer
