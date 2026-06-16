# Snowplow Elasticsearch Loader

[![Release][release-image]][releases]
[![License][license-image]][license]

## Introduction

The Snowplow Elasticsearch Loader consumes Snowplow enriched events or failed events from an
[Amazon Kinesis][kinesis] stream, transforms them to JSON, and writes them to
[Elasticsearch][elasticsearch]. Events which cannot be transformed or which are rejected by
Elasticsearch are written to a separate Kinesis stream.

## Find out more

| Technical Docs             | Setup Guide          |
|:--------------------------:|:--------------------:|
| ![i1][techdocs-image]      | ![i2][setup-image]   |
| [Technical Docs][techdocs] | [Setup Guide][setup] |

## Copyright and license

Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Limited Use License Agreement][license]. _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions][faq].)_


[release-image]: https://img.shields.io/github/v/release/snowplow/snowplow-elasticsearch-loader
[releases]: https://github.com/snowplow/snowplow-elasticsearch-loader/releases

[license-image]: https://img.shields.io/badge/license-Snowplow--Limited--Use-blue.svg?style=flat
[license]: https://docs.snowplow.io/limited-use-license-1.1

[kinesis]: http://aws.amazon.com/kinesis/
[elasticsearch]: http://www.elasticsearch.org/

[setup]: https://docs.snowplowanalytics.com/docs/getting-started-on-snowplow-open-source/
[techdocs]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/loaders-storage-targets/elasticsearch-loader/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png

[faq]: https://docs.snowplow.io/docs/contributing/limited-use-license-faq/