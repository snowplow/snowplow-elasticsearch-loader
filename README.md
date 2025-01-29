# Snowplow Elasticsearch Loader

[![Release][release-image]][releases]
[![License][license-image]][license]

## Introduction

The Snowplow Elasticsearch Loader consumes Snowplow enriched events or failed events from an
[Amazon Kinesis][kinesis] stream or [NSQ][nsq] topic, transforms them to JSON, and writes them to
[Elasticsearch][elasticsearch]. Events which cannot be transformed or which are rejected by
Elasticsearch are written to a separate Kinesis stream.

## Building

Assuming you already have [SBT][sbt] installed:

```bash
$ git clone git://github.com/snowplow/snowplow-elasticsearch-loader.git
$ sbt compile
```

## Usage

The Snowplow Elasticsearch Loader has the following command-line interface:

```
snowplow-elasticsearch-loader 2.1.3

Usage: snowplow-elasticsearch-loader [options]

  --config <filename>
```

## Running

Create your own config file:

```bash
$ cp config/config.kinesis.reference.hocon my.conf
```

Update the configuration to fit your needs.

Next, start the loader, making sure to specify your new config file:

```bash
$ java -jar snowplow-elasticsearch-loader-2.1.3.jar --config my.conf
```

## Find out more

| Technical Docs             | Setup Guide          | Roadmap              | Contributing                 |
|:--------------------------:|:--------------------:|:--------------------:|:----------------------------:|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image] | ![i4][contributing-image]    |
| [Technical Docs][techdocs] | [Setup Guide][setup] | [Roadmap][roadmap]   | [Contributing][contributing] |

## Copyright and license

Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Limited Use License Agreement][license]. _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions][faq].)_


[release-image]: https://img.shields.io/github/v/release/snowplow/snowplow-elasticsearch-loader
[releases]: https://github.com/snowplow/snowplow-elasticsearch-loader/releases

[license-image]: https://img.shields.io/badge/license-Snowplow--Limited--Use-blue.svg?style=flat
[license]: https://docs.snowplow.io/limited-use-license-1.1

[kinesis]: http://aws.amazon.com/kinesis/
[nsq]: http://nsq.io
[snowplow]: http://snowplowanalytics.com
[elasticsearch]: http://www.elasticsearch.org/
[sbt]: http://www.scala-sbt.org

[setup]: https://docs.snowplowanalytics.com/docs/getting-started-on-snowplow-open-source/
[roadmap]: https://github.com/snowplow/snowplow-elasticsearch-loader/issues
[techdocs]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/loaders-storage-targets/elasticsearch-loader/
[contributing]: https://docs.snowplowanalytics.com/docs/contributing/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[faq]: https://docs.snowplow.io/docs/contributing/limited-use-license-faq/