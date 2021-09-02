# Snowplow Elasticsearch Loader

[![Build Status][travis-image]][travis]
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
snowplow-elasticsearch-loader 1.0.1

Usage: snowplow-elasticsearch-loader [options]

  --config <filename>
```

## Running

Create your own config file:

```bash
$ cp examples/config.hocon.sample my.conf
```

Update the configuration to fit your needs like modifying the AWS credentials:

```json
aws {
  access-key: "default"
  secret-key: "default"
}
```

Next, start the loader, making sure to specify your new config file:

```bash
$ java -jar snowplow-elasticsearch-loader-1.0.1.jar --config my.conf
```

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]       | ![i2][setup-image]    | ![i3][roadmap-image]                 |
| [Technical Docs][techdocs]  | [Setup Guide][setup]  | _coming soon_                        |

## Copyright and license

Copyright 2014-2021 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[travis-image]: https://travis-ci.org/snowplow/snowplow-elasticsearch-loader.png?branch=master
[travis]: http://travis-ci.org/snowplow/snowplow-elasticsearch-loader

[release-image]: https://api.bintray.com/packages/snowplow/registry/snowplow%3Aelasticsearch-loader/images/download.svg
[releases]: https://bintray.com/snowplow/registry/snowplow%3Aelasticsearch-loader/_latestVersion

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE

[kinesis]: http://aws.amazon.com/kinesis/
[nsq]: http://nsq.io
[snowplow]: http://snowplowanalytics.com
[elasticsearch]: http://www.elasticsearch.org/
[sbt]: http://www.scala-sbt.org

[setup]: https://github.com/snowplow/snowplow/wiki/elasticsearch-loader-setup
[techdocs]: https://github.com/snowplow/snowplow/wiki/elasticsearch-loader

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[license]: http://www.apache.org/licenses/LICENSE-2.0
