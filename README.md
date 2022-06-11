# Snowplow Elasticsearch Loader

[![Build Status][travis-image]][travis]
[![GithubRelease][release-image]][releases]
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
snowplow-elasticsearch-loader 2.0.6

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
$ java -jar snowplow-elasticsearch-loader-2.0.6.jar --config my.conf
```

## Find out more

| Technical Docs             | Setup Guide          | Roadmap              | Contributing                 |
|:--------------------------:|:--------------------:|:--------------------:|:----------------------------:|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image] | ![i4][contributing-image]    |
| [Technical Docs][techdocs] | [Setup Guide][setup] | [Roadmap][roadmap]   | [Contributing][contributing] |

## Copyright and license

Copyright 2014-2022 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[travis-image]: https://travis-ci.org/snowplow/snowplow-elasticsearch-loader.png?branch=master
[travis]: http://travis-ci.org/snowplow/snowplow-elasticsearch-loader

[release-image]: https://img.shields.io/github/v/release/snowplow/snowplow-elasticsearch-loader
[releases]: https://github.com/snowplow/snowplow-elasticsearch-loader/releases

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE

[kinesis]: http://aws.amazon.com/kinesis/
[nsq]: http://nsq.io
[snowplow]: http://snowplowanalytics.com
[elasticsearch]: http://www.elasticsearch.org/
[sbt]: http://www.scala-sbt.org

[setup]: https://docs.snowplowanalytics.com/docs/getting-started-on-snowplow-open-source/
[roadmap]: https://github.com/snowplow/enrich/issues
[techdocs]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/loaders-storage-targets/elasticsearch-loader/
[contributing]: https://docs.snowplowanalytics.com/docs/contributing/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png
[license]: http://www.apache.org/licenses/LICENSE-2.0
