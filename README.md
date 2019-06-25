# LogDevice
[![Build Status](https://circleci.com/gh/facebookincubator/LogDevice.svg?style=shield&circle-token=1a68ba9a5f81ea693f341726bc4039980490f16e)](https://circleci.com/gh/facebookincubator/LogDevice)

LogDevice is a scalable and fault tolerant distributed log system. While a
file-system stores and serves data organized as files, a log system stores and
delivers data organized as logs. The log can be viewed as a record-oriented,
append-only, and trimmable file.

LogDevice is designed from the ground up to serve many types of logs with high
reliability and efficiency at scale. It's also highly tunable, allowing each use
case to be optimized for the right set of trade-offs in the durability-efficiency
and consistency-availability space. Here are some examples of workloads supported
 by LogDevice:

* Write-ahead logging for durability
* Transaction logging in a distributed database
* Event logging
* Stream processing
* ML training pipelines
* Replicated state machines
* Journals of deferred work items

For full documentation, please visit [logdevice.io](https://logdevice.io/). 

## Requirements
LogDevice is only supported on Ubuntu 18.04 (Bionic Beaver). However, it should
be possible to build it on any other Linux distribution without significant
challenges. LogDevice relies on some c++17 features, so building LogDevice
requires a compiler that supports c++17.

## Quick Start
* [Run a local cluster in Docker](https://logdevice.io/docs/LocalCluster.html)

## Documentation
* [Overview](https://logdevice.io/docs/Overview.html)
* [Concepts and Architecture](https://logdevice.io/docs/Concepts.html)

## Build LogDevice
* [Build](https://logdevice.io/docs/Installation.html)

## Join the LogDevice community
* Facebook: [LogDevice Users Group](https://www.facebook.com/groups/logdevice.oss/)
* [GitHub Issues](https://github.com/facebookincubator/LogDevice/issues)

## Contributors
See the [CONTRIBUTING](CONTRIBUTING.md) file for how to help out.

## License
LogDevice is BSD licensed, as found in the [LICENSE](LICENSE) file.
