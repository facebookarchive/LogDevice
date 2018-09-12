# LogDevice
[![Build Status](https://circleci.com/gh/facebookincubator/LogDevice.svg?style=shield&circle-token=1a68ba9a5f81ea693f341726bc4039980490f16e)](https://circleci.com/gh/facebookincubator/LogDevice)

LogDevice is a scalable and fault tolerant distributed log system. While a
file-system stores and serves data organized as files, a log system stores and
delivers data organized as logs. The log can be viewed as a record-oriented,
append-only, and trimmable file.

LogDevice is designed from the ground up to serve many types of logs with high
reliability and efficiency at scale. It is also highly tunable allowing each use
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

for full documentation, please visit the [logdevice.io](http://logdevice.io/) website

## Requirements
LogDevice is supported on Ubuntu 18.04 (Bionic Beaver) only. However, it should
be possible to build it on any other Linux distribution without significant
challenges.

## Building LogDevice
* [Building](https://facebookincubator.github.io/LogDevice/docs/Installation.html)

## Full documentation
* [Introduction](https://facebookincubator.github.io/LogDevice/docs/Overview.html)
* [Cluster Configuration Guide](https://facebookincubator.github.io/LogDevice/docs/Config.html)
* [LogDevice API](https://facebookincubator.github.io/LogDevice/docs/API_Intro.html)
* [LogDevice Shell](https://facebookincubator.github.io/LogDevice/docs/LDShell.html)
* [LDQuery](https://facebookincubator.github.io/LogDevice/docs/LDQuery.html)

## Join the LogDevice community
* Website: [logdevice.io](https://logdevice.io/)
* Facebook: [LogDevice Users Group](https://www.facebook.com/groups/logdevice.oss/)

See the [CONTRIBUTING](CONTRIBUTING.md) file for how to help out.

## License
LogDevice is BSD licensed, as found in the [LICENSE](LICENSE) file.
