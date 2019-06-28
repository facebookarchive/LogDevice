---
id: Overview
title: LogDevice Overview
sidebar_label: Overview
---
***
LogDevice is a *distributed log system*. While a file system stores
and serves data organized as files, a log system stores and delivers
data organized as logs. The log can be viewed as a record-oriented,
append-only, and trimmable file. In more detail:

* _Record-oriented_ means that data is written into the log as
   indivisible records, rather than individual bytes. A record is the
   smallest unit of addressing: a reader application always starts
   reading a log from a particular record, or from the next record to
   be appended to the log. The reader receives one or more
   records at a time. Records of a log are identified by monotonically
   increasing _log sequence numbers_ (LSNs). Record numbering is not
   guaranteed to be continuous. There may be gaps in the numbering
   sequence. The writer does not know in advance what LSN its record
   will be assigned upon a successful write.

* Logs are _append-only_. No support for modifying existing records is
  provided.

* Logs are expected to live for a relatively long time: days, months,
  or even years before they are deleted. The primary space
  reclamation mechanism for logs is _trimming_: dropping the oldest
  records according to either a time-based or space-based retention
  policy, or in response to an explicit request to trim a log.

LogDevice is designed from the ground up to serve many types of logs
with high reliability and efficiency at scale. It is also highly
tunable, allowing each use case to be optimized for the right set of
trade-offs in the durability-efficiency and consistency-availability
space. Here are some examples of workloads supported by LogDevice:

* Write-ahead logging for durability
* Transaction logging in a distributed database
* Event logging
* Stream processing
* ML training pipelines
* Replicated state machines
* Journals of deferred work items

## Getting started

Start by [running a LogDevice cluster locally](localcluster.md).

After that, try [Creating your first cluster](firstcluster.md), which tells
you how to configure a fully functional LogDevice cluster on multiple servers.

To learn more about LogDevice, there's an explanation of its
[concepts and architecture](concepts.md) and an overview of the
[API](API_Introduction.md).

The [build guide](installation.md) explains how to obtain the
source code and build LogDevice components including the logdeviced
server, the client library, and the administrative shell utility called
`ldshell`.

## Administration

This release contains minimal support for LogDevice cluster
administration. You can use the supplied ldshell utility to [create
and configure logs](log_configuration.md). [LDQuery](ldquery.md) provides a
powerful SQL-based mechanism for querying the state of a LogDevice
cluster and its various components, as well as several types of log
metadata.
