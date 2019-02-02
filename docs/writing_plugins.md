---
id: WritingPlugins
title: Writing plugins
sidebar_label: Writing plugins
---
***
The LogDevice plugin system lets developers add new functionality to LogDevice.
Examples of plugins include:

* a statistics publisher that collects LogDevice counters
* trace logger that traces both server- and client-side events
* configuration plugin that loads a configuration file and notifies LogDevice
when it changes
* build info plugin that exposes build-related information
* location plugin that determines the location of the client in the network
topology.

A plugin is an object that is characterized by plugin type. Plugin type defines
which functionality the plugin is implementing and each type corresponds to a
particular type-specific interface that a plugin should implement.

Read this article if you want to write a plugin or if you want to understand
what kind of functionality can be incorporated into a plugin.

Some plugins are built and provided as part of LogDevice. These are:

* `BuiltinConfigSourceFactory`, which creates `FileConfigSource` and
`ZookeeperConfigSource` - the `ConfigSource`s used to load configuration from
files or ZooKeeper.
* `BuildInfo`, which only exposes `LOGDEVICE_VERSION` in the `version` field.

# General guidance on writing plugins

## How do I write a plugin?

As described in [Plugin types](#plugin-types), there are a number of predefined
plugin types. Look at the list of plugin types and choose the one you want to
implement. This document describes how you can write a new plugin of an existing
type.

A plugin is implemented by deriving a class from the base/interface class for
the type (`StatsPublisherFactory`, `TraceLoggerFactory`, etc.),  and overriding
the methods that implement the corresponding functionality. You should also
override the `identifier()` and `displayName()` methods. The value returned by
`identifier()` will be used to identify the plugin - you cannot load more than
one plugin that has the same `{type(), identifier()}` tuple. The value returned
by `displayName()` will be used for debug output listing the plugins that are
loaded.

## How do I make LogDevice pick up my plugin?

Build your plugin into a static library and link it to the binary.  Define a
`logdevice_plugin()` function in the global namespace that returns a raw pointer
to the heap-allocated plugin instance and export it, e.g., like this:

```c++
extern "C" __attribute__((__used__)) facebook::logdevice::Plugin*
logdevice_plugin() {
  return new MySpecialPlugin();
}
```

You may need to specify some additional linker options (probably
`--export-dynamic-symbol=logdevice_plugin`) in order to make sure
that this unused symbol isn't dropped when linking.

If you would like to export several plugins,  combine them into a
`PluginProvider`-derived plugin (a special type that provides other plugins) and
export that instead.

At the moment LogDevice allows only one plugin of each type to be loaded (aside
from built-in plugins listed in the section above). In the future this may be
changed so that you can load multiple plugins of the same type and pick which
specific plugin you want to use for each plugin type using the configuration
file (see
[issue #57 on github](https://github.com/facebookincubator/LogDevice/issues/57)).
Another planned future improvement here is loading plugins via dynamic libraries
instead of static linking (see
[issue #56 on github](https://github.com/facebookincubator/LogDevice/issues/56)).

## Considerations when writing a plugin

* All method calls on a plugin should be thread-safe.
* Make your plugin objects as lightweight as possible. Servers and clients load
all available plugins, even if not all of them will be used. During
construction, don't create threads, allocate chunks of memory, perform heavy
I/O, or execute blocking operations.
* All calls to plugin methods (and methods on objects created by plugins) should
be non-blocking. Blocking I/O (e.g., posting data to an external system) or
long-running operations should execute on a separate thread(s).

# Plugin types

For a complete up-to-date list of plugin types, refer to
[`common/plugin/plugin_types.inc`](https://github.com/facebookincubator/LogDevice/blob/master/logdevice/common/plugin/plugin_types.inc)
under the logdevice source tree.

These are the major plugin types that are most likely to be useful to LogDevice
users.

## Stats Publisher Factory

This plugin allows you to collect LogDevice stats (counters) from a LogDevice
instance. These allow you to get some insight into various aspect of LogDevice
servers and clients, and can be used to answer questions like:

* how many appends is a client making?
* how many appends are going into the node?
* what's the write throughput in bytes?
* what's the append latency like?
* are appends succeeding?
* if they are failing, what are the reasons for failures?
* how many records have been read from the node (or have been received by the
client)?
* what's the read throughput in bytes?
* are there any stalled readers?
* is rebuilding going on in the cluster?

For a full list of counters and their descriptions, refer to `.inc` files under
[`common/stats/`](https://github.com/facebookincubator/LogDevice/blob/master/logdevice/common/stats/).

The plugin should be a factory which creates an object that implements the
`StatsPublisher` interface (see
[`common/StatsPublisher.h`](https://github.com/facebookincubator/LogDevice/blob/master/logdevice/common/StatsPublisher.h)).

Publishing stats is based on a push model . Roughly every
`stats-collection-interval` (see [list of settings](settings.md)),
`StatsCollectionThread` calls the `publish()` method on the publisher with fresh
stats and a copy of previously submitted stats (so that the publisher can
calculate deltas). `StatsPublisher` would typically define a class that derives
from `Stats::EnumerationCallbacks`. That class would define callbacks that are
called for each different type of counter. The `StatsPublisher` implementation
would then call `Stats::enumerate()` with an instance of that type.

## Trace Logger Factory

This plugin allows you to trace various events, or a sample of them, in
LogDevice.

Tracers can broadly be separated by behavior into 3 categories:

* Not sampled. Every call to such tracer leads to an emitted trace to
`TraceLogger`
* Sampled. Only a fraction of calls to such a tracer lead to an emitted trace.
The sampling rate can be configured via the config file.
* Throttled. Each logging entity (typically client) is only allowed to issue a
certain number of traces per time interval. When that number is exceeded, no
more traces are emitted until the next time interval starts.

Current tracers include:

* Server-side
    * *Appender*. Traces appends on the server side. Sampled.
    * *FindKey*. Traces received findtime/findkey calls. Sampled.
    * *Client Info Tracer*. Traces all incoming connections from clients.
    Sampled.
    * *Logs Config API Tracer*. Traces calls to Logs Config API. Sampled.
    * *Slow storage task tracer*. Traces storage tasks. Sampled, weighted by
    task duration (longer tasks are more likely to get sampled).
    * *Rebuilding*. Traces rebuilding of records. Sampled.
    * *Rebuilding events*. Traces events such as starting/completing/aborting
    rebuilding for a given log, etc. Not sampled.
    * *Metadata*. Traces each sequencer activation / reprovisioning of metadata
    / metadata log write. Not sampled.
    * *Boycott*. Traces sequencer boycotts. Not sampled.
    * *Purging*. Traces purging of records on storage nodes. Throttled.
* Client-side
    * *Client append tracer*. Traces appends on the client-side. Sampled.
    * *Reads*. Traces reads of records by the client. Sampled.
    * *API hits*. Traces various LogDevice API calls, such as findtime,
    get-tail-attributes, is-log-empty, trim, etc. Sampled.
    * *Stalled reader tracer*. For readers that are unhealthy, emits a trace
    every 60 seconds with their state. Throttled.
    * *Reader flow tracer*. For every reader, generates a trace every
    `client-readers-flow-tracer-period` with various stats describing the
    reader's state. Sampled.
    * *Dataloss gap tracer*. Traces delivery of data loss gaps by the client.
    Throttled.

The plugin is a factory that creates an object that implements the `TraceLogger`
interface (see
[`common/TraceLogger.h`](https://github.com/facebookincubator/LogDevice/blob/master/logdevice/common/TraceLogger.h)).
Tracers will call the `pushSample()` method on this object to emit a trace.

## Config Source Factory

This plugin allows you to implement custom backends for loading the
configuration file and notifying LogDevice when it changes. You can consult
`FileConfigSource` and `ZookeeperConfigSource` as reference implementations.

The plugin is a factory that creates an object that implements the
`ConfigSource` interface (see
[`common/ConfigSource.h`](https://github.com/facebookincubator/LogDevice/blob/master/logdevice/common/ConfigSource.h))

## Build Info

This plugin allows you to expose build-related information (which package
includes LogDevice, which version has been built, when, by whom, etc.). This
information can then be used for debugging if tracers are used (e.g., see the
Client Info Tracer under the trace logger factory).

The plugin should derive from the `BuildInfo` class and override its methods.

## Location provider

This plugin is used on the client to determine the location of the client in the
network topology. Currently this is only used to determine whether to use SSL or
not for a particular connection (because that depends on the `ssl-boundary`
setting and the locations of both peers).

The plugin should implement the `LocationProvider` interface.
