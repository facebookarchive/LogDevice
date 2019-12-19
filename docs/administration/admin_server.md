---
id: AdminServer
title: LogDevice Admin Server (Control Plane)
sidebar_label: Admin Server (Control Plane)
---
The standalone Admin Server (`bin/ld-admin-server`) is a side-car service that runs next to any LogDevice cluster. It extracts the necessary components that are needed to serve the [Admin API](https://github.com/facebookincubator/LogDevice/blob/master/logdevice/admin/if/admin.thrift) requests into its own daemon that can be deployed with its own release cadence.

The admin server also runs embedded in every `logdeviced` instance but with a
lot of the features disabled. This gives the flexibility of running a separate
administrative service that runs on its own host (for reliability and separation
 of concerns purposes) or you can pick one of the nodes of the cluster and
enable the necessary features on the admin server component of this `logdeviced` instance. (e.g, by passing to enable maintenance manager `--enable-maintenance-manager`).

Note that the temporary unavailability of the standalone admin server will not
affect the critical path of the service. LogDevice storage nodes and sequencers
will continue to be operational but you will lose the ability to introspect some
of the state and all [maintenance operations](maintenances.md) will wait until
the standalone admin server is started again.

> At Facebook, we prefer the separate admin server (standalone service) which 
> can be updated and pushed with a different release train than the storage 
> daemons. **This is our recommended setup.**

## What is the Admin API?
The Admin API is a thrift RPC service that offers a gateway for tooling and
automation to perform introspection queries or maintenance operations on a given
cluster. The [thrift IDL file](https://github.com/facebookincubator/LogDevice/blob/master/logdevice/admin/if/admin.thrift) is well documented.

Most of the command that you see in [LDShell](ldshell.md) use the Admin API
behind the scenes to offer the functionality but you can write your own client
in your favorite language if you need to build automation on top of it.

## Why do we need Admin Server?
The admin server hosts a critical event-driven orchestration component called
the [Maintenance Manager](#the-maintenance-manager). It's the component that powers the automatic
execution and tracking of [maintenance operations](maintenances.md) in a safe
and convergent fashion. Later in this document we will explain all of that in
details.

The admin server is also a great non-intrusive way to simulate outages via the
internal [Safety Checker](safety_checker.md) that will give you confidence in
whether an operation can be done safely or not.

> Note that **Maintenance Manager** will make sure that any maintenance request
> is safe before performing any action on the cluster, so in most cases if you
> don't need to run the safety checker simulations manually. But it can be
> useful if you just want to test a scenario without applying it.

## The Maintenance Manager

The maintenane manager is a central event-driven orchestration system for
LogDevice clusters. It is responsible for guaranteeing safety of maintenance
operations and automatic sequencing and resolution of maintenance requests
coming from different actors or users.

### Key Features
1. **Cluster membership management**: In conjunction with the
   `node-self-registration` feature in logdeviced and the zookeeper-backed nodes
   configuration store. Maintenance manager orchestrates cluster toplogy changes
   by driving the maintenance transitions necessary to get the new nodes ready
   to serve production traffic or drain them cleanly to ensure safety of node
   removal if a cluster shrink operation is needed.
2. **Data durability management**: Upon requesting maintenance operations that
   will result in long-term unavailability of a storage node or in cases where
   data can be under-replicated for periods that go beyond the
   `self-initiated-rebuilding-grace-period`, Maintenance manager will trigger
   data re-replication/rebuilding to restore the durability requirements and
   apply internal maintenance automatically.
3. **Maintenance operations management**: It's responsible for storing
   maintenance requests along with metadata information in a couple of internal
   logdevice logs. The maintenance requests represent maintenance *targets* that
   will be stored and asynchronously evaluated. The maintenance manager will
   only attempt to change the current status to the target it it's safe to do so
   via the [Safety Checker](safety_checker.md). If an operation is unsafe, it
   will block the maintenance until it's safe. It will continuously evaluate all
   blocked maintenances and allow some of them to proceed if it became safe.
   There is no need to manually retry maintenances externally since this is an
   automatic convergent system.
4. **Metadata logs nodeset management**: As the user drains more nodes, the
   maintenance manager will attempt to extend the internal nodeset for metadata
   logs with nodes that are `ENABLED`. This increases the chances of future
   maintenances succeeding by reducing internal dependency on nodes that are
   already drained.
5. **Operational observability**: The maintenance manager offers great
   introspection abilities into the internals (available via ldshell's
   `maintenance` set of commands). It helps users understand why maintenances
   are blocked and what is the current state of the system.
