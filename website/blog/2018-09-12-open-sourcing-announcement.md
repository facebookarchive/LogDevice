---
title: Open-sourcing LogDevice, a distributed data store for sequential data
---

We are excited to announce that today we are open-sourcing LogDevice! The code
is now [available on GitHub](https://github.com/facebookincubator/LogDevice)
under the permissive 3-clause BSD license.

LogDevice is a distributed data store for sequential data originally developed
within Facebook. [This 2017 blog post](https://code.fb.com/core-data/logdevice-a-distributed-data-store-for-logs/)
in the Facebook Engineering blog explains the basic components of LogDevice. You
can also refer to the [recording](https://atscaleconference.com/videos/logdevice-a-file-structured-log-system/)
of a presentation from the @Scale conference for more detail.

LogDevice is widely deployed for use at Facebook. Existing use cases include:
* Stream processing pipelines
* Distribution of index updates in large distributed databases
* Machine learning pipelines
* Replication pipelines
* Durable reliable task queues

Over the past few years we have built a large number of internal tools to manage
LogDevice clusters. We are working on slowly shifting some of that automation to
be shipped in the open-source version as well. For this initial release, we are
including a bare-bones toolset that forms the core of LogDevice operational
infrastructure. In a large-scale production environment it will need to be
supplemented by other tools for tasks like monitoring and cluster topology
management.

LogDevice comes with a command-line administration tool called LDShell
(`ldshell`). LDShell is the primary management tool that we use here at Facebook
as well. Stay tuned as we add more commands and automation to it. This is one of
the areas where we expect support from the open-source community.

We are excited to see your contributions to LogDevice, especially those related
to integration with cloud computing environments, monitoring systems, and
configuration management. Getting LogDevice to run on Kubernetes is a good
starting point if you are interested in helping.

To get started with LogDevice, dive into the [overview of the
system](https://logdevice.io/docs/Overview.html) to learn how it works, or check
out [the code on GitHub](https://github.com/facebookincubator/LogDevice)

*By Ahmed Soliman and Andrejs Krasilnikovs, LogDevice team.*
