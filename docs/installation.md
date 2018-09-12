---
id: Installation
title: Installation
sidebar_label: Installation
---

## Building from sources

Currently the only supported platform is Ubuntu 18 LTS "Bionic Beaver".

**Clone the LogDevice GitHub repository, including its submodules.**

```shell
git clone --recurse-submodules git@github.com:facebookincubator/LogDevice.git
```

That will create a top-level `LogDevice` directory. The source code is in `LogDevice/logdevice`. There are two git submodules in the tree: `logdevice/external/folly` and `logdevice/external/rocksdb`.

**Install packages that LogDevice depends on.**

```shell
sudo apt-get install -y $(cat LogDevice/logdevice/build_tools/ubuntu.deps)
```

**Create a build directory.**

```shell
mkdir -p LogDevice/_build
cd LogDevice/_build
```

**Run cmake to configure the build.**

```shell
cmake ../logdevice/
```

**Run make from `LogDevice/_build` directory.**

```shell
make -j $(nproc)
```

-j $(nproc) sets building concurrency equal to the number of processor cores.

## Output

Upon successful completion, the build process will create the following binaries and libraries:

* `_build/bin/logdeviced` -- the LogDevice server
* `_build/lib/libldclient.{a,so}` -- the LogDevice client library
* `_build/bin/ld{write,cat,tail,trim}` -- simple utilities for writing into a log, reading from a log, tailing a log, and trimming a log.
* `_build/bin/ld-dev-cluster` -- a test utility that configures and runs a test LogDevice cluster on the local machine

If you want these binaries (and [LDShell](ldshell.md)) installed into your system, run:

```shell
sudo make install
```

## Debug and release builds

If you prefer to keep separate debug and release builds, we recommend creating build/debug and build/release subdirectories and running CMake in them with a flag requesting the desired build type. For example:

```shell
cd LogDevice/_build/debug
cmake -DCMAKE_BUILD_TYPE=Debug ../../logdevice
make -j$(nproc)
```
