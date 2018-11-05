---
id: Installation
title: Installation
sidebar_label: Installation
---
Follow these instructions to build LogDevice components including `logdeviced` (the LogDevice server), the client library, and `ldshell`, an administrative shell utility.

At this time, the only supported platform is Ubuntu 18 LTS "Bionic Beaver".

## Clone the repo and build from the source

**Clone the LogDevice GitHub repository, including its submodules.**

```shell
git clone --recurse-submodules git://github.com/facebookincubator/LogDevice
```

This creates a top-level `LogDevice` directory, with the source code in `LogDevice/logdevice`. There are two git submodules in the tree: `logdevice/external/folly` and `logdevice/external/rocksdb`.

**Install packages that LogDevice depends on.**

```shell
sudo apt-get install -y $(cat LogDevice/logdevice/build_tools/ubuntu.deps)
```

If the command fails with "Unable to locate package", run `sudo apt-get update` to update the package list.

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

If the build does not complete successfully, for example, if you get an internal compiler error,
you may need to reduce the number of parallel jobs. In the above make command, try `make -j 4` or `make -j 2`.

## Output

On successful completion, the build process creates the following binaries and libraries:

* `_build/bin/logdeviced` -- the LogDevice server
* `_build/lib/liblogdevice.{a,so}` -- the LogDevice client library
* `_build/bin/ld{write,cat,tail,trim}` -- simple utilities for writing into a log, reading from a log, tailing a log, and trimming a log.
* `_build/bin/ld-dev-cluster` -- a test utility that configures and runs a test LogDevice cluster on the local machine

To install these binaries and [LDShell](ldshell.md) installed into your system, run:

```shell
sudo make install
```

## Debug and release builds

If you prefer to keep separate debug and release builds, create `_build/debug` and `_build/release` subdirectories and run CMake in them with a flag to request the desired build type. For example:

```shell
cd LogDevice/_build/debug
cmake -DCMAKE_BUILD_TYPE=Debug ../../logdevice
make -j$(nproc)
```

## Build using Docker

You can build a Docker container that runs LogDevice using the Dockerfile under the `docker` directory. First, install and launch Docker. You may need to increase the number of resources available to Docker to avoid a build failure.

From the root directory of the repo, enter the following command:

```shell
docker build -t logdevice-ubuntu -f docker/Dockerfile.ubuntu .
```

This builds the docker image, tags it with `logdevice-ubuntu`, and puts the binaries under `/usr/local/bin/` of the docker image. So you can, for example, start the test cluster by running:

```shell
docker run -it logdevice-ubuntu /usr/local/bin/ld-dev-cluster
```

## Documentation

This web site is created with [Docusaurus](https://docusaurus.io/).
The simplest way to test documentation changes or changes to the structure
of the site is to install Docusaurus locally as follows:

* [install Node.js](https://nodejs.org/en/download/)
* [install Yarn](https://yarnpkg.com/en/docs/install) (a package manager
for Node)

Docusarus requires Node.js >= 8.2 and Yarn >= 1.5.

* `cd LogDevice/website` (where 'LogDevice' is the root of your local LogDevice
source tree)
* `yarn add docusaurus --dev` This will create LogDevice/website/node_modules
directory with much Javascript. This may also update website/package.json with
the then-current version number of Docusaurus.

To start a Node.js server and load the doc site in a new browser tab
`cd LogDevice/website` and run `yarn run start`. To stop the server,
Ctrl+C the process.

Most of the LogDevice documentation lives in `LogDevice/docs`. The API reference
in `LogDevice/website/static/api` is generated with Doxygen.
