---
id: Installation
title: Installation
sidebar_label: Installation
---

## Building from sources

Currently the only supported platform is Ubuntu 18 LTS "Bionic Beaver".

Support for Fedora is experimental.

**Clone the LogDevice GitHub repository, including its submodules.**

```shell
git clone --recurse-submodules git://github.com/facebookincubator/LogDevice
```

That will create a top-level `LogDevice` directory. The source code is in `LogDevice/logdevice`. There are two git submodules in the tree: `logdevice/external/folly` and `logdevice/external/rocksdb`.

**Install packages that LogDevice depends on.**

```shell
sudo apt-get install -y $(cat LogDevice/logdevice/build_tools/ubuntu.deps)
```

If the above fails with "Unable to locate package", run `sudo apt-get update` first to update the package list.

On Fedora you may try:

```shell
sudo yum install $(cat LogDevice/logdevice/build_tools/fedora.deps)
```

You will also need mstch, which is not shipped with Fedora, so you will do have to do:

```shell
git clone https://github.com/no1msd/mstch.git
cd mstch
mkdir build
cd build
cmake ..
make && make install
cd
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
* `_build/lib/liblogdevice.{a,so}` -- the LogDevice client library
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

## Building using docker

A Dockerfile is also provided for building logdevice under the `docker` directory. To build logdevice using docker, run the following from the root of the repo:

```shell
docker build -t logdevice-ubuntu -f docker/Dockerfile.ubuntu .
```

This will build the docker image and tag it with `logdevice-ubuntu`. The binaries are then available under `/usr/local/bin/` of the docker image. So you can for example start the test cluster by running:

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

Most of LogDevice documentation lives in `LogDevice/docs`. The API reference
in `LogDevice/website/static/api` is generated with Doxygen.
