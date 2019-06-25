---
id: Installation
title: Build LogDevice
sidebar_label: Build LogDevice
---
Follow these instructions to build LogDevice components including `logdeviced` (the LogDevice server), the client library, and `ldshell`, an administrative shell utility.

At this time, the only supported platform is Ubuntu 18 LTS "Bionic Beaver". Support for Fedora is experimental. LogDevice relies on some c++17 features, so building LogDevice requires a compiler that supports c++17.

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

** Fedora only - Install packages. **

```shell
sudo yum install $(cat LogDevice/logdevice/build_tools/fedora.deps)
```

You also need mstch, which is not shipped with Fedora, so clone and build it:

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

`-j $(nproc)` sets building concurrency equal to the number of processor cores.

If the build does not complete successfully, particularly if you get an internal compiler error,
you may need to reduce the number of parallel jobs. In the above make command, try `make -j 4` or `make -j 2`.

## Built binaries and libraries

On successful completion, the build process creates the following binaries and libraries:

* `_build/bin/logdeviced` -- the LogDevice server
* `_build/lib/liblogdevice.{a,so}` -- the LogDevice client library
* `_build/bin/ld{write,cat,tail,trim}` -- simple utilities for writing into a log, reading from a log, tailing a log, and trimming a log.
* `_build/bin/ld-dev-cluster` -- a test utility that configures and runs a test LogDevice cluster on the local machine

To install these binaries and [LDShell](ldshell.md) into your system, run:

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

## Build Docker image

You can build a Docker container that runs LogDevice using the Dockerfile under the `docker` directory.

First, install and launch Docker. You may need to increase the number of resources available to Docker to avoid a build failure.

You can build a production Docker image or a development one.
The development image is the container with all the development packages, including compiler tools, against which LogDevice developers compile.
The binaries generated from the packages are combined with the run-time image to produce a Docker image.

The production image is a few hundred megabytes in size while the development image is many gigabytes.

**To build a production Docker image,** from the root directory of the repo, enter the following command:

```shell
docker build -t logdevice-ubuntu -f docker/Dockerfile.ubuntu .
```


**To build a development Docker image,** from the root directory of the repo, enter the following command:

```shell
docker build -t logdevice-ubuntu -f docker/Dockerfile.ubuntu --target=builder .
```

This builds the Docker image, tags it with `logdevice-ubuntu`, and puts the binaries under `/usr/local/bin/` of the Docker image. So you can, for example, start the test cluster utility by running:

```shell
docker run -it logdevice-ubuntu /usr/local/bin/ld-dev-cluster
```
