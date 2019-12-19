---
id: Installation
title: Build LogDevice
sidebar_label: Build LogDevice
---
Follow these instructions to build LogDevice components including `logdeviced` (the LogDevice server), `ld-admin-server` the standalone administration server, the client library, and `ldshell`, an administrative shell utility. See the [Build artifacts](#built-binaries-and-libraries-build-artifacts) for the full list.

At this time, the only supported platform is **Ubuntu 18 LTS "Bionic Beaver"**. LogDevice relies on some *C++17* features, so building LogDevice requires a compiler that supports at least *C++17*.

## Clone the repo and build from the source

Clone the LogDevice GitHub repository, **including its submodules**.

```shell-session
git clone --recurse-submodules git://github.com/facebookincubator/LogDevice
```

> Tip: If you already have a logdevice clone locally, we recommend that every time you
pull updates that you make sure that the submodules are synchronized. Inside the
*LogDevice* directory you can run this: `git submodule update --recursive`

This creates a top-level `LogDevice` directory, with the source code in `LogDevice/logdevice`. There are several git submodules in the tree: `LogDevice/logdevice/external/`.

## Install build dependencies

Key dependencies that you need to build LogDevice:
- CMake (minimum *3.4.0*)
- C++ compiler that support *C++17* or higher (We will require C++20 soon!)
- Python *3.6+* and virtualenv
- Cython *0.28.6* (due to https://github.com/cython/cython/issues/2985)
- Boost C++ Libraries (minimum *1.55.0*)
- LibEvent
- Zookeeper MT C++ client library
- Other packages in `LogDevice/logdevice/build_tools/ubuntu.deps`


```shell-session
sudo apt-get install -y $(cat LogDevice/logdevice/build_tools/ubuntu.deps)
```

> If the command fails with `Unable to locate package`, run `sudo apt-get update` to update the package list.


## Create a build directory

This directory will contain the build artifacts and the staging environment at
which we will some of the dependenciees that `cmake` will fetch during build. It
will also be the home for a Python virtual environment that can run `ldshell`
and has all the neecessary libraries and dependencies ready for use. This
staging environment will be created in `staging/usr/local/` under the build
directory.

> Make sure that you have `virtualenv` available by running `python3 -m pip
> install virtualenv` before proceeding.

```shell-session
# The top-level build directory
mkdir -p LogDevice/_build
# The home for the virtualenv
mkdir -p LogDevice/_build/staging/usr/local
# You need to run the next build commands
cd LogDevice/_build
# Create the virtualenv
python3 -m virtualenv staging/usr/local
# Use the virtualenv
source ./staging/usr/local/bin/activate
```

### Install the python dependencies in the virtualenv
> Make sure that you are (in) the virtualenv before running the next commands
> `source ./staging/usr/local/bin/activate`

```shell-session
python3 -m pip install --upgrade setuptools wheel cython==0.28.6
```

## Start the build

```shell-session
cmake -Dthriftpy3=ON ../logdevice/
```

**Run make from `LogDevice/_build` directory.**

```shell-session
make -j $(nproc)
```

`-j $(nproc)` sets building concurrency equal to the number of processor cores.

If the build does not complete successfully, particularly if you get an `internal compiler error`,
you may need to reduce the number of parallel jobs. In the above make command, try `make -j 4` or `make -j 2`.

## Built binaries and libraries (Build Artifacts)

Upon successful completion, the build process creates the following binaries and libraries:

### Daemon binaries
* `_build/bin/logdeviced` The LogDevice server
* `_build/bin/ld-admin-server` The LogDevice server admin server
### LogDevice Shared Libraries
* `_build/lib/liblogdevice.{a,so}` The logdevice client library for C++ clients.
* `_build/lib/liblogdevice_python_util.so` A utility library used by our Python bindings.

### Utilities
* `_build/bin/ld-replication-checker` A data consistency checker.
* `_build/bin/ld-dev-cluster` A utility that runs three node cluster locally
with minimal dependencies. Useful for testing and development.
* `_build/bin/ldcat` A utility to read a LogDevice log.  Records are printed to stdout, one per line.
* `_build/bin/ldtail` A utility to tail a LogDevice log.  Records are printed to stdout, one per line.
* `_build/bin/ldtrim` A utility to manually trim a LogDevice log.

### Runtime dependency libraries:

* `_build/staging/usr/local/lib/*.so` Folly and thrift libraries. Copy these
                                 over to your system if you want to run outside
                                 the virtualenv.

### Administrative Tooling
* `_build/staging/usr/local/bin/ldshell` LDShell has to run from the python
virtualenv. Or use `make install` to install everything on your system.
* `_build/python-out/dist/ldshell-*.whl` LDShell's Python3 wheel that you can
use to install LDShell on your system (requires the next two wheel to be
installed and the Runtime libraries (see above) to be installed)
* `_build/folly-prefix/src/folly-build/folly/cybld/dist/folly-*.whl` is folly's Python3 wheel that you can install on your system (already installed on the virtualenv).
* `_build/fbthrift-prefix/src/fbthrift-build/thrift/lib/py3/cybld/dist/thrift-*.whl` is thrift's Python3 wheel that you can install on your system (already installed on the virtualenv).

### Installation

You can use the standard `make install` inside the build directory to install
logdevice components on your system (including ldshell). But note the following:

> This will *not* install thrift and folly shared libararies. For these you need
> to manually copy them over onto your `/usr/local/lib/` directory.

## Debug and release builds

If you prefer to keep separate debug and release builds, create `_build/debug` and `_build/release` subdirectories and run CMake in them with a flag to request the desired build type. For example:

```shell-session
cd LogDevice/_build/debug
cmake -Dthriftpy3=ON -DCMAKE_BUILD_TYPE=Debug ../../logdevice
make -j$(nproc)
```

## Build Docker image

You can build a Docker container that runs LogDevice using the Dockerfile under the `docker` directory. We build nightly docker images available on dockerhub so that you don't need to build them yourself. You can pull these with:

```shell-sssion
docker pull facebookincubator/logdevice:latest
```

But if you want to build your own, make sure that you install and launch Docker. You may need to increase the number of resources available to Docker to avoid a build failure.

You can build a **production** Docker image or a **development** one.
The development image is the container with all the development packages, including compiler tools, against which LogDevice developers compile.
The binaries generated from the packages are combined with the run-time image to produce a Docker image.

The production image is a few hundred megabytes in size while the development image is many gigabytes.

**To build a production Docker image,** from the root directory of the repo, enter the following command:

```shell-session
docker build -t logdevice-ubuntu -f docker/Dockerfile.ubuntu .
```


**To build a development Docker image,** from the root directory of the repo, enter the following command:

```shell-session
docker build -t logdevice-ubuntu -f docker/Dockerfile.ubuntu --target=builder .
```

This builds the Docker image, tags it with `logdevice-ubuntu`, and puts the binaries under `/usr/local/bin/` of the Docker image. So you can, for example, start the test cluster utility by running:

```shell-session
docker run -it --rm logdevice-ubuntu /usr/local/bin/ld-dev-cluster
```
