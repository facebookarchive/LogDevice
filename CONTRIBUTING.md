# Contributing to LogDevice
We want to make contributing to this project as easy and transparent as
possible.

## Our Development Process
The LogDevice source on GitHub is replicated from the internally
managed LogDevice code. Pull requests are reviewed by the LogDevice team and,
when accepted, imported to the internal version. Once the change is imported,
export to GitHub is automatically triggered, and the pull request is closed.
Branch history may be "squashed" through this process, eliminating merges
and resulting in a single commit.

In the open source build, most third-party libraries are expected to be
preinstalled in system locations. The exceptions are:

**folly** changes relatively often and we want to be mostly up-to-date with it.
For this reason we include it in our repository as a git submodule. The git
submodule is updated as changes are made to folly. In practice this seems to
happen every few hours.

**RocksDB**: We use features from newer versions or RocksDB than what
distributions prepackage. For this reason we include it in our repository as a
git submodule. The git submodule is manually updated as needed.

## Pull Requests
We actively welcome your pull requests.

1. Fork the repo and create your branch from `master`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. If you haven't already, complete the Contributor License Agreement ("CLA").

## Test Suite
To build the test suite, after you have [run the build process](https://logdevice.io/docs/Installation.html),
run make from `LogDevice/_build` directory.

```shell
make ARGS="-j 8" test
```

## Contributor License Agreement ("CLA")
In order to accept your pull request, we need you to submit a CLA. You only need
to do this once to work on any of Facebook's open source projects.

Complete your CLA here: <https://code.facebook.com/cla>

## Issues
We use GitHub issues to track public bugs. Please ensure your description is
clear and has sufficient instructions to be able to reproduce the issue.

Facebook has a [bounty program](https://www.facebook.com/whitehat/) for the safe
disclosure of security bugs. In those cases, please go through the process
outlined on that page and do not file a public issue.

## Coding Style

For the LogDevice C++ code a [clang-format](https://clang.llvm.org/docs/ClangFormat.html)
file has been provided, [logdevice/.clang-format](logdevice/.clang-format). For
the Python code in LogDevice we use python [black formatting](https://github.com/ambv/black).
Please ensure your code is formatted with the appropriate formatter before
submitting the pull request.

## Documentation

This web site is created with [Docusaurus](https://docusaurus.io/).
The simplest way to test documentation changes or changes to the structure
of the site is to install Docusaurus locally as follows:

* [install Node.js](https://nodejs.org/en/download/)
* [install Yarn](https://yarnpkg.com/en/docs/install) (a package manager
for Node)

Docusarus requires Node.js >= 8.2 and Yarn >= 1.5.

* `cd LogDevice/website` (where `LogDevice` is the root of your local LogDevice
source tree)
* `yarn add docusaurus --dev` This creates the LogDevice/website/node_modules
directory with much Javascript. It may also update website/package.json with
the then-current version number of Docusaurus.

To start a Node.js server and load the doc site in a new browser tab,
`cd LogDevice/website` and run `yarn run start`. To stop the server,
Ctrl+C the process.

Most of the LogDevice documentation lives in `LogDevice/docs`. The API reference
in `LogDevice/website/static/api` is generated with Doxygen.

## License
By contributing to LogDevice, you agree that your contributions will be licensed
under the LICENSE file in the root directory of this source tree.
