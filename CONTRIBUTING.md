# Contributing to LogDevice
We want to make contributing to this project as easy and transparent as
possible.

## Our Development Process
The LogDevice source on GitHub is replicated from the internally
managed LogDevice code. Pull requests are reviewed by the LogDevice team and
when accepted, imported to the internal version. Once the change is imported,
export to GitHub is automatically triggered and the pull request is closed. 
Branch history may be "squashed" through this process, eliminating merges
and resulting in a single commit.

In the open source build, most third-party libraries are expected to be
preinstalled in system locations. The exceptions are:

**folly** changes relatively often and we want to be mostly uptodate with it.
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

## License
By contributing to LogDevice, you agree that your contributions will be licensed
under the LICENSE file in the root directory of this source tree.
