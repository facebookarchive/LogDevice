#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from setuptools import find_packages, setup


setup(
    name="ldshell",
    version="0.1",
    author="Ahmed Soliman",
    author_email="asoli@fb.com",
    description="The main tool for operating LogDevice clusters",
    url="https://github.com/facebookincubator/LogDevice",
    packages=find_packages(include=["ldshell", "ldshell.*"]),
    entry_points={"console_scripts": ["ldshell = ldshell.main:main"]},
    install_requires=["python-nubia"],
    python_requires=">=3.6",
    classifiers=(
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ),
)
