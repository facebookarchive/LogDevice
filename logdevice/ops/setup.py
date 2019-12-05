#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.


import os

# All rights reserved.
#
from distutils.file_util import copy_file
from pathlib import Path

# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext as _build_ext


class copy_cmake_built_libs_build_ext(_build_ext):
    def build_extension(self, ext):
        target_path = Path(self.get_ext_fullpath(ext.name))
        # Creates the parent directory
        target_path.parent.mkdir(parents=True, exist_ok=True)
        copy_file(
            ext.sources[0], str(target_path), verbose=self.verbose, dry_run=self.dry_run
        )


# Extension: filename created by cmake
def generate_extensions(directory, include_base_dir=True):
    path_list = Path(directory).glob("**/*.so")
    exts = []
    for path in path_list:
        if include_base_dir:
            parent_path = str(path.parent)
        else:
            parent_path = str(path.parent.relative_to(directory))
        package_name = parent_path.replace(os.path.sep, ".")
        mod_name = "{}.{}".format(package_name, path.name[:-3])
        print("Module {} from {}".format(mod_name, str(path)))
        exts.append(Extension(mod_name, sources=[str(path)]))
    return exts


setup(
    name="ldshell",
    version="0.1",
    author="Ahmed Soliman",
    author_email="asoli@fb.com",
    description="The main tool for operating LogDevice clusters",
    url="https://github.com/facebookincubator/LogDevice",
    packages=find_packages(
        include=[
            "ldshell",
            "ldshell.*",
            "ldops",
            "ldops.*",
            "logdevice",
            "logdevice.*",
        ],
        exclude=["logdevice.ldquery.external.*"],
    ),
    ext_modules=generate_extensions("gen-py3", include_base_dir=False)
    + generate_extensions("logdevice", include_base_dir=True),
    entry_points={"console_scripts": ["ldshell = ldshell.main:main"]},
    install_requires=["python-nubia", "tabulate", "humanize"],
    python_requires=">=3.6",
    cmdclass={"build_ext": copy_cmake_built_libs_build_ext},
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
