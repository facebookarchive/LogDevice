#!/usr/bin/env python

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import absolute_import, division, print_function, unicode_literals


"Facebook-specific additions to the fbcode_builder spec for LogDevice"

# read_fbcode_builder_config is not defined; this file is not run directly
# rather, it is called from fbcode/opensource/fbcode_builder/utils.py using
# exec; which itself defines read_fbcode_builder_config
config = read_fbcode_builder_config("fbcode_builder_config.py")  # noqa: F821
config["legocastle_opts"] = {
    "alias": "logdevice-oss-linux",
    "oncall": "asoli",
    "build_name": "Open-source build for LogDevice",
    "legocastle_os": "unstable",
}
