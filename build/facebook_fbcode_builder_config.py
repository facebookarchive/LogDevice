#!/usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals


"Facebook-specific additions to the fbcode_builder spec for LogDevice"

# read_fbcode_builder_config is not defined; this file is not run directly
# rather, it is called from fbcode/opensource/fbcode_builder/utils.py using
# exec; which itself defines read_fbcode_builder_config
config = read_fbcode_builder_config("fbcode_builder_config.py")  # noqa: F821
config["legocastle_opts"] = {
    "alias": "logdevice-oss-linux",
    "oncall": "cmarchent",
    "build_name": "Open-source build for LogDevice",
    "legocastle_os": "ubuntu_16.04",
}
