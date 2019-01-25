#!/usr/bin/env python
# Copyright (c) Facebook, Inc. and its affiliates.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
'Demo config, so that `facebook_make_legocastle_job.py --help` works here.'

config = read_fbcode_builder_config('fbcode_builder_config.py')
config['legocastle_opts'] = {
    'alias': 'example-oss',
    'oncall': 'example',
    'build_name': 'Open-source build for http://example.com',
}
