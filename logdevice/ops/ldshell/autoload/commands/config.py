#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from nubia import command, context

@command
class Config:
    """
    Commands about logdevice config
    """
    @command
    async def dump(self):
        """
        Prints the server config in JSON format
        """
        ctx = context.get_context()
        async with ctx.get_cluster_admin_client() as client:
            print(await client.dumpServerConfigJson())
