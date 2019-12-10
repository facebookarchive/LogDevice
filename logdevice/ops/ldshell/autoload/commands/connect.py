#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from ldshell.helpers import create_socket_address, parse_socket_address
from nubia import context
from nubia.internal.cmdbase import Command
from nubia.internal.io.eventbus import Message
from termcolor import cprint


class Connect(Command):
    cmds = {"connect": "connect to a given cluster"}

    def __init__(self):
        super(Connect, self).__init__()
        self._built_in = True

    def run_interactive(self, cmd, arg_str, raw):
        if len(arg_str) < 1:
            msg = "Cluster admin server address required"
            cprint(msg, "red")
            return -1

        ctx = context.get_context()
        # Updating the context with the new socket for the admin server
        ctx._set_admin_server_socket_address(parse_socket_address(arg_str))
        return self._run()

    def run_cli(self, args):
        return self._run()

    def _run(self):
        self._command_registry.dispatch_message(Message.CONNECTED)
        return 0

    def get_command_names(self):
        return self.cmds.keys()

    def add_arguments(self, parser):
        parser.add_parser("connect")

    def get_help(self, cmd, *args):
        return self.cmds[cmd]
