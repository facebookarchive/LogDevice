#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from ldshell.helpers import create_socket_address, parse_socket_address
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

        return self._run(parse_socket_address(arg_str))

    def run_cli(self, args):
        address = None
        if args.admin_server_host or args.admin_server_unix_path:
            address = create_socket_address(
                server_host=args.admin_server_host,
                server_path=args.admin_server_unix_path,
                server_port=args.admin_server_port,
            )
        return self._run(address)

    def _run(self, address):
        self._command_registry.dispatch_message(Message.CONNECTED, address)
        return 0

    def get_command_names(self):
        return self.cmds.keys()

    def add_arguments(self, parser):
        parser.add_parser("connect")

    def get_help(self, cmd, *args):
        return self.cmds[cmd]
