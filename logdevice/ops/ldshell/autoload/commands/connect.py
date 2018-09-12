#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from nubia.internal.cmdbase import Command
from nubia.internal.io.eventbus import Message
from termcolor import cprint


class Connect(Command):
    cmds = {"connect": "connect to a given cluster"}

    def __init__(self):
        super(Connect, self).__init__()
        self._built_in = True

    def run_interactive(self, cmd, args, raw):
        if len(args) < 1:
            msg = "Cluster configuration file required"
            cprint(msg, "red")
            return -1

        return self._run(args)

    def run_cli(self, args):
        return self._run(args.config_path)

    def _run(self, config_path):
        self._command_registry.dispatch_message(Message.CONNECTED, config_path)
        return 0

    def get_command_names(self):
        return self.cmds.keys()

    def add_arguments(self, parser):
        parser.add_parser("connect")

    def get_help(self, cmd, *args):
        return self.cmds[cmd]
