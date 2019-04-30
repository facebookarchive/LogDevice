#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse

from ldshell.autoload.commands import logsconfig
from ldshell.autoload.commands.connect import Connect
from ldshell.autoload.commands.query import SelectCommand
from ldshell.logdevice_context import Context
from nubia import PluginInterface
from nubia.internal.cmdbase import AutoCommand
from nubia.internal.constants import DEFAULT_COMMAND_TIMEOUT


class LogDevicePlugin(PluginInterface):
    """
    The PluginInterface class is a way to customize ldshell for every customer
    use case. It allowes custom argument validation, control over command
    loading, custom context objects, and much more.
    """

    def create_context(self):
        """
        Must create an object that inherits from `Context` parent class.
        The plugin can return a custom context but it has to inherit from the
        correct parent class.
        """
        return Context()

    def validate_args(self, args):
        """
        This will be executed when starting ldshell, the args passed is a
        dict-like object that contains the argparse result after parsing the
        command line arguments. The plugin can choose to update the context
        with the values, and/or decide to raise `ArgsValidationError` with
        the error message.
        """
        pass

    def get_commands(self):
        commands = [Connect(), SelectCommand()]

        auto_commands = [logsconfig.Logs]
        commands.extend(AutoCommand(c) for c in auto_commands)
        return commands

    def get_opts_parser(self, add_help=True):
        """
        Builds the ArgumentParser that will be passed to ldshell, use this to
        build your list of arguments that you want for your shell.
        """
        epilog = (
            "NOTES: LIST types are given as comma separated values, "
            "eg. a,b,c,d. DICT types are given as semicolon separated "
            "key:value pairs (or key=value), e.g., a:b;c:d and if a dict "
            "takes a list as value it look like a:1,2;b:1"
        )
        opts_parser = argparse.ArgumentParser(
            description="Logdevice Shell Utility",
            epilog=epilog,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            add_help=add_help,
        )
        opts_parser.add_argument(
            "-c",
            "--config-path",
            type=str,
            help="Connect to a cluster using config file path",
        )
        opts_parser.add_argument(
            "--verbose",
            "-v",
            action="count",
            default=0,
            help="Increase ldshell verbosity, can be specified multiple times",
        )
        opts_parser.add_argument(
            "--loglevel",
            type=str,
            dest="loglevel",
            default="warning",
            choices=["none", "debug", "info", "warning", "error", "critical"],
            help="Controls the logging level",
        )
        opts_parser.add_argument(
            "--stderr",
            "-s",
            action="store_true",
            help="By default the logging output goes to a "
            "temporary file. This disables this feature "
            "by sending the logging output to stderr",
        )
        opts_parser.add_argument(
            "--command-timeout",
            required=False,
            type=int,
            default=DEFAULT_COMMAND_TIMEOUT,
            help="Timeout for commands (default %ds)" % DEFAULT_COMMAND_TIMEOUT,
        )
        opts_parser.add_argument(
            "--yes",
            "-y",
            action="count",
            default=0,
            help="Say YES to all prompts. " "Use with caution.",
        )
        return opts_parser

    def create_usage_logger(self, context):
        """
        Override this and return you own usage logger.
        Must be a subtype of UsageLoggerInterface.
        """
        return None
