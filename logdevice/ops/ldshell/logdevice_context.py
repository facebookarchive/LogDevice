#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import getpass
import logging
import sys
import tempfile
import textwrap
import typing

from ldshell.helpers import create_socket_address
from logdevice.admin.clients import AdminAPI
from logdevice.admin.common.types import SocketAddress, SocketAddressFamily
from logdevice.client import (
    Client,
    LoggingLevel,
    parse_log_level,
    set_log_fd,
    setLoggingLevel,
)
from logdevice.ldquery import LDQuery
from nubia import context, exceptions
from nubia.internal.io.eventbus import Message
from pygments.token import Token
from termcolor import cprint
from thrift.py3 import get_client as create_thrift_client


class Context(context.Context):
    def __init__(self):
        super(Context, self).__init__()
        self._ld_level = LoggingLevel.NONE
        self._reset()

    def _set_log_level(self, level):
        ld_level = parse_log_level(level)
        self._ld_level = ld_level
        cprint("Logging Level: {}".format(ld_level), "magenta", file=sys.stderr)
        log_stream = (
            getattr(logging.root.handlers[0], "stream", sys.stderr)
            if logging.root.handlers
            else sys.stderr
        )
        set_log_fd(log_stream.fileno())
        setLoggingLevel(ld_level)

    def _reset(self):
        self._cluster_name = None
        self._admin_server_address = None
        self._config_file = None
        self._ldquery = None
        self._client = None
        self._is_connected = False

    def _set_arguments(self, args):
        self._loglevel = args.loglevel
        self._timeout = args.command_timeout
        # The cluster admin server socket address
        if args.admin_server_host or args.admin_server_unix_path:
            self._admin_server_address = create_socket_address(
                server_host=args.admin_server_host,
                server_path=args.admin_server_unix_path,
                server_port=args.admin_server_port,
            )
        self._set_log_level(args.loglevel)

    def on_cli(self, cmd, args):
        self._set_arguments(args)
        # dispatch the on connected message
        self.registry.dispatch_message(Message.CONNECTED, self._admin_server_address)

    def on_interactive(self, args):
        self._set_arguments(args)
        ret = self._registry.find_command("connect").run_cli(args)
        if ret:
            raise exceptions.CommandError("Failed starting interactive mode")

    def is_connected(self):
        return self._is_connected

    def require_connected(self):
        if not self.is_connected():
            cprint(
                "You need to be connected to a cluster in order to perform "
                "this operation, either pass a "
                "--admin-server-host/unix-path in CLI mode, or use the "
                "connect command in interactive"
            )
            raise Exception("A connection to a cluster is required!")

    @property
    def ldquery(self):
        self.require_connected()
        with self._lock:
            if not self._ldquery:
                self._build_ldquery()
            return self._ldquery

    def get_node_admin_client(self, address: SocketAddress):
        """
        Creates an Admin Client that connects to a given node.
        """
        if address.address_family == SocketAddressFamily.INET:
            return create_thrift_client(
                AdminAPI, host=address.address, port=address.port
            )
        else:
            # SocketAddressFamily::UNIX
            return create_thrift_client(AdminAPI, path=address.address)

    def get_cluster_admin_client(self):
        """
        Returns the Admin API client for the connected cluster. It uses the
        --admin-server-hostname/port/unix-path to target a specific
        admin server if specified.
        """
        self.require_connected()
        client = None
        address = self._admin_server_address
        if address.address_family == SocketAddressFamily.INET:
            client = create_thrift_client(
                AdminAPI, host=address.address, port=address.port
            )
        else:
            # SocketAddressFamily::UNIX
            client = create_thrift_client(AdminAPI, path=address.address)
        return client

    def get_client(self):
        self.require_connected()
        with self._lock:
            if not self._client:
                try:
                    self._client = self.build_client()
                except Exception as e:
                    cprint("Cannot connect to logdevice cluster!", "red")
                    self._reset()
                    raise e
            return self._client

    async def fetch_config(self):
        async with self.get_cluster_admin_client() as client:
            logging.info(
                "Fetching logdevice config via address {}", self._admin_server_address
            )
            config = await client.dumpServerConfigJson()
            self._config_file = tempfile.NamedTemporaryFile()
            self._config_file.write(config.encode("utf-8"))
            self._config_file.flush()
            self._config_path = self._config_file.name
            logging.info("Config downloaded and stored in {}", self._config_path)

    async def fetch_cluster_name(self):
        async with self.get_cluster_admin_client() as client:
            self._cluster_name = await client.getClusterName()

    def build_client(self, settings=None):
        logging.info("Creating a logdevice client using config {}", self._config_path)
        default_settings = {"on-demand-logs-config": "true", "num-workers": 2}
        default_settings.update(settings or {})
        return Client(
            "ldshell",
            self._config_path,
            timeout=self._timeout,
            settings=default_settings,
        )

    def _build_ldquery(self):
        config_path = self._config_path
        self._ldquery = LDQuery(config_path=config_path, timeout=self._timeout)

    def on_connected(self, *args, **kwargs):
        if args:
            self._admin_server_address = args[0]
        if not self._admin_server_address:
            cprint(
                textwrap.dedent(
                    """
                    You are not connected to any logdevice clusters, in order to do so,
                    please use the connect command and pass the admin server
                    host or unix path as an argument, for instance:
                    """
                ),
                "yellow",
                file=sys.stderr,
            )
            cprint("connect 192.168.0.4:6440", file=sys.stderr)
            self._is_connected = False
        else:
            self._is_connected = True
            # Fetch config, and cluster name.
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.fetch_config())
                loop.run_until_complete(self.fetch_cluster_name())
            except Exception as e:
                cprint("{}".format(e), "red")
                self._is_connected = False

    def get_prompt_tokens(self) -> typing.List[typing.Tuple[typing.Any, str]]:
        cluster = self._cluster_name

        if cluster is not None:
            tokens = [
                (Token.Username, getpass.getuser()),
                (Token.At, "@"),
                (Token.Tier, cluster),
                (Token.Pound, "> "),
            ]
        else:
            tokens = [
                (Token.Username, getpass.getuser()),
                (Token.Tier, "@"),
                (Token.RPrompt, "DISCONNECTED"),
                (Token.Pount, "> "),
            ]

        return tokens
