#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import sys
from typing import Optional

from ldops.const import DEFAULT_THRIFT_PORT
from logdevice.admin.common.types import SocketAddress, SocketAddressFamily


def confirm_prompt(prompt, stream=sys.stdout):
    """Asks a question until it gets a y/n answer.  Returns bool."""
    return ask_prompt(prompt, options=["y", "n"], stream=stream) == "y"


def ask_prompt(prompt, options=None, help_str=None, stream=sys.stdout):
    options = [option.lower() for option in options]
    answer = ""
    help_suffix = "(type ? for help) " if help_str else ""

    print("")

    while answer.lower() not in options:
        answer = input("{} [{}] {}".format(prompt, "/".join(options), help_suffix))

        if answer == "?" and help_str:
            print(help_str)

    print("", file=stream)

    return answer.lower()


def create_socket_address(
    server_host: Optional[str],
    server_port: int = DEFAULT_THRIFT_PORT,
    server_path: Optional[str] = None,
) -> SocketAddress:
    """
    Creates a SocketAddress object given a host/path/port
    """
    address_family = SocketAddressFamily.INET
    if server_path is not None:
        address_family = SocketAddressFamily.UNIX
    return SocketAddress(
        address_family=address_family,
        address=server_path
        if address_family == SocketAddressFamily.UNIX
        else server_host,
        port=server_port,
    )


def parse_socket_address(raw: str, is_unix: bool = False) -> SocketAddress:
    if not is_unix:
        port = DEFAULT_THRIFT_PORT
        tup = raw.rsplit(":", 1)
        if len(tup) >= 2:
            port = int(tup[1])
        address = tup[0]
        return SocketAddress(
            address_family=SocketAddressFamily.INET, address=address, port=port
        )
    else:
        # TODO: Support parsing unix socket if input is not ipaddress/hostname.
        raise NotImplementedError("Parsing unix sockets is not implemented yet.")
