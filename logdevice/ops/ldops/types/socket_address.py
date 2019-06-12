#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import logging
import random
import socket
from dataclasses import dataclass
from ipaddress import AddressValueError, IPv4Address, IPv6Address
from typing import Any, List, Optional, Tuple, Union

from logdevice.admin.common.types import (
    SocketAddress as ThriftSocketAddress,
    SocketAddressFamily,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SocketAddress:
    address_family: SocketAddressFamily
    address: Optional[Union[IPv6Address, IPv4Address]] = None
    path: Optional[str] = None
    port: Optional[int] = None

    def __post_init__(self) -> None:
        if self.address_family == SocketAddressFamily.INET:
            if not self.port:
                raise ValueError(f"Invalid port: {self.port}")
            if not self.address:
                raise ValueError(f"Invalid address: {self.address}")
        elif self.address_family == SocketAddressFamily.UNIX:
            if not self.path:
                raise ValueError(f"Invalid path: {self.path}")
        else:
            assert False, "unreachable"  # pragma: nocover

    def to_thrift(self) -> ThriftSocketAddress:
        """
        Returns Thrift-representation of SocketAddress to use in communication
        with AdminAPI.
        """
        addr: str
        if self.address_family == SocketAddressFamily.INET:
            assert self.address is not None
            # TODO Admin Server uses string comparison for IP addresses
            # on 'compressed' (T45290450)
            addr = self.address.compressed
        elif self.address_family == SocketAddressFamily.UNIX:
            assert self.path is not None
            addr = self.path
        else:
            assert False, "unreachable"  # pragma: nocover

        return ThriftSocketAddress(
            address_family=self.address_family,
            address=addr,
            port=self.port if self.address_family == SocketAddressFamily.INET else None,
        )

    @classmethod
    def from_ip_port(cls, ipaddr: str, port: int) -> "SocketAddress":
        """
        Convenience helper which just parses IP-address and returns instance
        """
        addr: Union[IPv4Address, IPv6Address]
        try:
            addr = IPv6Address(ipaddr)
        except AddressValueError:
            addr = IPv4Address(ipaddr)

        return SocketAddress(
            address_family=SocketAddressFamily.INET, address=addr, port=port
        )

    @classmethod
    def from_host_port(cls, host: str, port: int) -> "SocketAddress":
        """
        Convenience helper which does resolving and returns instance
        """
        socket_address: SocketAddress
        info: List[Tuple[int, int, int, str, Tuple[Any, ...]]] = socket.getaddrinfo(
            host, port
        )
        return cls.from_ip_port(random.choice(info)[4][0], port)

    @classmethod
    def from_thrift(cls, src: ThriftSocketAddress) -> "SocketAddress":
        """
        Parses Thrift-representation of SocketAddress and returns instance
        """
        socket_address: SocketAddress
        if src.address_family == SocketAddressFamily.INET:
            assert src.address is not None
            socket_address = cls.from_ip_port(src.address, src.port)
        elif src.address_family == SocketAddressFamily.UNIX:
            socket_address = SocketAddress(
                address_family=src.address_family, path=src.address
            )
        else:
            assert False, "unreachable"  # pragma: nocover

        return socket_address

    def __str__(self) -> str:
        if self.address_family == SocketAddressFamily.INET:
            if isinstance(self.address, IPv4Address):
                return f"{self.address.compressed}:{self.port}"
            elif isinstance(self.address, IPv6Address):
                return f"[{self.address.compressed}]:{self.port}"
            else:
                assert False, "unreachable"  # pragma: nocover
        else:
            return f"unix:{self.path}"
