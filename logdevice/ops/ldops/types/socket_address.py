#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import logging
from dataclasses import dataclass
from ipaddress import AddressValueError, IPv4Address, IPv6Address
from typing import Optional, Union

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
            addr = self.address.exploded
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
    def from_thrift(cls, src: ThriftSocketAddress) -> "SocketAddress":
        """
        Parses Thrift-representation of SocketAddress and returns instance
        """
        socket_address: SocketAddress
        if src.address_family == SocketAddressFamily.INET:
            addr: Union[IPv4Address, IPv6Address]
            try:
                addr = IPv6Address(src.address)
            except AddressValueError:
                addr = IPv4Address(src.address)

            socket_address = SocketAddress(
                address_family=src.address_family, address=addr, port=src.port
            )
        elif src.address_family == SocketAddressFamily.UNIX:
            socket_address = SocketAddress(
                address_family=src.address_family, path=src.address
            )
        else:
            assert False, "unreachable"  # pragma: nocover

        return socket_address
