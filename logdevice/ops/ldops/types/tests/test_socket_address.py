#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import socket
from ipaddress import AddressValueError, IPv4Address, IPv6Address
from unittest import TestCase
from unittest.mock import patch

from ldops.types.socket_address import SocketAddress
from logdevice.admin.common.types import (
    SocketAddress as ThriftSocketAddress,
    SocketAddressFamily,
)


class TestSocketAddress(TestCase):
    ip6_addr = "2001:db8::1"
    ip4_addr = "192.0.2.1"
    unix_path = "/path/to/unix.sock"
    port = 42

    def test_init(self):
        # Smoke INET
        SocketAddress(
            address_family=SocketAddressFamily.INET,
            address=IPv6Address(self.ip6_addr),
            port=self.port,
        )

        # Smoke UNIX
        SocketAddress(address_family=SocketAddressFamily.UNIX, path=self.unix_path)

        # Non-specified port
        with self.assertRaises(ValueError):
            SocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv6Address(self.ip6_addr),
            )

        # Non-specified address
        with self.assertRaises(ValueError):
            SocketAddress(address_family=SocketAddressFamily.INET, port=self.port)

        # Invalid UNIX path
        with self.assertRaises(ValueError):
            SocketAddress(address_family=SocketAddressFamily.UNIX)

    def test_to_thrift(self):
        # Valid IPv6
        self.assertEqual(
            SocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv6Address(self.ip6_addr),
                port=self.port,
            ).to_thrift(),
            ThriftSocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv6Address(self.ip6_addr).compressed,
                port=self.port,
            ),
        )

        # Valid IPv4
        self.assertEqual(
            SocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv4Address(self.ip4_addr),
                port=self.port,
            ).to_thrift(),
            ThriftSocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv4Address(self.ip4_addr).compressed,
                port=self.port,
            ),
        )

        # Valid UNIX
        self.assertEqual(
            SocketAddress(
                address_family=SocketAddressFamily.UNIX, path=self.unix_path
            ).to_thrift(),
            ThriftSocketAddress(
                address_family=SocketAddressFamily.UNIX, address=self.unix_path
            ),
        )

    def test_from_ip_port(self):
        # Valid IPv6
        self.assertEqual(
            SocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv6Address(self.ip6_addr),
                port=self.port,
            ),
            SocketAddress.from_ip_port(ipaddr=self.ip6_addr, port=self.port),
        )

        # Valid IPv4
        self.assertEqual(
            SocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv4Address(self.ip4_addr),
                port=self.port,
            ),
            SocketAddress.from_ip_port(ipaddr=self.ip4_addr, port=self.port),
        )

        # Definitely invalid IP-address
        with self.assertRaises(AddressValueError):
            SocketAddress.from_ip_port(ipaddr="invalid", port=self.port),

    @patch(
        "socket.getaddrinfo",
        return_value=[
            (
                socket.AF_INET6,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                ("192.0.2.1", 6440),
            )
        ],
    )
    def test_from_host_port(self, mock_getaddrinfo):
        self.assertEqual(
            SocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv4Address(self.ip4_addr),
                port=self.port,
            ),
            SocketAddress.from_host_port(host="example.tld", port=self.port),
        )

    def test_from_thrift(self):
        # Valid IPv6
        self.assertEqual(
            SocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv6Address(self.ip6_addr),
                port=self.port,
            ),
            SocketAddress.from_thrift(
                ThriftSocketAddress(
                    address_family=SocketAddressFamily.INET,
                    address=IPv6Address(self.ip6_addr).compressed,
                    port=self.port,
                )
            ),
        )

        # Valid IPv4
        self.assertEqual(
            SocketAddress(
                address_family=SocketAddressFamily.INET,
                address=IPv4Address(self.ip4_addr),
                port=self.port,
            ),
            SocketAddress.from_thrift(
                ThriftSocketAddress(
                    address_family=SocketAddressFamily.INET,
                    address=IPv4Address(self.ip4_addr).compressed,
                    port=self.port,
                )
            ),
        )

        # Valid UNIX
        self.assertEqual(
            SocketAddress(address_family=SocketAddressFamily.UNIX, path=self.unix_path),
            SocketAddress.from_thrift(
                ThriftSocketAddress(
                    address_family=SocketAddressFamily.UNIX, address=self.unix_path
                )
            ),
        )

        # Invalid Thrift struct
        with self.assertRaises(AddressValueError):
            SocketAddress.from_thrift(
                ThriftSocketAddress(
                    address_family=SocketAddressFamily.INET,
                    address="invalid ip-address",
                    port=self.port,
                )
            )

    def test_str(self):
        # Valid IPv6
        self.assertEqual(
            str(
                SocketAddress(
                    address_family=SocketAddressFamily.INET,
                    address=IPv6Address(self.ip6_addr),
                    port=self.port,
                )
            ),
            f"[{self.ip6_addr}]:{self.port}",
        )

        # Valid IPv4
        self.assertEqual(
            str(
                SocketAddress(
                    address_family=SocketAddressFamily.INET,
                    address=IPv4Address(self.ip4_addr),
                    port=self.port,
                )
            ),
            f"{self.ip4_addr}:{self.port}",
        )

        # Valid UNIX
        self.assertEqual(
            str(
                SocketAddress(
                    address_family=SocketAddressFamily.UNIX, path=self.unix_path
                )
            ),
            f"unix:{self.unix_path}",
        )
