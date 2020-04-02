#!/usr/bin/env python3

from enum import IntEnum, auto

class AdminCommandClientException(Exception):
    pass

class ConnectionType(IntEnum):
    PLAIN = auto()
    SSL = auto()

class AdminCommandClient:
    def send(
        self, cmd: str, host: str, port: int, timeout: float, conntype: ConnectionType
    ) -> str: ...
