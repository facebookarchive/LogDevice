#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import datetime
import json
import sys
from collections import Counter, OrderedDict, defaultdict
from dataclasses import asdict, dataclass, field, fields
from datetime import timedelta
from ipaddress import ip_address
from itertools import chain, zip_longest
from typing import Any, Dict, List, Optional

from humanize import naturaltime
from ldops import admin_api
from ldops.const import DEFAULT_THRIFT_PORT
from logdevice.admin.nodes.types import (
    MaintenanceStatus,
    NodesStateResponse,
    SequencingState,
    ServiceHealthStatus as DaemonHealthStatus,
    ServiceState as DaemonState,
    ShardDataHealth,
    ShardOperationalState,
)
from logdevice.common.types import SocketAddress, SocketAddressFamily
from logdevice.membership.Membership.types import MetaDataStorageState
from nubia import argument, command, context
from prettytable import PLAIN_COLUMNS, PrettyTable
from termcolor import colored, cprint
from thrift.py3 import RpcOptions


TIMEOUT = 5  # seconds


try:
    # pyre-ignore
    from ldshell.fb.command_extensions.status import (
        add_additional_table_columns,
        get_additional_info,
        merge_additional_information,
        add_additional_host_tasks,
        additional_validation,
        HostInfo,
        NodeInfo,
    )
except ImportError:

    async def async_noop(*args, **kwargs):
        pass

    (
        add_additional_table_columns,
        get_additional_info,
        merge_additional_information,
        add_additional_host_tasks,
        additional_validation,
    ) = (async_noop, async_noop, async_noop, async_noop, async_noop)

    @dataclass
    class HostInfo:
        version: str
        uptime: int

    @dataclass
    class NodeInfo:
        hostname: str
        node_id: Optional[int] = None
        package: Optional[str] = None
        ld_version: Optional[str] = None
        uptime: Optional[int] = None
        is_metadata: Optional[bool] = None
        location: Optional[str] = None
        # Internal state variables
        state: Optional[str] = None
        health_status: Optional[str] = None
        sequencing_state: Optional[str] = None
        shard_health_state: Optional[str] = None
        shard_storage_state: Optional[str] = None
        shard_operational_state: Optional[str] = None
        additional_info: Dict = field(default_factory=lambda: {})


def get_rpc_options() -> RpcOptions:
    options = RpcOptions()
    options.timeout = TIMEOUT
    return options


async def get_host_info(client_factory, *args, **kwargs) -> Optional[HostInfo]:
    options = get_rpc_options()
    async with client_factory(*args, **kwargs) as client:
        host_tasks = {
            "version": client.getVersion(rpc_options=options),
            "uptime": client.aliveSince(rpc_options=options),
        }

        # pyre-ignore
        await add_additional_host_tasks(
            client=client, rpc_options=options, host_tasks=host_tasks
        )

        results = await asyncio.gather(*host_tasks.values(), return_exceptions=True)
        # TODO: better / granular handling of exceptions
        if any(isinstance(e, Exception) for e in results):
            return None
        else:
            # This way the order of HostInfo's properties does not matter.
            return HostInfo(**dict(zip(host_tasks.keys(), results)))


async def print_results_tabular(results, *args, **kwargs):
    sort_key = kwargs["sort"] if "sort" in kwargs else "hostname"
    if results:
        results.sort(key=lambda x: getattr(x, sort_key))
    else:
        results = []

    table = PrettyTable()
    table.set_style(PLAIN_COLUMNS)
    table.right_padding_width = 2

    columns = OrderedDict(
        [
            (
                "ID",
                lambda result: (
                    colored(result.node_id, "magenta")
                    if result.is_metadata
                    else result.node_id
                ),
            ),
            (
                "NAME",
                lambda result: colored(result.hostname, attrs=["bold"])
                if not result.uptime
                else result.hostname,
            ),
            ("PACKAGE", lambda result: result.package or result.ld_version or "?"),
            ("STATE", lambda result: result.state if result.state else "?"),
            (
                "UPTIME",
                lambda result: naturaltime(timedelta(seconds=int(result.uptime)))
                if result.uptime
                else "?",
            ),
            ("LOCATION", lambda result: result.location),
            (
                "SEQ.",
                lambda result: result.sequencing_state
                if result.sequencing_state
                else "?",
            ),
            (
                "DATA HEALTH",
                lambda result: result.shard_health_state
                if result.shard_health_state
                else "?",
            ),
            (
                "STORAGE STATE",
                lambda result: result.shard_storage_state
                if result.shard_storage_state
                else "?",
            ),
            (
                "SHARD OP.",
                lambda result: result.shard_operational_state
                if result.shard_operational_state
                else "?",
            ),
            (
                "HEALTH STATUS",
                lambda result: result.health_status if result.health_status else "?",
            ),
        ]
    )

    apply_additional_changes = await add_additional_table_columns(columns)

    for name, lambd in columns.items():
        table.add_column(name, [lambd(result) for result in results])

    table.align["NAME"] = "l"
    table.align["PACKAGE"] = "l"
    table.align["UPTIME"] = "r"
    table.align["SHARD OP."] = "l"

    if apply_additional_changes is not None:
        apply_additional_changes(table=table)

    print(table)


async def print_results_json(results, *args, **kwargs):
    representations = {}

    for result in results:
        representation = asdict(result)

        hostname = representation.pop("hostname")
        representations[hostname] = representation

    print(json.dumps(representations))


def color_op_state(op_state: ShardOperationalState):
    if op_state == ShardOperationalState.DRAINED:
        return colored(op_state.name, "red")
    elif op_state == ShardOperationalState.MAY_DISAPPEAR:
        return colored(op_state.name, "yellow")
    return op_state.name


def color_maintenance_state(maintenance_status: MaintenanceStatus):
    if maintenance_status in [
        MaintenanceStatus.NOT_STARTED,
        MaintenanceStatus.BLOCKED_UNTIL_SAFE,
        MaintenanceStatus.REBUILDING_IS_BLOCKED,
        MaintenanceStatus.AWAITING_SAFETY_CHECK,
        MaintenanceStatus.BLOCKED_BY_ADMIN_OVERRIDE,
        MaintenanceStatus.AWAITING_NODE_TO_BE_ALIVE,
        MaintenanceStatus.RETRY,
    ]:
        return colored(maintenance_status.name, "red")
    return colored(maintenance_status.name, "green")


def color_data_health(data_health: ShardDataHealth):
    if data_health == ShardDataHealth.UNAVAILABLE:
        return colored(data_health.name, "red")
    elif data_health == ShardDataHealth.LOST_REGIONS:
        return colored(data_health.name, "yellow")
    elif data_health == ShardDataHealth.LOST_ALL:
        return colored(data_health.name, "magenta")
    elif data_health == ShardDataHealth.EMPTY:
        return colored(data_health.name, "white")
    return data_health.name


def color_seq_state(seq_state: SequencingState):
    if seq_state == SequencingState.BOYCOTTED:
        return colored(seq_state.name, "yellow")
    elif seq_state == SequencingState.DISABLED:
        return colored(seq_state.name, "red")
    return seq_state.name


def color_service_state(service_state: DaemonState):
    if service_state == DaemonState.UNKNOWN:
        return colored(service_state.name, "red")
    elif service_state == DaemonState.STARTING_UP:
        return colored(service_state.name, "yellow")
    elif service_state == DaemonState.SHUTTING_DOWN:
        return colored(service_state.name, "yellow")
    elif service_state == DaemonState.DEAD:
        return colored(service_state.name, "red")
    return service_state.name


def color_service_health_status(service_health_status: DaemonHealthStatus):
    if service_health_status == DaemonHealthStatus.UNKNOWN:
        return colored(service_health_status.name, "red")
    elif service_health_status == DaemonHealthStatus.UNDEFINED:
        return colored(service_health_status.name, "white")
    elif service_health_status == DaemonHealthStatus.OVERLOADED:
        return colored(service_health_status.name, "yellow")
    elif service_health_status == DaemonHealthStatus.UNHEALTHY:
        return colored(service_health_status.name, "red")
    return service_health_status.name


def interpret_by_frequency(items):
    return ",".join(
        "{}({})".format(name, count) for name, count in Counter(items).most_common()
    )


def interpret_shard_health_states(shard_states):
    if not shard_states:
        return " "
    return interpret_by_frequency(
        [color_data_health(shard.data_health) for shard in shard_states]
    )


def interpret_shard_storage_states(shard_states):
    if not shard_states:
        return " "
    return interpret_by_frequency([shard.storage_state.name for shard in shard_states])


def interpret_shard_operational_states(shard_states):
    if not shard_states:
        return " "
    current = interpret_by_frequency(
        [color_op_state(shard.current_operational_state) for shard in shard_states]
    )
    targets = []
    progress = ""
    for s in shard_states:
        if s.maintenance is None:
            targets.append(s.current_operational_state)
        else:
            if ShardOperationalState.DRAINED in s.maintenance.target_states:
                targets.append(ShardOperationalState.DRAINED)
            else:
                targets.extend(s.maintenance.target_states)
            progress = color_maintenance_state(s.maintenance.status)
    target = interpret_by_frequency([color_op_state(target_i) for target_i in targets])
    if current != target:
        return f"{current} -> {target}  {progress}"
    else:
        return current


def filter_merged_information(status_data, nodes, hostnames):
    if not nodes and not hostnames:
        return status_data
    hostnames = hostnames or []
    nodes = nodes or []

    return list(
        filter(lambda x: x.node_id in nodes or x.hostname in hostnames, status_data)
    )


async def merge_information(
    nodes_state: NodesStateResponse,
    hosts_info: List[HostInfo],
    # pyre-fixme[9]: additional_info has type `List[Dict[str, typing.Any]]`; used as
    #  `None`.
    additional_info: List[Dict[str, Any]] = None,
):
    if additional_info is None:
        additional_info = []

    data = []
    for node_state, host_info, add_info in zip_longest(
        nodes_state.states, hosts_info, additional_info
    ):
        node_config = node_state.config
        info = NodeInfo(
            hostname=node_config.name,
            node_id=node_state.node_index,
            location=node_config.location,
            is_metadata=False,
            sequencing_state=(
                color_seq_state(node_state.sequencer_state.state)
                if node_state.sequencer_state
                else " "
            ),
            state=color_service_state(node_state.daemon_state),
            shard_health_state=interpret_shard_health_states(node_state.shard_states),
            shard_storage_state=interpret_shard_storage_states(node_state.shard_states),
            shard_operational_state=interpret_shard_operational_states(
                node_state.shard_states
            ),
            health_status=color_service_health_status(node_state.daemon_health_status),
        )

        for shard in node_state.shard_states or []:
            # pyre-fixme[16]: `Optional` has no attribute `__ior__`.
            info.is_metadata |= shard.metadata_state == MetaDataStorageState.METADATA

        if host_info:
            info.ld_version = host_info.version.split(" ")[0]
            info.uptime = host_info.uptime

        # pyre-fixme[16]: Module `ldshell` has no attribute `fb`.
        await merge_additional_information(
            info=info,
            node_state=node_state,
            host_info=host_info,
            additional_info=add_info,
        )

        data.append(info)

    return data


async def get_nodes_state(admin_client):
    try:
        res = await admin_api.get_nodes_state(admin_client)
    except Exception as ex:
        cprint("Failed to request NodesState(): {}".format(str(ex)), file=sys.stderr)
        return None
    return res


async def run_status(nodes, hostnames, extended, formatter, **kwargs):
    ctx = context.get_context()
    async with ctx.get_cluster_admin_client() as client:
        nodes_state = await get_nodes_state(client)
        if nodes_state is None:
            return

        host_tasks = []
        for node_state in nodes_state.states:
            config = node_state.config
            use_data_address = (
                config.other_addresses is None or config.other_addresses.admin is None
            )
            address = (
                SocketAddress(
                    address_family=SocketAddressFamily.INET,
                    address=config.data_address.address,
                    port=DEFAULT_THRIFT_PORT,
                )
                if use_data_address
                else config.other_addresses.admin
            )
            host_tasks.append(get_host_info(ctx.get_node_admin_client, address=address))

        hosts_info = await asyncio.gather(*host_tasks)
        additional_info_mapping = defaultdict(dict)
        additional_info = await get_additional_info(hosts_info)

        if additional_info:
            # Add the info to the defaultdict mapping.
            for address, values in additional_info.items():
                if not values:
                    continue
                additional_info_mapping[address].update(values)

        # Convert the additional info dict mapping to a list that is aligned
        # with both the node_state.states and hosts_info so they can be
        # easily zipped.
        additional_info = []
        for node_state in nodes_state.states:
            config = node_state.config
            use_data_address = (
                config.other_addresses is None or config.other_addresses.admin is None
            )
            socket_address = (
                config.data_address
                if use_data_address
                else config.other_addresses.admin
            )
            mapping_key = (
                ip_address(socket_address.address)
                if socket_address.address_family == SocketAddressFamily.INET
                else socket_address.address
            )
            additional_info.append(additional_info_mapping[mapping_key])

        merged_info = await merge_information(
            nodes_state=nodes_state,
            hosts_info=hosts_info,
            additional_info=additional_info,
        )
    merged_info = filter_merged_information(merged_info, nodes, hostnames)
    await formatter(merged_info, **kwargs)


FORMATTERS = {"json": print_results_json, "tabular": print_results_tabular}
FIELDS = [field.name for field in fields(NodeInfo)]


@command
@argument(
    "format",
    type=str,
    aliases=["f"],
    description="Possible output formats",
    choices=list(FORMATTERS),
)
@argument("extended", type=bool, description="Include internal node state as well")
@argument("nodes", type=List[int], aliases=["n"], description="list of node ids")
@argument("hostnames", type=List[str], aliases=["o"], description="list of hostnames")
@argument("hosts", type=List[str], description="list of hostnames")
@argument(
    "sort",
    type=str,
    description="What field to sort by the tabular output",
    choices=list(FIELDS),
)
async def status(
    format="tabular",
    sort="node_id",
    nodes=None,
    hosts=None,
    hostnames=None,
    extended=False,
):
    """
    Next gen status command using the Thrift interfaces
    """

    hostnames = set(chain.from_iterable((hostnames or [], hosts or [])))

    await additional_validation()

    start = datetime.datetime.now()

    await run_status(
        nodes=nodes,
        hostnames=hostnames,
        extended=extended,
        formatter=FORMATTERS[format],
        sort=sort,
    )
    stop = datetime.datetime.now()
    duration = int((stop - start).total_seconds() * 1000)
    print(f"Took {duration}ms", file=sys.stderr)
