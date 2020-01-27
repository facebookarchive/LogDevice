# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import absolute_import, division, print_function, unicode_literals

from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import prettytable as pt
from ldops.cluster import get_cluster_view
from ldops.types.cluster_view import ClusterView
from ldops.util import convert
from ldops.util.helpers import parse_shards
from logdevice.admin.exceptions.types import OperationError
from logdevice.admin.nodes.types import ShardDataHealth, ShardStorageState
from logdevice.admin.safety.types import (
    CheckImpactRequest,
    CheckImpactResponse,
    ImpactOnEpoch,
    OperationImpact,
)
from logdevice.client import get_internal_log_name, is_internal_log
from logdevice.common.types import (
    Location,
    LocationScope,
    NodeID,
    ReplicationProperty,
    ShardID,
)
from nubia import context
from nubia.internal.typing import argument, command
from termcolor import colored, cprint
from thrift.py3 import exceptions as texceptions


MAX_COLS_PER_TABLE = 4


@command
@argument(
    "shards",
    type=Optional[List[str]],
    description="List of strings in the format NX[:SY] where X is the "
    "node id and Y is the shard id",
)
@argument("node_indexes", type=Optional[List[int]], description="List of node indexes")
@argument(
    "node_names",
    type=Optional[List[str]],
    description="List of node names either hosts or tw tasks",
)
@argument(
    "target_state",
    type=str,
    choices=["read-only", "disabled"],
    description="The storage state that we want to set the storage to. If you "
    "would like to disable writes, then the target-state is read-only. If you "
    "would like to disable reads, then the target-state should be disabled",
)
@argument(
    "safety_margin",
    type=Mapping[str, int],
    description="Extra domains which should be available. Dictionary"
    " format <scope>:<replication> "
    '(e.g., "rack:1;node:2").',
)
@argument(
    "skip_metadata_logs",
    type=bool,
    description="Whether to check the metadata logs or not",
)
@argument(
    "skip_internal_logs",
    type=bool,
    description="whether to check the internal logs or not",
)
@argument(
    "logs",
    type=List[int],
    description="If None, checks all logs, but you can specify the log-ids",
)
@argument(
    "short",
    type=bool,
    description="Disables the long detailed description of the output",
)
@argument("timeout", type=int, description="Timeout in seconds")
@argument(
    "max_unavailable_sequencing_capacity_pct",
    type=int,
    description="The maximum percentage of sequencing capacity that can be "
    "unavailable",
)
@argument(
    "max_unavailable_storage_capacity_pct",
    type=int,
    description="The maximum percentage of storage capacity that can be unavailable",
)
async def check_impact(
    shards: Optional[List[str]] = None,
    node_indexes: Optional[List[int]] = None,
    node_names: Optional[List[str]] = None,
    target_state: str = "disabled",
    # pyre-fixme[9]: safety_margin has type `Mapping[str, int]`; used as `None`.
    safety_margin: Mapping[str, int] = None,
    timeout: int = 600,
    skip_metadata_logs: bool = False,
    skip_internal_logs: bool = False,
    # pyre-fixme[9]: logs has type `List[int]`; used as `None`.
    logs: List[int] = None,
    short: bool = False,
    max_unavailable_storage_capacity_pct=25,
    max_unavailable_sequencing_capacity_pct=25,
):
    """
    Return true if performaing operations to the given shards will cause
    loss of read/write availiability or data loss.
    """

    if shards is None:
        shards = []

    ctx = context.get_context()

    def _combine(
        cv: ClusterView,
        shards: Optional[List[str]] = None,
        node_names: Optional[List[str]] = None,
        node_indexes: Optional[List[int]] = None,
    ) -> Tuple[ShardID, ...]:

        shards = list(shards or [])
        node_names = list(node_names or [])
        node_indexes = list(node_indexes or [])

        shard_ids = parse_shards(shards)
        for nn in node_names:
            shard_ids.add(ShardID(node=cv.get_node_id(node_name=nn), shard_index=-1))
        for ni in node_indexes:
            shard_ids.add(ShardID(node=NodeID(node_index=ni), shard_index=-1))
        shard_ids_expanded = cv.expand_shards(shard_ids)

        return shard_ids_expanded

    if not ctx.is_connected():
        cprint("LDShell must be connected to a cluster!", "red")
        return 1

    cprint("Starting, this may take a while...", "yellow")

    # pyre-fixme[9]: target_state has type `str`; used as `ShardStorageState`.
    target_state = convert.to_storage_state(target_state)

    async with ctx.get_cluster_admin_client() as client:
        try:
            cv = await get_cluster_view(client)
            # pyre-fixme[9]: shards has type `Optional[List[str]]`; used as
            #  `Tuple[ShardID, ...]`.
            shards = _combine(cv, shards, node_names, node_indexes)
            req = CheckImpactRequest(
                # pyre-fixme[6]: Expected `Optional[Sequence[ShardID]]` for 1st
                #  param but got `List[str]`.
                shards=shards,
                target_storage_state=target_state,
                log_ids_to_check=logs,
                abort_on_negative_impact=True,
                safety_margin=convert.to_replication(safety_margin),
                return_sample_size=20,
                check_metadata_logs=not skip_metadata_logs,
                check_internal_logs=not skip_internal_logs,
                max_unavailable_storage_capacity_pct=max_unavailable_storage_capacity_pct,
                max_unavailable_sequencing_capacity_pct=max_unavailable_sequencing_capacity_pct,
            )
            response = await client.checkImpact(req)
        except OperationError as e:
            cprint(
                f"There was error during check execution, Status {e}, "
                "result is not known",
                "red",
            )
            return 1
        except texceptions.TransportError as e:
            cprint(f"Couldn't connect to the Admin Server: {e}", "red")
            return 1

    delta = response.total_duration
    lines = []
    if not response.impact:
        lines.append(colored("ALL GOOD.\n", "green"))
        lines.append(f"Total logs checked ({response.total_logs_checked}) in {delta}s")
    else:
        lines.append(
            colored(f"UNSAFE. Impact: {impacts_to_string(response.impact)}", "red")
        )
        lines.append(f"Total logs checked ({response.total_logs_checked}) in {delta}s")
    print("\n".join(lines))
    if not short:
        print(check_impact_string(response, shards, target_state))
    if not response.impact:
        return 0
    return 1


@command
async def check_internal_logs():
    """
    Performs a check on the metadata logs and internal logs to see if we lost
    read, write, or rebuilding availability on these critical logs.
    """
    return await check_impact(
        shards=[],
        target_state="disabled",
        skip_metadata_logs=False,
        skip_internal_logs=False,
        logs=[],
    )


def impact_to_string(impact: OperationImpact):
    return impact.name


def impacts_to_string(impacts: Sequence[OperationImpact]):
    return ", ".join(impact_to_string(x) for x in impacts)


def check_impact_string(
    response: CheckImpactResponse,
    shards: List[ShardID],
    target_state: ShardStorageState,
) -> str:
    lines = []
    if response.internal_logs_affected:
        lines.append(colored("CRITICAL: Internal Logs are affected negatively!", "red"))
    if response.logs_affected is not None:
        # pyre-ignore
        for impact_on_epoch in response.logs_affected:
            lines.append(impact_on_log_string(impact_on_epoch, shards, target_state))
    return "\n".join(lines)


def shard_to_str(shard: ShardID):
    return f"N{shard.node.node_index}:S{shard.shard_index}"


def is_same_scope(scope1: ShardID, scope2: ShardID):
    if scope1.node.node_index == scope2.node.node_index:
        if scope1.shard_index == -1 or scope2.shard_index == -1:
            # compare node-ids only
            return True
        # Are we comparing shards, or only nodes
        return scope1.shard_index == scope2.shard_index
    return False


def match_shards(scope: ShardID, input_shards: List[ShardID]):
    return any(is_same_scope(scope, x) for x in input_shards)


def replication_factor(replication):
    return max(replication.values())


def make_table():
    table = pt.PrettyTable(align="c")
    return table


def reverse_sort_replication(
    replication: Mapping[LocationScope, Any]
) -> List[Tuple[LocationScope, Any]]:
    """
    Sort scopes from smaller to bigger (NODE, RACK, ROW, etc.)
    """
    output = []
    for loc_scope in sorted(replication.keys(), key=lambda x: x.value):
        output.append((loc_scope, replication[loc_scope]))
    return output


def get_biggest_scope(replication: ReplicationProperty) -> LocationScope:
    """
    Returns the biggest scope at given replication property
    """
    biggest_scope = LocationScope.NODE
    for scope in replication.keys():
        if scope.value >= biggest_scope.value:
            biggest_scope = scope
    return biggest_scope


def location_up_to_scope(
    shard: ShardID, location_per_scope: Location, scope: LocationScope
) -> str:
    """
    Generates a string of the location string up to a given scope. The input
    scope is inclusive.
    """
    if not location_per_scope:
        return "UNKNOWN"
    locs = []
    # Sort scopes from bigger to smaller (ROOT > REGION > CLUSTER > ...)
    for loc_scope in sorted(
        location_per_scope.keys(), key=lambda x: x.value, reverse=True
    ):
        if loc_scope.value >= scope.value:
            locs.append(location_per_scope[loc_scope])
        else:
            break
    if scope == LocationScope.NODE:
        locs.append(str(shard.node.node_index))
    return ".".join(locs)


def analyze_read_availability(
    read_unavailable: Dict[LocationScope, Dict[str, ShardID]],
    replication: ReplicationProperty,
) -> str:
    formatted_read_unavailable = []
    sorted_read_unavailable = reverse_sort_replication(read_unavailable)
    # biggest_failing_domain is a tuple (LocationScope, set(locations))
    biggest_failing_domain = (
        sorted_read_unavailable[-1] if sorted_read_unavailable else None
    )
    for scope, v in sorted_read_unavailable:
        max_loss_at_scope = replication[scope] - 1
        actual = " "
        if biggest_failing_domain is not None and scope == biggest_failing_domain[0]:
            actual = f" (actual {len(v)}) "
            if len(v) > max_loss_at_scope:
                actual = colored(actual, "red")
            else:
                actual = colored(actual, "green")
        formatted_read_unavailable.append(
            f"{max_loss_at_scope}{actual}{scope.name.lower()}s"
        )
    expectation = " across more than ".join(formatted_read_unavailable)

    return (
        f"Read availability: We can't afford losing more than {expectation}. "
        "Nodes must be healthy, readable, and alive."
    )


def normalize_replication(replication: ReplicationProperty) -> ReplicationProperty:
    """
    If we got replication {cluster: 3}, it actually means {cluster: 3, node: 3}
    """
    repl = dict(replication)
    replication_factor = max(v for v in repl.values())
    repl[LocationScope.NODE] = replication_factor
    return ReplicationProperty(repl)


def analyze_write_availability(
    n_writeable_before: int, n_writeable_loss: int, replication: ReplicationProperty
) -> str:
    n_writeable_after = n_writeable_before - n_writeable_loss
    n_writeable_after_visual = ""
    sorted_replication = reverse_sort_replication(replication)
    formatted_replications = []
    for scope, v in sorted_replication:
        formatted_replications.append(f"{v} {scope.name.lower()}s")
    formatted_replication_str = " in ".join(formatted_replications)
    if n_writeable_after != n_writeable_before:
        n_writeable_after_visual = f" \u2192 {n_writeable_after} "
    return (
        f"Write/Rebuilding availability: "
        f"{n_writeable_before:}{n_writeable_after_visual} "
        f"(we need at least {formatted_replication_str} "
        f"that are healthy, writable, and alive.)"
    )


def impact_on_log_string(
    impact: ImpactOnEpoch, shards: List[ShardID], target_state: ShardStorageState
) -> str:
    lines = []
    log_id = (
        str(impact.log_id) if impact.log_id != 0 else colored("METADATA-LOGS", "cyan")
    )
    epoch = str(impact.epoch)
    impact_str = impacts_to_string(impact.impact)
    # location -> [shard+status]
    location_map: Mapping[str, List[str]] = defaultdict(lambda: [])

    # Maps LocationScope to the unique locations of read available domains.
    # {Domain -> {loc -> set(shard_id)}}
    read_unavailable = defaultdict(lambda: defaultdict(lambda: set()))
    n_writeable = 0
    n_writeable_loss = 0

    # pyre-ignore
    replication = normalize_replication(impact.replication)
    biggest_replication_scope = get_biggest_scope(replication)
    for i, shard in enumerate(impact.storage_set):
        assert impact.storage_set_metadata is not None
        # pyre-ignore
        meta = impact.storage_set_metadata[i]
        loc_per_scope = meta.location_per_scope
        location = location_up_to_scope(shard, loc_per_scope, biggest_replication_scope)
        is_in_target_shards = match_shards(shard, shards)

        # A writable shard is a fully authoritative
        # ALIVE + HEALTHY + READ_WRITE
        if (
            meta.storage_state == ShardStorageState.READ_WRITE
            and meta.is_alive
            and meta.data_health == ShardDataHealth.HEALTHY
        ):
            n_writeable += 1
            if is_in_target_shards and target_state != ShardStorageState.READ_WRITE:
                n_writeable_loss += 1

        # A readable shard is a fully authoritative
        # ALIVE + HEALTHY + READ_ONLY/WRITE
        if meta.storage_state in [
            ShardStorageState.READ_WRITE,
            ShardStorageState.READ_ONLY,
        ]:
            # The part of the storage set that should be read available is all
            # READ_{ONLY, WRITE} shards that are non-EMPTY
            if meta.data_health != ShardDataHealth.EMPTY:
                # This is a shard is/may/will be read unavailable.
                if (
                    not meta.is_alive
                    or meta.data_health != ShardDataHealth.HEALTHY
                    or (
                        is_in_target_shards
                        and target_state == ShardStorageState.DISABLED
                    )
                ):
                    # For each domain in the replication property, add the location
                    # string as a read unavailable target
                    for scope in replication.keys():
                        loc_tag = location_up_to_scope(shard, loc_per_scope, scope)
                        # If shard location is x.y.a.b and replication is rack:
                        # X, node: Y. Then the x.y.a should be added to key
                        # NodeLocation.RACK and x.y.a.b to NodeLocation.NODE
                        read_unavailable[scope][loc_tag].add(shard)

        color = "green" if meta.is_alive else "red"
        attrs = ["bold"] if is_in_target_shards else []
        if meta.data_health != ShardDataHealth.HEALTHY:
            color = "red"
        # except if it's empty
        if meta.data_health == ShardDataHealth.EMPTY:
            color = "white"

        visual = colored(f"{shard_to_str(shard):<8} ", color, attrs=attrs)
        visual += colored(f"{meta.storage_state.name} ", color)
        if is_in_target_shards:
            visual += colored(f"\u2192 {target_state.name} ", "cyan")
        if not meta.is_alive:
            visual += colored("(DEAD) ", color)
        if meta.data_health != ShardDataHealth.HEALTHY:
            visual += colored(f"({meta.data_health.name})", color)

        location_map[location].append(visual)

    tables = [make_table()]
    # Because add_column can't handle if columns are of different lengths. Let's
    # fill the visuals to the maximum length
    max_nodes_per_location = max(len(x) for x in location_map.values())
    cols_added = 0
    for loc, visuals in location_map.items():
        if cols_added >= MAX_COLS_PER_TABLE:
            tables.append(make_table())
            cols_added = 0
        visuals.extend(["-"] * (max_nodes_per_location - len(visuals)))
        tables[-1].add_column(loc, visuals, align="l")
        cols_added += 1

    internal_visual = ""
    if is_internal_log(impact.log_id):
        internal_log_name = colored(get_internal_log_name(impact.log_id), "cyan")
        internal_visual = f"({internal_log_name})"

    lines.append("")
    replication_str = ", ".join(
        [f"{k.name.lower()}: {v}" for k, v in impact.replication.items()]
    )
    lines.extend(
        [
            f"Log: {log_id} {internal_visual}  ",
            f"Epoch: {epoch:<12}  ",
            f"Storage-set Size: {len(impact.storage_set):<5}",
        ]
    )
    lines.append(f"Replication: {{{replication_str}}}  ")
    # Write/Rebuilding Availability
    lines.append(analyze_write_availability(n_writeable, n_writeable_loss, replication))
    # Read/F-Majority Availability
    # pyre-ignore
    lines.append(analyze_read_availability(read_unavailable, replication))
    lines.append(colored(f"Impact: {impact_str}", "red"))
    for table in tables:
        lines.append(table.get_string())
        lines.append("")
    return "\n".join(lines)
