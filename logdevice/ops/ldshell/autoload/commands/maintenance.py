#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from collections import Counter
from datetime import timedelta
from enum import Enum
from getpass import getuser
from textwrap import indent, shorten
from typing import Collection, Generator, List, Optional, Sequence, Tuple, Union

from humanize import naturaltime
from ldops.cluster import get_cluster_view
from ldops.exceptions import NodeNotFoundError
from ldops.maintenance import (
    apply_maintenance,
    mark_all_shards_unrecoverable,
    remove_maintenances,
)
from ldops.types.cluster_view import ClusterView
from ldops.types.maintenance_view import MaintenanceOverallStatus, MaintenanceView
from ldops.types.shard_maintenance_progress import ShardMaintenanceProgress
from ldops.util.helpers import parse_shards
from ldshell.autoload.commands import safety
from ldshell.helpers import confirm_prompt
from logdevice.admin.maintenance.types import (
    MaintenanceDefinition,
    MarkAllShardsUnrecoverableResponse,
)
from logdevice.admin.nodes.types import MaintenanceStatus, ShardOperationalState
from logdevice.common.types import ShardID
from nubia import argument, command, context
from tabulate import tabulate
from termcolor import colored, cprint


class RenderingMode(Enum):
    COMPACT = "compact"
    EXPANDED = "expanded"
    EXPANDED_WITH_SHARDS = "expanded_with_shards"
    EXPANDED_WITH_SAFETY_CHECKS = "expanded_with_safety_checks"

    def __repr__(self):
        return "<%s.%s>" % (self.__class__.__name__, self.name)


def _render(
    maintenance_views: Sequence[MaintenanceView],
    cluster_view: ClusterView,
    mode: RenderingMode = RenderingMode.COMPACT,
) -> str:
    # pyre-ignore
    return {
        RenderingMode.COMPACT: _render_compact,
        RenderingMode.EXPANDED: _render_expanded,
        RenderingMode.EXPANDED_WITH_SHARDS: _render_expanded_with_shards,
        RenderingMode.EXPANDED_WITH_SAFETY_CHECKS: _render_expanded_with_safety_checks,
    }[mode](maintenance_views, cluster_view)


def _render_compact(
    maintenance_views: Sequence[MaintenanceView], cluster_view: ClusterView
) -> str:
    def mv_to_row(mv: MaintenanceView, cv: ClusterView) -> Tuple[str, ...]:
        id = mv.group_id
        affected = shorten(
            ",".join(f"N{ni}" for ni in mv.affected_node_indexes), 30, placeholder="..."
        )

        status = colored(mv.overall_status.name, _color(mv.overall_status))

        if mv.affects_shards:
            if mv.is_blocked:
                color = "red"
            elif not mv.are_all_shards_done:
                color = "yellow"
            else:
                color = "green"

            shard_progress = colored(
                # pyre-ignore
                f"{mv.shard_target_state.name}"
                f"({mv.num_shards_done}/{mv.num_shards_total})",
                color=color,
            )
        else:
            shard_progress = "-"

        if mv.affects_sequencers:
            if mv.are_all_sequencers_done:
                color = "green"
            else:
                color = "yellow"

            sequencer_progress = colored(
                # pyre-ignore
                f"{mv.sequencer_target_state.name}"
                f"({mv.num_sequencers_done}/{mv.num_sequencers_total})",
                color=color,
            )
        else:
            sequencer_progress = "-"

        if mv.reason:
            created_by = shorten(f"{mv.user} ({mv.reason})", 40, placeholder="...")
        else:
            created_by = f"{mv.user}"

        if mv.created_on:
            created_on = str(mv.created_on)
        else:
            created_on = "-"

        if mv.expires_on:
            expires_on = naturaltime(mv.expires_on)
        else:
            expires_on = "-"

        return (
            id,
            affected,
            status,
            shard_progress,
            sequencer_progress,
            created_by,
            created_on,
            expires_on,
        )

    return tabulate(
        headers=[
            "MNT. ID",
            "AFFECTED",
            "STATUS",
            "SHARDS",
            "SEQUENCERS",
            "CREATED BY",
            "CREATED AT",
            "EXPIRES IN",
        ],
        tabular_data=(mv_to_row(mv, cluster_view) for mv in maintenance_views),
        tablefmt="plain",
    )


def _render_expanded(
    maintenance_views: Sequence[MaintenanceView],
    cluster_view: ClusterView,
    expand_shards: bool = False,
    show_safety_check_results: bool = False,
) -> str:
    def one(mv: MaintenanceView, cv: ClusterView, expand_shards: bool) -> str:
        def overview(mv: MaintenanceView, cv: ClusterView) -> str:
            tbl = []
            tbl.append(["Maintenance ID", mv.group_id])

            tbl.append(
                [
                    "Affected ",
                    f"{mv.num_shards_total} shards on "
                    f"{len(mv.affected_storage_node_ids)} nodes, "
                    f"{mv.num_sequencers_total} sequencers",
                ]
            )

            tbl.append(
                [
                    "Overall Status",
                    colored(mv.overall_status.name, _color(mv.overall_status)),
                ]
            )

            if (
                mv.overall_status == MaintenanceOverallStatus.BLOCKED
                and mv.last_check_impact_result
            ):
                tbl.append(
                    [
                        "Impact Result",
                        colored(
                            safety.impacts_to_string(
                                mv.last_check_impact_result.impact
                            ),
                            "red",
                        ),
                    ]
                )

            if mv.reason:
                created_by = f"{mv.user} ({mv.reason})"
            else:
                created_by = mv.user
            tbl.append(["Created By", created_by])

            if mv.extras:
                extras = "\n".join(f"{k}={v}" for k, v in mv.extras.items())
            else:
                extras = "-"
            tbl.append(["Extras", extras])

            if mv.created_on:
                created_on = f"{str(mv.created_on)} ({naturaltime(mv.created_on)})"
            else:
                created_on = "-"
            tbl.append(["Created On", created_on])

            if mv.expires_on:
                expires_on = f"{str(mv.expires_on)} ({naturaltime(mv.expires_on)})"
            else:
                expires_on = "-"
            tbl.append(["Expires On", expires_on])

            if mv.skip_safety_checks:
                skip_safety_checks = colored(str(mv.skip_safety_checks), "red")
            else:
                skip_safety_checks = str(mv.skip_safety_checks)
            tbl.append(["Skip Safety Checks", skip_safety_checks])

            tbl.append(["Allow Passive Drains", str(mv.allow_passive_drains)])
            tbl.append(
                ["RESTORE rebuilding enforced", str(mv.force_restore_rebuilding)]
            )

            tbl = [[f"{row[0]}:", row[1]] for row in tbl]
            return tabulate(tabular_data=tbl, tablefmt="plain")

        def shards(
            mv: MaintenanceView, cv: ClusterView, expand_shards: bool
        ) -> Optional[str]:
            def aggregated(mv: MaintenanceView, cv: ClusterView) -> str:
                headers = [
                    "NODE INDEX",
                    "NODE NAME",
                    "LOCATION",
                    "TARGET STATE",
                    "CURRENT STATE",
                    "MAINTENANCE STATUS",
                    "LAST UPDATED",
                ]
                tbl = []
                for ni in mv.affected_storage_node_indexes:
                    nv = cv.get_node_view(node_index=ni)
                    node_index = ni
                    node_name = nv.node_name
                    location = nv.location
                    target_state = (
                        # pyre-ignore
                        f"{mv.shard_target_state.name}"
                        f"({len(mv.get_shards_by_node_index(ni))})"
                    )

                    # current_state
                    chunks = []
                    for cur_op_state, num in sorted(
                        Counter(
                            (
                                mv.get_shard_state(shard).current_operational_state
                                for shard in mv.get_shards_by_node_index(ni)
                            )
                        ).items(),
                        key=lambda x: x[0].name,
                    ):
                        chunks.append(
                            colored(
                                f"{cur_op_state.name}({num})",
                                _color_shard_op_state(
                                    cur_op_state,
                                    # pyre-ignore
                                    mv.shard_target_state,
                                ),
                            )
                        )

                    current_state = ",".join(chunks)

                    # maintenance status
                    mnt_statuses = [
                        mv.get_shard_maintenance_status(shard)
                        for shard in mv.get_shards_by_node_index(ni)
                    ]
                    chunks = []
                    for mnt_status, num in sorted(
                        Counter(mnt_statuses).items(), key=lambda x: x[0].name
                    ):
                        chunks.append(
                            colored(f"{mnt_status.name}({num})", _color(mnt_status))
                        )

                    maintenance_status = ",".join(chunks)

                    last_updated_at_time = min(
                        (
                            ShardMaintenanceProgress.from_thrift(
                                # pyre-ignore
                                ss.maintenance
                            ).last_updated_at
                            for ss in nv.shard_states
                            if ss.maintenance
                        )
                    )
                    last_updated_at = (
                        f"{last_updated_at_time} ({naturaltime(last_updated_at_time)})"
                    )

                    tbl.append(
                        [
                            node_index,
                            node_name,
                            location,
                            target_state,
                            current_state,
                            maintenance_status,
                            last_updated_at,
                        ]
                    )
                return tabulate(tabular_data=tbl, headers=headers, tablefmt="plain")

            def expanded(mv: MaintenanceView, cv: ClusterView) -> str:
                def one_node(mv: MaintenanceView, cv: ClusterView, ni: int) -> str:
                    def shards_table(
                        mv: MaintenanceView, cv: ClusterView, ni: int
                    ) -> str:
                        headers = [
                            "SHARD INDEX",
                            "CURRENT STATE",
                            "TARGET STATE",
                            "MAINTENANCE STATUS",
                            "LAST UPDATED",
                        ]
                        tbl = []
                        for shard in mv.get_shards_by_node_index(ni):
                            target_state = mv.shard_target_state
                            shard_state = mv.get_shard_state(shard)
                            cur_op_state = shard_state.current_operational_state
                            current_state = colored(
                                cur_op_state.name,
                                _color_shard_op_state(
                                    cur_op_state,
                                    # pyre-ignore
                                    mv.shard_target_state,
                                ),
                            )

                            mnt_status = mv.get_shard_maintenance_status(shard)
                            maintenance_status = colored(
                                mnt_status.name, _color(mnt_status)
                            )
                            last_updated_at = mv.get_shard_last_updated_at(shard)
                            if last_updated_at:
                                last_updated = (
                                    f"{last_updated_at} "
                                    f"({naturaltime(last_updated_at)})"
                                )
                            else:
                                last_updated = "-"

                            tbl.append(
                                [
                                    shard.shard_index,
                                    current_state,
                                    # pyre-ignore
                                    target_state.name,
                                    maintenance_status,
                                    last_updated,
                                ]
                            )
                        return tabulate(tbl, headers=headers, tablefmt="plain")

                    nv = cv.get_node_view(node_index=ni)
                    return "N{node_index} ({node_name}):\n{shards_table}".format(
                        node_index=nv.node_index,
                        node_name=nv.node_name,
                        shards_table=indent(shards_table(mv, cv, ni), "  "),
                    )

                return "\n\n".join(
                    [one_node(mv, cv, ni) for ni in mv.affected_node_indexes]
                )

            if not mv.affects_shards:
                return None

            if expand_shards:
                f = expanded
            else:
                f = aggregated

            return "Shard Maintenances:\n" + indent(f(mv, cv), "  ")

        def sequencers(mv: MaintenanceView, cv: ClusterView) -> Optional[str]:
            if not mv.affects_sequencers:
                return None

            headers = [
                "NODE INDEX",
                "NODE NAME",
                "LOCATION",
                "TARGET STATE",
                "CURRENT STATE",
                "MAINTENANCE STATUS",
                "LAST UPDATED",
            ]
            tbl = []
            for n in mv.sequencer_nodes:
                nv = cv.get_node_view(node_index=n.node_index)
                node_index = nv.node_index
                node_name = nv.node_name
                location = nv.location
                # pyre-ignore
                target_state = mv.sequencer_target_state.name
                mnt_status = mv.get_sequencer_maintenance_status(n)
                current_state = colored(
                    # pyre-ignore
                    nv.sequencer_state.state.name,
                    _color(mnt_status),
                )
                maintenance_status = colored(mnt_status.name, _color(mnt_status))

                last_updated_at = mv.get_sequencer_last_updated_at(n)
                if last_updated_at:
                    last_updated = f"{last_updated_at} ({naturaltime(last_updated_at)})"
                else:
                    last_updated = "-"

                tbl.append(
                    [
                        node_index,
                        node_name,
                        location,
                        target_state,
                        current_state,
                        maintenance_status,
                        last_updated,
                    ]
                )
            return "Sequencer Maintenances:\n{}".format(
                indent(tabulate(tbl, headers=headers, tablefmt="plain"), prefix="  ")
            )

        def impact(mv: MaintenanceView, show_safety_check_results: bool) -> str:
            if (
                mv.overall_status == MaintenanceOverallStatus.BLOCKED
                and show_safety_check_results
            ):
                response = mv.last_check_impact_result
                shards = []
                for ni in mv.affected_node_indexes:
                    shards.extend(mv.get_shards_by_node_index(ni))
                impact_string = safety.check_impact_string(
                    response=response,
                    shards=shards,
                    # pyre-ignore
                    target_state=mv.shard_target_state,
                )
                return f"Safety Check Impact:\n\n{impact_string}"
            else:
                return ""

        return "\n\n".join(
            filter(
                None,
                [
                    overview(mv, cv),
                    shards(mv, cv, expand_shards),
                    sequencers(mv, cv),
                    impact(mv, show_safety_check_results),
                ],
            )
        )

    return "\n\n---\n".join(
        one(mv, cluster_view, expand_shards) for mv in maintenance_views
    )


def _render_expanded_with_shards(
    maintenance_views: Sequence[MaintenanceView], cluster_view: ClusterView
) -> str:
    return _render_expanded(
        maintenance_views=maintenance_views,
        cluster_view=cluster_view,
        expand_shards=True,
    )


def _render_expanded_with_safety_checks(
    maintenance_views: Sequence[MaintenanceView], cluster_view: ClusterView
) -> str:
    return _render_expanded(
        maintenance_views=maintenance_views,
        cluster_view=cluster_view,
        show_safety_check_results=True,
    )


def _color_maintenance_status(arg: MaintenanceStatus) -> str:
    if arg in {
        MaintenanceStatus.BLOCKED_UNTIL_SAFE,
        MaintenanceStatus.REBUILDING_IS_BLOCKED,
    }:
        color = "red"
    elif arg == MaintenanceStatus.COMPLETED:
        color = "green"
    else:
        color = "yellow"
    return color


def _color_maintenance_overall_status(arg: MaintenanceOverallStatus) -> str:
    color = "white"
    if arg == MaintenanceOverallStatus.COMPLETED:
        color = "green"
    elif arg == MaintenanceOverallStatus.BLOCKED:
        color = "red"
    elif arg == MaintenanceOverallStatus.IN_PROGRESS:
        color = "yellow"
    return color


def _color(arg: Union[MaintenanceOverallStatus, MaintenanceStatus]) -> str:
    return {
        MaintenanceOverallStatus: _color_maintenance_overall_status,
        MaintenanceStatus: _color_maintenance_status,
        # pyre-ignore
    }[type(arg)](arg)


def _satisfy_shard_op_state(
    cur: ShardOperationalState, tgt: ShardOperationalState
) -> bool:
    if tgt == ShardOperationalState.MAY_DISAPPEAR:
        if cur in {
            ShardOperationalState.MAY_DISAPPEAR,
            ShardOperationalState.DRAINED,
            ShardOperationalState.PROVISIONING,
            ShardOperationalState.MIGRATING_DATA,
        }:
            return True
    elif tgt == ShardOperationalState.DRAINED:
        if cur == ShardOperationalState.DRAINED:
            return True

    return False


def _color_shard_op_state(
    cur: ShardOperationalState, tgt: ShardOperationalState
) -> str:
    if _satisfy_shard_op_state(cur, tgt):
        return "green"
    else:
        return "yellow"


def _filter_maintenance_views(
    maintenance_views: Sequence[MaintenanceView],
    cluster_view: ClusterView,
    ids: Optional[Collection[str]] = None,
    users: Optional[List[str]] = None,
    node_indexes: Optional[List[int]] = None,
    node_names: Optional[List[str]] = None,
    blocked: Optional[bool] = None,
    completed: Optional[bool] = None,
    in_progress: Optional[bool] = None,
    include_internal_maintenances: Optional[bool] = None,
) -> Generator[MaintenanceView, None, None]:
    cv = cluster_view
    mvs = (mv for mv in maintenance_views)

    # Filter out internal maintenances unless explicitly requested to unhide
    # them
    if (
        # if include_internal_maintenances is None, the view is not modified.
        include_internal_maintenances is not None
        and include_internal_maintenances is False
    ):
        mvs = (mv for mv in mvs if not mv.is_internal)

    if ids is not None:
        ids_set = set(ids)
        mvs = (mv for mv in mvs if mv.group_id in ids_set)

    if users is not None:
        users_set = set(users)
        mvs = (mv for mv in mvs if mv.user in users_set)

    if node_indexes is not None:
        node_indexes_set = set(node_indexes)
        mvs = (
            mv
            for mv in mvs
            if set(mv.affected_node_indexes).intersection(node_indexes_set)
        )

    if node_names is not None:
        node_indexes_set = {
            cv.get_node_index(node_name=node_name) for node_name in node_names
        }
        mvs = (
            mv
            for mv in mvs
            if set(mv.affected_node_indexes).intersection(node_indexes_set)
        )

    if blocked is not None:
        mvs = (mv for mv in mvs if mv.is_blocked == blocked)

    if completed is not None:
        mvs = (mv for mv in mvs if mv.is_completed == completed)

    if in_progress is not None:
        mvs = (mv for mv in mvs if mv.is_in_progress == in_progress)

    return mvs


def _parse_shard_target_state(tgt_state: str) -> ShardOperationalState:
    if tgt_state == "may-disappear":
        return ShardOperationalState.MAY_DISAPPEAR
    elif tgt_state == "drained":
        return ShardOperationalState.DRAINED
    else:
        raise ValueError(f"Can't parse shard_target_state: {tgt_state}")


@command("maintenance")
class MaintenanceCommand:
    """
    Allows to manipulate maintenances in Maintenance Manager
    """

    async def _select_and_render(
        self,
        ids: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        node_indexes: Optional[List[int]] = None,
        node_names: Optional[List[str]] = None,
        blocked: Optional[bool] = None,
        completed: Optional[bool] = None,
        in_progress: Optional[bool] = None,
        rendering_mode: Optional[RenderingMode] = RenderingMode.COMPACT,
    ) -> str:
        ctx = context.get_context()

        async with ctx.get_cluster_admin_client() as client:
            cv = await get_cluster_view(client)

        mvs = list(
            _filter_maintenance_views(
                # pyre-ignore
                cv.get_all_maintenance_views(),
                cv,
                ids=ids,
                users=users,
                node_indexes=node_indexes,
                node_names=node_names,
                blocked=blocked,
                completed=completed,
                in_progress=in_progress,
            )
        )
        if len(mvs) == 0:
            return colored("No maintenances matching given criteria", "red")
        else:
            # pyre-ignore
            return _render(maintenance_views=mvs, cluster_view=cv, mode=rendering_mode)

    @command("list")
    @argument(
        "ids", description="Show only maintenances with specified Maintenance Group IDs"
    )
    @argument("users", description="Show only maintenances created by specified user")
    @argument(
        "node_indexes", description="Show only maintenances affecting specified nodes"
    )
    @argument(
        "node_names", description="Show only maintenances affecting specified nodes"
    )
    @argument(
        "blocked",
        description="Show only maintenances which are blocked due to some reason",
    )
    @argument("completed", description="Show only maintenances which are finished")
    @argument(
        "in_progress",
        description="Show only maintenances which are in progress (including blocked)",
    )
    async def list_maintenances(
        self,
        ids: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        node_indexes: Optional[List[int]] = None,
        node_names: Optional[List[str]] = None,
        blocked: Optional[bool] = None,
        completed: Optional[bool] = None,
        in_progress: Optional[bool] = None,
    ) -> None:
        """
        Prints compact list of maintenances applied to the cluster
        """
        print(
            await self._select_and_render(
                ids=ids,
                users=users,
                node_indexes=node_indexes,
                node_names=node_names,
                blocked=blocked,
                completed=completed,
                in_progress=in_progress,
                rendering_mode=RenderingMode.COMPACT,
            )
        )

    @command
    @argument(
        "ids", description="List only maintenances with specified Maintenance Group IDs"
    )
    @argument("users", description="List only maintenances created by specified user")
    @argument(
        "node_indexes", description="List only maintenances affecting specified nodes"
    )
    @argument(
        "node_names", description="List only maintenances affecting specified nodes"
    )
    @argument(
        "blocked",
        description="List only maintenances which are blocked due to some reason",
    )
    @argument("completed", description="List only maintenances which are finished")
    @argument(
        "in_progress",
        description="List only maintenances which are in progress (including blocked)",
    )
    @argument("expand_shards", description="Show also per-shard information")
    @argument(
        "show_safety_check_results",
        description="Show the entire output (includes all logs) of the impact check",
    )
    async def show(
        self,
        ids: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        node_indexes: Optional[List[int]] = None,
        node_names: Optional[List[str]] = None,
        blocked: Optional[bool] = None,
        completed: Optional[bool] = None,
        in_progress: Optional[bool] = None,
        expand_shards: Optional[bool] = False,
        show_safety_check_results: bool = False,
    ) -> None:
        """
        Shows maintenances in expanded format with more information
        """
        rendering_mode = RenderingMode.EXPANDED
        if expand_shards:
            rendering_mode = RenderingMode.EXPANDED_WITH_SHARDS
        if show_safety_check_results:
            rendering_mode = RenderingMode.EXPANDED_WITH_SAFETY_CHECKS
        print(
            await self._select_and_render(
                ids=ids,
                users=users,
                node_indexes=node_indexes,
                node_names=node_names,
                blocked=blocked,
                completed=completed,
                in_progress=in_progress,
                rendering_mode=rendering_mode,
            )
        )

    @command
    @argument("node_indexes", description="Apply maintenance to specified nodes")
    @argument("node_names", description="Apply maintenance to specified nodes")
    @argument(
        "shards",
        description="Apply maintenance to specified shards "
        'in notation like "N1:S2", "N3:S4", "N165:S14"',
    )
    @argument(
        "shard_target_state",
        description='Shard Target State, either "may-disappear" or "drained"',
    )
    @argument(
        "sequencer_node_indexes",
        description="Apply maintenance to specified sequencers",
    )
    @argument(
        "sequencer_node_names", description="Apply maintenance to specified sequencers"
    )
    @argument(
        "user",
        description="User for logging and auditing, by default taken from environment",
    )
    @argument("reason", description="Reason for logging and auditing")
    @argument(
        "group",
        description="Defines should MaintenanceManager group this maintenance or not",
    )
    @argument("skip_safety_checks", description="If set safety-checks will be skipped")
    @argument(
        "ttl",
        description="If set this maintenance will be auto-expired "
        "after given number of seconds",
    )
    @argument(
        "allow_passive_drains", description="If set passive drains will be allowed"
    )
    @argument(
        "force_restore_rebuilding",
        description="Forces rebuilding to run in RESTORE mode",
    )
    async def apply(
        self,
        reason: str,
        node_indexes: Optional[List[int]] = None,
        node_names: Optional[List[str]] = None,
        shards: Optional[List[str]] = None,
        shard_target_state: Optional[str] = "may-disappear",
        sequencer_node_indexes: Optional[List[int]] = None,
        sequencer_node_names: Optional[List[str]] = None,
        user: Optional[str] = "",
        group: Optional[bool] = True,
        skip_safety_checks: Optional[bool] = False,
        ttl: Optional[int] = 0,
        allow_passive_drains: Optional[bool] = False,
        force_restore_rebuilding: Optional[bool] = False,
    ):
        """
        Applies new maintenance to Maintenance Manager
        """
        ctx = context.get_context()

        try:
            async with ctx.get_cluster_admin_client() as client:
                cv = await get_cluster_view(client)

            all_node_indexes = set()
            if node_indexes is not None:
                all_node_indexes = all_node_indexes.union(set(node_indexes))
            if node_names is not None:
                all_node_indexes = all_node_indexes.union(
                    {
                        cv.get_node_index(node_name=node_name)
                        for node_name in set(node_names)
                    }
                )

            shard_ids = set()
            sequencer_nodes = set()
            for ni in all_node_indexes:
                nv = cv.get_node_view(node_index=ni)
                if nv.is_storage:
                    shard_ids.add(ShardID(node=nv.node_id, shard_index=-1))
                if nv.is_sequencer:
                    sequencer_nodes.add(nv.node_id)

            if sequencer_node_indexes is not None:
                for ni in set(sequencer_node_indexes):
                    nv = cv.get_node_view(node_index=ni)
                    if nv.is_sequencer:
                        sequencer_nodes.add(nv.node_id)

            if sequencer_node_names is not None:
                for nn in set(sequencer_node_names):
                    nv = cv.get_node_view(node_name=nn)
                    if nv.is_sequencer:
                        sequencer_nodes.add(nv.node_id)

            if shards is not None:
                shard_ids = shard_ids.union(cv.expand_shards(parse_shards(shards)))

        except NodeNotFoundError as e:
            print(colored(f"Node not found: {e}", "red"))
            return

        try:
            async with ctx.get_cluster_admin_client() as client:
                maintenances: Collection[MaintenanceDefinition]
                maintenances = await apply_maintenance(
                    client=client,
                    shards=shard_ids,
                    shard_target_state=_parse_shard_target_state(shard_target_state),
                    sequencer_nodes=list(sequencer_nodes),
                    group=group,
                    ttl=timedelta(seconds=ttl),
                    user=user or getuser(),
                    reason=reason,
                    skip_safety_checks=skip_safety_checks,
                    allow_passive_drains=allow_passive_drains,
                    force_restore_rebuilding=force_restore_rebuilding,
                )
                cv = await get_cluster_view(client)
        except Exception as e:
            print(colored(f"Cannot apply maintenance: {e}", "red"))
            return

        print(
            _render(
                [
                    cv.get_maintenance_view_by_id(id)
                    for id in [mnt.group_id for mnt in maintenances]
                ],
                cv,
                mode=RenderingMode.EXPANDED,
            )
        )

    @command
    @argument(
        "reason",
        description="The reason of removing the maintenance, "
        "this is used for maintenance auditing and logging.",
    )
    @argument(
        "ids", description="Show only maintenances with specified Maintenance Group IDs"
    )
    @argument("users", description="Show only maintenances created by specified user")
    @argument(
        "node_indexes", description="Show only maintenances affecting specified nodes"
    )
    @argument(
        "node_names", description="Show only maintenances affecting specified nodes"
    )
    @argument(
        "blocked",
        description="Show only maintenances which are blocked due to some reason",
    )
    @argument("completed", description="Show only maintenances which are finished")
    @argument(
        "in_progress",
        description="Show only maintenances which are in progress (including blocked)",
    )
    @argument(
        "log_user",
        description="The user doing the removal operation, this is used for "
        "maintenance auditing and logging.",
    )
    @argument(
        "include_internal_maintenances",
        description="Should we include internal maintenances in our removal "
        "request?",
    )
    async def remove(
        self,
        reason: str,
        ids: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        node_indexes: Optional[List[int]] = None,
        node_names: Optional[List[str]] = None,
        blocked: Optional[bool] = None,
        completed: Optional[bool] = None,
        in_progress: Optional[bool] = None,
        log_user: Optional[str] = None,
        include_internal_maintenances=False,
    ) -> None:
        """
        Removes maintenances specified by filters.
        """
        ctx = context.get_context()

        async with ctx.get_cluster_admin_client() as client:
            cv = await get_cluster_view(client)

        all_maintenances = cv.get_all_maintenance_views()
        mvs = list(
            _filter_maintenance_views(
                # pyre-ignore
                all_maintenances,
                cv,
                ids=ids,
                users=users,
                node_indexes=node_indexes,
                node_names=node_names,
                blocked=blocked,
                completed=completed,
                in_progress=in_progress,
                include_internal_maintenances=include_internal_maintenances,
            )
        )

        if len(mvs) == 0 and include_internal_maintenances:
            print(colored("No maintenances matching given criteria", "white"))
            return
        elif len(mvs) == 0:
            print(
                colored(
                    "No maintenances matching given criteria, did you "
                    "mean to target internal maintenances?\nUse "
                    "`include-internal-maintenances` for this.",
                    "white",
                )
            )
            return

        print("You are going to remove following maintenances:\n")
        print(_render_expanded(mvs, cv, False))

        # Only user-created maintenances.
        if not include_internal_maintenances:
            cprint(
                "NOTE: Your query might have matched internal maintenances.\n"
                "We have excluded them from your remove request for "
                "safety. If you really need to remove internal maintenances,"
                " You need to to set `include-internal-maintenances to "
                "True`",
                "yellow",
            )
            if not confirm_prompt("Continue?"):
                return
        else:
            cprint(
                "\n\nWARNING: You might be deleting internal maintenances.\n "
                "This is a DANGEROUS operation. Only proceed if you are "
                "absolutely sure.",
                "red",
            )
            if not confirm_prompt("Take the RISK?"):
                return

        group_ids = [mv.group_id for mv in mvs]
        print(f"Removing maintenances {group_ids}")

        async with ctx.get_cluster_admin_client() as client:
            maintenances = await remove_maintenances(
                client=client,
                group_ids=group_ids,
                log_user=log_user or getuser(),
                log_reason=reason,
            )

        print(f"Removed maintenances {[mnt.group_id for mnt in maintenances]}")

    @command
    @argument(
        "user",
        description="User for logging and auditing, by default taken from environment",
    )
    @argument("reason", description="Reason for logging and auditing")
    async def mark_data_unrecoverable(self, reason: str, user: Optional[str] = ""):
        """
        [DANGER] Marks all the UNAVAILABLE shards (stuck on DATA_MIGRATION
        storage state) as unrecoverable. This will advice the readers to not
        wait for data on these shards and issue data loss gaps if necessary.
        """
        ctx = context.get_context()

        try:
            async with ctx.get_cluster_admin_client() as client:
                response: MarkAllShardsUnrecoverableResponse
                response = await mark_all_shards_unrecoverable(
                    client=client, user=user or getuser(), reason=reason
                )
                if response.shards_succeeded:
                    succeeded_str = ", ".join(
                        [str(shard) for shard in response.shards_succeeded]
                    )
                    cprint(f"Suceeded: {succeeded_str}", "green")
                if response.shards_failed:
                    failed_str = ", ".join(
                        [str(shard) for shard in response.shards_failed]
                    )
                    cprint(f"Failed: {failed_str}", "red")
                # Both are empty
                if not (response.shards_succeeded or response.shards_failed):
                    cprint("No UNAVAILABLE shards to mark unrecoverable!", "yellow")

        except Exception as e:
            print(colored(f"Cannot mark the data unrecoverable: {e}", "red"))
            return
