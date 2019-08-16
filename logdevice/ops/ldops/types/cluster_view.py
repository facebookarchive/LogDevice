#!/usr/bin/env python3
# pyre-strict

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from dataclasses import dataclass, field
from typing import Collection, Dict, Generator, List, Optional, Tuple

from ldops.exceptions import NodeNotFoundError
from ldops.types.maintenance_view import MaintenanceView
from ldops.types.node_view import NodeView
from logdevice.admin.maintenance.types import MaintenanceDefinition
from logdevice.admin.nodes.types import NodeConfig, NodeState


@dataclass
class ClusterView:
    nodes_config: Collection[NodeConfig]
    nodes_state: Collection[NodeState]
    maintenances: Collection[MaintenanceDefinition]

    _node_indexes_tuple: Optional[Tuple[int, ...]] = field(
        default=None, init=False, repr=False, compare=False
    )
    _node_index_to_node_config_dict: Optional[Dict[int, NodeConfig]] = field(
        default=None, init=False, repr=False, compare=False
    )
    _node_index_to_node_state_dict: Optional[Dict[int, NodeState]] = field(
        default=None, init=False, repr=False, compare=False
    )
    _node_index_to_maintenance_ids_dict: Optional[Dict[int, Tuple[str, ...]]] = field(
        default=None, init=False, repr=False, compare=False
    )
    _node_index_to_maintenances_dict: Optional[
        Dict[int, Tuple[MaintenanceDefinition, ...]]
    ] = field(default=None, init=False, repr=False, compare=False)
    _node_index_to_node_view_dict: Optional[Dict[int, NodeView]] = field(
        default=None, init=False, repr=False, compare=False
    )
    _node_name_to_node_view_dict: Optional[Dict[str, NodeView]] = field(
        default=None, init=False, repr=False, compare=False
    )
    _maintenance_ids_tuple: Optional[Tuple[str, ...]] = field(
        default=None, init=False, repr=False, compare=False
    )
    _maintenance_id_to_maintenance_dict: Optional[
        Dict[str, MaintenanceDefinition]
    ] = field(default=None, init=False, repr=False, compare=False)
    _maintenance_id_to_node_indexes_dict: Optional[Dict[str, Tuple[int, ...]]] = field(
        default=None, init=False, repr=False, compare=False
    )
    _maintenance_id_to_maintenance_view_dict: Optional[
        Dict[str, MaintenanceView]
    ] = field(default=None, init=False, repr=False, compare=False)

    @property
    def _node_indexes(self) -> Tuple[int, ...]:
        if self._node_indexes_tuple is None:
            self._node_indexes_tuple = tuple(
                sorted(
                    {nc.node_index for nc in self.nodes_config}.intersection(
                        {ns.node_index for ns in self.nodes_state}
                    )
                )
            )
        return self._node_indexes_tuple

    @property
    def _node_index_to_node_config(self) -> Dict[int, NodeConfig]:
        if self._node_index_to_node_config_dict is None:
            self._node_index_to_node_config_dict = {
                nc.node_index: nc for nc in self.nodes_config
            }
        return self._node_index_to_node_config_dict

    @property
    def _node_index_to_node_state(self) -> Dict[int, NodeState]:
        if self._node_index_to_node_state_dict is None:
            self._node_index_to_node_state_dict = {
                ns.node_index: ns for ns in self.nodes_state
            }
        return self._node_index_to_node_state_dict

    @property
    def _node_index_to_maintenance_ids(self) -> Dict[int, Tuple[str, ...]]:
        if self._node_index_to_maintenance_ids_dict is None:
            ni_to_mnt_ids: Dict[int, List[str]] = {ni: [] for ni in self._node_indexes}
            for mnt in self.maintenances:
                nis = set(
                    {
                        n.node_index
                        for n in mnt.sequencer_nodes
                        if n.node_index is not None
                    }
                ).union(
                    {
                        s.node.node_index
                        for s in mnt.shards
                        if s.node.node_index is not None
                    }
                )
                for ni in nis:
                    if mnt.group_id is not None:
                        # pyre-fixme[6]: Expected `int` for 1st param but got
                        #  `Optional[int]`.
                        # pyre-fixme[6]: Expected `str` for 1st param but got
                        #  `Optional[str]`.
                        ni_to_mnt_ids[ni].append(mnt.group_id)

            self._node_index_to_maintenance_ids_dict = {
                ni: tuple(sorted(mnt_ids)) for ni, mnt_ids in ni_to_mnt_ids.items()
            }
        # pyre-fixme[7]: Expected `Dict[int, Tuple[str, ...]]` but got
        #  `Optional[Dict[int, Tuple[str, ...]]]`.
        return self._node_index_to_maintenance_ids_dict

    @property
    def _maintenance_ids(self) -> Tuple[str, ...]:
        if self._maintenance_ids_tuple is None:
            self._maintenance_ids_tuple = tuple(
                sorted(str(mnt.group_id) for mnt in self.maintenances)
            )
        return self._maintenance_ids_tuple

    @property
    def _maintenance_id_to_maintenance(self) -> Dict[str, MaintenanceDefinition]:
        if self._maintenance_id_to_maintenance_dict is None:
            self._maintenance_id_to_maintenance_dict = {
                str(mnt.group_id): mnt for mnt in self.maintenances
            }
        return self._maintenance_id_to_maintenance_dict

    @property
    def _maintenance_id_to_node_indexes(self) -> Dict[str, Tuple[int, ...]]:
        if self._maintenance_id_to_node_indexes_dict is None:
            # pyre-fixme[8]: Attribute has type `Optional[Dict[str, Tuple[int,
            #  ...]]]`; used as `Dict[Optional[str], Tuple[Optional[int], ...]]`.
            self._maintenance_id_to_node_indexes_dict = {
                mnt.group_id: tuple(
                    sorted(
                        set(
                            {
                                n.node_index
                                for n in mnt.sequencer_nodes
                                if n.node_index is not None
                            }
                        ).union(
                            {
                                s.node.node_index
                                for s in mnt.shards
                                if s.node.node_index is not None
                            }
                        )
                    )
                )
                for mnt in self.maintenances
                if mnt.group_id is not None
            }
        # pyre-fixme[7]: Expected `Dict[str, Tuple[int, ...]]` but got
        #  `Optional[Dict[str, Tuple[int, ...]]]`.
        return self._maintenance_id_to_node_indexes_dict

    @property
    def _maintenance_id_to_maintenance_view(self) -> Dict[str, MaintenanceView]:
        if self._maintenance_id_to_maintenance_view_dict is None:
            self._maintenance_id_to_maintenance_view_dict = {
                mnt_id: MaintenanceView(
                    maintenance=self._maintenance_id_to_maintenance[mnt_id],
                    node_index_to_node_view={
                        ni: self._node_index_to_node_view[ni]
                        for ni in self._maintenance_id_to_node_indexes[mnt_id]
                    },
                )
                for mnt_id in self._maintenance_ids
            }
        return self._maintenance_id_to_maintenance_view_dict

    @property
    def _node_index_to_maintenances(
        self
    ) -> Dict[int, Tuple[MaintenanceDefinition, ...]]:
        if self._node_index_to_maintenances_dict is None:
            self._node_index_to_maintenances_dict = {
                ni: tuple(
                    self._maintenance_id_to_maintenance[mnt_id]
                    for mnt_id in self._node_index_to_maintenance_ids[ni]
                )
                for ni in self._node_indexes
            }
        return self._node_index_to_maintenances_dict

    @property
    def _node_index_to_node_view(self) -> Dict[int, NodeView]:
        if self._node_index_to_node_view_dict is None:
            self._node_index_to_node_view_dict = {
                ni: NodeView(
                    node_config=self._node_index_to_node_config[ni],
                    node_state=self._node_index_to_node_state[ni],
                    maintenances=self._node_index_to_maintenances[ni],
                )
                for ni in self._node_indexes
            }
        return self._node_index_to_node_view_dict

    @property
    def _node_name_to_node_view(self) -> Dict[str, NodeView]:
        if self._node_name_to_node_view_dict is None:
            self._node_name_to_node_view_dict = {
                nv.node_name: nv for nv in self._node_index_to_node_view.values()
            }
        return self._node_name_to_node_view_dict

    ### Public interface
    def get_all_node_indexes(self) -> Generator[int, None, None]:
        return (ni for ni in self._node_indexes)

    def get_all_node_views(self) -> Generator[NodeView, None, None]:
        return (self.get_node_view(node_index=ni) for ni in self.get_all_node_indexes())

    def get_all_node_names(self) -> Generator[str, None, None]:
        return (nv.node_name for nv in self.get_all_node_views())

    def get_all_maintenance_ids(self) -> Generator[str, None, None]:
        return (mnt_id for mnt_id in self._maintenance_ids)

    def get_all_maintenances(self) -> Generator[MaintenanceDefinition, None, None]:
        return (
            self.get_maintenance_by_id(mnt_id)
            for mnt_id in self.get_all_maintenance_ids()
        )

    def get_all_maintenance_views(self) -> Generator[MaintenanceView, None, None]:
        return (
            self.get_maintenance_view_by_id(mnt_id)
            for mnt_id in self.get_all_maintenance_ids()
        )

    # By node_index
    def get_node_view_by_node_index(self, node_index: int) -> NodeView:
        node_view = self._node_index_to_node_view.get(node_index, None)
        if node_view is None:
            raise NodeNotFoundError(f"node_index={node_index}")
        return node_view

    def get_node_name_by_node_index(self, node_index: int) -> str:
        return self.get_node_view(node_index=node_index).node_name

    def get_node_config_by_node_index(self, node_index: int) -> NodeConfig:
        return self.get_node_view(node_index=node_index).node_config

    def get_node_state_by_node_index(self, node_index: int) -> NodeState:
        return self.get_node_view(node_index=node_index).node_state

    def get_node_maintenances_by_node_index(
        self, node_index: int
    ) -> Tuple[MaintenanceDefinition, ...]:
        return self.get_node_view(node_index=node_index).maintenances

    # By node_name
    def get_node_view_by_node_name(self, node_name: str) -> NodeView:
        node_view = self._node_name_to_node_view.get(node_name, None)
        if node_view is None:
            raise NodeNotFoundError(f"node_name={node_name}")
        return node_view

    def get_node_index_by_node_name(self, node_name: str) -> int:
        return self.get_node_view(node_name=node_name).node_index

    def get_node_config_by_node_name(self, node_name: str) -> NodeConfig:
        return self.get_node_view(node_name=node_name).node_config

    def get_node_state_by_node_name(self, node_name: str) -> NodeState:
        return self.get_node_view(node_name=node_name).node_state

    def get_node_maintenances_by_node_name(
        self, node_name: str
    ) -> Tuple[MaintenanceDefinition, ...]:
        return self.get_node_view(node_name=node_name).maintenances

    # By whatever
    def get_node_view(
        self, node_index: Optional[int] = None, node_name: Optional[str] = None
    ) -> NodeView:
        if node_name is None and node_index is None:
            raise ValueError("Either node_name or node_index must be specified")

        by_node_index: Optional[NodeView] = None
        by_node_name: Optional[NodeView] = None

        if node_index is not None:
            by_node_index = self.get_node_view_by_node_index(node_index=node_index)
        if node_name is not None:
            by_node_name = self.get_node_view_by_node_name(node_name=node_name)

        if (
            by_node_index is not None
            and by_node_name is not None
            and by_node_index != by_node_name
        ):
            raise ValueError(
                f"Node with node_name={node_name} and Node with "
                f"node_index={node_index} are not the same node"
            )
        elif by_node_index is not None:
            return by_node_index
        elif by_node_name is not None:
            return by_node_name
        else:
            assert False, "unreachable"  # pragma: nocover

    def get_node_index(self, node_name: str) -> int:
        return self.get_node_view(node_name=node_name).node_index

    def get_node_name(self, node_index: int) -> str:
        return self.get_node_view(node_index=node_index).node_name

    def get_node_config(
        self, node_index: Optional[int] = None, node_name: Optional[str] = None
    ) -> NodeConfig:
        return self.get_node_view(
            node_index=node_index, node_name=node_name
        ).node_config

    def get_node_state(
        self, node_index: Optional[int] = None, node_name: Optional[str] = None
    ) -> NodeState:
        return self.get_node_view(node_index=node_index, node_name=node_name).node_state

    def get_node_maintenances(
        self, node_index: Optional[int] = None, node_name: Optional[str] = None
    ) -> Tuple[MaintenanceDefinition, ...]:
        return self.get_node_view(
            node_index=node_index, node_name=node_name
        ).maintenances

    # Maintenances
    def get_maintenance_by_id(self, maintenance_id: str) -> MaintenanceDefinition:
        return self._maintenance_id_to_maintenance[maintenance_id]

    def get_maintenance_view_by_id(self, maintenance_id: str) -> MaintenanceView:
        return self._maintenance_id_to_maintenance_view[maintenance_id]

    def get_node_indexes_by_maintenance_id(
        self, maintenance_id: str
    ) -> Tuple[int, ...]:
        return self._maintenance_id_to_node_indexes[maintenance_id]
