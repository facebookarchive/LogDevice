#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from dataclasses import dataclass
from typing import Dict, List

from ldops.types.node_view import NodeView
from logdevice.admin.maintenance.types import MaintenanceDefinition
from logdevice.admin.nodes.types import NodeConfig, NodeState


@dataclass
class ClusterView:
    nodes_state: List[NodeState]
    nodes_config: List[NodeConfig]
    maintenances: List[MaintenanceDefinition]

    def __post_init__(self):
        self._node_indexes = None

        self._node_index_to_node_config = None
        self._node_index_to_node_state = None
        self._node_index_to_node_view = None
        self._node_index_to_maintenances = None

    @property
    def node_indexes(self) -> List[int]:
        if self._node_indexes is None:
            self._node_indexes = sorted(nc.node_index for nc in self.nodes_config)
        return self._node_indexes

    @property
    def node_index_to_node_config(self) -> Dict[int, NodeConfig]:
        if self._node_index_to_node_config is None:
            self._node_index_to_node_config = {
                nc.node_index: nc for nc in self.nodes_config
            }
        return self._node_index_to_node_config

    @property
    def node_index_to_node_state(self) -> Dict[int, NodeState]:
        if self._node_index_to_node_state is None:
            self._node_index_to_node_state = {
                ns.node_index: ns for ns in self.nodes_state
            }
        return self._node_index_to_node_state

    @property
    def node_index_to_maintenances(self) -> Dict[int, List[MaintenanceDefinition]]:
        if self._node_index_to_maintenances is None:
            ni_to_mnts = {ni: [] for ni in self.node_indexes}
            for mnt in self.maintenances:
                nis = set()
                for s in mnt.shards:
                    nis.add(s.node.node_index)
                for n in mnt.sequencer_nodes:
                    nis.add(n.node_index)
                for ni in nis:
                    ni_to_mnts[ni].append(mnt)
            self._node_index_to_maintenances = ni_to_mnts
        return self._node_index_to_maintenances

    @property
    def node_index_to_node_view(self) -> Dict[int, NodeView]:
        if self._node_index_to_node_view is None:
            self._node_index_to_node_view = {
                ni: NodeView(
                    node_state=self.node_index_to_node_state[ni],
                    node_config=self.node_index_to_node_config[ni],
                    maintenances=self.node_index_to_maintenances[ni],
                )
                for ni in self.node_indexes
            }
        return self._node_index_to_node_view

    @property
    def node_views(self) -> List[NodeView]:
        return [self.node_index_to_node_view[ni] for ni in self.node_indexes]

    @property
    def node_names(self) -> List[str]:
        return [nv.node_name for nv in self.node_views]

    @property
    def node_index_to_node_name(self) -> Dict[int, str]:
        return {nv.node_index: nv.node_name for nv in self.node_views}

    @property
    def node_name_to_node_index(self) -> Dict[str, int]:
        return {nv.node_name: nv.node_index for nv in self.node_views}

    @property
    def node_name_to_node_config(self) -> Dict[str, NodeConfig]:
        return {nv.node_name: nv.node_config for nv in self.node_views}

    @property
    def node_name_to_node_state(self) -> Dict[str, NodeState]:
        return {nv.node_name: nv.node_state for nv in self.node_views}

    @property
    def node_name_to_node_view(self) -> Dict[str, NodeView]:
        return {nv.node_name: nv for nv in self.node_views}

    @property
    def maintenance_ids(self) -> List[str]:
        return [mnt.group_id for mnt in self.maintenances]

    @property
    def maintenance_id_to_maintenance(self) -> Dict[str, MaintenanceDefinition]:
        return {mnt.group_id: mnt for mnt in self.maintenances}

    @property
    def node_name_to_maintenances(self) -> Dict[str, List[MaintenanceDefinition]]:
        return {nv.node_name: nv.maintenances for nv in self.node_views}

    # Short aliases
    @property
    def mnts(self) -> List[MaintenanceDefinition]:
        return self.maintenances

    nis = node_indexes
    nns = node_names
    nvs = node_views

    ni_to_nc = node_index_to_node_config
    ni_to_nn = node_index_to_node_name
    ni_to_ns = node_index_to_node_state
    ni_to_nv = node_index_to_node_view
    nn_to_nc = node_name_to_node_config
    nn_to_ni = node_name_to_node_index
    nn_to_ns = node_name_to_node_state
    nn_to_nv = node_name_to_node_view

    mnt_ids = maintenance_ids
    mnt_id_to_mnt = maintenance_id_to_maintenance
    ni_to_mnts = node_index_to_maintenances
    nn_to_mnts = node_name_to_maintenances
