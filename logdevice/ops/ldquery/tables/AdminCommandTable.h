/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/ops/admin_command_client/AdminCommandClient.h"
#include "logdevice/ops/ldquery/Context.h"
#include "logdevice/ops/ldquery/Table.h"

namespace facebook { namespace logdevice { namespace ldquery {

// A data structure that holds the results coming from a single node, this will
// be aggregated to form the complete results
struct PartialTableData {
  folly::Optional<TableData> data;
  bool success;
  std::string failure_reason;
};

// This class connects to LogDevice's admin ports with 1 thread per LD instance
// and queries data from it
class AdminCommandTable : public Table {
 public:
  // @see num_fetches_.
  static constexpr int MAX_FETCHES = 5;

  enum class Type { JSON_TABLE, STAT };

  explicit AdminCommandTable(std::shared_ptr<Context> ctx,
                             Type type = Type::JSON_TABLE)
      : Table(ctx), type_(type) {
    ld_check(ctx->commandTimeout.hasValue());
    command_timeout_ = ctx->commandTimeout.value();
  }

  void init() override;

  // launches an event loop to fetch all the data from the LD instances.
  // Not meant to be overridden
  std::shared_ptr<TableData> getData(QueryContext& ctx) final;

  // Returns the columns augmented with node identifying ones
  TableColumns getColumns() const final;

 protected:
  // Override this method with the command to send to clients
  virtual std::string getCommandToSend(QueryContext& ctx) const = 0;

  // converts json to a column representation
  PartialTableData jsonToTableData(std::string json) const;

  // converts stat output to a column representation
  PartialTableData statToTableData(std::string stat_output) const;

  // Method that transforms the response of a single node into TableData
  virtual PartialTableData transformData(std::string response_from_node) const;

  // Override this to aggregate rows before returning them
  virtual TableData aggregate(std::vector<TableData> results) const;

  // Override this with your column set (i.e. same as getColumns() in Table)
  virtual TableColumns getFetchableColumns() const = 0;

  void newQuery() override;

  // Changes how long data should be keept in cache. Causes the cache to be
  // cleared.
  void setCacheTTL(std::chrono::seconds ttl) override;

  void enableServerSideFiltering(bool val) override {
    enable_server_side_filtering_ = val;
  }

 private:
  mutable TableColumns columns_;

  // Checks whether a node matches a constraint. Used by `selectNodes`.
  // Retuns:
  // - MATCH if the node matches the constraint;
  // - NO_MATCH if the node does not match the constraint;
  // - UNUSED if the constraint could not be used to determine if the node must
  // be queried.
  enum class MatchResult { MATCH, NO_MATCH, UNUSED };
  static MatchResult nodeMatchesConstraint(node_index_t nid,
                                           const Constraint& c);

  // Returns the list of nodes that we should query. This may leverage the query
  // constraints to figure out which nodes need not to be queried.
  std::vector<node_index_t> selectNodes(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      QueryContext& ctx) const;

  const TableColumns& getColumnsImpl() const;

  // Get the address to use to issue admin commands to node at pos `nid` in the
  // cluster. and return both the address and wether it expects SSL.
  std::tuple<folly::SocketAddress, AdminCommandClient::ConnectionType>
  getAddrForNode(
      node_index_t nid,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_configuration);

  // Check if we need to fetch data from logdeviced instances in the cluster to
  // populate admin_cmd_cache_.
  void refillCache(QueryContext& ctx);

  // Find a constraint that we can use to build an index.
  // We currently only consider the first constraint that has an equality
  // operator.
  folly::Optional<std::pair<int, const Constraint*>>
  findIndexableConstraint(const QueryContext& ctx);

  // Map a column name to its position in the vector returned by
  // getFetchableColumns().
  std::unordered_map<ColumnName, int> nameToPosMap_;

  std::vector<TableData>
  transformDataParallel(AdminCommandClient::RequestResponses& responses);

  std::chrono::milliseconds command_timeout_;
  Type type_;

  // An index on a column. Map a value to the list of row positions in the
  // corresponding TableData object for which the value at the column being
  // index is the key.
  // TODO(#7646110): null should be a possible key. Currently we don't fill the
  // index with null values.
  typedef std::unordered_map<std::string, std::vector<size_t>> Index;

  // Wraps the data received from all logdeviced instances in the cluster as
  // well as a map of column name to index on that column.
  struct Data {
    std::shared_ptr<TableData> data;

    // Map a column pos to an index for that column.
    std::unordered_map<int, Index> indices;
  };

  typedef logdevice::entry_with_ttl<Data> DataWithTTL;

  // Cached data for a given ConstraintMap.
  // This data is re-used only if the ConstraintMap in the QueryContext has not
  // changed.
  // We allow building indices within that cache as we see constraints that have
  // not been used for server-side filtering. For instance, if this is the
  // "readers" table and there are two constraints: log_id=42 and
  // last_batch_status=WINDOW_END_REACHED, the Readers table may leverage the
  // fact that we can run "info readers <logid>". There would thus be an entry
  // in this cache for each query that was done with that constraint. Within
  // each cache entry, an index on last_batch_status will be created the first
  // time we see an additional constraint on last_batch_status that was not
  // realized using server-side filtering.
  std::unordered_map<ConstraintMap, std::unique_ptr<DataWithTTL>>
      admin_cmd_cache_;

  // How many times did we run an admin command on the server.
  // If you ran a query that contains a join for instance (like select * from
  // readers join sequencers), we will use a "log_id=" constraint on the
  // sequencer table for each row in the "readers" table.
  // It is probably faster to just get the full content of the "sequencers"
  // table instead, cache it, and build a local index on "logid=".
  // We use a heuristic here: if we've done 5+ admin commands within the same
  // query, give up on trying to use server-side filtering and fetch everything.
  // I wish there was a way for SQLite to give a hint on how many times it is
  // planning to do the same fetch with the same constraints on the same table
  // within a query so this decision could be made immediately on the first
  // fetch.
  size_t num_fetches_{0};

  // Defines how long we should keep data in cache.
  std::chrono::seconds cache_ttl_{std::chrono::seconds(0)};

  // Whether or not we are allowed to leverage server-side filtering features of
  // admin commands.
  bool enable_server_side_filtering_{true};

  // Build an index for the given constraint on the given column.
  void buildIndexForConstraint(Data& data, int col, const Constraint* ctx);

  // Check whether we have the data in cache for the given server-side filtering
  // constraints. If the constraints are empty, checks whether we have a full
  // copy of the table locally.
  bool dataIsInCacheForUsedConstraints(const ConstraintMap& used_constraints);

  bool allowServerSideFiltering() const;
};

}}} // namespace facebook::logdevice::ldquery
