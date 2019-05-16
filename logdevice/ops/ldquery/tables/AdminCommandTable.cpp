/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/AdminCommandTable.h"

#include <thread>

#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/json.h>

#include "external/gason/gason.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientImpl.h"

using facebook::logdevice::ClientImpl;
using folly::SocketAddress;
using std::chrono::steady_clock;

namespace facebook { namespace logdevice { namespace ldquery {

namespace {
struct ColumnAndDataType {
  Column* data = nullptr;
  DataType type;
};
} // namespace

void AdminCommandTable::init() {
  int i = 0;
  for (const auto& c : getFetchableColumns()) {
    nameToPosMap_[c.name] = i++;
  }
}

bool AdminCommandTable::allowServerSideFiltering() const {
  // Allow server side filtering if the user did not call
  // LDQuery::enableServerSideFiltering(true) and if we have not already done
  // more than MAX_FETCHES roundtrips to the server.
  return enable_server_side_filtering_ && num_fetches_ <= MAX_FETCHES;
}

AdminCommandTable::MatchResult
AdminCommandTable::nodeMatchesConstraint(node_index_t nid,
                                         const Constraint& c) {
  if (!c.expr.hasValue()) {
    return MatchResult::UNUSED;
  }
  std::string expr = c.expr.value();
  int node_id = folly::to<int>(expr);
  if (c.op == SQLITE_INDEX_CONSTRAINT_EQ) {
    if (nid != node_id) {
      return MatchResult::NO_MATCH;
    }
  } else if (c.op == SQLITE_INDEX_CONSTRAINT_LT) {
    if (nid >= node_id) {
      return MatchResult::NO_MATCH;
    }
  } else if (c.op == SQLITE_INDEX_CONSTRAINT_GT) {
    if (nid <= node_id) {
      return MatchResult::NO_MATCH;
    }
  } else if (c.op == SQLITE_INDEX_CONSTRAINT_LE) {
    if (nid > node_id) {
      return MatchResult::NO_MATCH;
    }
  } else if (c.op == SQLITE_INDEX_CONSTRAINT_GE) {
    if (nid < node_id) {
      return MatchResult::NO_MATCH;
    }
  } else {
    return MatchResult::UNUSED;
  }
  return MatchResult::MATCH;
}

std::vector<node_index_t> AdminCommandTable::selectNodes(
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    QueryContext& ctx) const {
  // Look for constraints on column 0 ("node_id").
  const int col_index = 0;
  auto it_c = ctx.constraints.find(col_index);
  std::unordered_set<int> used_constraints;
  std::vector<node_index_t> res;

  for (const auto& kv : *nodes_configuration.getServiceDiscovery()) {
    bool skip = false;
    if (it_c != ctx.constraints.end() && allowServerSideFiltering()) {
      const ConstraintList& constraints = it_c->second;
      for (int i = 0; i < constraints.constraints_.size(); ++i) {
        const Constraint& c = constraints.constraints_[i];
        auto res = nodeMatchesConstraint(kv.first, c);
        if (res != MatchResult::UNUSED) {
          used_constraints.insert(i);
        }
        skip |= res == MatchResult::NO_MATCH;
      }
    }

    if (!skip) {
      // The node matches all used constraints.
      res.push_back(kv.first);
    }
  }

  // Keep track of what constraints were used.
  for (int i : used_constraints) {
    ctx.used_constraints[col_index].add(
        ctx.constraints[col_index].constraints_[i]);
  }

  return res;
}

TableData AdminCommandTable::aggregate(std::vector<TableData> results) const {
  TableData result;

  size_t total_rows = 0;
  for (const auto& src : results) {
    total_rows += src.numRows();
  }

  size_t rows_so_far = 0;
  for (auto& src : results) {
    size_t rows_in_current = 0;
    for (const auto& kv : src.cols) {
      const ColumnName& name = kv.first;
      const Column& c = kv.second;

      ld_check(rows_in_current == 0 || rows_in_current == c.size());
      rows_in_current = c.size();

      Column& res = result.cols[name];
      if (res.empty()) { // inserted a new column
        res.reserve(total_rows);
      }
      // If some of the `results` didn't have this column, fill corresponding
      // rows with nulls.
      res.resize(rows_so_far);
      // Copy the data.
      res.insert(res.end(),
                 std::make_move_iterator(c.begin()),
                 std::make_move_iterator(c.end()));
    }
    rows_so_far += rows_in_current;
  }
  ld_check(rows_so_far == total_rows);

  for (auto& kv : result.cols) {
    // If the last of the `results` didn't contain some of the columns, fill
    // them with nulls.
    kv.second.resize(total_rows);
  }

  return result;
}

void AdminCommandTable::newQuery() {
  // We are beginning a new query. Clear the cache according to TTLs.

  auto it = admin_cmd_cache_.begin();
  while (it != admin_cmd_cache_.end()) {
    if (!it->second->isValid(cache_ttl_)) {
      it = admin_cmd_cache_.erase(it);
    } else {
      ++it;
    }
  }

  num_fetches_ = 0;
}

void AdminCommandTable::setCacheTTL(std::chrono::seconds ttl) {
  cache_ttl_ = ttl;
}

/*
 * Currently there is no easy way to figure out in which address each node is
 * listening for admin commands in a LogDevice cluster. Right now this is not an
 * issue because our clusters are set up so that each node listens to port 5440,
 * but this will be an issue when we start using different port. We will
 * probably need to add the port in the config of the cluster as well as SMC.
 */
std::tuple<SocketAddress, AdminCommandClient::ConnectionType>
AdminCommandTable::getAddrForNode(
    node_index_t nid,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_configuration) {
  auto ld_client = ld_ctx_->getClient();
  ld_check(ld_client);

  const auto* node_sd = nodes_configuration->getNodeServiceDiscovery(nid);
  ld_check(node_sd);

  if (node_sd->address.isUnixAddress()) {
    // The node is running locally and using a named socket. Expect another
    // named socket named "socket_command" to exist in the same directory.
    std::string path = node_sd->address.getPath();
    path = path.substr(0, path.find_last_of("/\\")) + "/socket_command";
    SocketAddress addr;
    addr.setFromPath(path);
    return std::make_tuple(addr, AdminCommandClient::ConnectionType::PLAIN);
  } else {
    // The node is using a TCP address. Use the same address but with port 5440
    // to send admin commands.
    // TODO: extract admin port from config
    auto addr = node_sd->address.getSocketAddress();
    addr.setPort(5440);
    // Is encryption needed?
    auto conntype = AdminCommandClient::ConnectionType::PLAIN;
    if (ld_ctx_->use_ssl) {
      conntype = AdminCommandClient::ConnectionType::ENCRYPTED;
    }
    return std::make_tuple(addr, conntype);
  }
}

bool AdminCommandTable::dataIsInCacheForUsedConstraints(
    const ConstraintMap& used_constraints) {
  auto it = admin_cmd_cache_.find(used_constraints);
  return it != admin_cmd_cache_.end();
}

void AdminCommandTable::refillCache(QueryContext& ctx) {
  auto ld_client = ld_ctx_->getClient();
  ld_check(ld_client);
  logdevice::ClientImpl* client_impl =
      static_cast<logdevice::ClientImpl*>(ld_client.get());
  const auto& nodes_configuration =
      client_impl->getConfig()->getNodesConfiguration();

  // `selectNodes` may decide to select by `node_id`. In that case it will
  // mutate `ctx.used_constraints`.
  auto selected_nodes = selectNodes(*nodes_configuration, ctx);

  auto used_constraints = ctx.used_constraints;

  std::string cmd;

  if (allowServerSideFiltering()) {
    // Find the admin command that needs to be sent to get the minimum amount of
    // data given the query constraints.
    // This function populates `ctx.used_constraints` which are the constraints
    // it could use to perform server-side filtering.
    cmd = getCommandToSend(ctx);
    if (dataIsInCacheForUsedConstraints(ctx.used_constraints)) {
      ld_info("Using cached results for admin command '%s'",
              folly::rtrimWhitespace(cmd.c_str()).str().c_str());
      return;
    }
  }

  if (!allowServerSideFiltering() || num_fetches_ > MAX_FETCHES) {
    // We can be here for two reasons:
    //
    // 1/ server side filtering is enabled but we did too many fetches already,
    //    we decide it's best to just fetch everything from the server and index
    //    locally.
    // 2/ server side filtering is disabled.
    //
    // Call `getCommandToSend()` again without allowing any server-side
    // filtering. Reset `ctx.used_constraints` to the previous value before the
    // initial call to `getCommandToSend` (assuming 1/) and pass an empty
    // `constraints` map so that the next call to getCommandToSend() will not
    // leverage any constraints for server-side filtering;
    ctx.used_constraints = std::move(used_constraints);
    auto constraints = std::move(ctx.constraints);
    cmd = getCommandToSend(ctx);
    ctx.constraints = std::move(constraints);
    if (dataIsInCacheForUsedConstraints(ctx.used_constraints)) {
      ld_info("Using cached results for admin command '%s'",
              folly::rtrimWhitespace(cmd.c_str()).str().c_str());
      return;
    }
  }

  AdminCommandClient ld_admin_client;

  std::unordered_map<folly::SocketAddress, node_index_t> addr_to_node_id;
  AdminCommandClient::RequestResponses request_response;
  request_response.reserve(selected_nodes.size());
  for (node_index_t i : selected_nodes) {
    ld_check(nodes_configuration->isNodeInServiceDiscoveryConfig(i));

    AdminCommandClient::ConnectionType conntype =
        AdminCommandClient::ConnectionType::UNKNOWN;
    SocketAddress addr;
    std::tie(addr, conntype) = getAddrForNode(i, nodes_configuration);
    addr_to_node_id[addr] = i;
    request_response.emplace_back(addr, cmd, conntype);
    ld_ctx_->activeQueryMetadata.contacted_nodes++;
  }

  ld_info("Sending '%s' admin command to %lu nodes...",
          folly::rtrimWhitespace(cmd.c_str()).str().c_str(),
          request_response.size());

  steady_clock::time_point tstart = steady_clock::now();
  ld_admin_client.send(request_response, command_timeout_);
  steady_clock::time_point tend = steady_clock::now();
  double duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  size_t replies = 0;
  for (const auto& r : request_response) {
    replies += r.success;
  }
  ld_info("Receiving data took %.1fs, %lu/%lu nodes replied",
          duration,
          replies,
          request_response.size());
  tstart = tend;

  ld_info("Parsing json data...");
  std::vector<TableData> results = transformDataParallel(request_response);
  ld_check(results.size() == request_response.size());
  for (int i = 0; i < results.size(); ++i) {
    if (!results[i].cols.empty()) {
      size_t rows = results[i].cols.begin()->second.size();
      std::string node_id_str =
          folly::to<std::string>(addr_to_node_id[request_response[i].sockaddr]);
      results[i].cols["node_id"] = Column(rows, node_id_str);
    }
  }

  for (const auto& r : request_response) {
    if (!r.success) {
      node_index_t node_id = addr_to_node_id[r.sockaddr];
      ld_info("Failed request for N%d (%s): %s",
              node_id,
              r.sockaddr.describe().c_str(),
              r.failure_reason.c_str());
      ld_ctx_->activeQueryMetadata.failures[node_id] =
          FailedNodeDetails{r.sockaddr.describe(), r.failure_reason};
    }
  }

  tend = steady_clock::now();
  duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  ld_info("Json parsing took %.1fs", duration);

  tstart = tend;
  Data data;
  ld_info("Aggregating data from %lu nodes...", request_response.size());
  data.data = std::make_shared<TableData>(aggregate(std::move(results)));

  tend = steady_clock::now();
  duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  ld_info("Aggregating %lu rows took %.1fs", data.data->numRows(), duration);

  admin_cmd_cache_[ctx.used_constraints] =
      std::make_unique<DataWithTTL>(DataWithTTL(std::move(data)));

  ++num_fetches_;
}

folly::Optional<std::pair<int, const Constraint*>>
AdminCommandTable::findIndexableConstraint(const QueryContext& ctx) {
  for (const auto& e : ctx.constraints) {
    const auto col = e.first;
    const auto& c = e.second;
    for (auto& constraint : c.constraints_) {
      if (constraint.op != SQLITE_INDEX_CONSTRAINT_EQ) {
        // TODO(#7646110): we discard constraints for which the operator is not
        // equality because we currently only support indexes for equality.
        continue;
      }
      return std::make_pair(col, &constraint);
    }
  }
  return folly::none;
}

void AdminCommandTable::buildIndexForConstraint(Data& data,
                                                int col,
                                                const Constraint* ctx) {
  ld_check(ctx->op == SQLITE_INDEX_CONSTRAINT_EQ);
  ld_check(col < getColumnsImpl().size());
  const ColumnName& col_name = getColumnsImpl()[col].name;

  ld_info("Building index for column `%s`...", col_name.c_str());
  steady_clock::time_point tstart = steady_clock::now();

  data.indices[col]; // create the index even if it's going to be empty

  if (!data.data->cols.count(col_name)) {
    // All values are null.
  } else {
    const Column& col_vec = data.data->cols.at(col_name);
    for (size_t i = 0; i < col_vec.size(); ++i) {
      const ColumnValue& v = col_vec[i];
      // We don't fill the index with null values.
      if (v.hasValue()) {
        data.indices[col][v.value()].push_back(i);
      }
    }
  }

  steady_clock::time_point tend = steady_clock::now();
  double duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  ld_info(
      "Building index for column `%s` took %.1fs", col_name.c_str(), duration);
}

std::shared_ptr<TableData> AdminCommandTable::getData(QueryContext& ctx) {
  // First, refill the cache if necessary.
  refillCache(ctx);
  ld_check(admin_cmd_cache_.count(ctx.used_constraints));

  Data& cached = admin_cmd_cache_[ctx.used_constraints]->get();

  // Then, check if we can serve the data from an index.
  auto c = findIndexableConstraint(ctx);
  if (!c.hasValue()) {
    // If we are here, we could not fetch data from an index. Return everything
    // we have, SQLite will filter it.
    return cached.data;
  }

  auto& expr = c.value().second->expr;
  if (!expr.hasValue()) {
    // TODO(#7646110): if the constraint compares a column with "null", we do
    // not check the index because the index currently does not support indexing
    // null values.
    return cached.data;
  }

  // There is an index we can use. If we have not built the index yet, do it
  // now.
  auto& indices = cached.indices;
  if (indices.find(c.value().first) == indices.end()) {
    buildIndexForConstraint(cached, c.value().first, c.value().second);
  }

  if (indices.find(c.value().first) == indices.end()) {
    // No index was created, there is no data.
    return std::make_shared<TableData>();
  }

  // Return the data stored in the index.
  auto it = indices[c.value().first].find(expr.value());
  if (it == indices[c.value().first].end()) {
    // There is no data found that matches the constraint.
    return std::make_shared<TableData>();
  }

  TableData result;
  for (const auto& cached_col : cached.data->cols) {
    Column& result_col = result.cols[cached_col.first];
    result_col.reserve(it->second.size());
    for (size_t idx : it->second) {
      result_col.push_back(cached_col.second[idx]);
    }
  }
  return std::make_shared<TableData>(std::move(result));
}

class NameNormalizer {
  struct both_underscores {
    bool operator()(char a, char b) const {
      return a == '_' && b == '_';
    }
  };

 public:
  static void normalize(std::string& name) {
    // Replacing all non-alphanumerics with _
    auto strip_chars = [](char ch) {
      if (ch >= '0' && ch <= '9') {
        return ch;
      }
      if (ch >= 'a' && ch <= 'z') {
        return ch;
      }
      if (ch >= 'A' && ch <= 'Z') {
        return (char)::tolower(ch);
      }
      return '_';
    };
    std::transform(name.begin(), name.end(), name.begin(), strip_chars);

    // trim trailing trash
    size_t endpos = name.find_last_not_of("_");
    if (std::string::npos != endpos) {
      name = name.substr(0, endpos + 1);
    }
    // trim leading trash
    size_t startpos = name.find_first_not_of("_");
    if (std::string::npos != startpos) {
      name = name.substr(startpos);
    }
    // replace multiple consecutive underscores
    name.erase(
        std::unique(name.begin(), name.end(), both_underscores()), name.end());
  }
};

PartialTableData AdminCommandTable::jsonToTableData(std::string json) const {
  char* endptr;
  steady_clock::time_point tstart = steady_clock::now();
  JsonValue value;
  JsonAllocator allocator;
  int status = jsonParse((char*)json.data(), &endptr, &value, allocator);
  steady_clock::time_point tend = steady_clock::now();
  double duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  if (status != JSON_PARSE_OK) {
    RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Cannot parse json result");

    return PartialTableData{folly::none, false, "JSON_PARSE_ERROR"};
  }

  // Descriptions of the columns in the json result. If `data` is nullptr,
  // the column is not present in getFetchableColumns(), so we should ignore it.
  std::vector<ColumnAndDataType> columns_present;
  TableData results;

  auto parse_headers = [&](const JsonValue& o) {
    if (o.getTag() != JsonTag::JSON_TAG_ARRAY) {
      ld_error("Expecting array for headers");
      return false;
    }
    for (const auto& i : o) {
      if (i->value.getTag() != JsonTag::JSON_TAG_STRING) {
        ld_error("Expecting string for column name");
        return false;
      }
      std::string name = i->value.toString();
      NameNormalizer::normalize(name);

      auto it = nameToPosMap_.find(name);
      if (it == nameToPosMap_.end()) {
        // Tell the row parser to ignore this column.
        columns_present.emplace_back();
      } else {
        const int col_pos = it->second;
        ld_check(getFetchableColumns()[col_pos].name == name);
        ColumnAndDataType column_info;
        column_info.type = getFetchableColumns()[col_pos].type;
        column_info.data = &results.cols[name];
        columns_present.push_back(column_info);
      }
    }
    return true;
  };

  auto parse_row = [&](size_t row_idx, const JsonValue& o) {
    if (o.getTag() != JsonTag::JSON_TAG_ARRAY) {
      ld_error("Expecting array for row");
      return false;
    }
    size_t col = 0;
    for (const auto& i : o) {
      ld_check(i);
      if (col >= columns_present.size()) {
        ++col;
        // The error will be reported after the loop.
        continue;
      }
      auto& column_info = columns_present[col++];
      if (column_info.data == nullptr) {
        // LDQuery does not know about this column. Just ignore it.
        continue;
      }
      switch (i->value.getTag()) {
        case JsonTag::JSON_TAG_STRING:
          column_info.data->emplace_back(i->value.toString());
          preprocessColumn(column_info.type, &column_info.data->back());
          break;
        case JsonTag::JSON_TAG_NULL:
          column_info.data->push_back(folly::none);
          break;
        default:
          ld_error("Expecting string or null value for column value, "
                   "but got %i",
                   i->value.getTag());
          return false;
      }
    }
    if (col != columns_present.size()) {
      ld_error("Invalid json table: header has %lu columns, but row %lu has "
               "%lu columns",
               columns_present.size(),
               row_idx,
               col);
      return false;
    }
    return true;
  };

  auto parse_rows = [&](const JsonValue& o) {
    if (o.getTag() != JsonTag::JSON_TAG_ARRAY) {
      ld_error("Expecting array for rows");
      return false;
    }
    size_t row_idx = 0;
    for (const auto& i : o) {
      if (!parse_row(row_idx++, i->value)) {
        return false;
      }
    }
    return true;
  };

  JsonNode* headers = nullptr;
  JsonNode* rows = nullptr;

  switch (value.getTag()) {
    case JsonTag::JSON_TAG_OBJECT:
      for (const auto& i : value) {
        if (i->key == std::string("headers")) {
          headers = i;
        } else if (i->key == std::string("rows")) {
          rows = i;
        }
      }
      break;
    default:
      ld_info("Root of json tree is not an object.");
      return PartialTableData{folly::none, false, "MALFORMED_RESPONSE"};
  };

  if (!headers) {
    ld_error("Missing headers section");
    return PartialTableData{folly::none, false, "MALFORMED_RESPONSE"};
  }
  if (!rows) {
    ld_error("Missing rows section");
    return PartialTableData{folly::none, false, "MALFORMED_RESPONSE"};
  }

  if (!parse_headers(headers->value) || !parse_rows(rows->value)) {
    return PartialTableData{folly::none, false, "MALFORMED_RESPONSE"};
  }

  return PartialTableData{std::move(results), true, ""};
}

PartialTableData
AdminCommandTable::statToTableData(std::string stat_output) const {
  TableData result;
  // Precalculate the Column pointers and data types to avoid unordered_map
  // lookup for every row.
  std::vector<ColumnAndDataType> columns;
  for (const auto& c : getFetchableColumns()) {
    columns.push_back({&result.cols[c.name], c.type});
  }

  std::vector<std::string> lines;
  folly::split("\n", stat_output, lines);
  for (auto& line : lines) {
    if (line.empty()) {
      continue;
    }
    std::vector<std::string> tokens;
    folly::split(' ', folly::trimWhitespace(line), tokens);
    if (tokens.size() != getFetchableColumns().size() + 1) {
      ld_error("Row size mismatch, row: [%s], got %lu but expected %lu",
               line.c_str(),
               tokens.size(),
               getFetchableColumns().size() + 1);
      continue;
    }
    if (tokens[0] != "STAT") {
      ld_error("Expecting first row to be \"STAT\", row: %s", line.c_str());
      continue;
    }
    for (int i = 1; i < tokens.size(); ++i) {
      auto& c = columns.at(i - 1);
      c.data->push_back(std::move(tokens[i]));
      preprocessColumn(c.type, &c.data->back());
    }
  }

  return PartialTableData{std::move(result), true, ""};
}

TableColumns AdminCommandTable::getColumns() const {
  return getColumnsImpl();
}

const TableColumns& AdminCommandTable::getColumnsImpl() const {
  if (columns_.empty()) {
    columns_ = this->getFetchableColumns();
    columns_.insert(columns_.begin(),
                    {"node_id", DataType::INTEGER, "Node ID this row is for."});
  }

  return columns_;
}

PartialTableData
AdminCommandTable::transformData(std::string response_from_node) const {
  switch (type_) {
    case Type::JSON_TABLE:
      return jsonToTableData(std::move(response_from_node));
    case Type::STAT:
      return statToTableData(std::move(response_from_node));
  }

  // Should never be here, the compiler should complain if there is a missing
  // case in the above switch statement.  ld_check(false);
  return PartialTableData{folly::none, false, "UNEXPECTED"};
}

std::vector<TableData> AdminCommandTable::transformDataParallel(
    AdminCommandClient::RequestResponses& responses) {
  std::vector<TableData> outputs(responses.size());

  auto thread_func = [&](size_t from, size_t to) {
    for (size_t i = from; i < std::min(to, responses.size()); ++i) {
      if (responses[i].success) {
        PartialTableData partial_data =
            transformData(std::move(responses[i].response));
        if (partial_data.success) {
          outputs[i] = std::move(*(partial_data.data));
        } else {
          responses[i].success = false;
          responses[i].failure_reason = partial_data.failure_reason;
        }
      }
    }
  };

  const size_t num_threads = 32;
  const size_t num_per_thread = responses.size() / num_threads + 1;

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads && i * num_per_thread < responses.size();
       i++) {
    const size_t from = i * num_per_thread;
    const size_t to = from + num_per_thread;
    threads.push_back(std::thread(thread_func, from, to));
  }

  for (int i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }

  return outputs;
}

}}} // namespace facebook::logdevice::ldquery
