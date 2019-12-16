/**
 * Copyright (c) 2019-present, CCBFT, Inc. and its affiliates.
 * All rights reserved.
 */

#include <chrono>

#include <boost/python.hpp>
#include <boost/python/suite/indexing/map_indexing_suite.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <cstring>

#include "logdevice/clients/python/util/util.h"
#include "logdevice/common/debug.h"
#include "logdevice/ops/ldquery/Errors.h"
#include "logdevice/ops/ldquery/LDQuery.h"
#include "logdevice/ops/ldquery/TableRegistry.h"
#include "logdevice/ops/ldquery/Utils.h"
#include "logdevice/clients/c/ld_c_query.h"

using namespace boost::python;
using namespace facebook::logdevice;
using namespace facebook::logdevice::ldquery;
using namespace facebook::logdevice::dbg;


using LDQuerySPtr = std::shared_ptr<LDQuery>;

#define DEREF_LDQ(A) DEREF_AS(LDQuerySPtr, A)


ld_err ldq_make_query_engine(const char* config,
                        int command_timeout_sec,
                        bool use_ssl,
                        PLDQuerySPtr* ppldquery_sp) {
  std::chrono::milliseconds cmd_timeout = std::chrono::milliseconds::max();
  if (command_timeout_sec > 0) {
    cmd_timeout = std::chrono::milliseconds{command_timeout_sec * 1000};
  }
  LDQuery* pldquery = new LDQuery(std::string(config), cmd_timeout, use_ssl);
  *ppldquery_sp = (PLDQuerySPtr)new std::shared_ptr<LDQuery>(pldquery);
  return LD_ERR_OK;
}


ld_err ldq_free_query_engine(PLDQuerySPtr ldquery) {
  delete (std::shared_ptr<LDQuery>*)ldquery;
  return LD_ERR_OK;
}

ld_err ldq_do_query(PLDQuerySPtr pldquery_sp,
                    const char* query,
                    PQueryResults* ppquery_results) {
  std::string query_stmt(query);
  LDQuery::QueryResults res;
  {
    gil_release_and_guard guard;
    res = DEREF_LDQ(pldquery_sp)->query(std::move(query_stmt));
  }

  *ppquery_results = (PQueryResults)(new LDQuery::QueryResults(std::move(res)));

  return LD_ERR_OK;
}

bool ldq_tablecolumn_eq(PCTableColumn lhs, PCTableColumn rhs) {
  const TableColumn* llhs = (const TableColumn*)lhs;
  const TableColumn* rrhs = (const TableColumn*)rhs;

  return llhs->name == rrhs->name && (llhs->type == rrhs->type);
}

bool ldq_tablemetadata_eq(PCTable lhs, PCTable rhs) {
  const TableMetadata* llhs = (const TableMetadata*)llhs;
  const TableMetadata* rrhs = (const TableMetadata*)rrhs;
  return (llhs->name == rrhs->name)
    && (llhs->description == rrhs->description)
    && (llhs->columns == rrhs->columns);
}

bool ldq_query_result_eq(PQueryResult lhs, PQueryResult rhs) {
  auto llhs = (LDQuery::QueryResult*)lhs;
  auto rrhs = (LDQuery::QueryResult*)rhs;
  return *llhs == *rrhs;
}

void ldq_set_pretty_output(PLDQuerySPtr pldquery_sp, bool val) {
  DEREF_LDQ(pldquery_sp)->setPrettyOutput(val);
}

bool ldq_get_pretty_output(PLDQuerySPtr pldquery_sp) {
  return DEREF_LDQ(pldquery_sp)->getPrettyOutput();
}

void ldq_set_cache_ttl(PLDQuerySPtr pldquery_sp, long val) {
  DEREF_LDQ(pldquery_sp)->setCacheTTL(std::chrono::seconds(val));
}

long ldq_get_cache_ttl(PLDQuerySPtr pldquery_sp) {
  return std::chrono::duration_cast<std::chrono::seconds>(
    DEREF_LDQ(pldquery_sp)->getCacheTTL()).count();
}

void ldq_enable_server_side_filtering(PLDQuerySPtr pldquery_sp, bool val) {
  DEREF_LDQ(pldquery_sp)->enableServerSideFiltering(val);
}

bool ldq_server_side_filtering_enabled(PLDQuerySPtr pldquery_sp) {
  return DEREF_LDQ(pldquery_sp)->serverSideFilteringEnabled();
}


size_t ldq_rows_size(PRows rows) {
  return ((std::vector<LDQuery::Rows>*)rows)->size();
}
PRow ldq_rows_at(PRows rows, size_t index) {
  return (PRow)(&((std::vector<LDQuery::Rows>*)rows)->at(index));
}
char* ldq_row_at(PRow prow, uint64_t index) {
  auto row = (std::vector<std::string>*)prow;
  return strdup(row->at(index).c_str());
}

PHeaders ldq_queryresult_headers(PQueryResult query_result) {
  return (PHeaders)(&(((LDQuery::QueryResult*)query_result)->headers));
}
size_t ldq_headers_size(PHeaders headers) {
  return ((LDQuery::ColumnNames*)headers)->size();
}
char* ldq_headers_at(PHeaders pheaders, size_t index) {
  auto headers = (LDQuery::ColumnNames*)pheaders;
  return strdup(headers->at(index).c_str());
}
PRows ldq_queryresult_rows(PQueryResult pquery_result) {
  return (PRows)(&((LDQuery::QueryResult*)pquery_result)->rows);
}

PUIntVec ldq_queryresult_cols_max_size(PQueryResult pquery_result) {
  return (PUIntVec)(&((LDQuery::QueryResult*)pquery_result)->cols_max_size);
}

// TODO shall the return result rely on the lifecycle of pquery_result
PActiveQueryMetaData ldq_queryresult_metadata(PQueryResult pquery_result) {
  return (PActiveQueryMetaData)
    (&((LDQuery::QueryResult*)pquery_result)->metadata);
}

char* ldq_failed_node_details_address(PFailedNodeDetails details) {
  auto det = (FailedNodeDetails*)details;
  return strdup(det->address.c_str());
}
char* ldq_failed_node_details_failure_reason(PFailedNodeDetails details) {
  auto det = (FailedNodeDetails*)details;
  return strdup(det->failure_reason.c_str());
}

char* ldq_failed_node_details_to_string(PFailedNodeDetails details) {
  auto det = (FailedNodeDetails*)details;
  return strdup(det->toString().c_str());
}

size_t ldq_failed_nodes_size(PFailedNodes failed_nodes) {
  return ((std::map<int, FailedNodeDetails>*)failed_nodes)->size();
}

PFailedNodes ldq_active_query_metadata_failures(PActiveQueryMetaData md) {
  return (PFailedNodes)(&((ActiveQueryMetadata*)md)->failures);
}

uint64_t ldq_active_query_metadata_contacted_nodes(PActiveQueryMetaData md) {
  return ((ActiveQueryMetadata*)md)->contacted_nodes;
}
uint64_t ldq_active_query_metadata_latency(PActiveQueryMetaData md) {
  return ((ActiveQueryMetadata*)md)->latency;
}
bool ldq_active_query_metadata_success(PActiveQueryMetaData md) {
  return ((ActiveQueryMetadata*)md)->success();
}

char* ldq_table_column_name(PTableColumn col) {
  auto co = (TableColumn*)col;
  return strdup(co->name.c_str());
}
LDQ_DataType ldq_table_column_type(PTableColumn col) {
  return (LDQ_DataType)(((TableColumn*)col)->type);
}
char* ldq_table_column_desc(PTableColumn col) {
  auto co = (TableColumn*)col;
  return strdup(co->description.c_str());
}

char* ldq_table_name(PTable table) {
  auto tab = (TableMetadata*)table;
  return strdup(tab->name.c_str());
}

char* ldq_table_desc(PTable table) {
  auto tab = (TableMetadata*)table;
  return strdup(tab->description.c_str());
}

PTableColumns ldq_table_columns(PTable table) {
  auto tab = (TableMetadata*)table;
  return (PTableColumns)(&(tab->columns));
}

size_t ldq_table_columns_size(PTableColumns cols) {
  auto tabcols = (TableColumns*)cols;
  return tabcols->size();
}

PTableColumn ldq_table_cloumns_at(PTable table, size_t index) {
  auto tab = (TableMetadata*)table;
  return (PTableColumn)(&(tab->columns.at(index)));
}

size_t ldq_tables_size(PTables tables) {
  auto tabs = (std::vector<TableMetadata>*)tables;
  return tabs->size();
}
PTable ldq_tables_at(PTables tables, size_t index) {
  auto tabs = (std::vector<TableMetadata>*)tables;
  return (PTable)(&(tabs->at(index)));
}
