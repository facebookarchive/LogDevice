/**
 * Copyright (c) 2019-present, CCBFT, Inc. and its affiliates.
 * All rights reserved.
 */

#pragma once

#include "ld_c_common.h"

#ifdef __cplusplus
extern "C" {
#endif

LD_C_DECLARE_HANDLE(LDQuery)
LD_C_DECLARE_HANDLE_SHARED_PTR(LDQuery)
LD_C_DECLARE_HANDLE(QueryResults)
LD_C_DECLARE_HANDLE(QueryResult)

// std::vector<TableMetaData>
LD_C_DECLARE_HANDLE(Tables)
// TableMetadata
LD_C_DECLARE_HANDLE(Table)
// std::vector<TableColumn>
LD_C_DECLARE_HANDLE(TableColumns)
LD_C_DECLARE_HANDLE(TableColumn)

// LDQuery::Rows == std::vector<Row>
LD_C_DECLARE_HANDLE(Rows)
// LDQuery::Row == std::vector<std::string>
LD_C_DECLARE_HANDLE(Row)
// LDQuery::ColumnNames => std::vector<std::string>
LD_C_DECLARE_HANDLE(Headers)
// struct ActiveQueryMetadata
LD_C_DECLARE_HANDLE(ActiveQueryMetaData)
// struct FailedNodeDetails
LD_C_DECLARE_HANDLE(FailedNodeDetails)

// std::map<int, FailedNodedetails>
LD_C_DECLARE_HANDLE(FailedNodes)
// // std::map<int, FailedNodedetails>
// LD_C_DECLARE_HANDLE(Failures)

enum LDQ_DataType { INTEGER, REAL, TEXT, BIGINT, LSN, BOOL, LOGID, TIME };

ld_err ldq_make_query_engine(const char* config,
                             int command_timeout_sec,
                             bool use_ssl,
                             PLDQuerySPtr* ppldquery_sp);

ld_err ldq_free_ldquery_engine(PLDQuerySPtr ldquery);

ld_err ldq_do_query(PLDQuerySPtr pldquery_sp,
                    const char* query,
                    PQueryResults* ppquery_results);
PTables ldq_get_tables(PLDQuery ldquery);

bool ldq_tablecolumn_eq(PCTableColumn lhs, PCTableColumn rhs);
bool ldq_tablemetadata_eq(PCTable lhs, PCTable rhs);
// bool ldq_query_result_eq(PCQueryResult lhs, PCQueryResult rhs);

void ldq_set_pretty_output(PLDQuery ldquery, bool val);
bool ldq_get_pretty_output(PLDQuery ldquery);
bool ldq_set_cache_ttl(PLDQuery ldquery, long val);
long ldq_get_cache_ttl(PLDQuery ldquery);
long ldq_enable_server_side_filtering(PLDQuery ldquery, bool val);
bool ldq_server_side_filtering_enabled(PLDQuery ldquery);

size_t ldq_rows_size(PRows rows);
PRow ldq_rows_at(PRows rows, size_t index);
char* ldq_row_at(PRow row, uint64_t index);

PHeaders ldq_queryresult_headers(PQueryResult query_result);
size_t ldq_headers_size(PHeaders headers);
char* ldq_headers_at(PHeaders headers, uint64_t index);
PRows ldq_queryresult_rows(PQueryResult query_result);

PUIntVec ldq_queryresult_cols_max_size(PQueryResult query_result);

PActiveQueryMetaData ldq_queryresult_metadata(PQueryResult query_result);

char* ldq_failed_node_details_address(PFailedNodeDetails details);
char* ldq_failed_node_details_failure_reason(PFailedNodeDetails details);
char* ldq_failed_node_details_to_string(PFailedNodeDetails details);


size_t ldq_failed_nodes_size(PFailedNodes failed_nodes);

PFailedNodes ldq_active_query_metadata_failures(PActiveQueryMetaData md);
uint64_t ldq_active_query_metadata_contacted_nodes(PActiveQueryMetaData md);
uint64_t ldq_active_query_metadata_latency(PActiveQueryMetaData md);
bool ldq_active_query_metadata_success(PActiveQueryMetaData md);

char* ldq_table_column_name(PTableColumn col);
LDQ_DataType ldq_table_column_type(PTableColumn col);
char* ldq_table_column_desc(PTableColumn col);

char* ldq_table_name(PTable table);
char* ldq_table_desc(PTable table);
PTableColumns ldq_table_columns(PTable table);
size_t ldq_table_columns_size(PTableColumns cols);
PTableColumn ldq_table_cloumns_at(PTable table, uint64_t index);

size_t ldq_tables_size(PTables tables);
PTable ldq_tables_at(PTables tables, uint64_t index);


#ifdef __cplusplus
}
#endif
