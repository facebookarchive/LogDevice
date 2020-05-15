#include "LogTreeBase.h"
#include <folly/Conv.h>
#include <folly/json.h>
#include "../Utils.h"
#include "logdevice/common/configuration/ReplicationProperty.h"

namespace facebook::logdevice::ldquery::tables {

TableColumns LogTreeBase::getAttributeColumns() {
  return {
      {"name", DataType::TEXT, "Name of the log group or the directory."},
      {"logid_lo", DataType::LOGID,
       "Defines the lower bound (inclusive) of the range of log ids in this "
       "log group."},
      {"logid_hi",
       DataType::LOGID,
       "Defines the upper bound (inclusive) of the range of log ids in this "
       "log group."},
      {"replication_property",
       DataType::TEXT,
       "Replication property configured for this log group."},
      {"synced_copies",
       DataType::INTEGER,
       "Number of copies that must be acknowledged by storage nodes are synced "
       "to disk before the record is acknowledged to the client as fully "
       "appended."},
      {"max_writes_in_flight",
       DataType::INTEGER,
       "The largest number of records not released for delivery that the "
       "sequencer allows to be outstanding."},
      {"backlog_duration_sec",
       DataType::INTEGER,
       "Time-based retention of records of logs in that log group.  If null or "
       "zero, this log group does not use time-based retention."},
      {"storage_set_size",
       DataType::INTEGER,
       "Size of the storage set for logs in that log group.  The storage set "
       "is the set of shards that may hold data for a log."},
      {"delivery_latency",
       DataType::INTEGER,
       "For logs in that log group, maximum amount of time that we can delay "
       "delivery of newly written records.  This option increases delivery "
       "latency but improves server and client performance."},
      {"scd_enabled",
       DataType::INTEGER,
       "Indicates whether the Single Copy Delivery optimization is enabled for "
       "this log group.  This efficiency optimization allows only one copy of "
       "each record to be served to readers."},
      {"parent_dir", DataType::TEXT, "Name of the parent directory."},
      {"custom_fields",
       DataType::TEXT,
       "Custom text field provided by the user."},
  };
}

void LogTreeBase::populateAttributeData(TableData& tab_data, const facebook::logdevice::logsconfig::LogAttributes& log_attrs) {
    ColumnValue custom;
    if (log_attrs.extras().hasValue() && (*log_attrs.extras()).size() > 0) {
      folly::dynamic extra_attrs = folly::dynamic::object;
      for (const auto& it : log_attrs.extras().value()) {
        extra_attrs[it.first] = it.second;
      }
      custom = folly::toJson(extra_attrs);
    }

    ReplicationProperty rprop =
        ReplicationProperty::fromLogAttributes(log_attrs);
    // This should remain the first ColumnValue as expected by the code below.
    tab_data.cols["replication_property"].push_back(rprop.toString());
    tab_data.cols["synced_copies"].push_back(s(*log_attrs.syncedCopies()));
    tab_data.cols["max_writes_in_flight"].push_back(
        s(*log_attrs.maxWritesInFlight()));
    tab_data.cols["backlog_duration_sec"].push_back(
        s(*log_attrs.backlogDuration()));
    tab_data.cols["storage_set_size"].push_back(s(*log_attrs.nodeSetSize()));
    tab_data.cols["delivery_latency"].push_back(s(*log_attrs.deliveryLatency()));
    tab_data.cols["scd_enabled"].push_back(s(*log_attrs.scdEnabled()));
    tab_data.cols["custom_fields"].push_back(custom);
}

}
