/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/common/if/common.thrift"

namespace cpp2 facebook.logdevice.logsconfig
namespace py3 logdevice

typedef byte (cpp2.type = "std::uint8_t") u8
typedef i16 (cpp2.type = "std::uint16_t") u16
typedef i32 (cpp2.type = "std::uint32_t") u32
typedef i64 (cpp2.type = "std::uint64_t") u64

struct NoValueSet {}

/**
 * A union of all possible data types that can be used a value for an attribute
 * on a log-group or a directory.
 */
union AnyValue {
  1: NoValueSet unset_;
  2: string str_;
  3: i32 int_;
  4: u8 u8_;
  5: u64 u64_;
  6: i64 long_;
  7: bool bool_;
}

/**
 * The set of actions as defined in (include/PermissionActions.h) allowed on a
 * log-group or a directory for a given pricipal.
 */
struct Permission {
  1: string principal;
  2: list<u8> actions;
}

struct Shadow {
  1: string destination;
  2: double ratio;
}

/**
 * An arbitrary key-value string pair that can associated to any log-group or
 * directory. LogDevice itself doesn't care about the value but this can useful
 * for users of the LogsConfig API to store metadata information.
 */
struct ExtraAttr {
  1: string key;
  2: string value;
}

/**
 * A single named attribute, if inherited is set this means that the value is
 * copied over from the parent node in the tree.
 */
struct LogAttr {
  1: string name;
  2: bool inherited;
  3: AnyValue value;
}

/**
 * This is the set of attributes that can be set to a log-group or directory.
 * See (logdevice/include/LogAttributes.h) for the full set of attributes.
 */
struct LogAttrs {
  1: list<LogAttr> attributes;
  2: list<Permission> permissions;
  3: list<ExtraAttr> extras;
  4: common.ReplicationProperty replicate_across;
  5: list<string> acls;
  6: list<string> acls_shadow;
  7: Shadow shadow;
}


/**
 * Defines a range of log IDs. Both from and to are included in the range
 * (inclusive range).
 */
struct LogRange {
  1: u64 from;
  2: u64 to;
}

struct LogGroup {
  1: string name;
  2: LogRange range;
  4: LogAttrs attrs;
}

struct Directory {
  1: string name;
  2: LogAttrs attrs;
  3: list<LogGroup> log_groups;
  4: list<Directory> children;
}

struct LogsConfig {
  1: u64 version;
  2: Directory root_dir;
}

/**
 * Following structure is only used for the Client / Messages. It associates a
 * log-group with a specific path in the tree.
 */
struct LogGroupInDirectory {
  1: LogGroup log_group;
  2: string parent_path;
}


// DELTA LOG ENTRIES
enum ConflictResolutionMode {
  STRICT = 0, /** CURRENTLY NOT USED */
  AUTO = 1, /** Optimistic conflict resolution algorithm is used */
}

/**
 * This header is set for every delta in the delta log.
 */
struct Header {
  1: u64 base_lsn;
  2: ConflictResolutionMode resolution_mode;
}

/**
 * A union of all possible data types that can be used a payload for the deltas
 * in the delta log.
 */
union DeltaPayload {
  1: MkDirDelta mkdir;
  2: RmDelta rm;
  3: RenameDelta rename;
  4: SetAttrsDelta set_attrs;
  5: SetLogRangeDelta set_log_range;
  6: MkLogDelta mklog;
  7: BatchDelta batch;
  8: SetTreeDelta set_tree;
}

/**
 * Structure of a delta in the delta log, it has a header and payload.
 */
struct Delta {
  1: Header header;
  2: DeltaPayload payload;
}

/**
 * A delta/message to create a new directory.
 */
struct MkDirDelta {
  1: string name;
  2: bool recursive = false;
  3: LogAttrs attrs;
}

/**
 * This is a special delta used to discard anything before
 * it and sets the tree to the supplied one. This is generally used to restore a
 * tree from a backup.
 */
struct SetTreeDelta {
  1: LogsConfig tree;
}

/**
 * A delta/message to remove a directory or log-group.
 */
struct RmDelta {
  1: string name;
  /**
   * The removal will fail if we are trying to remove a log-group and this field
   * is set to true.
   */
  2: bool validate_is_dir;
  3: bool recursive = false;
}

/**
 * A delta/message to remove a directory or log-group.
 */
struct RenameDelta {
  1: string from_path;
  2: string to_path;
}

/**
 * A delta/message to update attributes of a directory or log-group.
 */
struct SetAttrsDelta {
  1: string name;
  2: LogAttrs attrs;
}

/**
 * A delta/message to update the log ID range of a log-group.
 */
struct SetLogRangeDelta {
  1: string name;
  2: LogRange range;
}

/**
 * A delta/message to create a new log-group.
 */
struct MkLogDelta {
  1:  string name;
  2:  LogRange range;
  3:  bool mk_intermediate_paths;
  4:  LogAttrs attrs;
}

/**
 * A set of deltas that can be applied in batch.
 */
struct BatchDelta {
  /**
   * All Headers of the deltas are IGNORED. Only the header from the root delta
   * is respected.
   */
  1: list<Delta> operations;
}
