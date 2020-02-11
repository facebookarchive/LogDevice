/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/clients/python/logdevice_logsconfig.h"

#include <boost/python.hpp>
#include <boost/python/suite/indexing/map_indexing_suite.hpp>
#include <boost/shared_ptr.hpp>
#include <folly/String.h>

#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/include/PermissionActions.h"

using boost::python::dict;
using boost::python::object;
using boost::python::stl_input_iterator;

using namespace boost::python;
using namespace facebook::logdevice;

namespace facebook { namespace logdevice { namespace logsconfig {

constexpr char const* VALUE = "value";
constexpr char const* IS_INHERITED = "is_inherited";

/**
 * a list of all the *accepted* attribute keys, this is mainly used for
 * parsing and validating JSON configs
 */
constexpr auto logs_config_recognized_attributes = {
    REPLICATION_FACTOR,
    EXTRA_COPIES,
    SYNCED_COPIES,
    MAX_WRITES_IN_FLIGHT,
    SINGLE_WRITER,
    SYNC_REPLICATION_SCOPE,
    REPLICATE_ACROSS,
    NODESET_SIZE,
    BACKLOG,
    DELIVERY_LATENCY,
    SCD_ENABLED,
    LOCAL_SCD_ENABLED,
    WRITE_TOKEN,
    PERMISSIONS,
    ACLS,
    ACLS_SHADOW,
    STICKY_COPYSETS,
    MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED,
    SEQUENCER_AFFINITY,
    SEQUENCER_BATCHING,
    SEQUENCER_BATCHING_TIME_TRIGGER,
    SEQUENCER_BATCHING_SIZE_TRIGGER,
    SEQUENCER_BATCHING_COMPRESSION,
    SEQUENCER_BATCHING_PASSTHRU_THRESHOLD,
    SHADOW,
    TAIL_OPTIMIZED,
    EXTRAS};

static NodeLocationScope parse_location_scope_or_throw(std::string key) {
  std::transform(key.begin(), key.end(), key.begin(), ::toupper);
  NodeLocationScope e = NodeLocation::scopeNames().reverseLookup(key);
  if (e == NodeLocation::scopeNames().invalidEnum()) {
    std::string allowed_values;
    folly::join(", ",
                NodeLocation::scopeNames().begin(),
                NodeLocation::scopeNames().end(),
                allowed_values);

    throw_python_exception(PyExc_ValueError,
                           str("Invalid location scope name: '" + key +
                               "'. Allowed values:" + allowed_values + "."));
    ld_check(false);
    return NodeLocationScope::INVALID;
  }

  return e;
}

static LogAttributes::ScopeReplicationFactors
parse_replicate_across_or_throw(const dict& rf_dict) {
  LogAttributes::ScopeReplicationFactors replicate_across;

  stl_input_iterator<object> begin(rf_dict.keys()), end;
  std::for_each(begin, end, [&](object key) {
    NodeLocationScope scope =
        parse_location_scope_or_throw(extract_string(key, "location scope"));
    int rf = extract<int>(rf_dict.get(key));

    replicate_across.emplace_back(scope, rf);
  });

  return replicate_across;
}

static Compression parse_compression_or_throw(std::string key) {
  std::transform(key.begin(), key.end(), key.begin(), ::tolower);
  Compression c;
  if (parseCompression(key.c_str(), &c) == -1) {
    throw_python_exception(
        PyExc_ValueError, str("Invalid compression: '" + key + "'"));
    ld_check(false);
    return Compression::NONE;
  } else {
    return c;
  }
}

LogAttributes::PermissionsMap
parse_permissions_or_throw(dict permissions_dict) {
  if (permissions_dict.is_none()) {
    return LogAttributes::PermissionsMap();
  }
  LogAttributes::PermissionsMap permissions;
  stl_input_iterator<object> begin(permissions_dict.keys()), end;
  std::for_each(begin, end, [&](object key) {
    std::string principal = extract_string(key, "principal");
    if (!Principal::isAllowedInConfig(principal)) {
      throw_python_exception(
          PyExc_ValueError, str(principal + "is a reserved principal."));
    }
    object value = permissions_dict.get(key);
    extract<list> actions(value);
    if (!actions.check()) {
      // the value must be a list, failing.
      throw_python_exception(
          PyExc_ValueError,
          str(std::string("Invalid type for value in in '") + PERMISSIONS +
              "' the value must be a list of strings."));
    }

    std::array<bool, static_cast<int>(ACTION::MAX)> permission_array;
    permission_array.fill(false);
    stl_input_iterator<object> b(actions()), e;
    std::for_each(b, e, [&](object action) {
      std::string action_str = extract_string(action, "value in action list");
      if (action_str == "READ") {
        permission_array[static_cast<int>(ACTION::READ)] = true;
      } else if (action_str == "APPEND") {
        permission_array[static_cast<int>(ACTION::APPEND)] = true;
      } else if (action_str == "TRIM") {
        permission_array[static_cast<int>(ACTION::TRIM)] = true;
      } else {
        // something we don't expect. Fail.
        throw_python_exception(
            PyExc_ValueError,
            str(std::string("Invalid ACTION value \"") + action_str +
                "\" in '" + PERMISSIONS +
                "' allowed options are 'READ', 'APPEND', and 'TRIM'"));
      }
    });
    permissions.insert(std::make_pair(principal, permission_array));
  });
  return permissions;
}

static dict replicate_across_to_dict(
    const LogAttributes::ScopeReplicationFactors& replicate_across) {
  dict d;
  for (const auto& rf : replicate_across) {
    std::string key = NodeLocation::scopeNames()[rf.first];
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);
    d[key] = rf.second;
  }
  return d;
}

LogAttributes::ACLList parse_acls_or_throw(list acls_list) {
  if (acls_list.is_none()) {
    return LogAttributes::ACLList();
  }
  LogAttributes::ACLList acls;
  stl_input_iterator<object> begin(acls_list), end;
  std::for_each(begin, end, [&](object acl_obj) {
    std::string acl_name = extract_string(acl_obj, "ACL Name");
    acls.emplace_back(acl_name);
  });
  return acls;
}

dict permissions_to_dict(const LogAttributes::PermissionsMap& permissions) {
  dict output;
  for (const auto& kv : permissions) {
    list actions;
    std::string principal = kv.first;
    if (kv.second[static_cast<int>(ACTION::APPEND)]) {
      actions.append(str("APPEND"));
    }
    if (kv.second[static_cast<int>(ACTION::READ)]) {
      actions.append(str("READ"));
    }
    if (kv.second[static_cast<int>(ACTION::TRIM)]) {
      actions.append(str("TRIM"));
    }
    output[str(principal)] = actions;
  }
  return output;
}

list acls_to_list(const LogAttributes::ACLList& acls) {
  list output;
  for (const auto& acl : acls) {
    output.append(acl);
  }
  return output;
}

dict extras_to_dict(const LogAttributes::ExtrasMap& extras) {
  dict output;
  for (const auto& kv : extras) {
    output[str(kv.first)] = str(kv.second);
  }
  return output;
}

dict shadow_to_dict(const LogAttributes::Shadow& shadow) {
  dict output;
  output[str(SHADOW_DEST)] = str(shadow.destination());
  output[str(SHADOW_RATIO)] = shadow.ratio();
  return output;
}

template <typename T, typename K>
using AttributeConversion = K (*)(const Attribute<T>&);

template <typename T, typename K>
void add_log_attribute(const Attribute<T>& attr,
                       AttributeConversion<T, K> fun,
                       const char* name,
                       dict& obj) {
  if (attr) {
    dict output;
    output[VALUE] = fun(attr);
    output[IS_INHERITED] = attr.isInherited();
    obj[name] = output;
  }
}

dict LogAttributes_to_dict(const LogAttributes& attrs) {
  dict output;

  add_log_attribute<int, int>(attrs.replicationFactor(),
                              [](auto attr) { return attr.value(); },
                              REPLICATION_FACTOR,
                              output);

  add_log_attribute<int, int>(attrs.extraCopies(),
                              [](auto attr) { return attr.value(); },
                              EXTRA_COPIES,
                              output);

  add_log_attribute<int, int>(attrs.syncedCopies(),
                              [](auto attr) { return attr.value(); },
                              SYNCED_COPIES,
                              output);

  add_log_attribute<int, int>(attrs.maxWritesInFlight(),
                              [](auto attr) { return attr.value(); },
                              MAX_WRITES_IN_FLIGHT,
                              output);

  add_log_attribute<int, int>(attrs.maxWritesInFlight(),
                              [](auto attr) { return attr.value(); },
                              MAX_WRITES_IN_FLIGHT,
                              output);

  add_log_attribute<bool, bool>(attrs.singleWriter(),
                                [](auto attr) { return attr.value(); },
                                SINGLE_WRITER,
                                output);

  add_log_attribute<folly::Optional<std::string>, object>(
      attrs.sequencerAffinity(),
      [](auto attr) {
        return attr.value() ? object(attr.value().value()) : object();
      },
      SEQUENCER_AFFINITY,
      output);

  add_log_attribute<bool, bool>(attrs.sequencerBatching(),
                                [](auto attr) { return attr.value(); },
                                SEQUENCER_BATCHING,
                                output);

  add_log_attribute<ssize_t, ssize_t>(attrs.sequencerBatchingSizeTrigger(),
                                      [](auto attr) { return attr.value(); },
                                      SEQUENCER_BATCHING_SIZE_TRIGGER,
                                      output);

  add_log_attribute<std::chrono::milliseconds, object>(
      attrs.sequencerBatchingTimeTrigger(),
      [](auto attr) {
        return attr.value().count() ? object(attr.value().count()) : object();
      },
      SEQUENCER_BATCHING_TIME_TRIGGER,
      output);

  add_log_attribute<Compression, std::string>(
      attrs.sequencerBatchingCompression(),
      [](auto attr) { return compressionToString(attr.value()); },
      SEQUENCER_BATCHING_COMPRESSION,
      output);

  add_log_attribute<ssize_t, ssize_t>(
      attrs.sequencerBatchingPassthruThreshold(),
      [](auto attr) { return attr.value(); },
      SEQUENCER_BATCHING_PASSTHRU_THRESHOLD,
      output);

  add_log_attribute<logdevice::NodeLocationScope, std::string>(
      attrs.syncReplicationScope(),
      [](auto attr) { return NodeLocation::scopeNames()[attr.value()]; },
      SYNC_REPLICATION_SCOPE,
      output);

  add_log_attribute<LogAttributes::ScopeReplicationFactors, dict>(
      attrs.replicateAcross(),
      [](auto attr) { return replicate_across_to_dict(attr.value()); },
      REPLICATE_ACROSS,
      output);

  add_log_attribute<folly::Optional<int>, object>(
      attrs.nodeSetSize(),
      [](auto attr) {
        return attr.value() ? object(attr.value().value()) : object();
      },
      NODESET_SIZE,
      output);

  add_log_attribute<folly::Optional<std::chrono::seconds>, object>(
      attrs.backlogDuration(),
      [](auto attr) {
        return attr.value() ? object(attr.value().value().count()) : object();
      },
      BACKLOG,
      output);

  add_log_attribute<folly::Optional<std::chrono::milliseconds>, object>(
      attrs.deliveryLatency(),
      [](auto attr) {
        return attr.value() ? object(attr.value().value().count()) : object();
      },
      DELIVERY_LATENCY,
      output);

  add_log_attribute<bool, bool>(attrs.scdEnabled(),
                                [](auto attr) { return attr.value(); },
                                SCD_ENABLED,
                                output);

  add_log_attribute<bool, bool>(attrs.localScdEnabled(),
                                [](auto attr) { return attr.value(); },
                                LOCAL_SCD_ENABLED,
                                output);

  add_log_attribute<folly::Optional<std::string>, object>(
      attrs.writeToken(),
      [](auto attr) {
        return attr.value() ? object(attr.value().value()) : object();
      },
      WRITE_TOKEN,
      output);

  add_log_attribute<LogAttributes::PermissionsMap, dict>(
      attrs.permissions(),
      [](auto attr) { return permissions_to_dict(attr.value()); },
      PERMISSIONS,
      output);

  add_log_attribute<LogAttributes::ACLList, list>(
      attrs.acls(),
      [](auto attr) { return acls_to_list(attr.value()); },
      ACLS,
      output);

  add_log_attribute<LogAttributes::ACLList, list>(
      attrs.aclsShadow(),
      [](auto attr) { return acls_to_list(attr.value()); },
      ACLS_SHADOW,
      output);

  add_log_attribute<bool, bool>(attrs.stickyCopySets(),
                                [](auto attr) { return attr.value(); },
                                STICKY_COPYSETS,
                                output);

  add_log_attribute<bool, bool>(attrs.mutablePerEpochLogMetadataEnabled(),
                                [](auto attr) { return attr.value(); },
                                MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED,
                                output);

  add_log_attribute<LogAttributes::ExtrasMap, dict>(
      attrs.extras(),
      [](auto attr) { return extras_to_dict(attr.value()); },
      EXTRAS,
      output);

  add_log_attribute<LogAttributes::Shadow, dict>(
      attrs.shadow(),
      [](const auto& attr) { return shadow_to_dict(attr.value()); },
      SHADOW,
      output);

  return output;
}

LogAttributes::ExtrasMap dict_to_ExtrasMap(const dict& extras) {
  LogAttributes::ExtrasMap output;
  if (extras.is_none()) {
    return output;
  }
  stl_input_iterator<object> begin(extras.keys()), end;
  std::for_each(begin, end, [&](object key) {
    auto key_str = extract_string(key, "extra_attributes key");
    auto value = extract_string(extras.get(key), "extra_attributes value");
    output.insert(std::make_pair(key_str, value));
  });
  return output;
}

LogAttributes::Shadow dict_to_Shadow(const dict& shadow) {
  std::string destination =
      extract_string(shadow[SHADOW_DEST], "shadow destination");
  double ratio = convert_or_throw<double>(shadow[SHADOW_RATIO], "shadow ratio");
  return LogAttributes::Shadow{destination, ratio};
}

LogAttributes dict_to_LogAttributes(const dict& attrs) {
  LogAttributes log_attributes;
  // make sure we don't have attributes that we don't expect.
  stl_input_iterator<object> begin(attrs.keys()), end;
  std::for_each(begin, end, [&](object key) {
    auto key_string = extract_string(key, "attribute key");
    auto value = attrs.get(key);
    int result = -1;
    // make sure it's an attribute that we expect.
    /**
     * Note that we don't accept unknown attributes, this means that extras
     * will always be empty. This is a step towards removing this arbitrary
     * field altogether.
     */
    if (std::find(logs_config_recognized_attributes.begin(),
                  logs_config_recognized_attributes.end(),
                  key_string) == logs_config_recognized_attributes.end()) {
      std::string options;
      folly::join(", ", logs_config_recognized_attributes, options);
      throw_python_exception(
          PyExc_KeyError,
          str("Unknown log attribute '" + key_string +
              "' was passed! Valid attributes are: " + options));
    }
    if (key_string == REPLICATION_FACTOR) {
      int v = convert_or_throw<int>(value, REPLICATION_FACTOR);
      log_attributes = log_attributes.with_replicationFactor(v);
    } else if (key_string == EXTRA_COPIES) {
      int v = convert_or_throw<int>(value, EXTRA_COPIES);
      log_attributes = log_attributes.with_extraCopies(v);
    } else if (key_string == SYNCED_COPIES) {
      int v = convert_or_throw<int>(value, SYNCED_COPIES);
      log_attributes = log_attributes.with_syncedCopies(v);
    } else if (key_string == MAX_WRITES_IN_FLIGHT) {
      int v = convert_or_throw<int>(value, MAX_WRITES_IN_FLIGHT);
      log_attributes = log_attributes.with_maxWritesInFlight(v);
    } else if (key_string == SINGLE_WRITER) {
      bool v = convert_or_throw<bool>(value, SINGLE_WRITER);
      log_attributes = log_attributes.with_singleWriter(v);
    } else if (key_string == SEQUENCER_AFFINITY) {
      if (value.is_none()) {
        log_attributes = log_attributes.with_sequencerAffinity(folly::none);
      } else {
        std::string v = extract_string(value, SEQUENCER_AFFINITY);
        log_attributes = log_attributes.with_sequencerAffinity(v);
      }
    } else if (key_string == SEQUENCER_BATCHING) {
      bool v = convert_or_throw<bool>(value, SEQUENCER_BATCHING);
      log_attributes = log_attributes.with_sequencerBatching(v);
    } else if (key_string == SEQUENCER_BATCHING_SIZE_TRIGGER) {
      ssize_t v =
          convert_or_throw<ssize_t>(value, SEQUENCER_BATCHING_SIZE_TRIGGER);
      log_attributes = log_attributes.with_sequencerBatchingSizeTrigger(v);
    } else if (key_string == SEQUENCER_BATCHING_COMPRESSION) {
      std::string v = extract_string(value, SEQUENCER_BATCHING_COMPRESSION);
      Compression c = parse_compression_or_throw(v);
      log_attributes = log_attributes.with_sequencerBatchingCompression(c);
    } else if (key_string == SEQUENCER_BATCHING_PASSTHRU_THRESHOLD) {
      ssize_t v = convert_or_throw<ssize_t>(
          value, SEQUENCER_BATCHING_PASSTHRU_THRESHOLD);
      log_attributes =
          log_attributes.with_sequencerBatchingPassthruThreshold(v);

    } else if (key_string == SEQUENCER_BATCHING_TIME_TRIGGER) {
      if (value.is_none()) {
        log_attributes = log_attributes.with_sequencerBatchingTimeTrigger(
            std::chrono::milliseconds(0));
      } else {
        int v = convert_or_throw<int>(value, SEQUENCER_BATCHING_TIME_TRIGGER);
        log_attributes = log_attributes.with_sequencerBatchingTimeTrigger(
            std::chrono::milliseconds(v));
      }
    } else if (key_string == SYNC_REPLICATION_SCOPE) {
      std::string v = extract_string(value, SYNC_REPLICATION_SCOPE);
      NodeLocationScope scope = parse_location_scope_or_throw(v);
      log_attributes = log_attributes.with_syncReplicationScope(scope);
    } else if (key_string == REPLICATE_ACROSS) {
      dict v = convert_or_throw<dict>(value, REPLICATE_ACROSS);
      LogAttributes::ScopeReplicationFactors rf =
          parse_replicate_across_or_throw(v);
      log_attributes = log_attributes.with_replicateAcross(rf);
    } else if (key_string == BACKLOG) {
      if (value.is_none()) {
        log_attributes = log_attributes.with_backlogDuration(folly::none);
      } else {
        int v = convert_or_throw<int>(value, BACKLOG);
        log_attributes =
            log_attributes.with_backlogDuration(std::chrono::seconds(v));
      }
    } else if (key_string == NODESET_SIZE) {
      if (value.is_none()) {
        log_attributes = log_attributes.with_nodeSetSize(folly::none);
      } else {
        int v = convert_or_throw<int>(value, NODESET_SIZE);
        log_attributes = log_attributes.with_nodeSetSize(v);
      }
    } else if (key_string == DELIVERY_LATENCY) {
      if (value.is_none()) {
        log_attributes.with_deliveryLatency(folly::none);
      } else {
        int v = convert_or_throw<int>(value, DELIVERY_LATENCY);
        log_attributes =
            log_attributes.with_deliveryLatency(std::chrono::milliseconds(v));
      }
    } else if (key_string == SCD_ENABLED) {
      bool v = convert_or_throw<bool>(value, SCD_ENABLED);
      log_attributes = log_attributes.with_scdEnabled(v);
    } else if (key_string == LOCAL_SCD_ENABLED) {
      bool v = convert_or_throw<bool>(value, LOCAL_SCD_ENABLED);
      log_attributes = log_attributes.with_localScdEnabled(v);
    } else if (key_string == WRITE_TOKEN) {
      if (value.is_none()) {
        log_attributes = log_attributes.with_writeToken(folly::none);
      } else {
        std::string v = extract_string(value, WRITE_TOKEN);
        log_attributes = log_attributes.with_writeToken(v);
      }
    } else if (key_string == STICKY_COPYSETS) {
      bool v = convert_or_throw<bool>(value, STICKY_COPYSETS);
      log_attributes = log_attributes.with_stickyCopySets(v);
    } else if (key_string == MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED) {
      bool v =
          convert_or_throw<bool>(value, MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED);
      log_attributes = log_attributes.with_mutablePerEpochLogMetadataEnabled(v);
    }
    if (key == PERMISSIONS) {
      dict v = convert_or_throw<dict>(value, PERMISSIONS);
      log_attributes =
          log_attributes.with_permissions(parse_permissions_or_throw(v));
    }
    if (key == ACLS) {
      list v = convert_or_throw<list>(value, ACLS);
      log_attributes = log_attributes.with_acls(parse_acls_or_throw(v));
    }
    if (key == ACLS_SHADOW) {
      list v = convert_or_throw<list>(value, ACLS_SHADOW);
      log_attributes = log_attributes.with_aclsShadow(parse_acls_or_throw(v));
    }
    if (key_string == TAIL_OPTIMIZED) {
      bool v = convert_or_throw<bool>(value, TAIL_OPTIMIZED);
      log_attributes = log_attributes.with_tailOptimized(v);
    }
    if (key == EXTRAS) {
      dict v = convert_or_throw<dict>(value, EXTRAS);
      log_attributes = log_attributes.with_extras(dict_to_ExtrasMap(v));
    }
    if (key == SHADOW) {
      dict v = convert_or_throw<dict>(value, SHADOW);
      log_attributes = log_attributes.with_shadow(dict_to_Shadow(v));
    }
  });
  return log_attributes;
}

tuple get_lg_range(client::LogGroup& lg) {
  return make_tuple(lg.range().first.val(), lg.range().second.val());
}

dict get_lg_attrs(client::LogGroup& lg) {
  return LogAttributes_to_dict(lg.attrs());
}

dict get_dir_attrs(client::Directory& dir) {
  return LogAttributes_to_dict(dir.attrs());
}

ReplicationProperty replication_for_lg(client::LogGroup& lg) {
  return ReplicationProperty::fromLogAttributes(lg.attrs());
}

ReplicationProperty narrowest_replication_for_dir(client::Directory& dir) {
  ReplicationProperty narrowest;
  for (const auto& item : dir.children()) {
    narrowest =
        narrowest.narrowest(narrowest_replication_for_dir(*item.second));
  }
  for (const auto& item : dir.logs()) {
    narrowest = narrowest.narrowest(replication_for_lg(*item.second));
  }
  return narrowest;
}

dict get_narrowest_replication(client::Directory& dir) {
  dict result;
  ReplicationProperty repl = narrowest_replication_for_dir(dir);
  for (auto item : repl.getDistinctReplicationFactors()) {
    result[NodeLocation::scopeNames()[item.first]] = item.second;
  }
  return result;
}

int get_min_replication_factor(client::Directory& dir) {
  ReplicationProperty repl = narrowest_replication_for_dir(dir);
  return repl.getReplicationFactor();
}

dict get_tolerable_failure_domains(client::Directory& dir) {
  dict result;
  ReplicationProperty repl = narrowest_replication_for_dir(dir);
  NodeLocationScope biggest_scope = repl.getBiggestReplicationScope();
  result[NodeLocation::scopeNames()[biggest_scope]] =
      repl.getReplication(biggest_scope) - 1;
  return result;
}

int get_replication_factor_for_biggest_tolerable_failure_domain(
    client::Directory& dir) {
  ReplicationProperty repl = narrowest_replication_for_dir(dir);
  NodeLocationScope biggest_scope = repl.getBiggestReplicationScope();
  return repl.getReplication(biggest_scope);
}

std::string get_lg_repr(client::LogGroup& lg) {
  return "LogGroup(" + lg.getFullyQualifiedName() + ")";
}

std::string get_dir_repr(client::Directory& dir) {
  return "Directory(" + dir.getFullyQualifiedName() + ")";
}

void register_logsconfig_types() {
  class_<client::LogGroup,
         boost::shared_ptr<client::LogGroup>,
         boost::noncopyable>("LogGroup", "A LogGroup object", no_init)
      .add_property("version", &client::LogGroup::version)
      .def("__repr__", &get_lg_repr)
      .add_property("fully_qualified_name",
                    make_function(&client::LogGroup::getFullyQualifiedName,
                                  return_value_policy<copy_const_reference>()))

      .add_property("name",
                    make_function(&client::LogGroup::name,
                                  return_value_policy<copy_const_reference>()))
      .add_property("attrs", &get_lg_attrs)
      .add_property("range", &get_lg_range)
      .def("__str__",
           &client::LogGroup::getFullyQualifiedName,
           return_value_policy<copy_const_reference>());

  class_<client::DirectoryMap, boost::noncopyable>("Directories")
      .def("__getitem__", &Directories::get, return_internal_reference<>())
      .def("__len__", &Directories::len)
      .def("__contains__", &Directories::in)
      .def("keys", &Directories::keys)
      .def("items", &Directories::items)
      .def("values", &Directories::values);

  class_<client::LogGroupMap, boost::noncopyable>("LogGroups")
      .def("__getitem__", &LogGroups::get, return_internal_reference<>())
      .def("__len__", &LogGroups::len)
      .def("__contains__", &LogGroups::in)
      .def("keys", &LogGroups::keys)
      .def("items", &LogGroups::items)
      .def("values", &LogGroups::values);

  class_<client::Directory,
         boost::shared_ptr<client::Directory>,
         boost::noncopyable>(
      "Directory",
      "A Directory containing directories and/or LogGroups",
      no_init)
      .add_property("version", &client::Directory::version)
      .def("__repr__", &get_dir_repr)
      .def("get_narrowest_replication", &get_narrowest_replication)
      .def("get_min_replication_factor", &get_min_replication_factor)
      .def("get_tolerable_failure_domains", &get_tolerable_failure_domains)
      .def("get_replication_factor_for_biggest_tolerable_failure_domain",
           &get_replication_factor_for_biggest_tolerable_failure_domain)
      .add_property("fully_qualified_name",
                    make_function(&client::Directory::getFullyQualifiedName,
                                  return_value_policy<copy_const_reference>()))

      .add_property("name",
                    make_function(&client::Directory::name,
                                  return_value_policy<copy_const_reference>()))
      .add_property("attrs", &get_dir_attrs)
      .add_property("children",
                    make_function(&client::Directory::children,
                                  return_internal_reference<>()))
      .add_property("logs",
                    make_function(&client::Directory::logs,
                                  return_internal_reference<>()));
}

boost::shared_ptr<client::LogGroup> make_log_group(Client& client,
                                                   str path,
                                                   logid_t start_id,
                                                   logid_t end_id,
                                                   dict attrs,
                                                   bool mk_intermediate_dirs) {
  std::string path_str = extract_string(path, "path");
  logid_range_t range(start_id, end_id);
  LogAttributes attributes = dict_to_LogAttributes(attrs);
  std::shared_ptr<client::LogGroup> lg;
  std::string failure_reason;
  {
    gil_release_and_guard guard;
    lg = client.makeLogGroupSync(
        path_str, range, attributes, mk_intermediate_dirs, &failure_reason);
  }
  if (lg == nullptr) {
    // failed. Let's raise an exception with the error
    throw_logdevice_exception(object(failure_reason));
  }

  return make_boost_shared_ptr_from_std_shared_ptr(lg);
}

boost::shared_ptr<client::Directory> make_directory(Client& client,
                                                    str path,
                                                    bool mk_intermediate_dirs,
                                                    dict attrs) {
  std::string path_str = extract_string(path, "path");
  LogAttributes attributes = dict_to_LogAttributes(attrs);
  std::shared_ptr<client::Directory> dir;
  std::string failure_reason;
  {
    gil_release_and_guard guard;
    dir = client.makeDirectorySync(
        path_str, mk_intermediate_dirs, attributes, &failure_reason);
  }
  if (dir == nullptr) {
    // failed. Let's raise an exception with the error
    throw_logdevice_exception(object(failure_reason));
  }
  return make_boost_shared_ptr_from_std_shared_ptr(dir);
}

boost::shared_ptr<client::Directory> get_directory(Client& client, str path) {
  std::string path_str = extract_string(path, "path");
  std::shared_ptr<client::Directory> dir;
  {
    gil_release_and_guard guard;
    dir = client.getDirectorySync(path_str);
  }
  if (dir == nullptr) {
    // failed. Let's raise an exception with the error
    throw_logdevice_exception();
  }
  return make_boost_shared_ptr_from_std_shared_ptr(dir);
}

boost::shared_ptr<client::LogGroup> get_log_group_by_name(Client& client,
                                                          str path) {
  std::string path_str = extract_string(path, "path");
  std::shared_ptr<client::LogGroup> lg;
  {
    gil_release_and_guard guard;
    lg = client.getLogGroupSync(path_str);
  }
  if (lg == nullptr) {
    // failed. Let's raise an exception with the error
    throw_logdevice_exception();
  }
  return make_boost_shared_ptr_from_std_shared_ptr(lg);
}

boost::shared_ptr<client::LogGroup> get_log_group_by_id(Client& client,
                                                        logid_t id) {
  if (id.val() == 0) {
    throw_python_exception(PyExc_TypeError, "Invalid log ID");
  }

  std::shared_ptr<client::LogGroup> lg;
  {
    gil_release_and_guard guard;
    lg = client.getLogGroupByIdSync(id);
  }
  if (lg == nullptr) {
    // failed. Let's raise an exception with the error
    throw_logdevice_exception();
  }
  return make_boost_shared_ptr_from_std_shared_ptr(lg);
}

uint64_t remove_directory(Client& client, str path, bool recursive) {
  std::string path_str = extract_string(path, "path");
  uint64_t version;
  bool result;
  {
    gil_release_and_guard guard;
    result = client.removeDirectorySync(path_str, recursive, &version);
  }
  if (!result) {
    throw_logdevice_exception();
  }
  return version;
}

uint64_t remove_log_group(Client& client, str path) {
  std::string path_str = extract_string(path, "path");
  bool result = false;
  uint64_t version;
  {
    gil_release_and_guard guard;
    result = client.removeLogGroupSync(path_str, &version);
  }
  if (!result) {
    throw_logdevice_exception();
  }
  return version;
}

uint64_t rename_path(Client& client, str old_name, str new_name) {
  std::string old_path_str = extract_string(old_name, "old_name");
  std::string new_path_str = extract_string(new_name, "new_name");
  uint64_t version;
  std::string failure_reason;
  bool result;
  {
    gil_release_and_guard guard;
    result = client.renameSync(
        old_path_str, new_path_str, &version, &failure_reason);
  }
  if (!result) {
    throw_logdevice_exception(object(failure_reason));
  }
  return version;
}

uint64_t set_attributes(Client& client, str path, dict attrs) {
  std::string path_str = extract_string(path, "path");
  std::string failure_reason;
  LogAttributes attributes = dict_to_LogAttributes(attrs);
  uint64_t version;
  bool result;
  {
    gil_release_and_guard guard;
    result = client.setAttributesSync(
        path_str, attributes, &version, &failure_reason);
  }
  if (!result) {
    throw_logdevice_exception(object(failure_reason));
  }
  return version;
}

uint64_t set_log_group_range(Client& client,
                             str path,
                             logid_t from,
                             logid_t to) {
  std::string path_str = extract_string(path, "path");
  std::string failure_reason;
  logid_range_t range{from, to};
  uint64_t version;
  bool result;
  {
    gil_release_and_guard guard;
    result =
        client.setLogGroupRangeSync(path_str, range, &version, &failure_reason);
  }
  if (!result) {
    throw_logdevice_exception(object(failure_reason));
  }
  return version;
}

bool sync_logsconfig_version(Client& client, uint64_t version) {
  gil_release_and_guard guard;
  return client.syncLogsConfigVersion(version);
}
}}} // namespace facebook::logdevice::logsconfig
