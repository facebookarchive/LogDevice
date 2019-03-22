/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/python.hpp>

#include "logdevice/clients/python/util/util.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/LogsConfigTypes.h"
#include "logdevice/include/debug.h"

namespace facebook { namespace logdevice { namespace logsconfig {

/*
 * What we expect is a dictionary mapping a principal to the allowed actions
 * e.g., {"allPass": ["READ", "APPEND", "TRIM"]}
 */
LogAttributes::PermissionsMap
parse_permissions_or_throw(boost::python::dict value);

boost::python::dict
permissions_to_dict(const LogAttributes::PermissionsMap& permissions);

bool parse_location_scope_or_throw(std::string key, NodeLocationScope& value);

void register_logsconfig_types();

template <typename T>
boost::python::object ptr_to_python(T* t) {
  // Use the manage_new_object generator to transfer ownership to Python.
  typename boost::python::reference_existing_object::apply<T*>::type converter;

  boost::python::handle<> h(converter(t));

  return boost::python::object(h);
}

template <typename MapType>
struct Container {
  typedef typename MapType::mapped_type::element_type V_RAW;
  static V_RAW* get(const MapType& map, const std::string& key) {
    auto search = map.find(key);
    if (search != map.end()) {
      return search->second.get(); //;
    }
    throw_python_exception(
        PyExc_KeyError, boost::python::str("key '" + key + "' was not found."));
    return nullptr;
  }
  static size_t len(const MapType& map) {
    return map.size();
  }
  static bool in(const MapType& map, const std::string& key) {
    return map.find(key) != map.end();
  }
  static boost::python::list keys(const MapType& map) {
    boost::python::list t;
    for (const auto& it : map) {
      t.append(it.first);
    }
    return t;
  }
  static boost::python::list values(const MapType& map) {
    boost::python::list t;
    for (const auto& it : map) {
      t.append(boost::python::ptr(it.second.get()));
    }
    return t;
  }
  static boost::python::list items(const MapType& map) {
    boost::python::list t;
    for (const auto& it : map) {
      t.append(boost::python::make_tuple(
          boost::python::str(it.first), ptr_to_python(it.second.get())));
    }
    return t;
  }
};

using Directories = Container<client::DirectoryMap>;
using LogGroups = Container<client::LogGroupMap>;

boost::shared_ptr<client::LogGroup> make_log_group(Client& client,
                                                   boost::python::str path,
                                                   logid_t start_id,
                                                   logid_t end_id,
                                                   boost::python::dict attrs,
                                                   bool mk_intermediate_dirs);

boost::shared_ptr<client::Directory> make_directory(Client& client,
                                                    boost::python::str path,
                                                    bool mk_intermediate_dirs,
                                                    boost::python::dict attrs);

boost::shared_ptr<client::Directory> get_directory(Client& client,
                                                   boost::python::str path);

boost::shared_ptr<client::LogGroup>
get_log_group_by_name(Client& client, boost::python::str path);

boost::shared_ptr<client::LogGroup> get_log_group_by_id(Client& client,
                                                        logid_t id);

uint64_t remove_directory(Client& client,
                          boost::python::str path,
                          bool recursive);
uint64_t remove_log_group(Client& client, boost::python::str path);
uint64_t rename_path(Client& client,
                     boost::python::str old_name,
                     boost::python::str new_name);
uint64_t set_attributes(Client& client,
                        boost::python::str path,
                        boost::python::dict attrs);
uint64_t set_log_group_range(Client& client,
                             boost::python::str path,
                             logid_t from,
                             logid_t to);
bool sync_logsconfig_version(Client& client, uint64_t version);
}}} // namespace facebook::logdevice::logsconfig
