/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBCustomiser.h"

#include <boost/filesystem.hpp>

namespace facebook { namespace logdevice {

namespace fs = boost::filesystem;

RocksDBCustomiser* RocksDBCustomiser::defaultInstance() {
  static RocksDBCustomiser* c = new RocksDBCustomiser(); // leak it
  return c;
}

bool RocksDBCustomiser::isDBLocal() const {
  return true;
}

// Returns 1/0 if base_path looks like it contains an existing database, -1 on
// error.
static int databaseExists(const std::string& base_path) {
  boost::system::error_code code;

  bool exists = fs::exists(base_path, code);
  if (code.value() != boost::system::errc::success &&
      code.value() != boost::system::errc::no_such_file_or_directory) {
    ld_error("Error checking if path \"%s\" exists: %s",
             base_path.c_str(),
             code.message().c_str());
    return -1;
  }

  if (!exists) {
    return 0;
  }

  bool isdir = fs::is_directory(base_path, code);
  if (code.value() != boost::system::errc::success &&
      code.value() != boost::system::errc::not_a_directory) {
    ld_error("Error checking if path \"%s\" is a directory: %s",
             base_path.c_str(),
             code.message().c_str());
    return -1;
  }

  if (!isdir) {
    ld_error("Path \"%s\" exists but is not a directory, cannot open local "
             "log store",
             base_path.c_str());
    return -1;
  }

  bool isempty = fs::is_empty(base_path, code);
  if (code.value() != boost::system::errc::success) {
    ld_error("Error checking if path \"%s\" is empty: %s",
             base_path.c_str(),
             code.message().c_str());
    return -1;
  }

  return !isempty;
}

// Called when the database directory exists and contains a NSHARDS file.
// Checks that the file contains the correct number of shards.
static int checkNShardsFile(const std::string& base_path,
                            const shard_size_t nshards_expected) {
  fs::path nshards_path = fs::path(base_path) / fs::path("NSHARDS");
  FILE* fp = std::fopen(nshards_path.c_str(), "r");
  if (fp == nullptr) {
    ld_error("Database directory \"%s\" exists but there was an error opening "
             "the NSHARDS file, errno=%d (%s).  To create a new sharded "
             "database, please delete the directory and try again.",
             base_path.c_str(),
             errno,
             strerror(errno));
    return -1;
  }

  SCOPE_EXIT {
    std::fclose(fp);
  };

  shard_size_t nshards_read;
  if (std::fscanf(fp, "%hd", &nshards_read) != 1) {
    ld_error("Error reading file \"%s\"", nshards_path.c_str());
    return -1;
  }

  if (nshards_read != nshards_expected) {
    ld_error("Tried to open existing sharded database \"%s\" with a different "
             "number of shards (expected %d, found %d)",
             base_path.c_str(),
             nshards_expected,
             nshards_read);
    return -1;
  }

  return 0;
}

// Creates the base directory (not shard directories) and NSHARDS file in it.
static int createNewShardedDB(const std::string& base_path,
                              shard_size_t nshards) {
  boost::system::error_code code;
  fs::create_directories(base_path, code);
  if (code.value() != boost::system::errc::success) {
    ld_error("Error creating directory \"%s\": %s",
             base_path.c_str(),
             code.message().c_str());
    return -1;
  }

  fs::path nshards_path = fs::path(base_path) / fs::path("NSHARDS");

  FILE* fp = std::fopen(nshards_path.c_str(), "w");
  if (fp == nullptr) {
    ld_error("Failed to open \"%s\" for writing, errno=%d (%s)",
             nshards_path.c_str(),
             errno,
             strerror(errno));
    return -1;
  }

  SCOPE_EXIT {
    std::fclose(fp);
  };

  if (std::fprintf(fp, "%d", nshards) < 0) {
    ld_error("Error writing to \"%s\"", nshards_path.c_str());
    return -1;
  }

  return 0;
}

bool RocksDBCustomiser::validateAndOverrideBasePath(
    std::string base_path,
    shard_size_t num_shards,
    const RocksDBSettings& db_settings,
    std::string* out_new_base_path,
    std::vector<shard_index_t>* out_disabled_shards) {
  ld_check(isDBLocal());

  int rv = databaseExists(base_path);
  if (rv < 0) {
    return false;
  }

  // This value should have been validated by the Configuration module.
  ld_check(num_shards > 0 && num_shards <= MAX_SHARDS);

  if (rv) {
    // Database exists, check that the number of shards matches.  We do not
    // want to start up otherwise.
    if (checkNShardsFile(base_path, num_shards) < 0) {
      return false;
    }
  } else {
    if (!db_settings.auto_create_shards) {
      ld_error("Auto creation of shards & directories is not enabled and "
               "they do not exist");
      return false;
    }

    if (createNewShardedDB(base_path, num_shards) != 0) {
      return false;
    }
  }

  std::vector<boost::filesystem::path> paths(num_shards);
  std::vector<shard_index_t> missing;
  std::vector<shard_index_t> disabled;

  for (shard_index_t shard_idx = 0; shard_idx < num_shards; ++shard_idx) {
    fs::path path =
        fs::path(base_path) / fs::path("shard" + std::to_string(shard_idx));
    paths[shard_idx] = path;

    boost::system::error_code code;
    auto s = fs::status(path, code);
    if (code.value() != boost::system::errc::success &&
        code.value() != boost::system::errc::no_such_file_or_directory) {
      ld_error("Error while checking for existence of %s: %s. "
               "Not opening shard %d",
               path.string().c_str(),
               code.message().c_str(),
               shard_idx);
      disabled.push_back(shard_idx);
      continue;
    }

    if (!fs::exists(s)) {
      // Shard directory doesn't exist. RocksDB will create it.
      missing.push_back(shard_idx);
      continue;
    }

    if (!fs::is_directory(s)) {
      ld_info("%s exists but is not a directory; not opening shard %d",
              path.string().c_str(),
              shard_idx);
      disabled.push_back(shard_idx);
      continue;
    }

    auto disable_marker = path / fs::path("LOGDEVICE_DISABLED");
    bool marker_present = fs::exists(disable_marker, code);
    if (code.value() != boost::system::errc::success &&
        code.value() != boost::system::errc::no_such_file_or_directory) {
      ld_error("Error while checking for existence of %s: %s. "
               "Not opening shard %d",
               disable_marker.string().c_str(),
               code.message().c_str(),
               shard_idx);
      disabled.push_back(shard_idx);
      continue;
    }

    if (marker_present) {
      ld_info("Found %s, not opening shard %d",
              disable_marker.string().c_str(),
              shard_idx);
      disabled.push_back(shard_idx);
      continue;
    }
  }

  // Be overly pedantic/paranoid.
  if (!missing.empty() && missing.size() + disabled.size() != paths.size()) {
    ld_error("Some shard directories exist but others don't (e.g. %s). "
             "Suspicious; refusing to start.",
             paths[missing[0]].c_str());
    return false;
  }
  // No need to create missing directories - rocksdb will do it.

  *out_disabled_shards = std::move(disabled);
  return true;
}

rocksdb::Env* RocksDBCustomiser::getEnv() {
  return rocksdb::Env::Default();
}

rocksdb::Status RocksDBCustomiser::openDB(
    const rocksdb::DBOptions& db_options,
    const std::string& name, // path, from getShardPaths()
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles,
    rocksdb::DB** dbptr) {
  return rocksdb::DB::Open(db_options, name, column_families, handles, dbptr);
}

rocksdb::Status RocksDBCustomiser::openReadOnlyDB(
    const rocksdb::DBOptions& db_options,
    const std::string& name,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles,
    rocksdb::DB** dbptr,
    bool error_if_log_file_exist) {
  return rocksdb::DB::OpenForReadOnly(db_options,
                                      name,
                                      column_families,
                                      handles,
                                      dbptr,
                                      error_if_log_file_exist);
}

}} // namespace facebook::logdevice
