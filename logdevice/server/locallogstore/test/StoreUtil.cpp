/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/test/StoreUtil.h"

#include <deque>
#include <iterator>
#include <vector>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/server/locallogstore/WriteOps.h"

namespace facebook { namespace logdevice {

void store_fill(LocalLogStore& store,
                const std::vector<TestRecord>& data,
                folly::Optional<lsn_t> block_starting_lsn) {
  std::deque<std::string> header_bufs;
  std::deque<std::string> copyset_bufs;
  std::deque<PutWriteOp> ops;
  std::vector<const WriteOp*> op_ptrs;

  for (const TestRecord& tr : data) {
    header_bufs.emplace_back();
    std::string* buf = &header_bufs.back();
    Slice record_header = LocalLogStoreRecordFormat::formRecordHeader(
        tr.timestamp_.count(),
        tr.last_known_good_,
        tr.flags_,
        tr.wave_,
        folly::Range<const ShardID*>(tr.copyset_.data(), tr.copyset_.size()),
        tr.offsets_within_epoch_,
        tr.optional_keys_,
        buf);

    Slice payload_slice =
        tr.payload_.hasValue() ? Slice(tr.payload_.value()) : Slice("blah", 4);

    copyset_bufs.emplace_back();
    std::string* copyset_buf = &copyset_bufs.back();
    LocalLogStoreRecordFormat::csi_flags_t csi_flags =
        LocalLogStoreRecordFormat::formCopySetIndexFlags(tr.flags_);
    Slice copyset_slice =
        LocalLogStoreRecordFormat::formCopySetIndexEntry(tr.wave_,
                                                         tr.copyset_.data(),
                                                         tr.copyset_.size(),
                                                         block_starting_lsn,
                                                         csi_flags,
                                                         copyset_buf);

    std::vector<std::pair<char, std::string>> index_key_list;

    uint64_t timestamp_big_endian = htobe64(tr.timestamp_.count());
    std::string timestamp(
        reinterpret_cast<const char*>(&timestamp_big_endian), sizeof(uint64_t));
    index_key_list.emplace_back(FIND_TIME_INDEX, std::move(timestamp));
    const auto& optional_keys = tr.optional_keys_;
    if (optional_keys.find(KeyType::FINDKEY) != optional_keys.end()) {
      index_key_list.emplace_back(
          FIND_KEY_INDEX, optional_keys.at(KeyType::FINDKEY));
    }

    ops.emplace_back(tr.log_id_,
                     tr.lsn_,
                     record_header,
                     payload_slice,
                     folly::none,
                     block_starting_lsn,
                     copyset_slice,
                     index_key_list,
                     Durability::ASYNC_WRITE, // We explicitly sync at the end
                     false                    // not rebuilding
    );
    op_ptrs.push_back(&ops.back());
  }
  int rv;
  rv = store.writeMulti(op_ptrs);
  ld_check(rv == 0);
  rv = store.sync(Durability::ASYNC_WRITE);
  ld_check(rv == 0);
}

}} // namespace facebook::logdevice
