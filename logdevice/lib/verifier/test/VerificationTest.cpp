/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <algorithm>
#include <chrono>

#include <folly/Random.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/toString.h"
#include "logdevice/lib/verifier/GenVerifyData.h"
#include "logdevice/lib/verifier/MockDataSourceReader.h"
#include "logdevice/lib/verifier/MockDataSourceWriter.h"
#include "logdevice/lib/verifier/VerificationDataStructures.h"
#include "logdevice/lib/verifier/VerificationReader.h"
#include "logdevice/lib/verifier/VerificationWriter.h"

using namespace facebook::logdevice;

TEST(VerificationTest, serializeVerificationDataTest) {
  GenVerifyData gvd = GenVerifyData();
  for (int i = 0; i < 100; i++) {
    std::string ps = "";
    int ps_size8 = folly::Random::rand64(5, 20);
    for (int j = 0; j < ps_size8; j++) {
      ps += toString(folly::Random::rand64());
    }
    logid_t curr_logid = logid_t(folly::Random::rand64());
    auto& log_state = gvd.log_states_[curr_logid];
    log_state.next_seq_num = vsn_t(folly::Random::rand64());
    int num_in_flight = folly::Random::rand64(5, 20);
    for (int j = 0; j < num_in_flight; j++) {
      log_state.in_flight.insert(vsn_t(folly::Random::rand64()));
    }
    int num_to_ack = folly::Random::rand64(5, 20);
    for (int j = 0; j < num_to_ack; j++) {
      log_state.to_ack_list.insert(std::make_pair(
          vsn_t(folly::Random::rand64()), lsn_t(folly::Random::rand64())));
    }

    VerificationData gen_vdata = gvd.generateVerificationData(curr_logid, ps);

    std::string s;
    VerificationDataStructures::serializeVerificationData(gen_vdata, &s);
    s += ps;
    Payload p = Payload(s.data(), s.size());

    VerificationData vd;
    vd.vdh.magic_number = vflag_;
    vd.vdh.writer_id = gvd.getWriterID();
    vd.vdh.sequence_num = log_state.next_seq_num - 1;
    vd.vdh.oldest_in_flight = *(log_state.in_flight.begin());
    vd.vdh.payload_checksum = folly::hash::fnv32(ps);
    vd.ack_list = std::vector<std::pair<vsn_t, lsn_t>>(
        log_state.to_ack_list.begin(), log_state.to_ack_list.end());
    vd.vdh.payload_size = ps.size();
    vd.vdh.ack_list_length = log_state.to_ack_list.size();

    VerExtractionResult ver =
        VerificationDataStructures::extractVerificationData(p);

    EXPECT_EQ(ver.vd.vdh.magic_number, vd.vdh.magic_number);
    EXPECT_EQ(ver.vd.vdh.ack_list_length, vd.vdh.ack_list_length);
    EXPECT_EQ(ver.vd.vdh.payload_size, vd.vdh.payload_size);
    EXPECT_EQ(ver.vd.vdh.writer_id, vd.vdh.writer_id);
    EXPECT_EQ(ver.vd.vdh.sequence_num, vd.vdh.sequence_num);
    EXPECT_EQ(ver.vd.vdh.oldest_in_flight, vd.vdh.oldest_in_flight);
    EXPECT_EQ(ver.vd.vdh.payload_checksum, vd.vdh.payload_checksum);
    EXPECT_EQ(ver.vd.ack_list, vd.ack_list);
    EXPECT_EQ(ver.user_payload.toString(), ps);
  }
}

static void
initializeAndAppend(VerificationWriter& vw,
                    logid_t curr_logid,
                    const std::vector<std::string>& all_payload_strings,
                    std::map<int, int>& overall_append_order,
                    int num_in_flight = 7) {
  // Simulate multiple appends in flight, and callbacks being called out of
  // order within any set of appends in flight.
  MockDataSourceWriter* mdptr =
      static_cast<MockDataSourceWriter*>(vw.ds_.get());
  std::vector<int> in_flight_indices;
  for (int i = 0; i < all_payload_strings.size(); i++) {
    in_flight_indices.push_back(i);

    // Execute the last record's callback all by itself, so that we may see
    // the ack list of the entire log in that record and any data loss before
    // that record may be detected.
    if (in_flight_indices.size() == num_in_flight ||
        i == all_payload_strings.size() - 1 ||
        i == all_payload_strings.size() - 2) {
      // Append in-flight records in random order.
      std::random_shuffle(in_flight_indices.begin(), in_flight_indices.end());
      for (int j : in_flight_indices) {
        auto cb = [=](Status st, const DataRecord& r) {
          // Check that VerificationWriter callback logic is successfully
          // removing the verification data from the payload of the DataRecord
          // passed to the callback.
          EXPECT_EQ(all_payload_strings[j], r.payload.toString());
        };
        vw.append(curr_logid, all_payload_strings[j], cb);
        // overall_append_order[j] = the index in records_ (i.e. in the
        // simulated "log") of the j-th string in all_payload_strings.
        overall_append_order[j] = mdptr->records_[curr_logid].size() - 1;
      }

      // Call callbacks of in-flight indices out of order.
      std::random_shuffle(in_flight_indices.begin(), in_flight_indices.end());
      for (int j : in_flight_indices) {
        mdptr->records_[curr_logid][overall_append_order[j]].first(
            E::OK,
            *(mdptr->records_[curr_logid][overall_append_order[j]].second));
      }
      in_flight_indices.resize(0);
    }
  }
}

TEST(VerificationTest, AppendBehaviorAndCallbackTest) {
  VerificationWriter vw =
      VerificationWriter(std::make_unique<MockDataSourceWriter>(),
                         std::make_unique<GenVerifyData>());
  logid_t curr_logid = logid_t(folly::Random::rand64());

  std::vector<std::string> all_payload_strings;
  for (int i = 0; i < 100; i++) {
    std::string ps;
    int ps_size8 = folly::Random::rand64(5, 20);
    for (int j = 0; j < ps_size8; j++) {
      ps += toString(folly::Random::rand64());
    }

    all_payload_strings.push_back(ps);
  }
  std::map<int, int> overall_append_order;

  initializeAndAppend(
      vw, curr_logid, all_payload_strings, overall_append_order);

  MockDataSourceWriter* mdptr =
      static_cast<MockDataSourceWriter*>(vw.ds_.get());

  // Check that appended payload strings were correct.
  for (int j = 0; j < all_payload_strings.size(); j++) {
    VerExtractionResult ver =
        VerificationDataStructures::extractVerificationData(
            mdptr->records_[curr_logid][overall_append_order[j]]
                .second->payload);
    EXPECT_EQ(ver.user_payload.toString(), all_payload_strings[j]);
  }
}

TEST(VerificationTest, AppendCompletedVerificationDataUpdatesTest) {
  GenVerifyData gvd = GenVerifyData();
  for (int i = 0; i < 100; i++) {
    logid_t curr_logid = logid_t(10);
    std::vector<std::pair<uint64_t, lsn_t>> record_ack_list;
    int num_in_ack_list = folly::Random::rand64(5, 20);
    for (int j = 0; j < num_in_ack_list; j++) {
      std::pair<uint64_t, lsn_t> seq_lsn_pair = std::make_pair(
          folly::Random::rand64(), lsn_t(folly::Random::rand64()));
      if (folly::Random::oneIn(2)) {
        record_ack_list.push_back(seq_lsn_pair);
      }
      gvd.log_states_[curr_logid].to_ack_list.insert(seq_lsn_pair);
    }
    std::stable_sort(record_ack_list.begin(), record_ack_list.end());

    for (int j = 0; j < folly::Random::rand64(5, 20); j++) {
      gvd.log_states_[curr_logid].in_flight.insert(folly::Random::rand64());
    }
    lsn_t appended_lsn;
    bool appendsuccess = folly::Random::oneIn(2);
    if (appendsuccess) {
      // Append success.
      appended_lsn = folly::Random::rand64();

    } else {
      appended_lsn = LSN_INVALID;
    }
    std::set<uint64_t>::iterator it =
        gvd.log_states_[curr_logid].in_flight.begin();
    std::advance(
        it,
        folly::Random::rand64(0, gvd.log_states_[curr_logid].in_flight.size()));
    uint64_t appended_seq_num = *it;
    std::set<uint64_t> expected_in_flight =
        gvd.log_states_[curr_logid].in_flight;
    expected_in_flight.erase(appended_seq_num);
    std::set<std::pair<uint64_t, lsn_t>> expected_to_ack_list =
        gvd.log_states_[curr_logid].to_ack_list;
    expected_to_ack_list.insert(std::make_pair(appended_seq_num, appended_lsn));

    gvd.appendUpdateOnCallback(
        curr_logid, appended_seq_num, appended_lsn, record_ack_list);

    EXPECT_EQ(gvd.log_states_[curr_logid].in_flight, expected_in_flight);

    if (appendsuccess) {
      std::set<std::pair<uint64_t, lsn_t>> new_to_ack_list;
      std::set_difference(
          expected_to_ack_list.begin(),
          expected_to_ack_list.end(),
          record_ack_list.begin(),
          record_ack_list.end(),
          std::inserter(new_to_ack_list, new_to_ack_list.begin()));
      EXPECT_EQ(gvd.log_states_[curr_logid].to_ack_list, new_to_ack_list);
    } else {
      EXPECT_EQ(gvd.log_states_[curr_logid].to_ack_list, expected_to_ack_list);
    }
  }
}

TEST(VerificationTest, BasicReadBehaviorTest) {
  VerificationWriter vw =
      VerificationWriter(std::make_unique<MockDataSourceWriter>(),
                         std::make_unique<GenVerifyData>());
  logid_t curr_logid = logid_t(folly::Random::rand64());

  std::vector<std::string> all_payload_strings;
  for (int i = 0; i < 100; i++) {
    std::string ps;
    for (int j = 0; j < folly::Random::rand64(3, 5); j++) {
      ps += toString(folly::Random::rand64());
    }
    all_payload_strings.push_back(ps);
  }

  std::map<int, int> overall_append_order;

  initializeAndAppend(
      vw, curr_logid, all_payload_strings, overall_append_order);

  VerificationReader vr =
      VerificationReader(std::make_unique<MockDataSourceReader>(),
                         std::make_unique<ReadVerifyData>());

  MockDataSourceWriter* write_mdptr =
      static_cast<MockDataSourceWriter*>(vw.ds_.get());

  MockDataSourceReader* read_mdptr =
      static_cast<MockDataSourceReader*>(vr.ds_.get());

  read_mdptr->records_ = std::move(write_mdptr->records_);

  std::vector<std::unique_ptr<DataRecord>> data_out;

  vr.startReading(curr_logid, lsn_t(1), lsn_t(100));

  vr.read(100, &data_out, new GapRecord);
  for (int i = 0; i < 100; i++) {
    EXPECT_EQ(all_payload_strings[i],
              data_out.at(overall_append_order[i])->payload.toString());
  }
}

TEST(VerificationTest, DataLossDetectionTest) {
  VerificationWriter vw =
      VerificationWriter(std::make_unique<MockDataSourceWriter>(),
                         std::make_unique<GenVerifyData>());
  logid_t curr_logid = logid_t(folly::Random::rand64());

  std::vector<std::string> all_payload_strings;
  for (int i = 0; i < 100; i++) {
    std::string ps;
    int ps_size8 = folly::Random::rand64(3, 6);
    for (int j = 0; j < ps_size8; j++) {
      ps += toString(folly::Random::rand64());
    }
    all_payload_strings.push_back(ps);
  }

  std::map<int, int> overall_append_order;

  initializeAndAppend(
      vw, curr_logid, all_payload_strings, overall_append_order);

  VerificationReader vr =
      VerificationReader(std::make_unique<MockDataSourceReader>(),
                         std::make_unique<ReadVerifyData>());

  MockDataSourceWriter* write_mdptr =
      static_cast<MockDataSourceWriter*>(vw.ds_.get());

  MockDataSourceReader* read_mdptr =
      static_cast<MockDataSourceReader*>(vr.ds_.get());

  read_mdptr->records_ = std::move(write_mdptr->records_);
  auto& log_records = read_mdptr->records_[curr_logid];

  // Randomly introduce data loss. Note that at present, we cannot detect loss
  // of the last record. Moreover, in order to not break startReading(), we
  // will not lose the first record either.
  // Note that we do not allow loss of the second-to-last record either (with
  // sequence number = 98), because since the log ends at 99, then record 98
  // is the only record containing the ack list of records 91-97 (assuming
  // the default num_in_flight of 7); hence deleting 98 would prevent us from
  // detecting any deletions from 91-97.
  std::set<int> deleted_record_indices;
  for (int i = 0; i < 10; i++) {
    deleted_record_indices.insert(folly::Random::rand64(1, 98));
  }
  std::set<int>::reverse_iterator deleted_it = deleted_record_indices.rbegin();
  while (deleted_it != deleted_record_indices.rend()) {
    log_records.erase(log_records.begin() + *deleted_it);
    deleted_it++;
  }

  std::vector<std::unique_ptr<DataRecord>> data_out;
  GapRecord g;

  std::map<vsn_t, VerificationAppendInfo> lost_record_vai;

  error_callback_t ecb = [&](const VerificationFoundError& vfe) {
    if (vfe.vrs == VerificationRecordStatus::DATALOSS) {
      lost_record_vai.insert(vfe.error_record);
    }
  };

  vr.startReading(curr_logid, lsn_t(1), lsn_t(100));
  vr.read(100, &data_out, &g, ecb);

  EXPECT_EQ(data_out.size(),
            all_payload_strings.size() - deleted_record_indices.size());

  for (int x = 0; x < all_payload_strings.size(); x++) {
    // x = index of the payload string in all_payload_strings.
    // i = index of the payload string in records_ prior to deletion.
    int i = overall_append_order[x];
    if (deleted_record_indices.find(i) != deleted_record_indices.end()) {
      EXPECT_FALSE(lost_record_vai.find(i) == lost_record_vai.end());
    } else {
      // Convert i, the index in the records_ before data loss, to the new
      // index after deletion and check whether the payloads match.
      int idx_after_deletion = i -
          std::distance(deleted_record_indices.begin(),
                        deleted_record_indices.lower_bound(i));
      EXPECT_EQ(all_payload_strings[x],
                data_out.at(idx_after_deletion)->payload.toString());
    }
  }
}

TEST(VerificationTest, DuplicateDetectionTest) {
  VerificationWriter vw =
      VerificationWriter(std::make_unique<MockDataSourceWriter>(),
                         std::make_unique<GenVerifyData>());
  logid_t curr_logid = logid_t(folly::Random::rand64());

  std::vector<std::string> all_payload_strings;
  for (int i = 0; i < 100; i++) {
    std::string ps;
    int ps_size8 = folly::Random::rand64(3, 6);
    for (int j = 0; j < ps_size8; j++) {
      ps += toString(folly::Random::rand64());
    }
    all_payload_strings.push_back(ps);
  }

  std::map<int, int> overall_append_order;

  initializeAndAppend(
      vw, curr_logid, all_payload_strings, overall_append_order);

  VerificationReader vr =
      VerificationReader(std::make_unique<MockDataSourceReader>(),
                         std::make_unique<ReadVerifyData>());

  MockDataSourceWriter* write_mdptr =
      static_cast<MockDataSourceWriter*>(vw.ds_.get());

  MockDataSourceReader* read_mdptr =
      static_cast<MockDataSourceReader*>(vr.ds_.get());

  read_mdptr->records_ = std::move(write_mdptr->records_);
  auto& log_records = read_mdptr->records_[curr_logid];
  std::vector<std::string> all_payloads_incl_dup(100, "");
  for (int x = 0; x < 100; x++) {
    all_payloads_incl_dup[overall_append_order[x]] = all_payload_strings[x];
  }

  // Randomly introduce duplication. Note that at present, we cannot detect
  // duplication of the last record.
  std::set<int> duplicated_record_indices;
  for (int i = 0; i < 10; i++) {
    duplicated_record_indices.insert(folly::Random::rand64(0, 99));
  }
  std::set<int>::reverse_iterator duplicated_it =
      duplicated_record_indices.rbegin();
  while (duplicated_it != duplicated_record_indices.rend()) {
    all_payloads_incl_dup.insert(all_payloads_incl_dup.begin() + *duplicated_it,
                                 all_payloads_incl_dup[*duplicated_it]);
    const std::unique_ptr<DataRecordOwnsPayload>& dup_record_ptr =
        log_records[*duplicated_it].second;
    log_records.insert(log_records.begin() + *duplicated_it,
                       std::make_pair(log_records[*duplicated_it].first,
                                      std::make_unique<DataRecordOwnsPayload>(
                                          dup_record_ptr->logid,
                                          dup_record_ptr->payload.dup(),
                                          dup_record_ptr->attrs.lsn,
                                          dup_record_ptr->attrs.timestamp,
                                          dup_record_ptr->flags_)));
    duplicated_it++;
  }
  all_payloads_incl_dup.resize(100);

  std::vector<std::unique_ptr<DataRecord>> data_out;
  GapRecord g;
  std::map<vsn_t, VerificationAppendInfo> duplicate_vai;
  error_callback_t ecb = [&](const VerificationFoundError& vfe) {
    if (vfe.vrs == VerificationRecordStatus::DUPLICATE) {
      duplicate_vai.insert(vfe.error_record);
    }
  };

  vr.startReading(curr_logid, lsn_t(1), lsn_t(100));
  vr.read(100, &data_out, &g, ecb);

  EXPECT_EQ(data_out.size(), all_payloads_incl_dup.size());

  std::vector<std::string> data_out_strings;

  for (int i = 0; i < all_payloads_incl_dup.size(); i++) {
    data_out_strings.push_back(data_out[i]->payload.toString());
  }
  EXPECT_EQ(data_out_strings, all_payloads_incl_dup);
  int dup_count = 0;
  for (int i : duplicated_record_indices) {
    if (i + dup_count < 99) {
      // If the duplicate occurs at a high enough index such that it is pushed
      // beyond the read range by the duplicates before it, we may not be able
      // to detect it.
      // For example, suppose the 11th duplicate index in increasing order
      // is 95. Then that duplicated record is located at position 105 in the
      // records_ and hence won't be detected by a read of 100 records.
      // (Also, notably, suppose the 11th duplicate index is 89. Then the
      // index of the duplicate in the actual records_ is 99 which is within
      // read range but is the last record, so we won't be able to detect this
      // duplicate either.
      EXPECT_FALSE(duplicate_vai.find(i) == duplicate_vai.end());
      dup_count++;
    }
  }
}

TEST(VerificationTest, ReorderingDetectionTest) {
  bool debug = true;
  int num_test_iterations = 300;
  for (int z = 0; z < num_test_iterations; z++) {
    // Have a relatively large num_in_flight, so that it is unlikely that all
    // of a set of records in flight are reordered somewhere else (which would
    // then disrupt the detection of reorderings in the immediately preceding
    // records).
    int num_in_flight = 7;
    VerificationWriter vw =
        VerificationWriter(std::make_unique<MockDataSourceWriter>(),
                           std::make_unique<GenVerifyData>());
    logid_t curr_logid = logid_t(folly::Random::rand64());

    std::vector<std::string> all_payload_strings;
    for (int i = 0; i < 100; i++) {
      std::string ps;
      int ps_size8 = folly::Random::rand64(3, 6);
      for (int j = 0; j < ps_size8; j++) {
        ps += toString(folly::Random::rand64());
      }
      all_payload_strings.push_back(ps);
    }

    std::map<int, int> overall_append_order;

    initializeAndAppend(vw,
                        curr_logid,
                        all_payload_strings,
                        overall_append_order,
                        num_in_flight);

    VerificationReader vr =
        VerificationReader(std::make_unique<MockDataSourceReader>(),
                           std::make_unique<ReadVerifyData>());

    MockDataSourceWriter* write_mdptr =
        static_cast<MockDataSourceWriter*>(vw.ds_.get());

    MockDataSourceReader* read_mdptr =
        static_cast<MockDataSourceReader*>(vr.ds_.get());

    read_mdptr->records_ = std::move(write_mdptr->records_);
    auto& log_records = read_mdptr->records_[curr_logid];
    std::vector<std::string> all_reordered_payloads(100, "");
    for (int x = 0; x < 100; x++) {
      all_reordered_payloads[overall_append_order[x]] = all_payload_strings[x];
    }

    // Randomly introduce reordering. Note that at present, we cannot detect
    // reordering of the last record. Also, in order to not break startReading,
    // we won't reorder the first record.
    std::map<vsn_t, VerificationAppendInfo> reordered_vai;
    std::set<int> reordered_new_indices;
    int num_reordered = 10;
    for (int i = 0; i < num_reordered; i++) {
      // Note: upper bound of old_idx determined by how far we need to reorder
      // (see below).
      int old_idx;
      if (i == 0)
        old_idx = 70;
      else if (i == 1)
        old_idx = 77;
      else
        old_idx = folly::Random::rand64(
            1, all_payload_strings.size() - 2 - num_in_flight - num_reordered);

      ProtocolReader r(Slice(log_records[old_idx].second->payload.data(),
                             log_records[old_idx].second->payload.size()),
                       "reorder_testing",
                       0);
      VerificationDataHeader vdh;
      r.read(&vdh);

      VerificationAppendInfo vai;
      vai.lsn = log_records[old_idx].second->attrs.lsn;
      reordered_vai[vdh.sequence_num] = vai;

      // Reorder to a new location such that the distance between old_idx and
      // new_idx is greater than the
      // num_in_flight. Therefore, the record before new_idx is guaranteed
      // to have the record from old_idx in its ack list, and therefore
      // we should be able to detect this reordering.
      // Note: the minimum new_idx is old_idx + num_in_flight+1+num_reordered:
      //   +1 to account for us erasing the record at old_idx
      //   +num_in_flight because rearranging within in-flight elements isn't
      //      an error
      //   +num_reordered because in the worst case, this element could
      //      additionally get shifted back by this amount (if every reordered
      //      element gets pulled from before it and inserted after it).
      int new_idx = folly::Random::rand64(
          old_idx + num_in_flight + 1 + num_reordered, 99);
      reordered_new_indices.insert(new_idx);
      log_records.insert(
          log_records.begin() + new_idx,
          std::make_pair(log_records[old_idx].first,
                         std::make_unique<DataRecordOwnsPayload>(
                             log_records[old_idx].second->logid,
                             log_records[old_idx].second->payload.dup(),
                             log_records[old_idx].second->attrs.lsn,
                             log_records[old_idx].second->attrs.timestamp,
                             log_records[old_idx].second->flags_)));
      log_records.erase(log_records.begin() + old_idx);

      std::string curr_ps = all_reordered_payloads[old_idx];

      all_reordered_payloads.insert(
          all_reordered_payloads.begin() + new_idx, curr_ps);

      all_reordered_payloads.erase(all_reordered_payloads.begin() + old_idx);
    }

    std::vector<vsn_t> all_record_vsns;
    std::vector<std::vector<std::pair<vsn_t, lsn_t>>> all_ack_lists;
    if (debug) {
      for (int k = 0; k < log_records.size(); k++) {
        VerExtractionResult ver =
            VerificationDataStructures::extractVerificationData(
                log_records[k].second->payload);
        all_record_vsns.push_back(ver.vd.vdh.sequence_num);
        all_ack_lists.push_back(ver.vd.ack_list);
      }
    }

    std::vector<std::unique_ptr<DataRecord>> data_out;
    GapRecord g;
    std::map<vsn_t, VerificationAppendInfo> verified_reordered_records;
    std::map<vsn_t, VerificationAppendInfo> verified_lost_records;
    error_callback_t ecb = [&](const VerificationFoundError& vfe) {
      if (vfe.vrs == VerificationRecordStatus::REORDERING)
        verified_reordered_records.insert(vfe.error_record);
      else if (vfe.vrs == VerificationRecordStatus::DATALOSS) {
        verified_lost_records.insert(vfe.error_record);
      }
    };

    vr.startReading(curr_logid, lsn_t(1), lsn_t(100));
    vr.read(100, &data_out, &g, ecb);

    EXPECT_EQ(data_out.size(), all_reordered_payloads.size());

    std::vector<std::string> data_out_strings;

    for (int i = 0; i < all_reordered_payloads.size(); i++) {
      data_out_strings.push_back(data_out[i]->payload.toString());
    }
    EXPECT_EQ(data_out_strings, all_reordered_payloads);

    // All reordered records will initially be reported as data loss before
    // the reordered record is actually read.
    EXPECT_EQ(verified_lost_records, reordered_vai);
    if (debug) {
      if (reordered_vai != verified_reordered_records) {
        printf("record vsns: %s\n", toString(all_record_vsns).c_str());
        printf("record ack lists: %s\n", toString(all_ack_lists).c_str());
      }
    }
    EXPECT_EQ(reordered_vai, verified_reordered_records);
  }
}
