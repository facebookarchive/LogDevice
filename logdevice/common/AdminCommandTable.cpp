/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AdminCommandTable.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/ClientID.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace admin_command_table {

std::string describeConnection(Address addr) {
  return Sender::describeConnection(addr);
}

template <>
std::string Converter<LSN>::operator()(LSN lsn, bool prettify) {
  if (!prettify) {
    return folly::to<std::string>(lsn.val_);
  } else if (lsn.val_ == LSN_MAX) {
    return "LSN_MAX";
  } else if (lsn.val_ == LSN_MAX - 1) {
    return "LSN_MAX-1";
  } else {
    return lsn_to_string(lsn.val_);
  }
}

template <>
std::string Converter<BYTE_OFFSET>::operator()(BYTE_OFFSET byte_offset,
                                               bool prettify) {
  if (prettify && byte_offset.val_ == BYTE_OFFSET_INVALID) {
    return "BYTE_OFFSET_INVALID";
  }
  return folly::to<std::string>(byte_offset.val_);
}

template <>
std::string Converter<epoch_t>::operator()(epoch_t epoch, bool prettify) {
  if (!prettify) {
    return folly::to<std::string>(epoch.val_);
  } else if (epoch == EPOCH_INVALID) {
    return "EPOCH_INVALID";
  } else if (epoch == EPOCH_MIN) {
    return "EPOCH_MIN";
  } else if (epoch == EPOCH_MAX) {
    return "EPOCH_MAX";
  } else {
    return "e" + folly::to<std::string>(epoch.val_);
  }
}

template <>
std::string Converter<esn_t>::operator()(esn_t esn, bool prettify) {
  if (!prettify) {
    return folly::to<std::string>(esn.val_);
  } else if (esn == ESN_INVALID) {
    return "ESN_INVALID";
  } else if (esn == ESN_MIN) {
    return "ESN_MIN";
  } else if (esn == ESN_MAX) {
    return "ESN_MAX";
  } else {
    return "n" + folly::to<std::string>(esn.val_);
  }
}

template <>
std::string Converter<logid_t>::operator()(logid_t logid, bool /*prettify*/) {
  return folly::to<std::string>(logid.val_);
}

template <>
std::string Converter<ClientID>::operator()(ClientID client,
                                            bool /*prettify*/) {
  return describeConnection(Address(client));
}

template <>
std::string Converter<Address>::operator()(Address address, bool /*prettify*/) {
  return describeConnection(address);
}

template <>
std::string Converter<bool>::operator()(bool v, bool prettify) {
  if (!prettify) {
    return folly::to<std::string>(v);
  } else {
    return v ? "true" : "false";
  }
}

template <>
std::string Converter<std::chrono::milliseconds>::
operator()(std::chrono::milliseconds c, bool prettify) {
  return prettify ? format_time(c) : folly::to<std::string>(c.count());
}
template <>
std::string Converter<std::chrono::microseconds>::
operator()(std::chrono::microseconds c, bool prettify) {
  return prettify ? format_time(c) : folly::to<std::string>(c.count());
}

template <>
std::string Converter<std::chrono::seconds>::operator()(std::chrono::seconds c,
                                                        bool prettify) {
  return prettify ? format_time(c) : folly::to<std::string>(c.count());
}

template <>
std::string Converter<Status>::operator()(Status s, bool /*prettify*/) {
  return std::string(error_name(s));
}

template <>
std::string Converter<Sockaddr>::operator()(Sockaddr a, bool /*prettify*/) {
  return a.toString();
}

}}} // namespace facebook::logdevice::admin_command_table
