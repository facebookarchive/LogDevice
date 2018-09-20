/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Support for checksumming client payloads.
 *
 * On the send side, when the client initiates an append, AppendRequest checks
 * Settings::checksum_bits and sets appropriate checksum flags in
 * APPEND_Header.  When the APPEND_Message is getting serialized onto the
 * wire, the checksum is injected before the payload.  To the service, the
 * injected checksum will appear as part of the payload.  Checksum flags are
 * preserved when the sequencer sends STORE messages and when storage nodes
 * write the record into their local log store.
 *
 * On the read path, storage nodes see the checksum flags in the local log
 * store header and copy them into the RECORD message.  The client, while
 * parsing an incoming RECORD message, strips the checksum from the front of
 * the payload if flags said there was one.  Finally,
 * RECORD_Message::onReceived() calculates the payload's checksum.  If there
 * is a mismatch, a warning is logged, the record dropped and a gap is
 * reported to ClientReadStream.  From there, if only one node sent a record
 * that failed the checksum, ClientReadStream waits for other storage nodes to
 * send the record.  If all nodes send busted copies of the record, a DATALOSS
 * gap is reported to the application.
 */

uint32_t checksum_32bit(Slice slice);
uint64_t checksum_64bit(Slice slice);

/**
 * Writes a binary checksum of the given blob to the given output buffer.  The
 * output buffer must be at least 8 bytes large to fit a 64-bit checksum.
 *
 * @return Slice pointing into buf_out.
 */
Slice checksum_bytes(Slice blob, int nbits, char* buf_out);

}} // namespace facebook::logdevice
