/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <vector>

#include "logdevice/common/CompletionRequest.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/SequencersConfig.h"
#include "logdevice/include/types.h"

/**
 * @file SequencerLocator defines an interface for finding which node, if
 *       any, runs the sequencer for the most recent epoch of a particular log.
 */

namespace facebook { namespace logdevice {

enum class SequencerOptions : uint8_t {
  NONE = 0, // don't run any sequencers
  ALL,      // run sequencers for all logs in the config
  LAZY,     // bring up sequencers as needed
};

class SequencerLocator {
 public:
  /**
   * CompletionFunction is the type of callback that SequencerLocator calls
   * when an asynchronous request to locate the node running a sequencer for
   * a particular log completes. If the
   * request for which the completion is being called was initiated by a
   * Worker thread, the completion callback is guaranteed to be called
   * on the same Worker. Otherwise the completion callback will be called
   * on an unspecified Worker.
   *
   * @param status    status of the request. The other arguments are valid
   *                  only if this is E::OK. Possible values:
   *                  E::OK          request completed successfully
   *                  E::NOTFOUND    logid is not known or is not completely
   *                                 provisioned
   *                  E::ACCESS      location service denied access
   *                  E::CONNFAILED  lost connection to location service
   *                                 while waiting for reply, possibly due to
   *                                 timeout
   *                  E::INTERNAL    an assertion failed and we are in release
   *                                 mode
   * @parma logid     id of the log whose sequencer location was queried
   * @param nodeid    id of the node that was last running a sequencer for
   *                  this log, or an invalid NodeID if no sequencer has yet
   *                  been started for log
   * @return void
   */
  typedef logdevice::CompletionRequestBase<std::function, logid_t, NodeID>
      CompletionRequest;
  typedef CompletionRequest::CompletionFunction Completion;

  virtual ~SequencerLocator() {}

  /**
   * Initiate getting the NodeID of a LogDevice node that was most recently
   * running a sequencer for _logid_. Upon completion, cf will be called on
   * a Worker thread as specified above.
   *
   * @param sequencers  an optional list of sequencer nodes (including their
   *                    weights) that are presumably available; provided as a
   *                    hint, can be ignored by the implementation
   *
   * @return 0 if a request was successfully submitted, -1 on failure.
   *           Sets err to:
   *                  E::NOTCONN     not connected to the location store
   *                  E::ACCESS      location service denied access
   *                  E::SYSLIMIT    request failed because a system limit was
   *                                 reached, such as a memory limit
   *                  E::INVALID_PARAM  logid is invalid or cf is empty
   *                  E::INTERNAL    internal error, debug builds assert
   */
  virtual int locateSequencer(
      logid_t logid,
      Completion cf,
      const configuration::SequencersConfig* sequencers = nullptr) = 0;

  /**
   * Can upper layers cache results returned by locateSequencer() and bypass
   * it for future communication with the sequencer? If true, a sequencer node
   * is expected to be able to send a redirect if it's not the node responsible
   * for the log.
   */
  virtual bool isAllowedToCache() const = 0;
};

}} // namespace facebook::logdevice
