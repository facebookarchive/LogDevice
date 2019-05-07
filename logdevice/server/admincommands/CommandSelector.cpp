/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/admincommands/CommandSelector.h"

#include <folly/String.h>

#include "logdevice/common/libevent/compat.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice {

void CommandSelector::add(const char* prefix, CommandFactory factory) {
  std::vector<std::string> tokens;
  folly::split(' ', prefix, tokens);
  TrieNode* node = &root_;
  for (const std::string& token : tokens) {
    if (!node->children.count(token)) {
      node->children[token] = std::unique_ptr<TrieNode>(new TrieNode());
    }
    node = node->children[token].get();
  }
  node->factory = factory;
}

// Returns matching command and removes from inout_command the prefix that
// was used to select command.
// If there's no matching command returns nullptr and writes error to output.
std::unique_ptr<AdminCommand>
CommandSelector::selectCommand(std::vector<std::string>& inout_args,
                               struct evbuffer* output) {
  TrieNode* node = &root_;
  size_t pos = 0;
  while (pos < inout_args.size() && node->children.count(inout_args[pos])) {
    node = node->children[inout_args[pos]].get();
    ++pos;
  }

  // Assume that if a command is a prefix of another command then
  // it doesn't take positional arguments.
  bool looks_wrong = pos < inout_args.size() && !node->children.empty() &&
      inout_args[pos][0] != '-';

  if (!node->factory || looks_wrong) {
    LD_EV(evbuffer_add_printf)
    (output, "Unsupported command. Did you mean one of these?\r\n");
    std::string args_prefix;
    folly::join(' ', inout_args.begin(), inout_args.begin() + pos, args_prefix);
    for (const auto& it : node->children) {
      LD_EV(evbuffer_add_printf)
      (output, "%s", (args_prefix + ' ' + it.first).c_str());
      if (!it.second->children.empty()) {
        if (it.second->factory) {
          LD_EV(evbuffer_add_printf)(output, " [...]");
        } else {
          LD_EV(evbuffer_add_printf)(output, " ...");
        }
      }
      LD_EV(evbuffer_add_printf)(output, "\r\n");
    }
    LD_EV(evbuffer_add_printf)(output, "END\r\n");
    return nullptr;
  }

  inout_args.erase(inout_args.begin(), inout_args.begin() + pos);
  return node->factory();
}

}} // namespace facebook::logdevice
