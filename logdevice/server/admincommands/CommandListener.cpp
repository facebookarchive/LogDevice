/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/server/admincommands/CommandListener.h"

#include <pthread.h>

#include <boost/token_functions.hpp>
#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/String.h>
#include <rocksdb/statistics.h>

#include "event2/bufferevent.h"
#include "event2/bufferevent_ssl.h"
#include "event2/event.h"
#include "event2/util.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

std::unique_ptr<AdminCommandFactory> createAdminCommandFactory(bool test_mode) {
  if (test_mode) {
    return std::make_unique<TestAdminCommandFactory>();
  } else {
    return std::make_unique<AdminCommandFactory>();
  }
}

CommandListener::CommandListener(Listener::InterfaceDef iface,
                                 KeepAlive loop,
                                 Server* server)
    : Listener(std::move(iface), loop),
      server_(server),
      server_settings_(server_->getServerSettings()),
      command_factory_(
          createAdminCommandFactory(/*test_mode=*/server_settings_->test_mode)),
      ssl_fetcher_(
          server_->getParameters()->getProcessorSettings()->ssl_cert_path,
          server_->getParameters()->getProcessorSettings()->ssl_key_path,
          server_->getParameters()->getProcessorSettings()->ssl_ca_path,
          server_->getParameters()
              ->getProcessorSettings()
              ->ssl_cert_refresh_interval),
      loop_(loop) {
  ld_check(server_);
}

CommandListener::ConnectionState::~ConnectionState() {
  ld_check(bev_);

  ld_debug("Closing command connection with id %zu", id_);
  LD_EV(bufferevent_free)(bev_);
}

void CommandListener::acceptCallback(evutil_socket_t sock,
                                     const folly::SocketAddress& addr) {
  struct bufferevent* bev = LD_EV(bufferevent_socket_new)(
      loop_->getEventBase(), sock, BEV_OPT_CLOSE_ON_FREE);

  if (!bev) {
    ld_error("bufferevent_socket_new() failed. errno=%d (%s)",
             errno,
             strerror(errno));
    LD_EV(evutil_closesocket)(sock);
    return;
  }

  const int rcvbuf = COMMAND_RCVBUF;
  const int sndbuf = COMMAND_SNDBUF;

  int rv = setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  if (rv != 0) {
    ld_error("Failed to set rcvbuf size for TCP socket %d to %d: %s",
             sock,
             rcvbuf,
             strerror(errno));
    LD_EV(bufferevent_free)(bev);
    LD_EV(evutil_closesocket)(sock);
    return;
  }

  rv = setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
  if (rv != 0) {
    ld_error("Failed to set sndbuf size for TCP socket %d to %d: %s",
             sock,
             COMMAND_SNDBUF,
             strerror(errno));
    LD_EV(bufferevent_free)(bev);
    LD_EV(evutil_closesocket)(sock);
    return;
  }

#if LIBEVENT_VERSION_NUMBER >= 0x02010000
  // In libevent >= 2.1 we can tweak the amount of data libevent sends to
  // the TCP stack at once
  LD_EV(bufferevent_set_max_single_read)(bev, rcvbuf);
  LD_EV(bufferevent_set_max_single_write)(bev, sndbuf);
#endif

  const conn_id_t id = next_conn_id_++;
  auto state = std::make_unique<ConnectionState>(this, id, bev, Sockaddr(addr));

  ld_debug("Accepted connection from %s (id %zu, fd %d)",
           state->address_.toString().c_str(),
           id,
           LD_EV(bufferevent_getfd)(bev));

  const size_t conn_limit = server_settings_->command_conn_limit;
  while (conns_.size() >= conn_limit) {
    // no more connections available, free the oldest one (with the smallest id)
    ld_check(conns_.begin() != conns_.end());
    conns_.erase(conns_.begin());
  }

  auto res = conns_.insert(std::make_pair(id, std::move(state)));
  ld_check(res.second);

  auto& it = res.first->second;

  LD_EV(bufferevent_setcb)
  (it->bev_,
   CommandListener::readCallback,
   CommandListener::writeCallback,
   CommandListener::eventCallback,
   it.get());
  LD_EV(bufferevent_enable)(it->bev_, EV_READ | EV_WRITE);
}

static std::string peekInEvbuffer(struct evbuffer* input, size_t len) {
  std::string buf(len, '\0');
  LD_EV(evbuffer_copyout)(input, &buf[0], buf.size());
  return buf;
}

// check whether the packet looks like SSL handshake
static bool payloadIsSSL(struct evbuffer* input) {
  const char SSL_HANDSHAKE_RECORD_TAG = 0x16;
  char c = 0;
  LD_EV(evbuffer_copyout)(input, &c, 1);

  // Warning: Very weak validation here. We only check the first byte.
  // That should be good enough for admin commands however, since
  // we are expecting ascii charaters when in clear text.
  return c == SSL_HANDSHAKE_RECORD_TAG;
}

void CommandListener::readCallback(struct bufferevent* bev, void* arg) {
  ConnectionState* state = reinterpret_cast<ConnectionState*>(arg);
  ld_check(state && state->bev_ == bev);

  struct evbuffer* input = LD_EV(bufferevent_get_input)(bev);
  ld_check(input);

  CommandListener* listener = state->parent_;
  const conn_id_t id = state->id_;

  if (state->type_ == ConnectionType::UNKNOWN) {
    if (payloadIsSSL(input)) {
      if (listener->upgradeToSSL(state)) {
        STAT_INCR(listener->server_->getParameters()->getStats(),
                  command_port_connection_encrypted);
      } else {
        STAT_INCR(listener->server_->getParameters()->getStats(),
                  command_port_connection_failed_ssl_upgrade);
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        2,
                        "Failed to upgrade admin connection from %s to SSL",
                        state->address_.toString().c_str());
        listener->conns_.erase(id);
      }
      return;
    } else {
      if (listener->server_settings_->require_ssl_on_command_port) {
        STAT_INCR(listener->server_->getParameters()->getStats(),
                  command_port_connection_failed_ssl_required);
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            2,
            "Received non-encrypted traffic from %s while SSL is required "
            "on command port",
            state->address_.toString().c_str());
        listener->conns_.erase(id);
        return;
      }
      STAT_INCR(listener->server_->getParameters()->getStats(),
                command_port_connection_plain);
      state->type_ = ConnectionType::PLAIN;
    }
  }

  ld_spew("Reading command from %s", state->address_.toString().c_str());

  const bool is_localhost =
      state->address_.getSocketAddress().isLoopbackAddress();

  size_t len;
  char* command;

  while ((command = LD_EV(evbuffer_readln)(input, &len, EVBUFFER_EOL_CRLF))) {
    state->last_command_ = command;
    listener->processCommand(bev, command, is_localhost, state);
    free(command);
  }

  // we don't have a complete line; close the connection if the buffer size
  // is over the limit
  len = LD_EV(evbuffer_get_length)(input);
  if (len != 0) {
    ld_spew(
        "Don't have a complete command from %s yet. Command so far: %s (%lu "
        "bytes)",
        state->address_.toString().c_str(),
        peekInEvbuffer(input, std::min(len, 1500ul)).c_str(),
        len);
  }
  if (len > CommandListener::COMMAND_RCVBUF) {
    std::string prefix(CommandListener::COMMAND_RCVBUF, '\0');
    int l = LD_EV(evbuffer_remove)(
        input, &prefix[0], CommandListener::COMMAND_RCVBUF);
    ld_check(l == CommandListener::COMMAND_RCVBUF);
    ld_info("Command too long (at least %lu bytes), closing the connection to "
            "%s; command starts with: %s",
            len,
            state->address_.toString().c_str(),
            sanitize_string(prefix).c_str());
    listener->conns_.erase(id);
  }
}

void CommandListener::writeCallback(struct bufferevent* bev, void* arg) {
  ConnectionState* state = reinterpret_cast<ConnectionState*>(arg);
  ld_check(state && state->bev_ == bev);

  struct evbuffer* out_evbuf = LD_EV(bufferevent_get_output)(bev);
  ld_check(out_evbuf);

  ld_spew("Write callback called for %s. close_when_drained_: %d, out_evbuf "
          "length: %lu",
          state->address_.toString().c_str(),
          state->close_when_drained_,
          LD_EV(evbuffer_get_length)(out_evbuf));

  if (state->close_when_drained_ && !LD_EV(evbuffer_get_length)(out_evbuf)) {
    CommandListener* listener = state->parent_;
    listener->conns_.erase(state->id_);
  }
}

void CommandListener::eventCallback(struct bufferevent* bev,
                                    short events,
                                    void* arg) {
  int sock_errno = errno;

  ConnectionState* state = reinterpret_cast<ConnectionState*>(arg);
  ld_check(state && state->bev_ == bev);

  CommandListener* listener = state->parent_;
  const conn_id_t id = state->id_;

  if (events & BEV_EVENT_ERROR) {
    ld_info("Got an error on command socket %d to %s while %s. errno=%d (%s). "
            "command: %s",
            LD_EV(bufferevent_getfd)(bev),
            state->address_.toString().c_str(),
            (events & BEV_EVENT_WRITING) ? "writing" : "reading",
            sock_errno,
            strerror(sock_errno),
            sanitize_string(state->last_command_).c_str());

    listener->conns_.erase(id);
  } else if (events & BEV_EVENT_EOF) {
    struct evbuffer* out_evbuf = LD_EV(bufferevent_get_output)(bev);
    ld_check(out_evbuf);

    size_t len = LD_EV(evbuffer_get_length)(out_evbuf);

    ld_spew("Got EOF on command socket %d to %s while %s. errno=%d (%s). "
            "command: %s",
            LD_EV(bufferevent_getfd)(bev),
            state->address_.toString().c_str(),
            (events & BEV_EVENT_WRITING) ? "writing" : "reading",
            sock_errno,
            strerror(sock_errno),
            sanitize_string(state->last_command_).c_str());

    if (len > 0) {
      // there's more data that needs to be written, delay closing the
      // connection
      state->close_when_drained_ = true;
    } else {
      listener->conns_.erase(id);
    }
  }
}

void CommandListener::processCommand(struct bufferevent* bev,
                                     const char* command_line,
                                     const bool is_localhost,
                                     ConnectionState* state) {
  auto start_time = std::chrono::steady_clock::now();
  ld_debug("Processing command: %s", sanitize_string(command_line).c_str());

  struct evbuffer* output = LD_EV(bufferevent_get_output)(bev);
  std::vector<std::string> args;
  try {
    args = boost::program_options::split_unix(command_line);
  } catch (boost::escaped_list_error& e) {
    LD_EV(evbuffer_add_printf)
    (output, "Failed to split: %s\r\nEND\r\n", e.what());
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got bad admin command (split failed) from %s: %s",
                   state->address_.toString().c_str(),
                   sanitize_string(command_line).c_str());
    return;
  }

  auto command = command_factory_->get(args, output);

  if (!command) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got bad admin command (unknown command) from %s: %s",
                   state->address_.toString().c_str(),
                   sanitize_string(command_line).c_str());
    return;
  }

  // Enforce restriction level of admin command
  switch (command->getRestrictionLevel()) {
    case AdminCommand::RestrictionLevel::UNRESTRICTED:
      break;
    case AdminCommand::RestrictionLevel::LOCALHOST_ONLY:
      if (!is_localhost) {
        LD_EV(evbuffer_add_printf)
        (output, "Permission denied: command is localhost-only!\r\nEND\r\n");
        RATELIMIT_INFO(
            std::chrono::seconds(10),
            2,
            "Localhost-only admin command called from non-localhost %s: %s",
            state->address_.toString().c_str(),
            sanitize_string(command_line).c_str());
        return;
      }
      break;
  }

  command->setServer(server_);
  command->setOutput(output);

  boost::program_options::options_description options;
  boost::program_options::positional_options_description positional;
  namespace style = boost::program_options::command_line_style;

  try {
    command->getOptions(options);
    command->getPositionalOptions(positional);
    boost::program_options::variables_map vm;
    boost::program_options::store(
        boost::program_options::command_line_parser(args)
            .options(options)
            .positional(positional)
            .style(style::unix_style & ~style::allow_guessing)
            .run(),
        vm);
    boost::program_options::notify(vm);
  } catch (boost::program_options::error& e) {
    LD_EV(evbuffer_add_printf)(output, "Options error: %s\r\n", e.what());
    std::string usage = command->getUsage();
    if (!usage.empty()) {
      LD_EV(evbuffer_add_printf)(output, "USAGE %s\r\n", usage.c_str());
    }
    LD_EV(evbuffer_add_printf)(output, "END\r\n");
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got bad admin command (bad options: %s) from %s: %s",
                   e.what(),
                   state->address_.toString().c_str(),
                   sanitize_string(command_line).c_str());
    return;
  }

  struct evbuffer* tmp = LD_EV(evbuffer_new());
  command->setOutput(tmp);
  command->run();

  LD_EV(evbuffer_add_printf)(tmp, "END\r\n");
  size_t output_size = LD_EV(evbuffer_get_length)(tmp);
  LD_EV(evbuffer_add_buffer(output, tmp));
  LD_EV(evbuffer_free(tmp));

  auto duration = std::chrono::steady_clock::now() - start_time;
  ld_log(duration > std::chrono::milliseconds(50) ? dbg::Level::INFO
                                                  : dbg::Level::DEBUG,
         "Admin command from %s took %.3f seconds to output %lu bytes: %s",
         state->address_.toString().c_str(),
         std::chrono::duration_cast<std::chrono::duration<double>>(duration)
             .count(),
         output_size,
         sanitize_string(command_line).c_str());
}

bool CommandListener::upgradeToSSL(ConnectionState* state) {
  // clear existing callback from original bufferevent
  LD_EV(bufferevent_setcb)(state->bev_, nullptr, nullptr, nullptr, nullptr);

  auto ctx = ssl_fetcher_.getSSLContext(true, true, false);
  if (!ctx) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "Invalid SSLContext, can't create SSL socket for client %s",
                    state->address_.toString().c_str());
    return false;
  }

  SSL* ssl = ctx->createSSL();
  if (!ssl) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "Null SSL* returned, can't create SSL socket for client %s",
                    state->address_.toString().c_str());
    return false;
  }

  struct bufferevent* bev = nullptr;
  // wrap original bufferevent in a bufferevent_openssl_filter
  bev = bufferevent_openssl_filter_new(loop_->getEventBase(),
                                       state->bev_,
                                       ssl,
                                       BUFFEREVENT_SSL_ACCEPTING,
                                       BEV_OPT_CLOSE_ON_FREE);
  ld_check(bufferevent_get_openssl_error(bev) == 0);
#if LIBEVENT_VERSION_NUMBER >= 0x02010100
  bufferevent_openssl_set_allow_dirty_shutdown(bev, 1);
#endif

  // replace bufferevent in ConnectionState, set ssl boolean and
  // set appropriate callbacks
  state->bev_ = bev;
  state->type_ = ConnectionType::ENCRYPTED;
  LD_EV(bufferevent_setcb)
  (state->bev_,
   CommandListener::readCallback,
   CommandListener::writeCallback,
   CommandListener::eventCallback,
   state);
  LD_EV(bufferevent_enable)(state->bev_, EV_READ | EV_WRITE);
  return true;
}

}} // namespace facebook::logdevice
