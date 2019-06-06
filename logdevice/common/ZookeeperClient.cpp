/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ZookeeperClient.h"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <new>

#include <boost/filesystem.hpp>
#include <folly/futures/Future.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

ZookeeperClient::ZookeeperClient(std::string quorum,
                                 std::chrono::milliseconds session_timeout)
    : ZookeeperClientBase(quorum), session_timeout_(session_timeout) {
  ld_check(session_timeout.count() > 0);

  zoo_set_log_stream(fdopen(dbg::getFD(), "w"));
  int rv = reconnect(nullptr);
  if (rv != 0) {
    ld_check(err != E::STALE);
    throw ConstructorFailed();
  }

  ld_check(zh_.get()); // initialized by reconnect()
}

ZookeeperClient::~ZookeeperClient() {
  zh_.update(nullptr);
}

int ZookeeperClient::reconnect(zhandle_t* prev) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (prev && zh_.get().get() != prev) {
    err = E::STALE;
    return -1;
  }

  ld_check(!prev || zoo_state(prev) == ZOO_EXPIRED_SESSION_STATE);

  // prev will not be leaked. If zookeeper_init() below succeeds,
  // zookeeper_close() will be called on prev in zh_.update() before
  // the function returns. Otherwise prev will remain owned
  // by this ZookeeperClient through zh_.

  zhandle_t* next = zookeeper_init(quorum_.c_str(),
                                   &ZookeeperClient::sessionWatcher,
                                   session_timeout_.count(),
                                   nullptr,
                                   this,
                                   0);
  if (!next) {
    switch (errno) {
      case ENOMEM:
        throw std::bad_alloc();
      case EMFILE:
      case ENFILE: // failed to create a pipe to ZK thread
        err = E::SYSLIMIT;
        break;
      case EINVAL:
        err = E::INVALID_PARAM;
        break;
      default:
        ld_error("Unexpected errno value %d (%s) from zookeeper_init()",
                 errno,
                 strerror(errno));
        err = E::INTERNAL;
    }
    return -1;
  }

  zh_.update(std::shared_ptr<zhandle_t>(
      next, [](zhandle_t* zh) { zookeeper_close(zh); }));
  return 0;
}

std::string ZookeeperClient::stateString(int state) {
  if (state == ZOO_CONNECTING_STATE) {
    return "ZOO_CONNECTING_STATE";
  }
  if (state == ZOO_ASSOCIATING_STATE) {
    return "ZOO_ASSOCIATING_STATE";
  }
  if (state == ZOO_CONNECTED_STATE) {
    return "ZOO_CONNECTED_STATE";
  }
  if (state == ZOO_EXPIRED_SESSION_STATE) {
    return "ZOO_EXPIRED_SESSION_STATE";
  }
  if (state == ZOO_AUTH_FAILED_STATE) {
    return "ZOO_AUTH_FAILED_STATE";
  }
  char buf[64];
  snprintf(buf, sizeof buf, "unknown state (%d)", state);
  return buf;
}

void ZookeeperClient::setDebugLevel(dbg::Level loglevel) {
  ::ZooLogLevel zdl;

  switch (loglevel) {
    case dbg::Level::CRITICAL:
      zdl = ZOO_LOG_LEVEL_ERROR;
      break;
    case dbg::Level::ERROR:
      zdl = ZOO_LOG_LEVEL_ERROR;
      break;
    case dbg::Level::WARNING:
      zdl = ZOO_LOG_LEVEL_WARN;
      break;
    case dbg::Level::NOTIFY:
      zdl = ZOO_LOG_LEVEL_INFO;
      break;
    case dbg::Level::INFO:
      zdl = ZOO_LOG_LEVEL_INFO;
      break;
    case dbg::Level::DEBUG:
      zdl = ZOO_LOG_LEVEL_DEBUG;
      break;
    case dbg::Level::SPEW:
      zdl = ZOO_LOG_LEVEL_DEBUG;
      break;
    default:
      ld_error("Unknown loglevel: %u. Not changing Zookeeper debug level.",
               (unsigned)loglevel);
      return;
  }

  zoo_set_debug_level(zdl);
}

void ZookeeperClient::sessionWatcher(zhandle_t* zh,
                                     int type,
                                     int /*state*/,
                                     const char* /*path*/,
                                     void* watcherCtx) {
  ld_check(zh);
  ld_check(type == ZOO_SESSION_EVENT); // this is the session watcher, don't
                                       // expect any other events

  int session_state = zoo_state(zh);
  ZookeeperClient* self = reinterpret_cast<ZookeeperClient*>(watcherCtx);

  ld_check(self);

  ld_info(
      "Zookeeper client entered state %s", stateString(session_state).c_str());

  if (session_state == ZOO_EXPIRED_SESSION_STATE) {
    ld_info("Session expired, reconnecting...");
    int rv = self->reconnect(zh);
    if (rv != 0 && err == E::STALE) {
      ld_info("zhandle %p in SessionExpired watch does not match current "
              "zhandle %p in ZookeeperClient. Probably session watcher was "
              "called twice for the same state transition.",
              zh,
              self->zh_.get().get());
    }
  }
}

int ZookeeperClient::state() {
  return zoo_state(zh_.get().get());
}

int ZookeeperClient::setData(const char* znode_path,
                             const char* znode_value,
                             int znode_value_size,
                             int version,
                             stat_completion_t completion,
                             const void* data) {
  return zoo_aset(zh_.get().get(),
                  znode_path,
                  znode_value,
                  znode_value_size,
                  version,
                  completion,
                  data);
}

int ZookeeperClient::getData(const char* znode_path,
                             data_completion_t completion,
                             const void* data) {
  return zoo_aget(
      zh_.get().get(), znode_path, /* watch = */ 0, completion, data);
}

int ZookeeperClient::multiOp(int count,
                             const zoo_op_t* ops,
                             zoo_op_result_t* results,
                             void_completion_t completion,
                             const void* data) {
  return zoo_amulti(zh_.get().get(), count, ops, results, completion, data);
}

/* static */ zk::Stat ZookeeperClient::toStat(const struct Stat* stat) {
  using namespace std::chrono;
  auto dur = std::chrono::duration_cast<system_clock::duration>(
      std::chrono::milliseconds{stat->mtime});
  SystemTimestamp mtime = system_clock::time_point{dur};
  return zk::Stat{.version_ = stat->version, .mtime_ = mtime};
}
/* static */
void ZookeeperClient::getDataCompletion(int rc,
                                        const char* value,
                                        int value_len,
                                        const struct Stat* stat,
                                        const void* context) {
  if (!context) {
    return;
  }
  auto callback = std::unique_ptr<data_callback_t>(const_cast<data_callback_t*>(
      reinterpret_cast<const data_callback_t*>(context)));
  ld_check(callback);
  // callback should not be empty
  ld_check(*callback);
  if (rc == ZOK) {
    ld_check_ge(value_len, 0);
    ld_check(stat);
    (*callback)(rc, {value, static_cast<size_t>(value_len)}, toStat(stat));
  } else {
    std::string s;
    (*callback)(rc, s, {});
  }
}

void ZookeeperClient::getData(std::string path, data_callback_t cb) {
  // Use the callback function object as context, which must be freed in
  // completion. The callback could also be empty.
  const void* context = nullptr;
  if (cb) {
    auto p = std::make_unique<data_callback_t>(std::move(cb));
    context = p.release();
  }
  int rc = zoo_aget(zh_.get().get(),
                    path.data(),
                    /* watch = */ 0,
                    &ZookeeperClient::getDataCompletion,
                    context);
  if (rc != ZOK) {
    getDataCompletion(rc,
                      /* value = */ nullptr,
                      /* value_len = */ 0,
                      /* stat = */ nullptr,
                      context);
  }
}

/* static */ void ZookeeperClient::existsCompletion(int rc,
                                                    const struct Stat* stat,
                                                    const void* context) {
  if (!context) {
    return;
  }
  auto callback = std::unique_ptr<stat_callback_t>(const_cast<stat_callback_t*>(
      reinterpret_cast<const stat_callback_t*>(context)));
  ld_check(callback);
  // callback shouldn't be empty
  ld_check(*callback);
  if (rc == ZOK) {
    ld_check(stat);
    (*callback)(rc, toStat(stat));
  } else {
    (*callback)(rc, {});
  }
}

void ZookeeperClient::exists(std::string path, stat_callback_t cb) {
  const void* context = nullptr;
  if (cb) {
    auto p = std::make_unique<stat_callback_t>(std::move(cb));
    context = p.release();
  }
  int rc = zoo_aexists(zh_.get().get(),
                       path.data(),
                       /* watch = */ 0,
                       &ZookeeperClient::existsCompletion,
                       context);
  if (rc != ZOK) {
    existsCompletion(rc,
                     /* stat = */ nullptr,
                     context);
  }
}

/* static */ void ZookeeperClient::setDataCompletion(int rc,
                                                     const struct Stat* stat,
                                                     const void* context) {
  if (!context) {
    return;
  }
  auto callback = std::unique_ptr<stat_callback_t>(const_cast<stat_callback_t*>(
      reinterpret_cast<const stat_callback_t*>(context)));
  ld_check(callback);
  // callback shouldn't be empty
  ld_check(*callback);
  if (rc == ZOK) {
    ld_check(stat);
    (*callback)(rc, toStat(stat));
  } else {
    (*callback)(rc, {});
  }
}

void ZookeeperClient::setData(std::string path,
                              std::string data,
                              stat_callback_t cb,
                              zk::version_t base_version) {
  // Use the callback function object as context, which must be freed in
  // completion. The callback could also be nullptr.
  const void* context = nullptr;
  if (cb) {
    auto p = std::make_unique<stat_callback_t>(std::move(cb));
    context = p.release();
  }
  int rc = zoo_aset(zh_.get().get(),
                    path.data(),
                    data.data(),
                    data.size(),
                    base_version,
                    &ZookeeperClient::setDataCompletion,
                    context);
  if (rc != ZOK) {
    setDataCompletion(rc, /* stat = */ nullptr, context);
  }
}

/* static */ void
ZookeeperClient::setCACL(const std::vector<zk::ACL>& src,
                         ::ACL_vector* c_acl_vector,
                         c_acl_vector_data_t* c_acl_vector_data) {
  ld_check(c_acl_vector);
  ld_check(c_acl_vector_data);
  c_acl_vector->count = src.size();
  c_acl_vector_data->reserve(src.size());
  for (const zk::ACL& acl : src) {
    // const_cast note: Zookeeper does not modify the scheme_ or id_.
    // However the C struct Id contains char* instead of const char*.
    c_acl_vector_data->emplace_back(
        ::ACL{acl.perms_,
              ::Id{.scheme = const_cast<char*>(acl.id_.scheme_.data()),
                   .id = const_cast<char*>(acl.id_.id_.data())}});
  }
  c_acl_vector->data = c_acl_vector_data->data();
}

/* static */ int ZookeeperClient::preparePathBuffer(const std::string& path,
                                                    int32_t flags,
                                                    std::string* path_buffer) {
  ld_check(path_buffer);
  // The resulting path could include a 10-digit sequence number (which could
  // overflow, resulting in <path>-2147483647). Hence we reserve 11 bytes. 1
  // extra for '\0'.
  //
  // Technically if the path_buffer is not sufficiently long, the returned path
  // will be truncated and no error would ensue. It's unlikely to be helpful to
  // return a truncated znode path to the user though, hence we reserve enough
  // space here.
  size_t max_znode_path_size =
      path.size() + ((flags & ::ZOO_SEQUENCE) ? 11 : 0) + 1;
  ld_check_le(max_znode_path_size, std::numeric_limits<int>::max());
  path_buffer->resize(max_znode_path_size);
  return static_cast<int>(max_znode_path_size);
}

/* static */ void ZookeeperClient::createCompletion(int rc,
                                                    const char* path,
                                                    const void* context) {
  if (!context) {
    return;
  }

  std::unique_ptr<CreateContext> ctx(
      static_cast<CreateContext*>(const_cast<void*>(context)));
  if (!ctx->cb_) {
    return;
  }

  if (rc == ZOK) {
    ctx->cb_(rc, std::string(path));
  } else {
    ctx->cb_(rc, "");
  }
}

void ZookeeperClient::create(std::string path,
                             std::string data,
                             create_callback_t cb,
                             std::vector<zk::ACL> acl,
                             int32_t flags) {
  // All ancestors exist / were created, now create the leaf node
  auto p = std::make_unique<CreateContext>(std::move(cb), std::move(acl));
  setCACL(p->acl_,
          std::addressof(p->c_acl_vector_),
          std::addressof(p->c_acl_vector_data_));
  preparePathBuffer(path, flags, std::addressof(p->path_buffer_));
  CreateContext* context = p.release();
  int rc = zoo_acreate(zh_.get().get(),
                       path.data(),
                       /* value = */ data.data(),
                       /* valuelen = */ data.size(),
                       std::addressof(p->c_acl_vector_),
                       flags,
                       &ZookeeperClient::createCompletion,
                       static_cast<const void*>(context));
  if (rc != ZOK) {
    createCompletion(rc, /* path = */ "", static_cast<const void*>(context));
  }
}

/* static */ void ZookeeperClient::multiOpCompletion(int rc,
                                                     const void* context) {
  ld_check(context);
  std::unique_ptr<MultiOpContext> ctx(
      static_cast<MultiOpContext*>(const_cast<void*>(context)));
  if (!ctx->cb_) {
    return;
  }
  auto results =
      ZookeeperClient::MultiOpContext::toOpResponses(ctx->c_results_);
  ctx->cb_(rc, std::move(results));
}

void ZookeeperClient::multiOp(std::vector<zk::Op> ops, multi_op_callback_t cb) {
  int count = ops.size();
  if (count == 0) {
    cb(ZOK, {});
    return;
  }

  auto p = std::make_unique<MultiOpContext>(std::move(ops), std::move(cb));
  MultiOpContext* context = p.release();
  int rc = zoo_amulti(zh_.get().get(),
                      count,
                      context->c_ops_.data(),
                      context->c_results_.data(),
                      &ZookeeperClient::multiOpCompletion,
                      static_cast<const void*>(context));
  if (rc != ZOK) {
    multiOpCompletion(rc, static_cast<const void*>(context));
  }
}

/* static */ void ZookeeperClient::syncCompletion(int rc,
                                                  const char* /* unused */,
                                                  const void* context) {
  if (!context) {
    return;
  }
  auto callback = std::unique_ptr<sync_callback_t>(const_cast<sync_callback_t*>(
      reinterpret_cast<const sync_callback_t*>(context)));
  ld_check(callback);
  // callback shouldn't be empty
  ld_check(*callback);
  (*callback)(rc);
}

void ZookeeperClient::sync(sync_callback_t cb) {
  // Use the callback function object as context, which must be freed in
  // completion. The callback could also be nullptr.
  const void* context = nullptr;
  if (cb) {
    auto p = std::make_unique<sync_callback_t>(std::move(cb));
    context = static_cast<const void*>(p.release());
  }
  int rc = zoo_async(zh_.get().get(),
                     "/", // bogus
                     &ZookeeperClient::syncCompletion,
                     context);
  if (rc != ZOK) {
    syncCompletion(rc, /* value */ nullptr, context);
  }
}

/* static */ zk::OpResponse ZookeeperClient::MultiOpContext::toOpResponse(
    const zoo_op_result_t& op_result) {
  zk::OpResponse r{};
  r.rc_ = op_result.err;
  if (op_result.value) {
    r.value_ = std::string(op_result.value, op_result.valuelen);
  }
  if (op_result.stat) {
    r.stat_ = toStat(op_result.stat);
  }
  return r;
}

/* static */ std::vector<zk::OpResponse>
ZookeeperClient::MultiOpContext::toOpResponses(
    const folly::small_vector<zoo_op_result_t, kInlineOps>& op_results) {
  std::vector<zk::OpResponse> results;
  results.reserve(op_results.size());
  std::transform(op_results.begin(),
                 op_results.end(),
                 std::back_inserter(results),
                 toOpResponse);
  return results;
}

void ZookeeperClient::MultiOpContext::addCCreateOp(const zk::Op& op,
                                                   size_t index) {
  zoo_op_t& c_op = c_ops_.at(index);
  auto& create_op = boost::get<zk::detail::CreateOp>(op.op_);

  auto& c_acl_vector = c_acl_vectors_.at(index);
  auto& c_acl_vector_data = c_acl_vector_data_.at(index);
  setCACL(create_op.acl_,
          std::addressof(c_acl_vector),
          std::addressof(c_acl_vector_data));

  auto& path_buffer = path_buffers_.at(index);
  int max_znode_path_size = preparePathBuffer(
      create_op.path_, create_op.flags_, std::addressof(path_buffer));

  zoo_create_op_init(std::addressof(c_op),
                     create_op.path_.data(),
                     create_op.data_.data(),
                     create_op.data_.size(),
                     std::addressof(c_acl_vector),
                     create_op.flags_,
                     const_cast<char*>(path_buffer.data()),
                     max_znode_path_size);
}

void ZookeeperClient::MultiOpContext::addCDeleteOp(const zk::Op& op,
                                                   size_t index) {
  zoo_op_t& c_op = c_ops_.at(index);
  auto& delete_op = boost::get<zk::detail::DeleteOp>(op.op_);
  zoo_delete_op_init(
      std::addressof(c_op), delete_op.path_.data(), delete_op.version_);
}

void ZookeeperClient::MultiOpContext::addCSetOp(const zk::Op& op,
                                                size_t index) {
  zoo_op_t& c_op = c_ops_.at(index);
  auto& set_op = boost::get<zk::detail::SetOp>(op.op_);
  auto& c_stat = c_stats_.at(index);
  zoo_set_op_init(std::addressof(c_op),
                  set_op.path_.data(),
                  set_op.data_.data(),
                  set_op.data_.size(),
                  set_op.version_,
                  std::addressof(c_stat));
}

void ZookeeperClient::MultiOpContext::addCCheckOp(const zk::Op& op,
                                                  size_t index) {
  zoo_op_t& c_op = c_ops_.at(index);
  auto& check_op = boost::get<zk::detail::CheckOp>(op.op_);
  zoo_check_op_init(
      std::addressof(c_op), check_op.path_.data(), check_op.version_);
}

void ZookeeperClient::MultiOpContext::addCOp(const zk::Op& op, size_t index) {
  switch (op.getType()) {
    case zk::Op::Type::CREATE: {
      addCCreateOp(op, index);
      break;
    }
    case zk::Op::Type::DELETE: {
      addCDeleteOp(op, index);
      break;
    }
    case zk::Op::Type::SET: {
      addCSetOp(op, index);
      break;
    }
    case zk::Op::Type::CHECK: {
      addCCheckOp(op, index);
      break;
    }
    case zk::Op::Type::NONE:
    default:
      ld_error("Zookeeper multi_op included NONE op.");
  };
}

/* static */ std::vector<std::string>
ZookeeperClient::getAncestorPaths(const std::string& path) {
  std::vector<std::string> ancestor_paths;
  std::string path_string;
  boost::filesystem::path fp{path};

  fp = fp.parent_path();
  while (!fp.empty() && ((path_string = fp.string()) != "/")) {
    ancestor_paths.emplace_back(path_string);
    fp = fp.parent_path();
  }
  return ancestor_paths;
}

/* static */ int ZookeeperClient::aggregateCreateAncestorResults(
    folly::Try<std::vector<folly::Try<CreateResult>>>&& t) {
  auto handle_exception = [](folly::exception_wrapper& ew) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Unexpected error from Zookeeper createWithAncestors: %s",
                    ew.what().c_str());
  };
  if (FOLLY_UNLIKELY(t.hasException())) {
    handle_exception(t.exception());
    return ZAPIERROR;
  }
  auto results = std::move(t).value();
  for (auto& r : results) {
    if (FOLLY_UNLIKELY(r.hasException())) {
      handle_exception(r.exception());
      return ZAPIERROR;
    }
    // success: znode created or the ancestor znode already exists
    if ((r->rc_ != ZOK && r->rc_ != ZNODEEXISTS)) {
      return r->rc_;
    }
  }
  return ZOK;
}

void ZookeeperClient::createWithAncestors(std::string path,
                                          std::string data,
                                          create_callback_t cb,
                                          std::vector<zk::ACL> acl,
                                          int32_t flags) {
  auto ancestor_paths = getAncestorPaths(path);

  std::vector<folly::SemiFuture<CreateResult>> futures;
  futures.reserve(ancestor_paths.size());
  std::transform(
      ancestor_paths.rbegin(),
      ancestor_paths.rend(),
      std::back_inserter(futures),
      [this, acl](std::string& ancestor_path) mutable {
        auto promise = std::make_unique<folly::Promise<CreateResult>>();
        auto fut = promise->getSemiFuture();
        this->create(
            ancestor_path,
            /* data = */ "",
            [promise = std::move(promise)](int rc, std::string resulting_path) {
              CreateResult res;
              res.rc_ = rc;
              res.path_ = (rc == ZOK) ? std::move(resulting_path) : "";
              promise->setValue(std::move(res));
            },
            std::move(acl),
            /* flags = */ 0);
        return fut;
      });

  folly::collectAllSemiFuture(std::move(futures))
      .toUnsafeFuture()
      .thenTry(
          [this,
           path = std::move(path),
           data = std::move(data),
           cb = std::move(cb),
           acl = std::move(acl),
           flags](
              folly::Try<std::vector<folly::Try<CreateResult>>>&& t) mutable {
            int rc = aggregateCreateAncestorResults(std::move(t));
            if (rc != ZOK) {
              if (cb) {
                cb(rc, /* path = */ "");
              }
              return;
            }

            this->create(std::move(path),
                         std::move(data),
                         std::move(cb),
                         std::move(acl),
                         flags);
          });
}

}} // namespace facebook::logdevice
