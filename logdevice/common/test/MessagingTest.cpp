/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <chrono>
#include <memory>
#include <stdio.h>
#include <unistd.h>

#include <folly/Memory.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/ConnectThrottle.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/HELLO_Message.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

static Semaphore wait_sem;

struct DUMMY_Header {
  bool send_another;
} __attribute__((__packed__));

// message type not relevant as the message won't actually get transmitted
using DUMMY_Message =
    FixedSizeMessage<DUMMY_Header, MessageType::HELLO, TrafficClass::HANDSHAKE>;

template <>
Message::Disposition DUMMY_Message::onReceived(const Address& /*from*/) {
  return Disposition::NORMAL;
}

template <>
void DUMMY_Message::onSent(Status, const Address& to) const {
  if (header_.send_another) {
    auto msg = std::make_unique<DUMMY_Message>(DUMMY_Header{false});
    Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
    // shouldn't trip an assert
    wait_sem.post();
  }
}

struct DummySocketCallback : public SocketCallback {
  void operator()(Status /*st*/, const Address& to) override {
    auto msg = std::make_unique<DUMMY_Message>(DUMMY_Header{false});
    Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
    // shouldn't trip an assert
    wait_sem.post();
    delete this;
  }
};

}} // namespace facebook::logdevice

using namespace facebook::logdevice;

// Wrap test infrastructure in an anonymous namespace to prevent ODR issues.
namespace {

static const int TEST_PAYLOAD = 42;

TEST(MessagingTest, Address) {
  ClientID cid(1);
  Address caddr(cid);

  NodeID nid(0x7fff, 3);
  Address saddr(nid);

  Address invaddr(ClientID::INVALID);

  EXPECT_TRUE(caddr.isClientAddress());
  EXPECT_FALSE(saddr.isClientAddress());
  EXPECT_TRUE(invaddr.isClientAddress()); // ClientID::INVALID is still
                                          // considered a client address
  EXPECT_TRUE(caddr.valid());
  EXPECT_TRUE(saddr.valid());
  EXPECT_FALSE(invaddr.valid());
}

// a test request whose execute() method counts the number of times it's
// called and the total payload size
struct CountingRequest : public Request {
  explicit CountingRequest(int payload)
      : Request(RequestType::TEST_MESSAGING_COUNTING_REQUEST),
        payload_(payload) {}

  Request::Execution execute() override {
    std::shared_ptr<Configuration> cfg = Worker::getConfig();
    EXPECT_TRUE((bool)cfg);

    CountingRequest::n_requests_executed++;
    CountingRequest::payload_sum += payload_;
    return Execution::COMPLETE;
  }

  int payload_;

  static int n_requests_executed;
  static int payload_sum;
};

int CountingRequest::n_requests_executed;
int CountingRequest::payload_sum;

/**
 * Posts a request. Waits for the Worker thread to exit for up to 10s.
 * Verifies that request was processed.
 */
TEST(MessagingTest, EventLoop) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  auto processor = make_test_processor(settings);

  std::unique_ptr<Request> rq = std::make_unique<CountingRequest>(TEST_PAYLOAD);

  auto& loops = processor->getEventLoops();
  ASSERT_EQ(loops.size(), 1);
  auto& l = loops[0];

  EXPECT_EQ(0, processor->postRequest(rq));
  EXPECT_EQ(nullptr, rq.get());

  ASSERT_NE(std::this_thread::get_id(), l->getThread().get_id());

  // since no EventLoop is running on this thread
  ASSERT_EQ(nullptr, EventLoop::onThisThread());

  {
    // kill the process if thread does not exit in 10s
    Alarm alarm(std::chrono::seconds(10));
    processor.reset();
  }

  EXPECT_EQ(1, CountingRequest::n_requests_executed);
  EXPECT_EQ(TEST_PAYLOAD, CountingRequest::payload_sum);
}

// a test request whose execute() sleeps for a second to cause .request_pipe_
// to overflow
struct MessagingTest_SlowRequest : public Request {
  MessagingTest_SlowRequest()
      : Request(RequestType::TEST_MESSAGING_SLOW_REQUEST) {}

  Request::Execution execute() override {
    if (stall.load()) {
      ld_debug("Sleeping for 1s");
      /* sleep override */
      sleep(1);
      stall.store(false);
    }
    return Execution::COMPLETE;
  }

  static std::atomic<bool> stall;
};

using SlowRequest = MessagingTest_SlowRequest;

std::atomic<bool> SlowRequest::stall{true};

class MockConnectThrottle : public ConnectThrottle {
 public:
  explicit MockConnectThrottle(
      chrono_expbackoff_t<std::chrono::milliseconds> backoff_settings)
      : ConnectThrottle(std::move(backoff_settings)),
        current_(std::chrono::steady_clock::now()) {}

  std::chrono::steady_clock::time_point now() const override {
    return current_;
  }

  // simulate passing of time
  template <typename Duration>
  void sleep(const Duration& duration) {
    current_ += duration;
  }

  std::chrono::steady_clock::time_point current_;
};

/**
 * Creates a ConnectThrottle and verifies that mayConnect() reports results
 * as expected for a given sequence of connectSucceeded()/Failed() calls.
 */
TEST(MessagingTest, ConnectThrottle) {
  Settings settings = create_default_settings<Settings>();
  settings.connect_throttle =
      chrono_expbackoff_t<std::chrono::milliseconds>(1, 10000, 2u);
  MockConnectThrottle ct(settings.connect_throttle);

  dbg::currentLevel = dbg::Level::DEBUG;

  EXPECT_TRUE(ct.mayConnect());

  ct.connectSucceeded();

  EXPECT_TRUE(ct.mayConnect());

  ct.connectFailed(); // 1st failure

  EXPECT_FALSE(ct.mayConnect());

  ct.sleep(std::chrono::microseconds(1500));

  EXPECT_TRUE(ct.mayConnect());

  ct.connectFailed(); // 2nd failure

  ct.sleep(std::chrono::milliseconds(1));

  EXPECT_FALSE(ct.mayConnect());

  ct.sleep(std::chrono::microseconds(1500));

  EXPECT_TRUE(ct.mayConnect());

  for (int i = 0; i < 8; i++) {
    ct.connectFailed(); // 3rd through 10th failures
  }

  ct.sleep(std::chrono::milliseconds(500));

  EXPECT_FALSE(ct.mayConnect());

  ct.sleep(std::chrono::milliseconds(600));

  EXPECT_TRUE(ct.mayConnect()); // >1024ms passed

  for (int i = 0; i < 1000; i++) {
    ct.connectFailed(); // 1000 more failures
  }

  EXPECT_TRUE(ct.downUntil() <=
              ct.current_ + settings.connect_throttle.max_delay);
  ct.connectFailed();

  ct.connectSucceeded();

  EXPECT_TRUE(ct.mayConnect()); // may connect immediately upon success

  dbg::currentLevel = dbg::Level::ERROR;
}

/**
 * Simple request that naps for a second then produces very important output.
 */
struct SleepingRequest : public Request {
  SleepingRequest(int* outp, int val)
      : Request(RequestType::TEST_MESSAGING_SLEEPING_REQUEST),
        outp_(outp),
        val_(val) {}

  Request::Execution execute() override {
    sleep(1);
    *outp_ = val_;
    return Execution::COMPLETE;
  }

  int *outp_, val_;
};

/**
 * Exercises blockingRequest().
 */
TEST(MessagingTest, BlockingRequest) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  auto p = make_test_processor(settings);

  int val = 0;
  const int after_val = 0xFACE;
  std::unique_ptr<Request> rq =
      std::make_unique<SleepingRequest>(&val, after_val);

  Alarm alarm(DEFAULT_TEST_TIMEOUT);

  ASSERT_EQ(0, p->blockingRequest(rq));

  // Verify that the Request updated val.  If the above call was a
  // non-blocking postRequest(), this check would almost certainly execute
  // while SleepingRequest was still sleeping and before it got a chance to
  // update val.
  EXPECT_EQ(after_val, val);
}

// blockingRequest() should not crash if the pipe is full
TEST(MessagingTest, BlockingRequestPostFail) {
  struct PausedRequest : public Request {
    PausedRequest(Semaphore* block_on_sem, Semaphore* signal_sem)
        : block_on_sem_(block_on_sem), signal_sem_(signal_sem) {}
    Request::Execution execute() override {
      signal_sem_->post();
      block_on_sem_->wait();
      return Execution::COMPLETE;
    }
    Semaphore* block_on_sem_;
    Semaphore* signal_sem_;
  };

  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  settings.worker_request_pipe_capacity = 1;
  auto p = make_test_processor(settings);

  Semaphore block_on_sem;
  Semaphore signal_sem;
  Alarm alarm(DEFAULT_TEST_TIMEOUT);
  {
    // Post a sleeping request to tie up the worker
    std::unique_ptr<Request> rq =
        std::make_unique<PausedRequest>(&block_on_sem, &signal_sem);
    ASSERT_EQ(0, p->postImportant(rq));
    // Wait until it is being executed on the worker.
    signal_sem.wait();
  }

  // Fill up the pipe with non-blocking posts
  while (1) {
    std::unique_ptr<Request> rq = std::make_unique<CountingRequest>(1);
    if (p->postRequest(rq) != 0) {
      break;
    }
  }

  {
    // Now try a blocking post
    std::unique_ptr<Request> rq = std::make_unique<CountingRequest>(1);
    ASSERT_EQ(-1, p->blockingRequest(rq));
  }

  block_on_sem.post();
}

class OnClientCloseTestRequest : public Request {
 public:
  OnClientCloseTestRequest(int fd, ClientID cid)
      : Request(RequestType::TEST_MESSAGING_ON_CLIENT_CLOSE_REQUEST),
        sock_(fd),
        cid_(cid) {}

  Request::Execution execute() override {
    char port[32]; // use client id as port number
    snprintf(port, sizeof(port), "%d", cid_.getIdx());

    Worker* w = Worker::onThisThread();
    int rv = w->sender().addClient(sock_,
                                   Sockaddr("127.0.0.1", port),
                                   ResourceBudget::Token(),
                                   SocketType::DATA,
                                   ConnectionType::PLAIN);
    EXPECT_EQ(0, rv);

    rv = w->sender().registerOnSocketClosed(Address(cid_), *(new OnClose()));
    EXPECT_EQ(0, rv);

    rv = w->sender().registerOnSocketClosed(Address(cid_), *(new OnClose()));
    EXPECT_EQ(0, rv);

    const ClientID nonexisting_client(100);

    OnClose* cb2 = new OnClose();

    rv = w->sender().registerOnSocketClosed(Address(nonexisting_client), *cb2);

    delete cb2;

    EXPECT_EQ(-1, rv);
    EXPECT_EQ(E::NOTFOUND, err);

    return Execution::COMPLETE;
  }

  class OnClose : public SocketCallback {
   public:
    void operator()(Status /*st*/, const Address& /*name*/) override {
      OnClientCloseTestRequest::callbacksCnt++;
      delete this;
    }
  };

  static std::atomic<unsigned> callbacksCnt;

 private:
  int sock_; // read end of client "socket" fd
  ClientID cid_;
};

std::atomic<unsigned> OnClientCloseTestRequest::callbacksCnt;

/**
 * Create two connected pairs of local sockets to simulate client
 * connections. For each pair creates a Client socket with one of the
 * sockets and registers it with a Sender running on a Worker
 * thread. Register two onClose callbacks on each of those Sockets. Closes
 * the other ends of socketpairs. Expect all callbacks to be invoked.
 */
TEST(MessagingTest, OnClientClose) {
  const int nclients = 2;
  int clients[nclients][2]; // socketpairs

  Settings settings = create_default_settings<Settings>();
  settings.nagle = true; // to avoid a warning that TCP_NODELAY is unsupported
  settings.num_workers = 1;
  auto p = make_test_processor(settings);

  for (int i = 0; i < nclients; i++) {
    ClientID cid(i + 1);

    int rv = socketpair(AF_LOCAL, SOCK_STREAM, 0, clients[i]);
    ld_check(rv == 0);

    std::unique_ptr<Request> rq =
        std::make_unique<OnClientCloseTestRequest>(clients[i][0], cid);

    rv = p->blockingRequest(rq);
    EXPECT_EQ(0, rv);
    EXPECT_EQ(nullptr, rq);
  }

  for (int i = 0; i < nclients; i++) {
    close(clients[i][1]);
  }

  sleep(2);

  EXPECT_EQ(2 * nclients, OnClientCloseTestRequest::callbacksCnt);
}

// Verify that we don't hit an assert when sending a message from a socket
// close callback or the onSent callback that is issued during close.
TEST(MessagingTest, SendFromCallback) {
  NodeID target(0, 1);
  Configuration::Node node;
  // port nobody listens on hopefully
  node.address = Sockaddr("127.0.0.1", "65534"),
  node.gossip_address = Sockaddr("127.0.0.1", "65535"), node.generation = 1;
  node.addStorageRole();

  Configuration::NodesConfig nodes({{0, std::move(node)}});
  auto config =
      std::make_shared<UpdateableConfig>(std::make_shared<Configuration>(
          ServerConfig::fromDataTest(
              __FILE__, nodes, Configuration::MetaDataLogsConfig()),
          nullptr));

  struct SendRequest : public Request {
    explicit SendRequest(NodeID node)
        : Request(RequestType::TEST_MESSAGING_SEND_REQUEST), node_(node) {}

    Request::Execution execute() override {
      auto msg = std::make_unique<DUMMY_Message>(DUMMY_Header{true});
      int rv = Worker::onThisThread()->sender().sendMessage(
          std::move(msg), node_, new DummySocketCallback());
      EXPECT_EQ(0, rv) << "sendMessage() failed with err "
                       << errorStrings()[err].name;
      wait_sem.post();
      return Execution::COMPLETE;
    }

    NodeID node_;
  };

  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  // Prevent ConnectThrottle from synchronously failing our sendMessage() call
  settings.connect_throttle.max_delay = std::chrono::milliseconds::zero();
  auto p = make_test_processor(settings, config);

  std::unique_ptr<Request> rq = std::make_unique<SendRequest>(target);
  int rv = p->postRequest(rq);
  ASSERT_EQ(0, rv);

  for (int i = 0; i < 3; ++i) {
    wait_sem.wait();
  }
}

TEST(MessagingTest, ConnectionLimit) {
  ResourceBudget budget(1);
  Settings settings = create_default_settings<Settings>();
  settings.nagle = true;
  settings.num_workers = 1;
  settings.max_incoming_connections = 1;

  struct AddClientRequest : public Request {
    AddClientRequest(int fd, Semaphore& sem, ResourceBudget::Token token)
        : Request(RequestType::TEST_MESSAGING_ADD_CLIENT_REQUEST),
          fd_(fd),
          sem_(sem),
          token_(std::move(token)) {}
    Request::Execution execute() override {
      int rv = Worker::onThisThread()->sender().addClient(
          fd_,
          Sockaddr("127.0.0.1", folly::to<std::string>(fd_).c_str()),
          std::move(token_),
          SocketType::DATA,
          ConnectionType::PLAIN);
      EXPECT_EQ(0, rv);
      sem_.post();
      return Execution::COMPLETE;
    }
    int fd_;
    Semaphore& sem_;
    ResourceBudget::Token token_;
  };

  Semaphore sem;
  auto processor = make_test_processor(settings);

  int fds[2];
  int rv = socketpair(AF_LOCAL, SOCK_STREAM, 0, fds);
  ASSERT_EQ(0, rv);

  auto token1 = budget.acquireToken();
  ASSERT_TRUE(token1.valid());

  std::unique_ptr<Request> rq =
      std::make_unique<AddClientRequest>(fds[0], sem, std::move(token1));
  rv = processor->postRequest(rq);
  ASSERT_EQ(0, rv);
  sem.wait();

  // not accepting any more connections
  ASSERT_EQ(0, budget.available());

  processor.reset();
  ASSERT_EQ(1, budget.available());
}
} // namespace
