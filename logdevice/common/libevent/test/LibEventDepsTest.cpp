#include "logdevice/common/libevent/LibEventDeps.h"

#include <gtest/gtest.h>

namespace {

class LibEventDepsTest : public ::testing::Test {};

} // namespace

namespace facebook { namespace logdevice {

TEST(LibEventDepsTest, ConstructFree) {
  using S = LibEventDeps::Status;
  LibEventDeps deps;
  EXPECT_EQ(S::OK, deps.free());
  EXPECT_EQ(nullptr, deps.getBaseDEPRECATED());
  EXPECT_EQ(S::NOT_INITIALIZED, deps.loopOnce());
  EXPECT_EQ(S::OK, deps.init());
  EXPECT_NE(nullptr, deps.getBaseDEPRECATED());
  EXPECT_EQ(S::ALREADY_INITIALIZED, deps.init());
  EXPECT_EQ(S::OK, deps.free());
  EXPECT_EQ(nullptr, deps.getBaseDEPRECATED());
  EXPECT_EQ(S::OK, deps.init());
  EXPECT_NE(nullptr, deps.getBaseDEPRECATED());
}
}} // namespace facebook::logdevice
