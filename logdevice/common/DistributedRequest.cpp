#include "logdevice/common/DistributedRequest.h"

namespace facebook { namespace logdevice {

DistributedRequest::~DistributedRequest() {}

FailedShardsMap DistributedRequest::getFailedShards(Status status) const {
  if (status == Status::OK || !nodeset_accessor_) {
    return FailedShardsMap{};
  }

  return nodeset_accessor_->getFailedShards(
      [](Status s) -> bool { return s != Status::OK; });
}

}} // namespace facebook::logdevice
