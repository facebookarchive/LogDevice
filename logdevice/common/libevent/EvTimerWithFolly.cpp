// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/libevent/EvTimerWithFolly.h"

#include <event.h>

namespace facebook { namespace logdevice {
int EvTimerWithFolly::setPriority(int pri) {
  return event_priority_set(getEvent(), pri);
}
void EvTimerWithFolly::activate(int res, short ncalls) {
  event_active(getEvent(), res, ncalls);
}
}} // namespace facebook::logdevice
