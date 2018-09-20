/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef NODE_LOCATION_SCOPE
#error NODE_LOCATION_SCOPE() macro not defined
#define NODE_LOCATION_SCOPE(...)
#endif

// a special scope indicating the smallest scope: an individual node
NODE_LOCATION_SCOPE(NODE)

// actual (non-special) scopes representing the location of a node
NODE_LOCATION_SCOPE(RACK)
NODE_LOCATION_SCOPE(ROW)
NODE_LOCATION_SCOPE(CLUSTER)
NODE_LOCATION_SCOPE(DATA_CENTER)
NODE_LOCATION_SCOPE(REGION)

// the root scope is a special scope which guarantees to cover
// all other scopes defined
NODE_LOCATION_SCOPE(ROOT)

#undef NODE_LOCATION_SCOPE
