/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef DATA_CLASS
#error DATA_CLASS() macro not defined
#define DATA_CLASS(...)
#endif

DATA_CLASS(APPEND,  'A')   // Adding new records.
DATA_CLASS(REBUILD, 'R')   // Reading or writing a record to restore its
                           // replication factor.

#undef DATA_CLASS
