/**
 * Copyright (c) 2019-present, CCBFT, Inc. and its affiliates.
 * All rights reserved.
 */

#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define LD_C_DECLARE_HANDLE(SOME_TYPE)            \
  struct V##SOME_TYPE;                            \
  typedef struct V##SOME_TYPE V##SOME_TYPE;       \
  typedef V##SOME_TYPE* P##SOME_TYPE;             \
  typedef const V##SOME_TYPE CV##SOME_TYPE;       \
  typedef CV##SOME_TYPE* PC##SOME_TYPE;

// UInt to UInt hashmap
// Used to store logid => lsn
LD_C_DECLARE_HANDLE(UIntHashMap)

LD_C_DECLARE_HANDLE(UIntMap)

// List of UInt
LD_C_DECLARE_HANDLE(UIntList)

// Vec of UInt
LD_C_DECLARE_HANDLE(UIntVec)
LD_C_DECLARE_HANDLE(String)


LD_C_DECLARE_HANDLE(Client)
LD_C_DECLARE_HANDLE(ClientSettings)
LD_C_DECLARE_HANDLE(Reader)
LD_C_DECLARE_HANDLE(DataRecord)
LD_C_DECLARE_HANDLE(GapRecord)

typedef uint64_t logid_t;
typedef uint64_t lsn_t;
typedef uint32_t ld_err;

size_t ld_string_size(PString str);
ld_err ld_string_to_buffer(PString str, char* buffer, uint64_t buf_len);

size_t ld_uint_hashmap_size(PUIntHashMap hashmap);
ld_err ld_uint_hashmap_get_at(PUIntHashMap hashmap, uint64_t key, uint64_t* val);

size_t ld_uint_map_size(PUIntMap map);
ld_err ld_uint_map_get_at(PUIntMap map, uint64_t key, uint64_t* val);

size_t ld_uint_list_size(PUIntList hashmap);
ld_err ld_uint_list_get_at(PUIntList hashmap, uint64_t index, uint64_t* val);
ld_err ld_uint_list_append(PUIntList hashmap, uint64_t val);
ld_err ld_uint_list_remove_head(PUIntList hashmap, uint64_t val);

size_t ld_uint_vec_size(PUIntVec hashmap);
ld_err ld_uint_vec_get_at(PUIntVec hashmap, uint64_t* val);
ld_err ld_uint_vec_set_at(PUIntVec hashmap, uint64_t index, uint64_t val);


#ifdef __cplusplus
}
#endif
