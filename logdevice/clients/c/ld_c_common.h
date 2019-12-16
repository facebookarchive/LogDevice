/**
 * Copyright (c) 2019-present, CCBFT, Inc. and its affiliates.
 * All rights reserved.
 */

#pragma once

#include <stdint.h>
#include <stddef.h>

#include "ld_c_err.h"

#ifdef __cplusplus
extern "C" {
#endif


// VSOME_TYPE is a fake type, there is no real definition of it
#define LD_C_DECLARE_HANDLE(A)    \
  struct V##A;                    \
  typedef struct V##A V##A;       \
  typedef V##A* P##A;             \
  typedef const V##A CV##A;       \
  typedef CV##A* PC##A;

#define LD_C_DECLARE_HANDLE_SHARED_PTR(A)             \
  struct V##A##SPtr;                                  \
  typedef struct V##A##SPtr V##A##SPtr;       \
  typedef V##A##SPtr* P##A##SPtr;             \
  typedef const V##A##SPtr CV##A##SPtr;       \
  typedef CV##A##SPtr* PC##A##SPtr;

#define LD_C_DECLARE_HANDLE_UNIQUE_PTR(A)             \
  struct V##A##UPtr;                                  \
  typedef struct V##A##UPtr V##A##UPtr;       \
  typedef V##A##UPtr* P##A##UPtr;             \
  typedef const V##A##UPtr CV##A##UPtr;       \
  typedef CV##A##UPtr* PC##A##UPtr;

#define LD_C_DECLARE_HANDLE_WEAK_PTR(A)               \
  struct V##A##WPtr;                                  \
  typedef struct V##A##WPtr V##A##WPtr;       \
  typedef V##A##WPtr* P##A##WPtr;             \
  typedef const V##A##WPtr CV##A##WPtr;       \
  typedef CV##A##WPtr* PC##A##WPtr;


#define DEREF_AS(B, A) (*reinterpret_cast<B*>(A))

// UInt to UInt hashmap
// Used to store logid => lsn
LD_C_DECLARE_HANDLE(UInt2UIntHashMap)

LD_C_DECLARE_HANDLE(UInt2UIntMap)

LD_C_DECLARE_HANDLE(Str2IntOrStrMap)

// List of UInt
LD_C_DECLARE_HANDLE(UIntList)

// Vec of UInt
LD_C_DECLARE_HANDLE(UIntVec)
LD_C_DECLARE_HANDLE(String)

LD_C_DECLARE_HANDLE(Dynamic)
LD_C_DECLARE_HANDLE_SHARED_PTR(Dynamic)

LD_C_DECLARE_HANDLE(Client)
LD_C_DECLARE_HANDLE_SHARED_PTR(Client)
LD_C_DECLARE_HANDLE(ClientSettings)
LD_C_DECLARE_HANDLE_SHARED_PTR(ClientSettings)
LD_C_DECLARE_HANDLE(Reader)
LD_C_DECLARE_HANDLE_SHARED_PTR(Reader)

LD_C_DECLARE_HANDLE(DataRecord)
LD_C_DECLARE_HANDLE_SHARED_PTR(DataRecord)
LD_C_DECLARE_HANDLE(GapRecord)
LD_C_DECLARE_HANDLE_SHARED_PTR(GapRecord)

typedef uint64_t ldc_logid_t;
typedef uint64_t ldc_lsn_t;

/* size_t ld_string_size(PString str); */
/* ld_err ld_make_empty_string(PString* list); */
/* ld_err ld_make_string_from_c_string(PString* list, const char* str); */
/* ld_err ld_free_string(PString list); */
/* ld_err ld_string_to_buffer(PString str, char* buffer, uint64_t buf_len); */

/* ld_err ld_make_uint_hashmap(PUInt2UIntHashMap* hashmap); */
/* ld_err ld_free_uint_hashmap(PUInt2UIntHashMap hashmap); */
/* size_t ld_uint_hashmap_size(PUInt2UIntHashMap hashmap); */
/* ld_err ld_uint_hashmap_get(PUInt2UIntHashMap hashmap, uint64_t key, uint64_t* val); */
/* ld_err ld_uint_hashmap_set(PUInt2UIntHashMap hashmap, uint64_t key, uint64_t val); */

/* ld_err ld_make_uint_map(PUInt2UIntMap* list); */
/* ld_err ld_free_uint_map(PUInt2UIntMap list); */
/* size_t ld_uint_map_size(PUInt2UIntMap map); */
/* ld_err ld_uint_map_get(PUInt2UIntMap map, uint64_t key, uint64_t* val); */
/* ld_err ld_uint_map_set(PUInt2UIntMap map, uint64_t key, uint64_t val); */

/* size_t ld_uint_list_size(PUIntList list); */
/* ld_err ld_make_uint_list(PUIntList* list); */
/* ld_err ld_free_uint_list(PUIntList list); */
/* ld_err ld_uint_list_get_at(PUIntList list, uint64_t index, uint64_t* val); */
/* ld_err ld_uint_list_push_back(PUIntList list, uint64_t val); */
/* ld_err ld_uint_list_pop_back(PUIntList list); */
/* ld_err ld_uint_list_push_front(PUIntList list, uint64_t val); */
/* ld_err ld_uint_list_pop_front(PUIntList list); */

/* size_t ld_uint_vec_size(PUIntVec vec); */
/* ld_err ld_make_uint_vec(PUIntVec* vec); */
/* ld_err ld_free_uint_vec(PUIntVec vec); */
/* ld_err ld_uint_vec_get_at(PUIntVec vec, uint64_t* val); */
/* ld_err ld_uint_vec_set_at(PUIntVec vec, uint64_t index, uint64_t val); */
/* ld_err ld_uint_vec_push_back(PUIntVec vec, uint64_t val); */

/* ld_err ld_str_to_int_or_str_map_size(PStr2IntOrStrMap map); */
/* ld_err ld_make_str_to_int_or_str_map(PStr2IntOrStrMap* map); */
/* ld_err ld_free_str_to_int_or_str_map(PStr2IntOrStrMap map); */
/* ld_err ld_str_to_int_or_str_map_insert_str(const char* key, const char* val); */
/* ld_err ld_str_to_int_or_str_map_insert_int(const char* key, int64_t val); */


#ifdef __cplusplus
}
#endif
