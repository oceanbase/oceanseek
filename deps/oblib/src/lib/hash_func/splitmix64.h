/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_LIB_HASH_FUNC_SPLITMIX64_
#define OCEANBASE_LIB_HASH_FUNC_SPLITMIX64_

#include <stdint.h>

namespace oceanbase
{
namespace common
{

// splitmix64 finalizer (from java.util.SplittableRandom / Sebastiano Vigna).
//
// Maps a 64-bit integer to a quasi-random 64-bit output through a bijective
// avalanche mixing sequence.  Every output bit depends on every input bit.
//
inline uint64_t splitmix64(uint64_t x)
{
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_LIB_HASH_FUNC_SPLITMIX64_
