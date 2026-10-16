/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif

#define private public
#define protected public
#include "lib/ob_errno.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_local_cache_util.h"
#include "lib/random/ob_random.h"
#include "lib/compress/ob_compress_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

static const int64_t DEFAULT_BLOCK_SIZE = 2 * 1024 * 1024; // 2MB

struct TestSSCommonCheckTimeGuard
{
public:
  TestSSCommonCheckTimeGuard(const char *info_str)
    : start_time_us_(ObTimeUtility::current_time_us()), info_str_(info_str) {}
  ~TestSSCommonCheckTimeGuard()
  {
    int ret = OB_SUCCESS;
    const int64_t cost_us = ObTimeUtility::current_time_us() - start_time_us_;
    LOG_INFO("TimeGuard: check time", K_(info_str), K(cost_us));
  }

public:
  int64_t start_time_us_;
  ObString info_str_;
};

class TestSSCommonUtil
{
public:
  static MacroBlockId gen_macro_block_id(const int64_t second_id);
  static MacroBlockId gen_macro_block_id(const int64_t second_id, const int64_t fourth_id);
  static int gen_random_data(char *buf, const int64_t size);
  static ObCompressorType get_random_compress_type();
  static int init_io_info(ObIOInfo &io_info, const MacroBlockId &macro_id, const int32_t offset, const int32_t size, char *read_buf);
  static int use_up_tenant_memory(const uint64_t tenant_id, const int64_t reach_pct, ObIArray<void *> &alloc_ptrs);
};

MacroBlockId TestSSCommonUtil::gen_macro_block_id(const int64_t second_id)
{
  MacroBlockId macro_id(MacroBlockId::DEFAULT_FIRST_ID, second_id, 0, 0, 0);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  return macro_id;
}

MacroBlockId TestSSCommonUtil::gen_macro_block_id(const int64_t second_id, const int64_t fourth_id)
{
  MacroBlockId macro_id(MacroBlockId::DEFAULT_FIRST_ID, second_id, 0, 0, 0);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_reorganization_scn(fourth_id);
  return macro_id;
}

int TestSSCommonUtil::gen_random_data(char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObRandom rand;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t end = size - 1;
    for (int64_t i = 0; i < end; ++i) {
      buf[i] = static_cast<char>(ObRandom::rand(0, 128));
    }
    buf[end] = '\0';
  }
  return ret;
}

ObCompressorType TestSSCommonUtil::get_random_compress_type()
{
  const int64_t random_val = ObRandom::rand(1, 2);
  const ObCompressorType compress_type = (random_val % 2 == 0) ? ObCompressorType::SNAPPY_COMPRESSOR : ObCompressorType::NONE_COMPRESSOR;
  return compress_type;
}

int TestSSCommonUtil::init_io_info(
    ObIOInfo &io_info,
    const MacroBlockId &macro_id,
    const int32_t offset,
    const int32_t size,
    char *read_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid()) || OB_ISNULL(read_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(read_buf));
  } else {
    io_info.tenant_id_ = MTL_ID();
    io_info.offset_ = offset;
    io_info.size_ = size;
    io_info.flag_.set_wait_event(1);
    io_info.timeout_us_ = 5 * 1000 * 1000;
    io_info.buf_ = read_buf;
    io_info.user_data_buf_ = read_buf;
    io_info.effective_tablet_id_ = macro_id.second_id();
  }
  return ret;
}

int TestSSCommonUtil::use_up_tenant_memory(const uint64_t tenant_id, const int64_t reach_pct, ObIArray<void *> &alloc_ptrs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || reach_pct <= 0 || reach_pct > 100)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(reach_pct));
  } else {
    const int64_t ALLOC_SIZE = 1024 * 1024L;
    while (OB_SUCC(ret)) {
      int64_t total_mem = 0;
      int64_t total_used = 0;
      if (OB_FAIL(ObSSLocalCacheUtil::get_tenant_memory_info(tenant_id, total_mem, total_used))) {
        LOG_WARN("fail to get tenant memory info", KR(ret), K(tenant_id));
      } else if (total_mem > 0 && total_used > 0 && total_used * 100 >= total_mem * reach_pct) {
        break;
      } else {
        void *ptr = ob_malloc(ALLOC_SIZE, ObMemAttr(tenant_id, "TestAlloc"));
        if (ptr != nullptr) {
          if (OB_FAIL(alloc_ptrs.push_back(ptr))) {
            LOG_WARN("fail to push back", KR(ret), KP(ptr), K(alloc_ptrs.count()));
          }
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
    }

    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
