/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGETEST

#include <gtest/gtest.h>
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#undef private

#include "lib/allocator/page_arena.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
namespace observer
{

using namespace common;

class TestDirectLoadStreamTmpFileStore : public ::testing::Test
{
public:
  static const uint64_t TEST_TENANT_ID = 1002;

  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = ObServerMode::SHARED_STORAGE_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

    omt::ObTenantMeta tenant_meta;
    ASSERT_EQ(OB_SUCCESS,
              MockTenantModuleEnv::construct_default_tenant_meta(TEST_TENANT_ID, tenant_meta));
    ASSERT_EQ(OB_SUCCESS, GCTX.omt_->create_tenant(tenant_meta, false));
  }

  static void TearDownTestCase()
  {
    bool lock_succ = false;
    int ret = OB_EAGAIN;
    while (OB_EAGAIN == ret) {
      ret = GCTX.omt_->remove_tenant(TEST_TENANT_ID, lock_succ);
    }
    EXPECT_EQ(OB_SUCCESS, ret);

    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("fail to clean residual data after direct load stream tmp file test", KR(ret));
    }
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp() override
  {
    common::ObArray<int64_t> fds;
    ASSERT_EQ(OB_SUCCESS,
              FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(TEST_TENANT_ID, fds));
    ASSERT_EQ(0, fds.count());
  }

  void TearDown() override
  {
    common::ObArray<int64_t> fds;
    const int list_ret =
        FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(TEST_TENANT_ID, fds);
    // 查询失败意味着未完成泄漏检查，必须使当前用例失败。
    EXPECT_EQ(OB_SUCCESS, list_ret);
    if (OB_SUCCESS == list_ret) {
      EXPECT_EQ(0, fds.count());
      for (int64_t i = 0; i < fds.count(); ++i) {
        EXPECT_EQ(OB_SUCCESS,
                  FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(TEST_TENANT_ID, fds.at(i)));
      }
    }
  }

  static int get_only_tmp_file_fd(int64_t &fd)
  {
    int ret = OB_SUCCESS;
    common::ObArray<int64_t> fds;
    fd = tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(
            TEST_TENANT_ID, fds))) {
      LOG_WARN("fail to get direct load stream tmp file fds", KR(ret));
    } else if (OB_UNLIKELY(1 != fds.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected direct load stream tmp file count", KR(ret), K(fds.count()));
    } else {
      fd = fds.at(0);
    }
    return ret;
  }
};

TEST_F(TestDirectLoadStreamTmpFileStore, real_store_append_pread_remove)
{
  static const char FIRST_CHUNK[] = "first-real-tmp-file-chunk";
  static const char SECOND_CHUNK[] = {'s', 'e', 'c', 'o', 'n', 'd', '\0', 'x'};
  const ObString first_chunk(static_cast<int32_t>(sizeof(FIRST_CHUNK) - 1), FIRST_CHUNK);
  const ObString second_chunk(static_cast<int32_t>(sizeof(SECOND_CHUNK)), SECOND_CHUNK);
  const int64_t expected_size = first_chunk.length() + second_chunk.length();
  ObDirectLoadStreamTmpFileQueueStore store;
  int64_t first_offset = -1;
  int64_t second_offset = -1;
  int64_t fd = tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  int64_t file_size = -1;

  ASSERT_EQ(OB_SUCCESS, store.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, get_only_tmp_file_fd(fd));
  ASSERT_NE(tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD, fd);
  ASSERT_EQ(OB_SUCCESS, store.append(first_chunk, first_offset));
  ASSERT_EQ(0, first_offset);
  ASSERT_EQ(OB_SUCCESS, store.append(second_chunk, second_offset));
  ASSERT_EQ(first_chunk.length(), second_offset);
  ASSERT_EQ(OB_SUCCESS,
            FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
                TEST_TENANT_ID, fd, file_size));
  ASSERT_EQ(expected_size, file_size);

  ObArenaAllocator allocator(ObMemAttr(TEST_TENANT_ID, "DLSStoreMIT"));
  ObString read_chunk;
  ASSERT_EQ(OB_SUCCESS,
            store.read(first_offset, first_chunk.length(), allocator, read_chunk));
  ASSERT_EQ(first_chunk.length(), read_chunk.length());
  ASSERT_EQ(0, MEMCMP(first_chunk.ptr(), read_chunk.ptr(), first_chunk.length()));

  ASSERT_EQ(OB_SUCCESS,
            store.read(second_offset, second_chunk.length(), allocator, read_chunk));
  ASSERT_EQ(second_chunk.length(), read_chunk.length());
  ASSERT_EQ(0, MEMCMP(second_chunk.ptr(), read_chunk.ptr(), second_chunk.length()));

  ASSERT_EQ(OB_INVALID_ARGUMENT,
            store.read(second_offset, second_chunk.length() + 1, allocator, read_chunk));
  ASSERT_TRUE(read_chunk.empty());

  tmp_file::ObITmpFileHandle file_handle;
  ASSERT_EQ(OB_SUCCESS,
            FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file(
                TEST_TENANT_ID, fd, file_handle));
  file_handle.reset();
  ASSERT_EQ(OB_SUCCESS, store.remove());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file(
                TEST_TENANT_ID, fd, file_handle));
  ASSERT_EQ(OB_NOT_INIT,
            store.read(first_offset, first_chunk.length(), allocator, read_chunk));
  ASSERT_EQ(OB_SUCCESS, store.remove());
}

TEST_F(TestDirectLoadStreamTmpFileStore, manager_queue_real_store_lifecycle)
{
  static const char FIRST_CHUNK[] = "queue-first-chunk";
  static const char SECOND_CHUNK[] = "queue-second-chunk-with-different-size";
  const ObString first_chunk(static_cast<int32_t>(sizeof(FIRST_CHUNK) - 1), FIRST_CHUNK);
  const ObString second_chunk(static_cast<int32_t>(sizeof(SECOND_CHUNK) - 1), SECOND_CHUNK);
  const int64_t expected_size = first_chunk.length() + second_chunk.length();
  const ObDirectLoadStreamBufferQueueKey key(7001, 0);
  ObDirectLoadStreamBufferQueueManager manager(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle queue_handle;

  ASSERT_EQ(OB_SUCCESS, manager.init());
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(key, queue_handle));
  ASSERT_TRUE(queue_handle.is_valid());
  ASSERT_NE(nullptr, queue_handle.get_queue());

  int64_t fd = tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  ASSERT_EQ(OB_SUCCESS, get_only_tmp_file_fd(fd));
  ObDirectLoadStreamAppendHandle append_handle;
  ASSERT_EQ(OB_SUCCESS, queue_handle.get_queue()->begin_append(append_handle));
  ASSERT_EQ(OB_SUCCESS, queue_handle.get_queue()->append_chunk(append_handle, first_chunk));
  ASSERT_EQ(OB_SUCCESS, queue_handle.get_queue()->append_chunk(append_handle, second_chunk));
  ASSERT_EQ(OB_SUCCESS, queue_handle.get_queue()->end_append(append_handle));
  ASSERT_EQ(OB_SUCCESS, manager.seal_queue(key));

  int64_t file_size = -1;
  ASSERT_EQ(OB_SUCCESS,
            FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
                TEST_TENANT_ID, fd, file_size));
  ASSERT_EQ(expected_size, file_size);

  ObArenaAllocator allocator(ObMemAttr(TEST_TENANT_ID, "DLSQueueMIT"));
  ObString read_chunk;
  ASSERT_EQ(OB_SUCCESS, queue_handle.get_queue()->get_next_chunk(allocator, read_chunk));
  ASSERT_EQ(first_chunk.length(), read_chunk.length());
  ASSERT_EQ(0, MEMCMP(first_chunk.ptr(), read_chunk.ptr(), first_chunk.length()));
  ASSERT_EQ(OB_SUCCESS, queue_handle.get_queue()->get_next_chunk(allocator, read_chunk));
  ASSERT_EQ(second_chunk.length(), read_chunk.length());
  ASSERT_EQ(0, MEMCMP(second_chunk.ptr(), read_chunk.ptr(), second_chunk.length()));
  ASSERT_EQ(OB_ITER_END, queue_handle.get_queue()->get_next_chunk(allocator, read_chunk));
  ASSERT_TRUE(read_chunk.empty());
  ASSERT_EQ(OB_SUCCESS, queue_handle.get_queue()->check_reader_drain_completed());

  // 消费只推进内存 meta，不截断真实 tmp file。
  ASSERT_EQ(OB_SUCCESS,
            FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
                TEST_TENANT_ID, fd, file_size));
  ASSERT_EQ(expected_size, file_size);

  queue_handle.reset();
  ASSERT_EQ(OB_SUCCESS, manager.cleanup_queue(key));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, manager.get_queue(key, queue_handle));
  tmp_file::ObITmpFileHandle file_handle;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file(
                TEST_TENANT_ID, fd, file_handle));
  ASSERT_EQ(OB_SUCCESS, manager.cleanup_queue(key));
}

}  // namespace observer
}  // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_direct_load_stream_tmp_file_store.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
