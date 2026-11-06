/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gmock/gmock.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/tmp_file/ob_ss_tmp_file_shrink_manager.h"
#include "lib/utility/ob_test_util.h"
#include "lib/resource/ob_resource_mgr.h"

namespace oceanbase
{
using namespace tmp_file;

static const int64_t INIT_BUCKET_ARRAY_CAPACITY = ObSSTmpFileWBPIndexCache::INIT_BUCKET_ARRAY_CAPACITY;
static const int64_t MAX_BUCKET_ARRAY_CAPACITY = ObSSTmpFileWBPIndexCache::MAX_BUCKET_ARRAY_CAPACITY;
static const int64_t BUCKET_CAPACITY = ObSSTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::BUCKET_CAPACITY;
static const int64_t fd = 1;
static uint64_t tenant_id = OB_SYS_TENANT_ID;
static const int64_t TENANT_MEMORY = 8L * 1024L * 1024L * 1024L /* 8 GB */;
static const double TMP_FILE_WBP_MEM_LIMIT_PROP = 50; //[0, 100]
/* ----------------------------- Mock Class -------------------------------- */
class MockWBPIndexCache
{
public:
  MockWBPIndexCache(ObSSTmpWriteBufferPool &wbp) : wbp_(wbp), sparsify_count_(0), ignored_push_count_(0) {}
  int push(uint32_t page_index);
  int truncate(const int64_t truncate_page_virtual_id);
  int compare(const ObSSTmpFileWBPIndexCache &index_cache);
private:
  int sparsify_(const int64_t sparsify_modulus);
public:
  ObArray<uint32_t> mock_index_cache_;
  ObSSTmpWriteBufferPool &wbp_;
  int64_t sparsify_count_;
  int64_t ignored_push_count_;
};

int MockWBPIndexCache::push(uint32_t page_index)
{
  int ret = OB_SUCCESS;
  if (mock_index_cache_.count() >= MAX_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY) {
    if (OB_FAIL(sparsify_(2))) {
      LOG_WARN("failed to sparsify", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ignored_push_count_ < ((1 << sparsify_count_) - 1)) {
    ignored_push_count_ += 1;
  } else if (FALSE_IT(ignored_push_count_ = 0)) {
  } else if (OB_FAIL(mock_index_cache_.push_back(page_index))) {
    LOG_WARN("failed to push back page index", KR(ret), K(page_index));
  }
  return ret;
}

int MockWBPIndexCache::truncate(const int64_t truncate_page_virtual_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> new_mock_index_cache;
  if (truncate_page_virtual_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(truncate_page_virtual_id));
  } else {
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < mock_index_cache_.count(); ++i) {
      int64_t virtual_page_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      if (OB_FAIL(wbp_.get_page_virtual_id(fd, mock_index_cache_.at(i), virtual_page_id))) {
        LOG_WARN("fail to get page virtual id", KR(ret), K(mock_index_cache_.at(i)));
      } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == virtual_page_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid virtual page id", KR(ret), K(virtual_page_id));
      } else if (virtual_page_id >= truncate_page_virtual_id) {
        break;
      }
    }
    for (int64_t j = i; j < mock_index_cache_.count(); ++j) {
      if (OB_FAIL(new_mock_index_cache.push_back(mock_index_cache_.at(j)))) {
        LOG_WARN("fail to push back page index", KR(ret), K(mock_index_cache_.at(j)));
      }
    }
    mock_index_cache_ = new_mock_index_cache;
  }
  return ret;
}

int MockWBPIndexCache::sparsify_(const int64_t sparsify_modulus)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> new_mock_index_cache;
  if (OB_UNLIKELY(sparsify_modulus < 2 ||
      mock_index_cache_.count() != MAX_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(sparsify_modulus), K(mock_index_cache_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mock_index_cache_.count(); ++i) {
      if (i % sparsify_modulus != 0 &&
          OB_FAIL(new_mock_index_cache.push_back(mock_index_cache_.at(i)))) {
        LOG_WARN("fail to push back page index", KR(ret), K(mock_index_cache_.at(i)));
      }
    }
    mock_index_cache_ = new_mock_index_cache;
    sparsify_count_ += 1;
    ignored_push_count_ = 0;
  }
  return ret;
}

int MockWBPIndexCache::compare(const ObSSTmpFileWBPIndexCache &index_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_cache.is_empty() && mock_index_cache_.empty())) {
    // do nothing
  } else {
    if (OB_UNLIKELY(index_cache.is_empty() || mock_index_cache_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("one cache is empty", KR(ret), K(index_cache.is_empty()), K(mock_index_cache_.empty()));
    } else if (OB_ISNULL(index_cache.page_buckets_) || OB_ISNULL(index_cache.page_buckets_->at(index_cache.left_)) ||
               OB_ISNULL(index_cache.page_buckets_->at(index_cache.right_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KR(ret), KP(index_cache.page_buckets_), K(index_cache.left_), K(index_cache.right_));
    } else if (OB_UNLIKELY(index_cache.page_buckets_->at(index_cache.left_)->is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty bucket", KR(ret), K(index_cache.left_));
    } else if (OB_UNLIKELY(index_cache.page_buckets_->at(index_cache.right_)->is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty bucket", KR(ret), K(index_cache.right_));
    } else if (index_cache.size() == 1) {
      if (mock_index_cache_.count() != index_cache.page_buckets_->at(index_cache.left_)->size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected size", KR(ret), K(index_cache.size()), K(mock_index_cache_.count()));
      }
    } else {
      int64_t real_cache_size = index_cache.page_buckets_->at(index_cache.left_)->size() +
                                index_cache.page_buckets_->at(index_cache.right_)->size() +
                                (index_cache.get_logic_tail_() - index_cache.left_ - 1) *
                                BUCKET_CAPACITY;
      if (mock_index_cache_.count() != real_cache_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected size", KR(ret), K(real_cache_size), K(index_cache.page_buckets_->at(index_cache.left_)->size()),
                 K(index_cache.page_buckets_->at(index_cache.right_)->size()), K(index_cache.size()), K(mock_index_cache_.count()));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t logic_end_bkt_pos = index_cache.get_logic_tail_();
      int64_t cur_logic_bkt_pos = index_cache.left_;
      int64_t cur_logic_index_pos = index_cache.page_buckets_->at(cur_logic_bkt_pos)->left_;
      int64_t i = 0;
      for (;OB_SUCC(ret) && i < mock_index_cache_.count() && cur_logic_bkt_pos <= logic_end_bkt_pos; ++i) {
        int64_t real_bkt_pos = cur_logic_bkt_pos % index_cache.capacity_;
        int64_t real_index_pos = cur_logic_index_pos % BUCKET_CAPACITY;
        const ObSSTmpFileWBPIndexCache::ObTmpFilePageIndexBucket *bkt = index_cache.page_buckets_->at(real_bkt_pos);
        uint32_t page_index_in_real_cache = ObTmpFileGlobal::INVALID_PAGE_ID;
        uint32_t page_index_in_mock_cache = mock_index_cache_.at(i);

        if (OB_ISNULL(bkt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", KR(ret), KP(bkt));
        } else if (FALSE_IT(page_index_in_real_cache = bkt->page_indexes_.at(real_index_pos))) {
        } else if (OB_UNLIKELY(page_index_in_mock_cache != page_index_in_real_cache)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("page index is not same", KR(ret), K(page_index_in_mock_cache), K(page_index_in_real_cache),
                   K(i), K(cur_logic_bkt_pos), K(cur_logic_index_pos), K(index_cache.left_), K(logic_end_bkt_pos));
        } else if (OB_UNLIKELY(cur_logic_index_pos > bkt->get_logic_tail_())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", KR(ret), K(cur_logic_index_pos), K(bkt->get_logic_tail_()));
        } else if (cur_logic_index_pos < bkt->get_logic_tail_()) {
          cur_logic_index_pos += 1;
        } else { // cur_logic_index_pos == bkt->get_logic_tail_()
          cur_logic_bkt_pos += 1;
          if (cur_logic_bkt_pos <= logic_end_bkt_pos) {
            const ObSSTmpFileWBPIndexCache::ObTmpFilePageIndexBucket *next_bkt = index_cache.page_buckets_->at(cur_logic_bkt_pos % index_cache.capacity_);
            if (OB_ISNULL(next_bkt)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null", KR(ret), K(mock_index_cache_.count()),
                       K(index_cache.page_buckets_->at(index_cache.left_)->size()),
                       K(index_cache.page_buckets_->at(index_cache.right_)->size()),
                       K(index_cache.size()), K(index_cache.capacity_),
                       K(i), K(cur_logic_bkt_pos),
                       K(cur_logic_index_pos), K(index_cache.left_),
                       K(logic_end_bkt_pos), KP(next_bkt));
            } else if (OB_UNLIKELY(next_bkt->is_empty())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected empty", KR(ret), K(mock_index_cache_.count()),
                       K(index_cache.page_buckets_->at(index_cache.left_)->size()),
                       K(index_cache.page_buckets_->at(index_cache.right_)->size()),
                       K(index_cache.size()), K(index_cache.capacity_),
                       K(i), K(cur_logic_bkt_pos), K(cur_logic_index_pos),
                       K(index_cache.left_), K(logic_end_bkt_pos),
                       KPC(next_bkt));
            } else {
              cur_logic_index_pos = next_bkt->left_;
            }
          }
        }
      } // end for
    }
  }

  return ret;
}

/* ---------------------------- Unittest Class ----------------------------- */
class TestTmpFileWBPIndexCache : public ::testing::Test
{
public:
  TestTmpFileWBPIndexCache() = default;
  virtual ~TestTmpFileWBPIndexCache() = default;
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  int write_and_push_pages(const uint32_t end_page_id,const int64_t page_num,
                           MockWBPIndexCache &mock_cache, uint32_t &new_end_page_id);
  int truncate_pages(const int64_t truncate_page_num, MockWBPIndexCache &mock_cache);
  int truncate_and_free_pages(const uint32_t wbp_begin_page_id, const int64_t truncate_page_num_in_cache,
                              MockWBPIndexCache &mock_cache, uint32_t &new_begin_page_id);
private:
  int write_pages_(const int64_t page_num, const int64_t begin_page_virtual_id, ObArray<uint32_t> &page_indexes);
  int free_pages_(const uint32_t begin_page_id, const int64_t end_page_virtual_id, uint32_t &new_begin_page_id);
  int check_cache_status_after_truncate_(MockWBPIndexCache &mock_cache);
public:
  ObSSTmpWriteBufferPool wbp_;
  ObSSTmpFileWBPIndexCache wbp_index_cache_;
  common::ObFIFOAllocator wbp_index_cache_allocator_;
  common::ObFIFOAllocator wbp_index_cache_bkt_allocator_;
};

void TestTmpFileWBPIndexCache::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, wbp_index_cache_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                        OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                        ObMemAttr(tenant_id, "TmpFileIndCache",
                                                                  ObCtxIds::DEFAULT_CTX_ID)));
  ASSERT_EQ(OB_SUCCESS, wbp_index_cache_bkt_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                            OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                            ObMemAttr(tenant_id, "TmpFileIndCBkt",
                                                                      ObCtxIds::DEFAULT_CTX_ID)));
  ASSERT_EQ(OB_SUCCESS, wbp_.init());
  ASSERT_EQ(OB_SUCCESS, wbp_index_cache_.init(fd, &wbp_, &wbp_index_cache_allocator_, &wbp_index_cache_bkt_allocator_));
}

void TestTmpFileWBPIndexCache::TearDown()
{
  wbp_index_cache_allocator_.reset();
  wbp_index_cache_bkt_allocator_.reset();
  wbp_.destroy();
  wbp_index_cache_.destroy();
}

void TestTmpFileWBPIndexCache::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  CHUNK_MGR.set_limit(TENANT_MEMORY);
  ObMallocAllocator::get_instance()->set_tenant_limit(1, TENANT_MEMORY);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ASSERT_EQ(true, tenant_config.is_valid());
  tenant_config->_temporary_file_io_area_size = TMP_FILE_WBP_MEM_LIMIT_PROP;
}

void TestTmpFileWBPIndexCache::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

// end_page_id: the page_id of the last page of wbp
// page_num: the number of pages to be alloced.
// new_end_page_id: page_id of the last page of alloced pages.
// this function will alloc 'page_num' pages and link them after the page of end_page_id,
// then put them into mock_cache and wbp_index_cache_.
int TestTmpFileWBPIndexCache::write_and_push_pages(const uint32_t end_page_id, const int64_t page_num,
                                                   MockWBPIndexCache &mock_cache, uint32_t &new_end_page_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> page_indexes;
  int64_t end_page_virtual_id = 0;
  int64_t new_begin_page_virtual_id = 0;
  if (OB_UNLIKELY(page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(page_num));
  } else if (end_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
    if (OB_FAIL(wbp_.get_page_virtual_id(fd, end_page_id, end_page_virtual_id))) {
      LOG_WARN("fail to get page virtual id", KR(ret), K(end_page_id), K(end_page_virtual_id));
    } else if (OB_UNLIKELY(end_page_virtual_id < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid virtual id", KR(ret), K(end_page_virtual_id));
    } else {
      new_begin_page_virtual_id = end_page_virtual_id + 1;
    }
  }

  if (FAILEDx(write_pages_(page_num, new_begin_page_virtual_id, page_indexes))) {
    LOG_WARN("fail to write pages", KR(ret), K(page_num));
  } else if (OB_UNLIKELY(page_indexes.count() != page_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(page_num), K(page_indexes.count()));
  } else if (end_page_id != ObTmpFileGlobal::INVALID_PAGE_ID &&
             OB_FAIL(wbp_.link_page(fd, page_indexes.at(0), end_page_id, ObTmpFilePageUniqKey(end_page_virtual_id)))) {
    LOG_WARN("fail to link page", KR(ret), K(page_indexes.at(0)), K(end_page_id), K(end_page_virtual_id));
  } else if (FALSE_IT(new_end_page_id = page_indexes.at(page_num - 1))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < page_num; ++i) {
      if (OB_FAIL(mock_cache.push(page_indexes.at(i)))) {
        LOG_WARN("fail to push", KR(ret), K(i), K(page_indexes.at(i)));
      } else if (OB_FAIL(wbp_index_cache_.push(page_indexes.at(i)))) {
        LOG_WARN("fail to push", KR(ret), K(i), K(page_indexes.at(i)));
      }
    }
  }
  return ret;
}

int TestTmpFileWBPIndexCache::truncate_pages(const int64_t truncate_page_num, MockWBPIndexCache &mock_cache)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> page_indexes;
  if (OB_UNLIKELY(truncate_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(truncate_page_num));
  } else {
    int64_t truncate_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    uint32_t truncate_page_id = mock_cache.mock_index_cache_.at(truncate_page_num - 1);
    if (OB_FAIL(wbp_.get_page_virtual_id(fd, truncate_page_id, truncate_page_virtual_id))) {
      LOG_WARN("fail to get page virtual id", KR(ret), K(truncate_page_id));
    } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == truncate_page_virtual_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid virtual page id", KR(ret), K(truncate_page_virtual_id));
    } else if (FALSE_IT(truncate_page_virtual_id += 1)) {
    } else if (OB_FAIL(mock_cache.truncate(truncate_page_virtual_id))) {
      LOG_WARN("fail to truncate", KR(ret), K(truncate_page_virtual_id));
    } else if (OB_FAIL(wbp_index_cache_.truncate(truncate_page_virtual_id))) {
      LOG_WARN("fail to truncate", KR(ret), K(truncate_page_virtual_id));
    } else if (OB_FAIL(check_cache_status_after_truncate_(mock_cache))) {
      LOG_WARN("fail to check cache status after truncate", KR(ret));
    }
  }
  return ret;
}

// wbp_begin_page_id: the page_id of the first page of wbp, the pos of it in the wbp list should be
//                    less than or equal to the first cached index in wbp_index_cache_.
// truncate_page_num_in_cache: the number of page indexes to be truncated in mock_cache,
//                             we will free pages between [wbp_begin_page_id, begin_page_id_in_cache + truncate_page_num_in_cache]
// new_begin_page_id: page_id of the first page after free pages in wbp.
int TestTmpFileWBPIndexCache::truncate_and_free_pages(const uint32_t wbp_begin_page_id, const int64_t truncate_page_num_in_cache,
                                                      MockWBPIndexCache &mock_cache, uint32_t &new_begin_page_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> page_indexes;
  if (OB_UNLIKELY(truncate_page_num_in_cache <= 0 || ObTmpFileGlobal::INVALID_PAGE_ID == wbp_begin_page_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(truncate_page_num_in_cache), K(wbp_begin_page_id));
  } else {
    int64_t truncate_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    uint32_t truncate_page_id = mock_cache.mock_index_cache_.at(truncate_page_num_in_cache - 1);
    if (OB_FAIL(wbp_.get_page_virtual_id(fd, truncate_page_id, truncate_page_virtual_id))) {
      LOG_WARN("fail to get page virtual id", KR(ret), K(truncate_page_id));
    } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == truncate_page_virtual_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid virtual page id", KR(ret), K(truncate_page_virtual_id));
    } else if (FALSE_IT(truncate_page_virtual_id += 1)) {
    } else if (OB_FAIL(mock_cache.truncate(truncate_page_virtual_id))) {
      LOG_WARN("fail to truncate", KR(ret), K(truncate_page_virtual_id));
    } else if (OB_FAIL(wbp_index_cache_.truncate(truncate_page_virtual_id))) {
      LOG_WARN("fail to truncate", KR(ret), K(truncate_page_virtual_id));
    } else if (OB_FAIL(check_cache_status_after_truncate_(mock_cache))) {
      LOG_WARN("fail to check cache status after truncate", KR(ret));
    } else if (OB_FAIL(free_pages_(wbp_begin_page_id, truncate_page_virtual_id, new_begin_page_id))) {
      LOG_WARN("fail to free pages", KR(ret), K(wbp_begin_page_id), K(truncate_page_virtual_id));
    }
  }
  return ret;
}

int TestTmpFileWBPIndexCache::write_pages_(const int64_t page_num, const int64_t begin_page_virtual_id, ObArray<uint32_t> &page_indexes)
{
  int ret = OB_SUCCESS;
  page_indexes.reset();
  if (OB_UNLIKELY(page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(page_num));
  } else{
    for (int64_t i = 0; OB_SUCC(ret) && i < page_num; ++i) {
      uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      char *unused_buf = nullptr;
      int64_t new_page_begin_page_id = begin_page_virtual_id + i;
      if (OB_FAIL(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(new_page_begin_page_id), new_page_id, unused_buf))) {
        LOG_WARN("fail to alloc data page", KR(ret), K(i));
      } else if (OB_FAIL(wbp_.notify_dirty(fd, new_page_id, ObTmpFilePageUniqKey(new_page_begin_page_id)))) {
        LOG_WARN("fail to notify dirty", KR(ret), K(new_page_id), K(new_page_begin_page_id));
      } else if (!page_indexes.empty() &&
                 OB_FAIL(wbp_.link_page(fd, new_page_id, page_indexes.at(page_indexes.count()-1),
                                        ObTmpFilePageUniqKey(new_page_begin_page_id - 1)))) {
        LOG_WARN("fail to link page", KR(ret), K(i), K(new_page_id),
                 K(page_indexes.at(page_indexes.count()-1)), K(new_page_begin_page_id));
      } else if (OB_FAIL(page_indexes.push_back(new_page_id))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      }
    }
  }
  return ret;
}


int TestTmpFileWBPIndexCache::free_pages_(const uint32_t begin_page_id, const int64_t end_page_virtual_id, uint32_t &new_begin_page_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(end_page_virtual_id <= 0 || begin_page_id == ObTmpFileGlobal::INVALID_PAGE_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(end_page_virtual_id), K(begin_page_id));
  } else {
    bool free_over = false;
    uint32_t cur_page_id = begin_page_id;
    int64_t cnt = 0;
    while(!free_over && OB_SUCC(ret) && cur_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      int64_t virtual_page_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      if (OB_FAIL(wbp_.get_page_virtual_id(fd, cur_page_id, virtual_page_id))) {
        LOG_WARN("fail to get page virtual id", KR(ret), K(cur_page_id), K(cnt));
      } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == virtual_page_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid virtual page id", KR(ret), K(virtual_page_id), K(cnt));
      } else if (virtual_page_id >= end_page_virtual_id) {
        free_over = true;
        new_begin_page_id = cur_page_id;
      } else if (OB_FAIL(wbp_.free_page(fd, cur_page_id, ObTmpFilePageUniqKey(virtual_page_id), next_page_id))) {
        LOG_WARN("fail to alloc data page", KR(ret), K(cur_page_id), K(cnt));
      } else {
        cur_page_id = next_page_id;
        cnt += 1;
      }
    }
  }
  return ret;
}

int TestTmpFileWBPIndexCache::check_cache_status_after_truncate_(MockWBPIndexCache &mock_cache)
{
  int ret = OB_SUCCESS;
  int64_t bkt_num = wbp_index_cache_.get_logic_tail_() - wbp_index_cache_.left_ + 1;
  int64_t page_num = 0;
  if (OB_UNLIKELY(bkt_num != wbp_index_cache_.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket num is invalid", KR(ret), K(bkt_num), K(wbp_index_cache_.size()));
  }
  for (int64_t bkt_arr_logic_idx = wbp_index_cache_.left_;
       OB_SUCC(ret) && bkt_arr_logic_idx <= wbp_index_cache_.get_logic_tail_();
       bkt_arr_logic_idx++) {
    int64_t bkt_arr_idx = bkt_arr_logic_idx % wbp_index_cache_.capacity_;
    int64_t bkt_left_idx = 0;
    if (OB_ISNULL(wbp_index_cache_.page_buckets_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket array is null", KR(ret));
    } else if (OB_ISNULL(wbp_index_cache_.page_buckets_->at(bkt_arr_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket is null", KR(ret), K(bkt_arr_idx));
    } else if (OB_UNLIKELY(wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket is empty", KR(ret), K(bkt_arr_idx));
    } else if (FALSE_IT(bkt_left_idx = wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->left_)) {
    } else if (OB_UNLIKELY(wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->min_page_index_ !=
                           wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->page_indexes_.at(bkt_left_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min page index is not equal to left page index", KR(ret),
               K(wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->min_page_index_),
               K(wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->page_indexes_.at(bkt_left_idx)));
    } else {
      for (int64_t bkt_logic_idx = wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->left_;
           OB_SUCC(ret) && bkt_logic_idx <= wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->get_logic_tail_();
           bkt_logic_idx++) {
        int64_t bkt_idx = bkt_logic_idx % wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->capacity_;
        uint32_t page_idx = wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->page_indexes_.at(bkt_idx);
        if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == page_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("page index is invalid", KR(ret), K(bkt_arr_idx), K(bkt_idx));
        } else {
          page_num += 1;
        }
      } // end for
    }
  } // end for
  if (OB_UNLIKELY(page_num != mock_cache.mock_index_cache_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page num is invalid", KR(ret), K(page_num), K(mock_cache.mock_index_cache_.count()));
  }
  return ret;
}

TEST_F(TestTmpFileWBPIndexCache, test_push_and_pop)
{
  int ret = OB_SUCCESS;
  uint32_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  // 1. push indexes
  int64_t page_num = BUCKET_CAPACITY / 2;
  MockWBPIndexCache mock_cache(wbp_);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  begin_page_id = mock_cache.mock_index_cache_.at(0);

  // 2. fill a bucket of wbp_index_cache
  page_num = BUCKET_CAPACITY - page_num;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, wbp_index_cache_.size_);
  ASSERT_EQ(true, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->is_full());

  // 3. continue to push some index when bucket is full
  page_num = BUCKET_CAPACITY / 4;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, wbp_index_cache_.size_);
  ASSERT_EQ(false, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->is_full());

  // 4. push more than a bucket number of indexes
  page_num = BUCKET_CAPACITY * 2 + BUCKET_CAPACITY / 4;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, wbp_index_cache_.size_);
  ASSERT_EQ(false, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->is_full());

  // 5. free indexes of wbp_index_cache and mock_cache
  page_num = BUCKET_CAPACITY / 4;
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  uint32_t remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 6. push some indexes after freeing
  page_num = 2 * BUCKET_CAPACITY / 4;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7. free more than a bucket number of indexes
  page_num = BUCKET_CAPACITY * 2 + BUCKET_CAPACITY / 2;
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 8. free all indexes of wbp_index_cache and mock_cache
  page_num = mock_cache.mock_index_cache_.count();
  remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  // 9. push indexes into both wbp_index_cache and mock_cache
  page_num = BUCKET_CAPACITY / 3;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("test_push_and_pop");
}

TEST_F(TestTmpFileWBPIndexCache, test_expand_and_sparsify)
{
  int ret = OB_SUCCESS;
  uint32_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  // 1. push indexes into both wbp_index_cache and mock_cache
  int64_t page_num = INIT_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY;
  MockWBPIndexCache mock_cache(wbp_);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, wbp_index_cache_.is_full());
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  begin_page_id = mock_cache.mock_index_cache_.at(0);

  // 2. push indexes to trigger expand
  page_num = 30;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, wbp_index_cache_.is_full());
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY * 2, wbp_index_cache_.capacity_);

  // 3. push indexes to trigger expand until reach the max capacity
  page_num = MAX_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY
             - mock_cache.mock_index_cache_.count();
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, wbp_index_cache_.is_full());
  ASSERT_EQ(MAX_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);

  int64_t actual_write_page_num = 0;
  int64_t bucket_num = 0;
  // 4. push indexes to trigger sparsify
  page_num = BUCKET_CAPACITY / 2;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, wbp_index_cache_.sparsify_count_);
  ASSERT_EQ(false, wbp_index_cache_.is_full());
  ASSERT_EQ(MAX_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  actual_write_page_num = page_num / (1 << wbp_index_cache_.sparsify_count_); // in order to keep same sparse interval,
                                                                              // the other push callings are ignored
  bucket_num = wbp_index_cache_.size_;
  ASSERT_EQ(wbp_index_cache_.capacity_ / 2 + 1, bucket_num);
  ASSERT_EQ(actual_write_page_num % BUCKET_CAPACITY, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->size_);

  // 5. push indexes to trigger sparsify again
  page_num = ((1 << wbp_index_cache_.sparsify_count_) - wbp_index_cache_.ignored_push_count_ - 1) + 1 +
             (1 << wbp_index_cache_.sparsify_count_) *
             (BUCKET_CAPACITY - wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->size_ +
              BUCKET_CAPACITY * (MAX_BUCKET_ARRAY_CAPACITY - wbp_index_cache_.size_) - 1);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, wbp_index_cache_.sparsify_count_);
  ASSERT_EQ(true, wbp_index_cache_.is_full());

  page_num = BUCKET_CAPACITY / 2;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, wbp_index_cache_.sparsify_count_);
  ASSERT_EQ(false, wbp_index_cache_.is_full());
  ASSERT_EQ(MAX_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  actual_write_page_num = page_num / (1 << wbp_index_cache_.sparsify_count_);
  bucket_num = wbp_index_cache_.size_;
  ASSERT_EQ(wbp_index_cache_.capacity_ / 2 + 1, bucket_num);
  ASSERT_EQ(actual_write_page_num % BUCKET_CAPACITY, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->size_);

  LOG_INFO("test_expand_and_sparsify");
}

TEST_F(TestTmpFileWBPIndexCache, test_shrink)
{
  int ret = OB_SUCCESS;
  uint32_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  // 1. push indexes to trigger expand
  int64_t page_num = 2 * INIT_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY;
  MockWBPIndexCache mock_cache(wbp_);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2 * INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.size_);
  ASSERT_EQ(2 * INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  begin_page_id = mock_cache.mock_index_cache_.at(0);

  // 2. free 7/8 indexes to trigger shrink
  page_num = wbp_index_cache_.capacity_ * BUCKET_CAPACITY * 7 / 8;
  ASSERT_GE(page_num, mock_cache.mock_index_cache_.count() - mock_cache.mock_index_cache_.count() / ObSSTmpFileWBPIndexCache::SHRINK_THRESHOLD);
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  uint32_t remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);

  // 3. push indexes to fill buckets with INIT_BUCKET_ARRAY_CAPACITY
  page_num = INIT_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY
             - mock_cache.mock_index_cache_.count();
  ASSERT_LE(page_num, INIT_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY);
  ASSERT_GT(page_num, 0);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.size_);
  ASSERT_EQ(wbp_index_cache_.size_ * BUCKET_CAPACITY,
            mock_cache.mock_index_cache_.count());

  // 4. free 7/8 indexes, but doesn't trigger shrink
  page_num = wbp_index_cache_.capacity_ * BUCKET_CAPACITY * 7 / 8;
  ASSERT_GE(page_num, mock_cache.mock_index_cache_.count() - mock_cache.mock_index_cache_.count() / ObSSTmpFileWBPIndexCache::SHRINK_THRESHOLD);
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);

  LOG_INFO("test_shrink");
}

int mock_circle_bucket(ObSSTmpFileWBPIndexCache::ObTmpFilePageIndexBucket &bucket)
{
  int ret = OB_SUCCESS;
  if (bucket.right_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bucket", KR(ret), K(bucket));
  } else if (bucket.left_ < bucket.right_) {
    ObArray<uint32_t> array;
    for (int64_t i = bucket.left_; OB_SUCC(ret) && i <= bucket.right_; ++i) {
      if (OB_FAIL(array.push_back(bucket.page_indexes_.at(i)))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t mid = bucket.left_ + (bucket.right_ - bucket.left_) / 2;
      for (int64_t i = 0; i < array.size(); ++i) {
        int64_t new_pos = (bucket.left_ + i + mid) > bucket.right_ ?
                          bucket.left_ + i + mid - array.size() :
                          bucket.left_ + i + mid;
        bucket.page_indexes_.at(new_pos) = array.at(i);
      }
      bucket.left_ = mid;
      bucket.right_= mid - 1;
    }
  }
  return ret;
}

int mock_circle_cache(ObSSTmpFileWBPIndexCache &wbp_index_cache)
{
  int ret = OB_SUCCESS;
  if (wbp_index_cache.right_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bucket", KR(ret), K(wbp_index_cache));
  } else if (wbp_index_cache.left_ < wbp_index_cache.right_) {
    ObArray<ObSSTmpFileWBPIndexCache::ObTmpFilePageIndexBucket*> array;
    for (int64_t i = wbp_index_cache.left_; OB_SUCC(ret) && i <= wbp_index_cache.right_; ++i) {
      if (OB_ISNULL(wbp_index_cache.page_buckets_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(wbp_index_cache));
      } else if (OB_ISNULL(wbp_index_cache.page_buckets_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(wbp_index_cache));
      } else if (OB_FAIL(array.push_back(wbp_index_cache.page_buckets_->at(i)))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t mid = wbp_index_cache.left_ + (wbp_index_cache.right_ - wbp_index_cache.left_) / 2;
      for (int64_t i = 0; i < array.size(); ++i) {
        int64_t new_pos = (wbp_index_cache.left_ + i + mid) > wbp_index_cache.right_ ?
                          wbp_index_cache.left_ + i + mid - array.size() :
                          wbp_index_cache.left_ + i + mid;
        wbp_index_cache.page_buckets_->at(new_pos) = array.at(i);
      }
      wbp_index_cache.left_ = mid;
      wbp_index_cache.right_= mid - 1;
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = wbp_index_cache.left_; OB_SUCC(ret) && i <= wbp_index_cache.get_logic_tail_(); ++i) {
      int64_t pos = i % wbp_index_cache.capacity_;
      if (OB_ISNULL(wbp_index_cache.page_buckets_->at(pos))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(wbp_index_cache));
      } else if (OB_FAIL(mock_circle_bucket(*wbp_index_cache.page_buckets_->at(pos)))) {
        LOG_WARN("fail to mock circle bucket", KR(ret), K(wbp_index_cache));
      }
    }
  }
  return ret;
}

TEST_F(TestTmpFileWBPIndexCache, test_search)
{
  int ret = OB_SUCCESS;
  uint32_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  // 1. push indexes until reach the max capacity
  int64_t page_num = MAX_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY;
  MockWBPIndexCache mock_cache(wbp_);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  begin_page_id = mock_cache.mock_index_cache_.at(0);
  ASSERT_NE(begin_page_id, ObTmpFileGlobal::INVALID_PAGE_ID);
  ret = mock_circle_cache(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(wbp_index_cache_.left_, wbp_index_cache_.right_);
  ASSERT_GT(wbp_index_cache_.page_buckets_->at(wbp_index_cache_.left_)->left_,
            wbp_index_cache_.page_buckets_->at(wbp_index_cache_.left_)->right_);

  // 2. push indexes to trigger sparsify
  page_num = 20;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. truncate indexes to make the first bucket not be full
  page_num = 10;
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  ret = truncate_pages(page_num, mock_cache);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. search page_index which exists in wbp_index_cache
  int64_t search_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  uint32_t search_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t remove_page_id = mock_cache.mock_index_cache_.at(mock_cache.mock_index_cache_.count() / 5 + 1);
  ret = wbp_.get_page_virtual_id(fd, remove_page_id, search_page_virtual_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = wbp_index_cache_.binary_search(search_page_virtual_id, search_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(search_page_id, remove_page_id);

  // 5. search page_index which doesn't exist in wbp_index_cache and whose virtual id is not in the range of wbp_index_cache.
  //    the range of cache is [the page_virtual_id of the first index, INFINITY)
  ret = wbp_index_cache_.binary_search(1, search_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, search_page_id);

  // 6. search page_index which doesn't exist in wbp_index_cache and whose virtual id is in the range of wbp_index_cache.
  //    the range of cache is [the page_virtual_id of the first index, INFINITY)
  int64_t page_virtual_id1 = -1;
  int64_t page_virtual_id2 = -1;
  ret = wbp_.get_page_virtual_id(fd, mock_cache.mock_index_cache_.at(0), page_virtual_id1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = wbp_.get_page_virtual_id(fd, mock_cache.mock_index_cache_.at(1), page_virtual_id2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(page_virtual_id2 - page_virtual_id1, 1);
  search_page_virtual_id = page_virtual_id1 + 1;
  ret = wbp_index_cache_.binary_search(page_virtual_id1, search_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t res_virtual_id = -1;
  ret = wbp_.get_page_virtual_id(fd, search_page_id, res_virtual_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(page_virtual_id1, res_virtual_id);

  LOG_INFO("test_search");
}

// ============================================================================
// WBP 缩容策略回归：比例与目标 (A，3 个用例)，状态推进与保护 (B，7 个用例)。
// ============================================================================

using common::ObTimeUtility;

// A1. 非零页向上取整，释放最后一页后下一个窗口准确恢复为空池。
// 场景：扩出 32 个块，只分配一页，使实际使用率低于 1%。
// 操作：分配、释放该页，再结束一个带有旧高峰的统计窗口。
// 校验：分配后峰值为 1%；窗口结束后为空池，下一分钟目标仅保留一个块。
TEST_F(TestTmpFileWBPIndexCache, test_shrink_percentage_rounds_up)
{
  LOG_INFO("========== test_shrink_percentage_rounds_up begin ==========");
  wbp_.default_wbp_memory_limit_ = 32 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }

  // 单页不能因整数截断而被当成空池。
  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *page_buf = nullptr;
  const ObTmpFilePageUniqKey page_key(0 /*virtual_page_id*/);
  OK(wbp_.alloc_page(fd, page_key, page_id, page_buf));
  LOG_INFO("single-page usage", K(wbp_.used_page_num_), K(wbp_.fat_.size()),
      K(wbp_.max_used_watermark_after_shrinking_));
  ASSERT_EQ(1, wbp_.max_used_watermark_after_shrinking_);
  uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  OK(wbp_.free_page(fd, page_id, page_key, next_page_id));

  // 旧高峰只阻止当前窗口；新窗口以当前零用量重新开始。
  wbp_.max_used_watermark_after_shrinking_ = 100;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  bool is_auto = true;
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);
  ASSERT_EQ(0, wbp_.max_used_watermark_after_shrinking_);
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_TRUE(is_auto);
  OK(wbp_.begin_shrinking(is_auto));
  ASSERT_EQ(ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS, wbp_.shrink_ctx_.lower_page_id_);
  OK(wbp_.advance_shrink_state());
  OK(wbp_.finish_shrinking());
  LOG_INFO("========== test_shrink_percentage_rounds_up end ==========");
}

// A2. 8% 历史水位启动后可继续执行，分钟门控和 40% 排他边界均生效。
// 场景：32 个空闲块，构造已结束窗口的 8% 历史峰值。
// 操作：选择目标并进入 SWAP，再检查本次回调末尾的继续条件；随后检查 39%/40%。
// 校验：16% 目标向上对齐为 6 块，不会沿用旧 5% 阈值立即取消。
TEST_F(TestTmpFileWBPIndexCache, test_shrink_window_and_continuation)
{
  LOG_INFO("========== test_shrink_window_and_continuation begin ==========");
  wbp_.default_wbp_memory_limit_ = 32 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  wbp_.max_used_watermark_after_shrinking_ = 8;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us();
  bool is_auto = true;
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);

  // 初始化读取完整旧窗口，再保存目标并重置统计；后续继续条件使用上下文目标。
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  const int64_t expected_target = 6 * ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS;
  ASSERT_EQ(8, wbp_.max_used_watermark_after_shrinking_);
  ObSSTmpFileFlushManager flush_mgr;
  ObTmpFileShrinkWBPTask task;
  OK(task.init(&wbp_, &flush_mgr));
  task.runTimerTask();
  ASSERT_TRUE(wbp_.shrink_ctx_.is_valid());
  ASSERT_TRUE(wbp_.shrink_ctx_.is_auto());
  ASSERT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp_.get_wbp_state());
  ASSERT_EQ(expected_target, wbp_.shrink_ctx_.lower_page_id_);
  ASSERT_EQ(0, wbp_.max_used_watermark_after_shrinking_);
  // 活跃轮次依据固定目标继续，历史水位重新升高不能触发旧的 5% 取消条件。
  wbp_.max_used_watermark_after_shrinking_ = 100;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  LOG_INFO("8 percent round remains active", K(wbp_.shrink_ctx_));
  OK(wbp_.finish_shrinking());

  // 触发判断验证严格小于 40%；目标表的预期块数独立给出，覆盖公式两侧。
  ASSERT_FALSE(wbp_.is_tenant_memory_under_pressure_());
  wbp_.max_used_watermark_after_shrinking_ = 39;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_TRUE(is_auto);
  {
    common::TCRWLock::RLockGuard guard(wbp_.lock_);
    ObSpinLockGuard list_guard(wbp_.free_list_lock_);
    constexpr int64_t TARGET_CASES[][2] = {{0, 1}, {8, 6}, {19, 13}, {20, 13},
        {21, 14}, {39, 19}, {40, 0}};
    for (const int64_t (&target_case)[2] : TARGET_CASES) {
      wbp_.max_used_watermark_after_shrinking_ = target_case[0];
      const int64_t expected_pages = target_case[1] * ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS;
      LOG_INFO("verify historical target", K(target_case[0]), K(expected_pages));
      ASSERT_EQ(expected_pages, wbp_.cal_auto_shrink_target_page_num_());
    }
  }
  // 拒绝启动的窗口也必须记录现存页面，而不能只清零等待下一次分配。
  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *page_buf = nullptr;
  OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*virtual_page_id*/), page_id, page_buf));
  wbp_.max_used_watermark_after_shrinking_ = 40;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);
  ASSERT_EQ(1, wbp_.max_used_watermark_after_shrinking_);
  LOG_INFO("========== test_shrink_window_and_continuation end ==========");
}

// A3. 无参目标计算读取当前租户压力，达到 85% 后采用当前用量绕过旧高峰。
// 场景：32 个块只持有一页，历史峰值设为 90%。
// 操作：切换到隔离租户，以固定 85MB 占用配合不同限额构造真实压力统计。
// 校验：压力启动也遵守一分钟；当前用量 50% 时可绕过 40% 门槛，目标为 23 块。
//       阈值以下或限额无效时旧高峰阻止缩容，当前用量过高时跳过无收益目标。
TEST_F(TestTmpFileWBPIndexCache, test_shrink_tenant_pressure_target)
{
  LOG_INFO("========== test_shrink_tenant_pressure_target begin ==========");
  wbp_.default_wbp_memory_limit_ = 32 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *page_buf = nullptr;
  OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*virtual_page_id*/), page_id, page_buf));

  // 隔离租户只登记合成 hold，不申请等量缓冲，也不修改原租户的额度和统计。
  constexpr uint64_t PRESSURE_TENANT_ID = 9001;
  constexpr int64_t SYNTHETIC_HOLD_BYTES = 85_MB;
  lib::ObTenantResourceMgrHandle resource_handle;
  OK(lib::ObResourceMgr::get_instance().get_tenant_resource_mgr(
      PRESSURE_TENANT_ID, resource_handle));
  lib::ObTenantMemoryMgr *memory_mgr = resource_handle.get_memory_mgr();
  ASSERT_NE(nullptr, memory_mgr);
  ASSERT_EQ(0, memory_mgr->get_sum_hold());
  share::ObTenantBase pressure_tenant(PRESSURE_TENANT_ID);
  struct MemoryAccountingGuard
  {
    lib::ObTenantMemoryMgr *manager_;
    int64_t original_limit_;
    int64_t hold_bytes_;
    share::ObTenantBase *original_tenant_;
    // Restore the MTL context, synthetic accounting, and limit on assertion failure too.
    ~MemoryAccountingGuard()
    {
      share::ObTenantEnv::set_tenant(original_tenant_);
      bool reach_ctx_limit = false;
      if (hold_bytes_ > 0 && !manager_->update_hold(-hold_bytes_,
          common::ObCtxIds::DEFAULT_CTX_ID, lib::ObLabel(), reach_ctx_limit)) {
        const int ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to restore synthetic tenant memory accounting", KR(ret), K(hold_bytes_));
      }
      manager_->set_limit(original_limit_);
    }
  } accounting_guard{memory_mgr, memory_mgr->get_limit(), 0 /*hold_bytes_*/,
      share::ObTenantEnv::get_tenant()};
  bool reach_ctx_limit = false;
  ASSERT_TRUE(memory_mgr->update_hold(SYNTHETIC_HOLD_BYTES,
      common::ObCtxIds::DEFAULT_CTX_ID, lib::ObLabel(), reach_ctx_limit));
  accounting_guard.hold_bytes_ = SYNTHETIC_HOLD_BYTES;
  ASSERT_EQ(SYNTHETIC_HOLD_BYTES, memory_mgr->get_sum_hold());
  // 低于压力阈值时，真实启动判断仍被 90% 历史峰值阻止。
  bool is_auto = true;
  memory_mgr->set_limit(101_MB /*limit*/);
  share::ObTenantEnv::set_tenant(&pressure_tenant);
  ASSERT_EQ(PRESSURE_TENANT_ID, MTL_ID());
  wbp_.max_used_watermark_after_shrinking_ = 90;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);
  share::ObTenantEnv::set_tenant(accounting_guard.original_tenant_);
  {
    common::TCRWLock::RLockGuard guard(wbp_.lock_);
    ObSpinLockGuard list_guard(wbp_.free_list_lock_);
    share::ObTenantEnv::set_tenant(&pressure_tenant);
    wbp_.max_used_watermark_after_shrinking_ = 90;
    LOG_INFO("verify tenant pressure boundary", K(SYNTHETIC_HOLD_BYTES));
    memory_mgr->set_limit(101_MB /*limit*/);
    ASSERT_FALSE(wbp_.is_tenant_memory_under_pressure_());
    ASSERT_EQ(0, wbp_.cal_auto_shrink_target_page_num_());
    memory_mgr->set_limit(100_MB /*limit*/);
    ASSERT_TRUE(wbp_.is_tenant_memory_under_pressure_());
    ASSERT_EQ(ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS,
        wbp_.cal_auto_shrink_target_page_num_());
    memory_mgr->set_limit(0 /*limit*/);
    ASSERT_FALSE(wbp_.is_tenant_memory_under_pressure_());
    ASSERT_EQ(0, wbp_.cal_auto_shrink_target_page_num_());
    memory_mgr->set_limit(100_MB /*limit*/);
    share::ObTenantEnv::set_tenant(accounting_guard.original_tenant_);
  }
  LOG_INFO("pressure ignores historical peak", K(wbp_.used_page_num_),
      K(wbp_.max_used_watermark_after_shrinking_));

  // 压力不能绕过周期，但到期后必须绕过 90% 的历史高峰触发自动缩容。
  share::ObTenantEnv::set_tenant(&pressure_tenant);
  is_auto = true;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us();
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_TRUE(is_auto);
  share::ObTenantEnv::set_tenant(accounting_guard.original_tenant_);
  LOG_INFO("pressure starts an eligible round", K(is_auto), K(wbp_.used_page_num_));

  // 实际使用一半页面：压力下应按 50% + 20% 保留 70%，32 块向上对齐为 23 块。
  const int64_t half_pages = wbp_.fat_.size() / 2;
  for (int64_t i = 1; i < half_pages; ++i) {
    OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(i), page_id, page_buf));
  }
  {
    common::TCRWLock::RLockGuard guard(wbp_.lock_);
    ObSpinLockGuard list_guard(wbp_.free_list_lock_);
    share::ObTenantEnv::set_tenant(&pressure_tenant);
    ASSERT_EQ(23 * ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS,
        wbp_.cal_auto_shrink_target_page_num_());
    share::ObTenantEnv::set_tenant(accounting_guard.original_tenant_);
  }
  LOG_INFO("pressure permits usage above normal watermark", K(wbp_.used_page_num_));

  // 当前使用量较高时，即使有租户压力也不能启动没有容量收益的缩容。
  const int64_t busy_pages = (wbp_.fat_.size() * 80 + 99) / 100;
  for (int64_t i = half_pages; i < busy_pages; ++i) {
    OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(i), page_id, page_buf));
  }
  {
    common::TCRWLock::RLockGuard guard(wbp_.lock_);
    ObSpinLockGuard list_guard(wbp_.free_list_lock_);
    share::ObTenantEnv::set_tenant(&pressure_tenant);
    ASSERT_EQ(0, wbp_.cal_auto_shrink_target_page_num_());
    share::ObTenantEnv::set_tenant(accounting_guard.original_tenant_);
  }
  LOG_INFO("busy pool has no useful target", K(wbp_.used_page_num_), K(wbp_.fat_.size()));
  LOG_INFO("========== test_shrink_tenant_pressure_target end ==========");
}

// B1. 完全空池复用原有状态机，一轮释放到一个块，并保留后续分配和扩容能力。
// 场景：32 个块全部空闲，周期结束时选择最小池目标。
// 操作：推进 SWAP、释放块、完成收尾，再分配一页并主动扩容。
// 校验：只保留 253 页；上下文清除，释放后页可正常重新分配。
TEST_F(TestTmpFileWBPIndexCache, test_empty_pool_shrinks_to_one_block)
{
  LOG_INFO("========== test_empty_pool_shrinks_to_one_block begin ==========");
  wbp_.default_wbp_memory_limit_ = 32 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  bool is_auto = false;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_TRUE(is_auto);

  // 空池不需要真实 IO，直接验证原有逐页检查和释放路径。
  OK(wbp_.begin_shrinking(is_auto));
  OK(wbp_.advance_shrink_state());
  OK(wbp_.advance_shrink_state());
  ASSERT_EQ(WBPShrinkContext::SHRINKING_RELEASE_BLOCKS, wbp_.get_wbp_state());
  OK(wbp_.release_blocks_in_shrink_range());
  OK(wbp_.advance_shrink_state());
  OK(wbp_.finish_shrinking());
  ASSERT_FALSE(wbp_.shrink_ctx_.is_valid());
  ASSERT_EQ(ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE, wbp_.capacity_);
  ASSERT_EQ(ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS, wbp_.fat_.size());
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);

  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *page_buf = nullptr;
  OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*virtual_page_id*/), page_id, page_buf));
  OK(wbp_.expand_());
  ASSERT_EQ(2 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE, wbp_.capacity_);
  LOG_INFO("========== test_empty_pool_shrinks_to_one_block end ==========");
}

// B2. 新写入占满本轮目标时取消缩容，原收尾恢复禁分配页并重新允许扩容。
// 场景：32 个块按 8% 历史峰值选择 6 块目标，尚未释放内存。
// 操作：在保留区间内分配到目标容量，由真实定时回调执行取消收尾。
// 校验：差一页时继续，恰满时取消；扩容先被禁止、取消后恢复，存活页水位为 19%。
TEST_F(TestTmpFileWBPIndexCache, test_shrink_aborts_when_target_is_full)
{
  LOG_INFO("========== test_shrink_aborts_when_target_is_full begin ==========");
  wbp_.default_wbp_memory_limit_ = 32 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  wbp_.max_used_watermark_after_shrinking_ = 8;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  bool is_auto = false;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  OK(wbp_.begin_shrinking(is_auto));
  OK(wbp_.advance_shrink_state());
  const int64_t target_pages = wbp_.shrink_ctx_.lower_page_id_;
  const int64_t original_capacity = wbp_.capacity_;
  wbp_.default_wbp_memory_limit_ = 64 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  ASSERT_EQ(OB_OP_NOT_ALLOW, wbp_.expand_());
  ASSERT_EQ(original_capacity, wbp_.capacity_);

  // 不模拟计数，真实分配以覆盖禁分配区间和缩容链表。
  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *page_buf = nullptr;
  for (int64_t i = 0; i < target_pages - 1; ++i) {
    OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*tree_level*/, i), page_id, page_buf));
  }
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*tree_level*/, target_pages - 1),
      page_id, page_buf));
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_GT(wbp_.shrink_ctx_.shrink_list_size_, 0);
  LOG_INFO("target filled before release", K(target_pages), K(wbp_.used_page_num_),
      K(wbp_.shrink_ctx_));
  // 取消路径不进入 SWAP，未初始化的刷盘管理器不会参与 IO。
  wbp_.max_used_watermark_after_shrinking_ = 100;
  ObSSTmpFileFlushManager flush_mgr;
  ObTmpFileShrinkWBPTask task;
  OK(task.init(&wbp_, &flush_mgr));
  const int64_t before_cancel_ts_us = ObTimeUtility::current_time_us();
  task.runTimerTask();
  ASSERT_FALSE(wbp_.shrink_ctx_.is_valid());
  ASSERT_EQ(WBPShrinkContext::INVALID, wbp_.get_wbp_state());
  ASSERT_EQ(original_capacity, wbp_.capacity_);
  ASSERT_EQ(19, wbp_.max_used_watermark_after_shrinking_);
  ASSERT_GE(wbp_.last_shrink_complete_ts_, before_cancel_ts_us);
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*tree_level*/, target_pages), page_id, page_buf));
  ASSERT_GE(page_id, target_pages);
  OK(wbp_.expand_());
  ASSERT_EQ(wbp_.default_wbp_memory_limit_, wbp_.capacity_);
  LOG_INFO("========== test_shrink_aborts_when_target_is_full end ==========");
}

// B3. 超配置上限仍优先启动，单轮五分钟超时不随一分钟统计周期改变。
// 场景：32 个块的池将配置上限调低到 16 块，当前周期尚未结束。
// 操作：启动超限缩容，再把执行起点推进到五分钟前并检查取消。
// 校验：启动不受周期或历史峰值限制，超时后沿用原收尾并保留原容量。
TEST_F(TestTmpFileWBPIndexCache, test_manual_shrink_and_execution_timeout)
{
  LOG_INFO("========== test_manual_shrink_and_execution_timeout begin ==========");
  wbp_.default_wbp_memory_limit_ = 32 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  const int64_t original_capacity = wbp_.capacity_;
  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *page_buf = nullptr;
  OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*virtual_page_id*/), page_id, page_buf));
  wbp_.default_wbp_memory_limit_ = 16 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  wbp_.max_used_watermark_after_shrinking_ = 100;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us();
  bool is_auto = true;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);
  OK(wbp_.begin_shrinking(is_auto));
  ASSERT_EQ(1, wbp_.max_used_watermark_after_shrinking_);
  OK(wbp_.advance_shrink_state());
  ASSERT_EQ(16 * ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS, wbp_.shrink_ctx_.lower_page_id_);

  // 超时使用原上下文起点，不依赖等待五分钟，也不修改超时常量。
  wbp_.shrink_ctx_.shrink_begin_ts_ = ObTimeUtility::current_time_us() - 2_min;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  wbp_.shrink_ctx_.shrink_begin_ts_ = ObTimeUtility::current_time_us() - 5_min - 1_us;
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  OK(wbp_.finish_shrinking());
  ASSERT_EQ(original_capacity, wbp_.capacity_);
  ASSERT_FALSE(wbp_.shrink_ctx_.is_valid());
  ASSERT_EQ(1, wbp_.max_used_watermark_after_shrinking_);
  LOG_INFO("========== test_manual_shrink_and_execution_timeout end ==========");
}

// B4. 整块对齐后没有缩容收益时，定时任务保持未启动状态且不立即重试。
// 场景：正常租户内存压力下，两个空闲块带有 39% 历史水位，满足触发条件。
// 操作：连续调用真实定时回调，先验证跳过限频，再将时间推进一分钟重试。
// 校验：周期内保持 INVALID 且时间戳不变；下个周期可按空池目标正常启动。
TEST_F(TestTmpFileWBPIndexCache, test_shrink_timer_skips_unprofitable_target)
{
  LOG_INFO("========== test_shrink_timer_skips_unprofitable_target begin ==========");
  ASSERT_FALSE(wbp_.is_tenant_memory_under_pressure_());
  wbp_.default_wbp_memory_limit_ = 2 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  const int64_t original_capacity = wbp_.capacity_;
  wbp_.max_used_watermark_after_shrinking_ = 39;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;

  // 未初始化的刷盘管理器刻意保证此路径不能进入 SWAP 或发出 IO。
  ObSSTmpFileFlushManager flush_mgr;
  ObTmpFileShrinkWBPTask task;
  OK(task.init(&wbp_, &flush_mgr));
  task.runTimerTask();
  LOG_INFO("unprofitable round skipped", K(wbp_.get_wbp_state()), K(wbp_.capacity_),
      K(wbp_.max_used_watermark_after_shrinking_));
  ASSERT_EQ(WBPShrinkContext::INVALID, wbp_.get_wbp_state());
  ASSERT_FALSE(wbp_.shrink_ctx_.is_valid());
  ASSERT_EQ(original_capacity, wbp_.capacity_);
  ASSERT_EQ(0, wbp_.max_used_watermark_after_shrinking_);
  const int64_t skipped_round_ts_us = wbp_.last_shrink_complete_ts_;
  for (int64_t i = 0; i < 3; ++i) {
    task.runTimerTask();
    LOG_INFO("verify skipped-round throttle", K(i), K(wbp_.get_wbp_state()));
    ASSERT_EQ(WBPShrinkContext::INVALID, wbp_.get_wbp_state());
    ASSERT_EQ(skipped_round_ts_us, wbp_.last_shrink_complete_ts_);
  }
  bool is_auto = true;
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);

  // 旧峰值已清理，下一分钟无需新分配也能按空池目标启动。
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  task.runTimerTask();
  ASSERT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp_.get_wbp_state());
  ASSERT_TRUE(wbp_.shrink_ctx_.is_valid());
  ASSERT_EQ(ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS, wbp_.shrink_ctx_.lower_page_id_);
  OK(wbp_.finish_shrinking());
  LOG_INFO("========== test_shrink_timer_skips_unprofitable_target end ==========");
}

// B5. 预判后历史水位升高时，初始化拒绝本轮并正常结束旧窗口。
// 场景：32 个空闲块以 8% 历史水位获得启动资格，随后出现短时使用高峰并释放。
// 操作：初始化前将历史水位提高到 40%，使其不再满足正常缩容条件。
// 校验：不创建上下文、不推进状态，窗口重置，下一次检查不会高频重试。
TEST_F(TestTmpFileWBPIndexCache, test_shrink_init_revalidates_usage_window)
{
  LOG_INFO("========== test_shrink_init_revalidates_usage_window begin ==========");
  ASSERT_FALSE(wbp_.is_tenant_memory_under_pressure_());
  wbp_.default_wbp_memory_limit_ = 32 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  wbp_.max_used_watermark_after_shrinking_ = 8;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  bool is_auto = false;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_TRUE(is_auto);
  ASSERT_EQ(8, wbp_.max_used_watermark_after_shrinking_);

  // 模拟两次持锁区间之间新增的历史峰值；初始化必须读取它，不能提前清零。
  wbp_.max_used_watermark_after_shrinking_ = 40;
  ASSERT_EQ(OB_OP_NOT_ALLOW, wbp_.begin_shrinking(is_auto));
  LOG_INFO("changed usage rejected", K(wbp_.get_wbp_state()),
      K(wbp_.max_used_watermark_after_shrinking_));
  ASSERT_EQ(WBPShrinkContext::INVALID, wbp_.get_wbp_state());
  ASSERT_FALSE(wbp_.shrink_ctx_.is_valid());
  ASSERT_EQ(0, wbp_.max_used_watermark_after_shrinking_);
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);
  LOG_INFO("========== test_shrink_init_revalidates_usage_window end ==========");
}

// B6. 配置超限初始化失败也重置旧峰值，并保留当前存活页作为新窗口起点。
// 场景：四个块中分配一页，降低上限触发非自动缩容，初始化前恢复上限。
// 操作：按原超限模式初始化，目标回到池末尾而返回边界错误。
// 校验：未启动、容量未变；即使 is_auto 为 false 且初始化失败，水位仍重置为 1%。
TEST_F(TestTmpFileWBPIndexCache, test_manual_shrink_init_failure_reseeds_usage)
{
  LOG_INFO("========== test_manual_shrink_init_failure_reseeds_usage begin ==========");
  constexpr int64_t INITIAL_BLOCKS = 4;
  wbp_.default_wbp_memory_limit_ = INITIAL_BLOCKS * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *page_buf = nullptr;
  OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*virtual_page_id*/), page_id, page_buf));

  // 先取得超限启动资格，随后恢复配置，模拟两次判断之间的配置变化。
  wbp_.default_wbp_memory_limit_ = 2 * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  wbp_.max_used_watermark_after_shrinking_ = 100;
  bool is_auto = true;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_FALSE(is_auto);
  wbp_.default_wbp_memory_limit_ = INITIAL_BLOCKS * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  ASSERT_EQ(OB_INVALID_ARGUMENT, wbp_.begin_shrinking(is_auto));
  LOG_INFO("manual initialization failed after config changed", K(wbp_.capacity_),
      K(wbp_.max_used_watermark_after_shrinking_), K(wbp_.get_wbp_state()));
  ASSERT_EQ(WBPShrinkContext::INVALID, wbp_.get_wbp_state());
  ASSERT_FALSE(wbp_.shrink_ctx_.is_valid());
  ASSERT_EQ(wbp_.default_wbp_memory_limit_, wbp_.capacity_);
  ASSERT_EQ(1, wbp_.used_page_num_);
  ASSERT_EQ(1, wbp_.max_used_watermark_after_shrinking_);
  ASSERT_EQ(1, wbp_.get_current_used_percent_());
  LOG_INFO("========== test_manual_shrink_init_failure_reseeds_usage end ==========");
}

// B7. 正常释放整块后按新容量记录存活页峰值，且没有新分配也能启动下一轮。
// 场景：先在低地址两块内分配元数据页，再扩到 32 块，历史峰值为 8%。
// 操作：完成到六块的缩容，保留原页面；一分钟后再选择目标，不执行新分配。
// 校验：容量为六块且原页面内容有效，水位从旧峰值重置为 34%，下一轮目标为四块。
TEST_F(TestTmpFileWBPIndexCache, test_shrink_completion_reseeds_surviving_pages)
{
  LOG_INFO("========== test_shrink_completion_reseeds_surviving_pages begin ==========");
  ASSERT_FALSE(wbp_.is_tenant_memory_under_pressure_());
  constexpr int64_t INITIAL_BLOCKS = 32;
  constexpr int64_t LIVE_BLOCKS = 2;
  constexpr int64_t FIRST_TARGET_BLOCKS = 6;
  wbp_.default_wbp_memory_limit_ = INITIAL_BLOCKS * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE;
  const int64_t live_pages = LIVE_BLOCKS * ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS;
  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *page_buf = nullptr;

  // 先分配再扩容，确保所有存活页都在本轮保留区间，释放尾块不需要 IO。
  for (int64_t i = 0; i < live_pages; ++i) {
    OK(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(0 /*tree_level*/, i), page_id, page_buf));
    ASSERT_LT(page_id, live_pages);
    page_buf[0] = 'w';
  }
  while (wbp_.capacity_ < wbp_.default_wbp_memory_limit_) {
    OK(wbp_.expand_());
  }
  wbp_.max_used_watermark_after_shrinking_ = 8;
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  bool is_auto = false;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_TRUE(is_auto);
  OK(wbp_.begin_shrinking(is_auto));
  ASSERT_EQ(FIRST_TARGET_BLOCKS * ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS,
      wbp_.shrink_ctx_.lower_page_id_);
  ASSERT_EQ(7, wbp_.max_used_watermark_after_shrinking_);
  OK(wbp_.advance_shrink_state());
  OK(wbp_.advance_shrink_state());
  ASSERT_EQ(WBPShrinkContext::SHRINKING_RELEASE_BLOCKS, wbp_.get_wbp_state());
  OK(wbp_.release_blocks_in_shrink_range());
  OK(wbp_.advance_shrink_state());
  ASSERT_EQ(WBPShrinkContext::SHRINKING_FINISH, wbp_.get_wbp_state());

  // 人为保留旧高峰以确认收尾既清理旧值，也按缩小后的分母记录现存用量。
  wbp_.max_used_watermark_after_shrinking_ = 90;
  const int64_t before_finish_ts_us = ObTimeUtility::current_time_us();
  OK(wbp_.finish_shrinking());
  LOG_INFO("surviving pages seeded after release", K(wbp_.capacity_), K(wbp_.used_page_num_),
      K(wbp_.max_used_watermark_after_shrinking_));
  ASSERT_EQ(FIRST_TARGET_BLOCKS * ObSSTmpWriteBufferPool::WBP_BLOCK_SIZE, wbp_.capacity_);
  ASSERT_EQ(live_pages, wbp_.used_page_num_);
  ASSERT_EQ('w', page_buf[0]);
  ASSERT_EQ(34, wbp_.max_used_watermark_after_shrinking_);
  ASSERT_FALSE(wbp_.shrink_ctx_.is_valid());
  ASSERT_GE(wbp_.last_shrink_complete_ts_, before_finish_ts_us);
  ASSERT_FALSE(wbp_.need_to_shrink(is_auto));

  // 下一分钟仍有相同页面，34% 峰值对应 54% 目标，六块向上对齐为四块。
  wbp_.last_shrink_complete_ts_ = ObTimeUtility::current_time_us() - 1_min;
  ASSERT_TRUE(wbp_.need_to_shrink(is_auto));
  ASSERT_TRUE(is_auto);
  OK(wbp_.begin_shrinking(is_auto));
  ASSERT_EQ(4 * ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS, wbp_.shrink_ctx_.lower_page_id_);
  OK(wbp_.advance_shrink_state());
  OK(wbp_.finish_shrinking());
  LOG_INFO("========== test_shrink_completion_reseeds_surviving_pages end ==========");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_write_buffer_pool_index_cache.log*");
  OB_LOGGER.set_file_name("test_tmp_file_write_buffer_pool_index_cache.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
