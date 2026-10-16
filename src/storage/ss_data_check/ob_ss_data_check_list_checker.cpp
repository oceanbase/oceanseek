/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_list_checker.h"
#include "lib/time/ob_time_utility.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_bucket_num_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObSSDataCheckListChecker::MetaRefKey::MetaRefKey()
  : ls_id_(),
    tablet_id_(),
    reorganize_scn_(),
    meta_scn_()
{
}

ObSSDataCheckListChecker::MetaRefKey::MetaRefKey(
    const ObSSDataCheckExpectedItem &item)
  : ls_id_(item.get_ls_id()),
    tablet_id_(item.get_tablet_id()),
    reorganize_scn_(item.get_reorganize_scn()),
    meta_scn_(item.get_meta_scn())
{
}

bool ObSSDataCheckListChecker::MetaRefKey::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && reorganize_scn_.is_valid()
      && meta_scn_.is_valid();
}

bool ObSSDataCheckListChecker::MetaRefKey::operator==(
    const MetaRefKey &other) const
{
  return ls_id_ == other.ls_id_ && tablet_id_ == other.tablet_id_
      && reorganize_scn_ == other.reorganize_scn_ && meta_scn_ == other.meta_scn_;
}

uint64_t ObSSDataCheckListChecker::MetaRefKey::hash() const
{
  uint64_t hash_value = 0;
  const int64_t ls_id = ls_id_.id();
  const uint64_t tablet_id = tablet_id_.id();
  const uint64_t reorganize_scn = reorganize_scn_.get_val_for_inner_table_field();
  const uint64_t meta_scn = meta_scn_.get_val_for_inner_table_field();
  hash_value = murmurhash(&ls_id, sizeof(ls_id), hash_value);
  hash_value = murmurhash(&tablet_id, sizeof(tablet_id), hash_value);
  hash_value = murmurhash(&reorganize_scn, sizeof(reorganize_scn), hash_value);
  hash_value = murmurhash(&meta_scn, sizeof(meta_scn), hash_value);
  return hash_value;
}

int ObSSDataCheckListChecker::MetaRefKey::hash(uint64_t &hash_value) const
{
  hash_value = hash();
  return OB_SUCCESS;
}

ObSSDataCheckListChecker::MetaRefCache::MetaRefCache()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    bucket_count_(0),
    meta_ref_checker_(nullptr),
    result_map_()
{
}

ObSSDataCheckListChecker::MetaRefCache::~MetaRefCache()
{
  reset();
}

int ObSSDataCheckListChecker::MetaRefCache::init(
    const uint64_t tenant_id,
    const int64_t expected_item_count,
    const ObISSDataCheckMetaRefChecker &meta_ref_checker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check meta reference cache initialized twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || expected_item_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check meta reference cache argument", KR(ret),
        K(tenant_id), K(expected_item_count));
  } else {
    const int64_t max_bucket_count = share::ObBucketNumHelper::get_bucket_num_by_mtl_mem(
            share::ObGetBucketNumType::TABLET_TYPE);
    tenant_id_ = tenant_id;
    bucket_count_ = MIN(expected_item_count, max_bucket_count);
    meta_ref_checker_ = &meta_ref_checker;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckListChecker::MetaRefCache::check_reference_exist(
    const ObSSDataCheckExpectedItem &item,
    bool &is_exist) const
{
  int ret = OB_SUCCESS;
  bool tmp_is_exist = false;
  const MetaRefKey key(item);
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check meta reference cache not initialized", KR(ret));
  } else if (OB_UNLIKELY(!item.is_valid() || !key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check meta reference cache key", KR(ret), K(item));
  } else if (!result_map_.created() && OB_FAIL(result_map_.create(
          bucket_count_, ObMemAttr(tenant_id_, "DCMetaRefMap")))) {
    LOG_WARN("failed to create data check meta reference result map",
        KR(ret), K_(tenant_id), K_(bucket_count));
  } else if (OB_FAIL(result_map_.get_refactored(key, tmp_is_exist))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(meta_ref_checker_->check_reference_exist(item, tmp_is_exist))) {
        LOG_WARN("failed to query data check meta reference", KR(ret), K(item));
      } else if (OB_FAIL(result_map_.set_refactored(key, tmp_is_exist))) {
        LOG_WARN("failed to cache data check meta reference result", KR(ret), K(key), K(tmp_is_exist));
      }
    } else {
      LOG_WARN("failed to get data check meta reference cache", KR(ret), K(key));
    }
  }
  if (OB_SUCC(ret)) {
    is_exist = tmp_is_exist;
  }
  return ret;
}

void ObSSDataCheckListChecker::MetaRefCache::reset()
{
  if (result_map_.created()) {
    result_map_.destroy();
  }
  is_inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  bucket_count_ = 0;
  meta_ref_checker_ = nullptr;
}

bool ObSSDataCheckListChecker::MetaRefCache::is_valid() const
{
  return is_inited_ && OB_INVALID_ID != tenant_id_ && bucket_count_ > 0
      && OB_NOT_NULL(meta_ref_checker_);
}

int ObSSDataCheckListCheckUtil::get_parent_prefix(
    const ObString &relative_path,
    char *prefix_buf,
    const int64_t prefix_buf_len,
    ObString &prefix)
{
  int ret = OB_SUCCESS;
  int64_t slash_pos = -1;
  prefix.reset();
  if (OB_UNLIKELY(relative_path.empty() || OB_ISNULL(prefix_buf) || prefix_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relative path prefix argument", KR(ret),
        K(relative_path), KP(prefix_buf), K(prefix_buf_len));
  } else {
    for (int64_t i = relative_path.length() - 1; slash_pos < 0 && i >= 0; --i) {
      if ('/' == relative_path.ptr()[i]) {
        slash_pos = i;
      }
    }
    const int64_t prefix_len = slash_pos + 1;
    if (OB_UNLIKELY(prefix_len <= 0 || prefix_len >= prefix_buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expected object parent prefix", KR(ret),
          K(relative_path), K(prefix_len), K(prefix_buf_len));
    } else {
      MEMCPY(prefix_buf, relative_path.ptr(), prefix_len);
      prefix_buf[prefix_len] = '\0';
      prefix.assign_ptr(prefix_buf, static_cast<int32_t>(prefix_len));
    }
  }
  return ret;
}

bool ObSSDataCheckListCheckUtil::has_exact_parent_prefix(
    const ObString &relative_path,
    const ObString &prefix)
{
  bool is_same_parent = relative_path.length() > prefix.length()
      && relative_path.prefix_match(prefix);
  int64_t slash_pos = -1;
  for (int64_t i = relative_path.length() - 1; is_same_parent && slash_pos < 0 && i >= 0; --i) {
    if ('/' == relative_path.ptr()[i]) {
      slash_pos = i;
    }
  }
  return is_same_parent && slash_pos + 1 == prefix.length();
}

ObSSDataCheckListChecker::ExpectedCursor::ExpectedCursor(
    const int64_t page_size)
  : iterator_(page_size),
    current_(nullptr),
    expected_count_(0),
    fetched_count_(0),
    is_end_(false),
    previous_path_buf_()
{
}

int ObSSDataCheckListChecker::ExpectedCursor::init(
    const ObSSDataCheckSortedFileInfo &expected_file,
    const int64_t expected_count,
    const uint64_t tenant_id,
    const int64_t io_expire_timestamp,
    const int64_t file_buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expected_count <= 0 || !expected_file.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected cursor argument", KR(ret), K(expected_file), K(expected_count));
  } else if (OB_FAIL(iterator_.init_for_pread(
      expected_file, io_expire_timestamp, tenant_id, file_buf_size))) {
    LOG_WARN("failed to initialize expected cursor", KR(ret), K(expected_file));
  } else if (OB_FAIL(iterator_.prefetch())) {
    LOG_WARN("failed to prefetch expected cursor", KR(ret));
  } else {
    expected_count_ = expected_count;
    if (OB_FAIL(advance())) {
      LOG_WARN("failed to read first expected item", KR(ret));
    }
  }
  return ret;
}

int ObSSDataCheckListChecker::ExpectedCursor::advance()
{
  int ret = OB_SUCCESS;
  ObString previous_path;
  if (OB_NOT_NULL(current_)) {
    const ObString &path = current_->get_relative_path();
    MEMCPY(previous_path_buf_, path.ptr(), path.length());
    previous_path.assign_ptr(previous_path_buf_, path.length());
  }
  current_ = nullptr;
  const ObSSDataCheckExpectedSortItem *next = nullptr;
  if (OB_FAIL(iterator_.get_next_item(next))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      is_end_ = true;
      if (OB_UNLIKELY(fetched_count_ != expected_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected file ended before recorded item count", KR(ret),
            K_(fetched_count), K_(expected_count));
      }
    } else {
      LOG_WARN("failed to read next expected item", KR(ret));
    }
  } else if (OB_ISNULL(next) || OB_UNLIKELY(!next->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expected cursor item", KR(ret), KP(next));
  } else if (OB_UNLIKELY(fetched_count_ >= expected_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected file contains more items than recorded", KR(ret),
        K_(fetched_count), K_(expected_count), KPC(next));
  } else if (!previous_path.empty()
      && OB_UNLIKELY(previous_path.compare(next->get_relative_path()) >= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected file is not strictly ordered", KR(ret), K(previous_path), KPC(next));
  } else {
    current_ = next;
    ++fetched_count_;
  }
  return ret;
}

bool ObSSDataCheckListChecker::ExpectedCursor::has_current() const
{
  return OB_NOT_NULL(current_);
}

const ObSSDataCheckExpectedSortItem &
ObSSDataCheckListChecker::ExpectedCursor::current() const
{
  return *current_;
}

bool ObSSDataCheckListChecker::ExpectedCursor::is_complete() const
{
  return is_end_ && fetched_count_ == expected_count_ && OB_ISNULL(current_);
}

ObSSDataCheckListChecker::PrefixCompareOp::PrefixCompareOp(
    const ObString &prefix,
    ExpectedCursor &cursor,
    const ObISSDataCheckMetaRefChecker &meta_ref_checker,
    const share::ObCheckStopProvider &stop_checker,
    ObSSDataCheckCtx &ctx,
    ObSSDataCheckListCompareStat &stat)
  : prefix_(prefix),
    cursor_(cursor),
    meta_ref_checker_(meta_ref_checker),
    stop_checker_(stop_checker),
    ctx_(ctx),
    stat_(stat),
    previous_name_buf_(),
    previous_name_()
{
}

int ObSSDataCheckListChecker::PrefixCompareOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry) || OB_UNLIKELY('\0' == entry->d_name[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid listed object entry", KR(ret), KP(entry));
  } else {
    const int64_t name_len = STRLEN(entry->d_name);
    const ObString actual_name(static_cast<int32_t>(name_len), entry->d_name);
    if (OB_UNLIKELY(name_len >= OB_MAX_URI_LENGTH)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("listed object name is too long", KR(ret), K(name_len));
    } else if (!previous_name_.empty() && OB_UNLIKELY(previous_name_.compare(actual_name) > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sorted LIST output is not ordered", KR(ret),
          K_(previous_name), K(actual_name), K_(prefix));
    } else {
      MEMCPY(previous_name_buf_, actual_name.ptr(), actual_name.length());
      previous_name_buf_[actual_name.length()] = '\0';
      previous_name_.assign_ptr(previous_name_buf_, actual_name.length());
      ++stat_.listed_object_count_;
      if (OB_FAIL(compare_one_actual_(actual_name))) {
        LOG_WARN("failed to compare listed object", KR(ret), K(actual_name), K_(prefix));
      }
    }
  }
  return ret;
}

int ObSSDataCheckListChecker::PrefixCompareOp::drain_missing()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && cursor_.has_current()
      && ObSSDataCheckListCheckUtil::has_exact_parent_prefix(
          cursor_.current().get_relative_path(), prefix_)) {
    if (OB_FAIL(handle_missing_())) {
      LOG_WARN("failed to handle remaining missing object", KR(ret), K_(prefix));
    }
  }
  return ret;
}

int ObSSDataCheckListChecker::PrefixCompareOp::compare_one_actual_(
    const ObString &actual_name)
{
  int ret = OB_SUCCESS;
  bool is_matched = false;
  while (OB_SUCC(ret) && !is_matched && cursor_.has_current()
      && ObSSDataCheckListCheckUtil::has_exact_parent_prefix(
          cursor_.current().get_relative_path(), prefix_)) {
    const ObString &expected_path = cursor_.current().get_relative_path();
    const ObString expected_name(expected_path.length() - prefix_.length(),
        expected_path.ptr() + prefix_.length());
    const int compare_result = expected_name.compare(actual_name);
    if (compare_result < 0) {
      if (OB_FAIL(handle_missing_())) {
        LOG_WARN("failed to handle missing object", KR(ret), K(expected_name), K(actual_name));
      }
    } else if (0 == compare_result) {
      ++stat_.found_count_;
      is_matched = true;
      if (OB_FAIL(cursor_.advance())) {
        LOG_WARN("failed to advance expected cursor", KR(ret));
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObSSDataCheckListChecker::PrefixCompareOp::handle_missing_()
{
  int ret = OB_SUCCESS;
  bool reference_exist = false;
  const ObSSDataCheckExpectedItem &item = cursor_.current().get_item();
  if (OB_FAIL(stop_checker_.check_stop())) {
    LOG_INFO("data check LIST comparison stopped before shared meta recheck", KR(ret), K(item));
  } else if (OB_FAIL(meta_ref_checker_.check_reference_exist(item, reference_exist))) {
    LOG_WARN("failed to recheck exact shared meta reference", KR(ret), K(item));
  } else if (reference_exist && OB_FAIL(ctx_.record_referenced_missing(item))) {
    LOG_WARN("failed to record referenced missing object", KR(ret), K(item));
  } else {
    ++stat_.missing_count_;
    if (reference_exist) {
      ++stat_.referenced_missing_count_;
    } else {
      ++stat_.unreferenced_missing_count_;
    }
    if (OB_FAIL(cursor_.advance())) {
      LOG_WARN("failed to advance after missing object", KR(ret), K(item));
    }
  }
  return ret;
}

ObSSDataCheckObjectLister::ObSSDataCheckObjectLister()
  : is_inited_(false),
    tenant_root_uri_(),
    storage_info_(nullptr)
{
}

int ObSSDataCheckObjectLister::init(
    const ObString &tenant_root_uri,
    const ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check object lister initialized twice", KR(ret));
  } else if (OB_UNLIKELY(tenant_root_uri.empty()
      || tenant_root_uri.length() >= sizeof(tenant_root_uri_) || OB_NOT_NULL(tenant_root_uri.find('\0'))
      || OB_ISNULL(storage_info) || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check object lister argument", KR(ret),
        K(tenant_root_uri), KP(storage_info));
  } else {
    MEMCPY(tenant_root_uri_, tenant_root_uri.ptr(), tenant_root_uri.length());
    tenant_root_uri_[tenant_root_uri.length()] = '\0';
    storage_info_ = storage_info;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckObjectLister::list_files(
    const ObString &relative_prefix,
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  char absolute_prefix[OB_MAX_URI_LENGTH] = {};
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check object lister not initialized", KR(ret));
  } else if (OB_UNLIKELY(relative_prefix.empty() || '/' == relative_prefix.ptr()[0]
      || '/' != relative_prefix.ptr()[relative_prefix.length() - 1]
      || OB_NOT_NULL(relative_prefix.find('\0')))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check relative LIST prefix", KR(ret), K(relative_prefix));
  } else if (OB_FAIL(databuff_printf(absolute_prefix, sizeof(absolute_prefix), pos, "%s%s%.*s",
      tenant_root_uri_, '/' == tenant_root_uri_[STRLEN(tenant_root_uri_) - 1] ? "" : "/",
      relative_prefix.length(), relative_prefix.ptr()))) {
    LOG_WARN("failed to build absolute LIST prefix", KR(ret),
        K(relative_prefix), KCSTRING(tenant_root_uri_));
  } else if (OB_FAIL(ObBackupIoAdapter::list_files(
      ObString(static_cast<int32_t>(pos), absolute_prefix), storage_info_, op))) {
    LOG_WARN("failed to list shared storage objects", KR(ret),
        K(relative_prefix), KCSTRING(absolute_prefix));
  }
  return ret;
}

void ObSSDataCheckObjectLister::reset()
{
  is_inited_ = false;
  tenant_root_uri_[0] = '\0';
  storage_info_ = nullptr;
}

bool ObSSDataCheckObjectLister::is_valid() const
{
  return is_inited_ && '\0' != tenant_root_uri_[0] && OB_NOT_NULL(storage_info_);
}

ObSSDataCheckListCompareStat::ObSSDataCheckListCompareStat()
{
  reset();
}

void ObSSDataCheckListCompareStat::reset()
{
  expected_count_ = 0;
  found_count_ = 0;
  missing_count_ = 0;
  referenced_missing_count_ = 0;
  unreferenced_missing_count_ = 0;
  listed_object_count_ = 0;
  listed_prefix_count_ = 0;
}

bool ObSSDataCheckListCompareStat::is_valid() const
{
  return expected_count_ >= 0 && expected_count_ == found_count_ + missing_count_
      && missing_count_ == referenced_missing_count_ + unreferenced_missing_count_
      && listed_object_count_ >= found_count_ && listed_prefix_count_ >= 0;
}

ObSSDataCheckListChecker::ListStopChecker::ListStopChecker(
    const share::ObCheckStopProvider &stop_checker,
    const int64_t io_expire_timestamp)
  : stop_checker_(stop_checker),
    io_expire_timestamp_(io_expire_timestamp)
{
}

int ObSSDataCheckListChecker::ListStopChecker::check_stop() const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stop_checker_.check_stop())) {
  } else if (io_expire_timestamp_ > 0 && ObTimeUtility::current_time_us() >= io_expire_timestamp_) {
    ret = OB_TIMEOUT;
  }
  return ret;
}

ObSSDataCheckListChecker::ObSSDataCheckListChecker()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    file_buf_size_(0),
    io_expire_timestamp_(0),
    object_lister_(nullptr),
    meta_ref_checker_(nullptr),
    stop_checker_(nullptr),
    ctx_(nullptr)
{
}

int ObSSDataCheckListChecker::init(
    const uint64_t tenant_id,
    const int64_t file_buf_size,
    const int64_t io_expire_timestamp,
    ObISSDataCheckObjectLister &object_lister,
    const ObISSDataCheckMetaRefChecker &meta_ref_checker,
    const share::ObCheckStopProvider &stop_checker,
    ObSSDataCheckCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check LIST checker initialized twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size)
      || io_expire_timestamp < 0 || !ctx.is_valid() || ctx.get_tenant_id() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check LIST checker argument", KR(ret),
        K(tenant_id), K(file_buf_size), K(io_expire_timestamp), "ctx_tenant_id", ctx.get_tenant_id());
  } else {
    tenant_id_ = tenant_id;
    file_buf_size_ = file_buf_size;
    io_expire_timestamp_ = io_expire_timestamp;
    object_lister_ = &object_lister;
    meta_ref_checker_ = &meta_ref_checker;
    stop_checker_ = &stop_checker;
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckListChecker::check(
    const ObSSDataCheckSortedFileInfo &expected_file,
    const int64_t expected_item_count,
    ObSSDataCheckListCompareStat &stat)
{
  int ret = OB_SUCCESS;
  stat.reset();
  stat.expected_count_ = expected_item_count;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check LIST checker not initialized", KR(ret));
  } else if (OB_UNLIKELY(expected_item_count < 0
      || (0 == expected_item_count && expected_file.is_valid()) || (expected_item_count > 0
          && (!expected_file.is_valid() || expected_file.get_tenant_id() != tenant_id_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected file for LIST check", KR(ret),
        K(expected_file), K(expected_item_count), K_(tenant_id));
  } else if (0 == expected_item_count) {
    // 空 Expected 无需访问对象存储。
  } else {
    ListStopChecker list_stop_checker(
        *stop_checker_, io_expire_timestamp_);
    MetaRefCache meta_ref_cache;
    ExpectedCursor cursor(file_buf_size_);
    if (OB_FAIL(cursor.init(expected_file, expected_item_count, tenant_id_,
        io_expire_timestamp_, file_buf_size_))) {
      LOG_WARN("failed to initialize expected LIST cursor", KR(ret));
    } else if (OB_FAIL(meta_ref_cache.init(tenant_id_, expected_item_count, *meta_ref_checker_))) {
      LOG_WARN("failed to initialize data check meta reference cache", KR(ret), K(expected_item_count));
    }
    while (OB_SUCC(ret) && cursor.has_current()) {
      char prefix_buf[OB_MAX_URI_LENGTH] = {};
      ObString prefix;
      if (OB_FAIL(list_stop_checker.check_stop())) {
        LOG_INFO("data check LIST checker stopped before next prefix", KR(ret));
      } else if (OB_FAIL(ObSSDataCheckListCheckUtil::get_parent_prefix(
          cursor.current().get_relative_path(), prefix_buf, sizeof(prefix_buf), prefix))) {
        LOG_WARN("failed to get expected parent prefix", KR(ret));
      } else {
        ObSSDataCheckListedObjectSorter sorter;
        PrefixCompareOp op(prefix, cursor, meta_ref_cache, list_stop_checker, *ctx_, stat);
        if (OB_FAIL(sorter.init(tenant_id_, ctx_->get_memory_limit(), file_buf_size_,
            io_expire_timestamp_, list_stop_checker, *ctx_))) {
          LOG_WARN("failed to initialize listed object sorter", KR(ret), K(prefix));
        } else if (OB_FAIL(object_lister_->list_files(prefix, sorter))) {
          LOG_WARN("failed to LIST expected parent prefix", KR(ret), K(prefix));
        } else if (OB_FAIL(list_stop_checker.check_stop())) {
          LOG_INFO("data check LIST checker stopped after prefix LIST", KR(ret), K(prefix));
        } else if (OB_FAIL(sorter.sort_and_replay(op))) {
          LOG_WARN("failed to sort and replay listed objects", KR(ret), K(prefix));
        } else {
          ++stat.listed_prefix_count_;
          if (OB_FAIL(list_stop_checker.check_stop())) {
            LOG_INFO("data check LIST checker stopped after prefix replay", KR(ret), K(prefix));
          } else if (OB_FAIL(op.drain_missing())) {
            LOG_WARN("failed to finish expected prefix comparison", KR(ret), K(prefix));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!cursor.is_complete())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected cursor is incomplete after LIST check", KR(ret), K(stat));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!stat.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid completed LIST check statistics", KR(ret), K(stat));
  }
  return ret;
}

void ObSSDataCheckListChecker::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  file_buf_size_ = 0;
  io_expire_timestamp_ = 0;
  object_lister_ = nullptr;
  meta_ref_checker_ = nullptr;
  stop_checker_ = nullptr;
  ctx_ = nullptr;
}

bool ObSSDataCheckListChecker::is_valid() const
{
  return is_inited_ && OB_INVALID_ID != tenant_id_
      && ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size_)
      && io_expire_timestamp_ >= 0 && OB_NOT_NULL(object_lister_) && OB_NOT_NULL(meta_ref_checker_)
      && OB_NOT_NULL(stop_checker_) && OB_NOT_NULL(ctx_);
}

} // namespace storage
} // namespace oceanbase
