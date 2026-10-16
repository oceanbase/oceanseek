/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_meta_ref_checker.h"
#include "storage/incremental/ob_shared_meta_service.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;

ObSSDataCheckMetaRefChecker::ObSSDataCheckMetaRefChecker()
  : is_inited_(false),
    meta_service_(nullptr)
{
}

int ObSSDataCheckMetaRefChecker::init(ObSSMetaService &meta_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check meta reference checker initialized twice", KR(ret));
  } else {
    meta_service_ = &meta_service;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckMetaRefChecker::check_reference_exist(
    const ObSSDataCheckExpectedItem &item,
    bool &is_exist) const
{
  int ret = OB_SUCCESS;
  bool tmp_is_exist = false;
  ObArenaAllocator allocator("DCMetaRef", OB_MALLOC_NORMAL_BLOCK_SIZE,
      MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObSSMetaTabletKey tablet_key;
  ObSSMetaTabletRow tablet_row;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check meta reference checker not initialized", KR(ret));
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check expected item", KR(ret), K(item));
  } else {
    tablet_key.ls_id_ = item.get_ls_id();
    tablet_key.tablet_id_ = item.get_tablet_id();
    tablet_key.reorganization_scn_ = item.get_reorganize_scn();
    tablet_key.meta_scn_ = item.get_meta_scn();
    if (OB_UNLIKELY(!tablet_key.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid exact shared meta tablet key", KR(ret), K(tablet_key), K(item));
    } else if (OB_FAIL(meta_service_->read_tablet_meta(tablet_key, allocator, tablet_row))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to read exact shared meta tablet row", KR(ret), K(tablet_key), K(item));
      }
    } else {
      tmp_is_exist = true;
    }
  }
  if (OB_SUCC(ret)) {
    is_exist = tmp_is_exist;
  }
  return ret;
}

void ObSSDataCheckMetaRefChecker::reset()
{
  is_inited_ = false;
  meta_service_ = nullptr;
}

bool ObSSDataCheckMetaRefChecker::is_valid() const
{
  return is_inited_ && OB_NOT_NULL(meta_service_);
}

} // namespace storage
} // namespace oceanbase
