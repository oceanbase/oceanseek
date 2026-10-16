/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/meta_store/ob_storage_meta_io_util.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_file_manager.h"
#endif

namespace oceanbase
{
namespace storage
{
#ifdef OB_BUILD_SHARED_STORAGE
int ObStorageMetaIOUtil::check_meta_existence(
    const blocksstable::ObStorageObjectOpt &opt, const int64_t ls_epoch, bool &is_exist)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId object_id;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
    LOG_WARN("fail to get object id", K(ret), K(opt));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_is_exist_object(object_id, ls_epoch, is_exist,
      blocksstable::ObStorageObjectType::PRIVATE_TABLET_META == opt.object_type_
          ? opt.ss_private_tablet_opt_.ls_id_ : 0))) {
    LOG_WARN("fail to check existence", K(ret), K(object_id));
  }
  return ret;
}
#endif

/*static*/int ObStorageMetaIOUtil::collect_private_tablet_versions(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObTabletID &tablet_id,
    const int64_t reorganization_scn,
    const int64_t start_version,
    const int64_t end_version,
    const bool allow_stale_versions,
    common::ObIArray<int64_t> &tablet_versions)
{
  int ret = OB_SUCCESS;
  static const int64_t LIST_THRESHOLD = 10;
  bool is_exist = false;
  bool use_list = false;
  tablet_versions.reset();
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for SN mode", K(ret), K(common::lbt()));
  } 
#ifdef OB_BUILD_SHARED_STORAGE
  else if (OB_UNLIKELY(!ls_id.is_valid() 
                       || (ls_epoch < 0) 
                       || !tablet_id.is_valid() 
                       || (reorganization_scn < 0)
                       || (start_version < 0) 
                       || (end_version < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", KR(ret), K(ls_id), K(ls_epoch), K(tablet_id),
             K(reorganization_scn),
             K(start_version), K(end_version));
  } else if (OB_UNLIKELY(start_version > end_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected version", KR(ret), K(start_version), K(end_version));
  } else if ((end_version - start_version + 1) < LIST_THRESHOLD) {
    for (int64_t ver = start_version; OB_SUCC(ret) && (ver <= end_version); ++ver) {
      blocksstable::ObStorageObjectOpt opt;
      opt.set_ss_private_tablet_meta_object_opt(ls_id.id(), tablet_id.id(), ver,
          tablet_id.is_ls_inner_tablet(), reorganization_scn);
      if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, ls_epoch, is_exist))) {
        LOG_WARN("failed to check meta existence", KR(ret), K(opt), K(ls_epoch));
      } else if (is_exist) {
        if (OB_FAIL(tablet_versions.push_back(ver))) {
          LOG_WARN("failed to push_back", KR(ret), K(ver));
        }
      }
    }
  } else {
    use_list = true;
    const int64_t private_macro_third_id = tablet_id.is_ls_inner_tablet()
        ? ls_id.id()
        : reorganization_scn;
    ObTenantFileManager *tfm = nullptr;
    if (OB_ISNULL(tfm = MTL(ObTenantFileManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant file manager should not be null", KR(ret));
    } else if (OB_FAIL(tfm->list_private_tablet_meta(
        ls_id.id(), ls_epoch, tablet_id.id(), private_macro_third_id, tablet_versions))) {
      LOG_WARN("failed to list private tablet meta", KR(ret), K(tablet_id),
               K(reorganization_scn));
    } else {
      const int64_t total = tablet_versions.count();
      int64_t cur = 0;
      for (int64_t i = 0; OB_SUCC(ret) && (i < total); ++i) {
        const int64_t ver = tablet_versions.at(i);
        if (ver < start_version) {
          // 对象存储写超时后，底层 IO 仍可能最终成功，导致版本在 GC 水位推进后才可见，
          // LIST 作为兜底操作，需要考虑到这种异常。
          if (allow_stale_versions) { 
            tablet_versions.at(cur) = ver;
            ++cur;
          }
          LOG_WARN("collect encounter stale tablet version written after gc watermark advanced",
                   K(ver), K(start_version), K(end_version), K(ls_id), K(tablet_id), 
                   K(reorganization_scn), K(allow_stale_versions));
        } else if (ver <= end_version) {
          tablet_versions.at(cur) = ver;
          ++cur;
        } else {
          // do nothing
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && (i < (total - cur)); ++i) {
        tablet_versions.pop_back();
      }
    }
  }
  int64_t cost_time_us = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("collect tablet meta version finished", KR(ret), K(cost_time_us), K(use_list),
           K(start_version), K(end_version), K(allow_stale_versions), "gc_versions", tablet_versions);
#endif
  return ret;
}
} // namespace storage
} // namespace oceanbase
