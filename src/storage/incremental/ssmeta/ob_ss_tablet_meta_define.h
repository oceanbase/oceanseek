/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_TABLET_META_DEFINE_H_
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_TABLET_META_DEFINE_H_

#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/share/ob_shared_meta_common.h"

namespace oceanbase
{
namespace common
{
class ObArenaAllocator;
}

namespace storage
{
class ObTablet;

class ObSSTabletMetaValue : public ObSSMetaHeadValue
{
  OB_UNIS_VERSION_V(1);
public:
  ObSSTabletMetaValue()
    : data_version_(0),
      macro_info_(nullptr),
      tablet_(nullptr),
      update_log_()
  {}
  ~ObSSTabletMetaValue() {}
  int deserialize(common::ObArenaAllocator &allocator,
                  const ObMetaDiskAddr &tablet_addr,
                  const char *buf,
                  const int64_t data_len,
                  int64_t &pos);
  bool operator==(const ObSSTabletMetaValue &rhs) const
  {
    return (tablet_ == rhs.tablet_
            && head_info_ == rhs.head_info_);
  }
  ObTablet *get_tablet() { return tablet_; }
  int set_tablet_info(const uint64_t data_version,
                      const ObTabletMacroInfo *macro_info,
                      ObTablet *tablet);
  int get_update_tablet_log(ObSSUpdateTabletLog &update_log);
  static int get_tablet_raw_buffer(
      const char *buf,
      const int64_t data_len,
      common::ObArenaAllocator &allocator,
      char *&dest_buf,
      int64_t &dest_buf_len);

  INHERIT_TO_STRING_KV("ObSSMetaHeadValue", ObSSMetaHeadValue, K_(data_version), KP_(macro_info),
    KP_(tablet), K_(update_log));
private:
  int deserialize_(common::ObArenaAllocator &allocator,
                   const ObMetaDiskAddr &tablet_addr,
                   const char *buf,
                   const int64_t data_len,
                   int64_t &pos);
public:
  uint64_t data_version_;
  const ObTabletMacroInfo *macro_info_; // only include shared_meta_macro_blocks
  ObTablet *tablet_;
  ObSSUpdateTabletLog update_log_;
};

struct ObSSTabletHdlDesFunc {
  ObSSTabletHdlDesFunc(const share::ObLSID ls_id,
                       const common::ObTabletID tablet_id,
                       const share::SCN reorganization_scn,
                       const uint64_t op_id,
                       common::ObArenaAllocator *allocator)
    : ls_id_(ls_id), tablet_id_(tablet_id), reorganization_scn_(reorganization_scn),
      op_id_(op_id), allocator_(allocator) {}
  int operator()(const ObSSMetaOpBuffer &file_buffer, ObTabletHandle &tablet_handle);
  int assign(const ObSSTabletHdlDesFunc &other) {
    ls_id_ = other.ls_id_;
    tablet_id_ = other.tablet_id_;
    reorganization_scn_ = other.reorganization_scn_;
    op_id_ = other.op_id_;
    allocator_ = other.allocator_;
    return OB_SUCCESS;
  }
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorganization_scn_;
  uint64_t op_id_;
  common::ObArenaAllocator *allocator_;
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(reorganization_scn), K_(op_id));
};

class ObSSTabletMetaUtil
{
public:
  static int get_tablet_addr(const share::ObLSID &ls_id,
                             const common::ObTabletID &tablet_id,
                             const share::SCN &reorganization_scn,
                             const uint64_t op_id,
                             const int64_t tablet_buffer_size,
                             ObMetaDiskAddr &tablet_addr);
  static int generate_file_obj_opt(const uint64_t ls_id,
                                   const uint64_t tablet_id,
                                   const share::SCN &reorganization_scn,
                                   const uint64_t op_id,
                                   const bool is_ls_inner_tablet,
                                   ObStorageObjectOpt &opt);
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_TABLET_META_DEFINE_H_ */
