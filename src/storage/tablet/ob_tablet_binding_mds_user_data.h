/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_BINDING_MDS_USER_DATA
#define OCEANBASE_STORAGE_OB_TABLET_BINDING_MDS_USER_DATA

#include <stdint.h>
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
class ObTabletBindingInfo;

class ObTabletBindingMdsUserData : public ObIStorageMetaObj
{
public:
  OB_UNIS_VERSION(1);

public:
  ObTabletBindingMdsUserData();
  virtual ~ObTabletBindingMdsUserData() = default;
  ObTabletBindingMdsUserData(const ObTabletBindingMdsUserData &) = delete;
  ObTabletBindingMdsUserData &operator=(const ObTabletBindingMdsUserData &) = delete;

public:
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const override;
  virtual int64_t get_deep_copy_size() const override;

public:
  bool is_valid() const;
  int assign(const ObTabletBindingMdsUserData &other);
  int assign_from_tablet_meta(const ObTabletBindingInfo &other);
  int dump_to_tablet_meta(ObTabletBindingInfo &other);
  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn);

  void reset();
  void set_default_value();

  TO_STRING_KV(K_(redefined),
                K_(snapshot_version),
                K_(schema_version),
                K_(data_tablet_id),
                K_(hidden_tablet_id),
                K_(lob_meta_tablet_id),
                K_(lob_piece_tablet_id));

public:
  int64_t snapshot_version_; // if redefined it is max readable snapshot, else it is min readable snapshot.
  int64_t schema_version_;
  // The host tablet this aux tablet is bound to (reverse pointer from aux to host).
  // Historically this was always the user main data table tablet; since vector-index
  // lob-aux support, the host MAY also be an index tablet (e.g. snapshot_data tablet
  // of an HNSW index). "data" here means "data-bearing host", NOT specifically the
  // user data table.
  common::ObTabletID data_tablet_id_;
  common::ObTabletID hidden_tablet_id_;
  // Sibling lob aux tablet ids, written on the host tablet's binding. The host may
  // be the user data table tablet OR an index tablet that owns its own lob aux.
  common::ObTabletID lob_meta_tablet_id_;
  common::ObTabletID lob_piece_tablet_id_;
  bool redefined_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_BINDING_MDS_USER_DATA
