/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_DEFINE_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_DEFINE_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{

// (tablet_id, reorg_scn) pair collected from ss_meta_ctrl lock-row iteration.
struct ObSSGCTabletBaseInfo final
{
public:
  ObSSGCTabletBaseInfo() : tablet_id_(), reorg_scn_() {}
  ObSSGCTabletBaseInfo(const common::ObTabletID &tablet_id, const share::SCN &reorg_scn)
    : tablet_id_(tablet_id), reorg_scn_(reorg_scn) {}
  ~ObSSGCTabletBaseInfo() = default;
  bool is_valid() const { return tablet_id_.is_valid() && reorg_scn_.is_valid(); }
  void reset() { tablet_id_.reset(); reorg_scn_.reset(); }
  bool operator==(const ObSSGCTabletBaseInfo &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  TO_STRING_KV(K_(tablet_id), K_(reorg_scn));
public:
  common::ObTabletID tablet_id_;
  share::SCN reorg_scn_;
};

class ObSSGCTablet final
{
public:
  static const int32_t SS_GC_TABLET_VERSION = 1;
  OB_UNIS_VERSION(SS_GC_TABLET_VERSION);
public:
  ObSSGCTablet() : ls_id_(), tablet_id_(), transfer_scn_() {}
  ObSSGCTablet(const share::ObLSID &ls_id,
               const common::ObTabletID &tablet_id,
               const share::SCN &transfer_scn)
    : ls_id_(ls_id), tablet_id_(tablet_id), transfer_scn_(transfer_scn)
  {}
  ~ObSSGCTablet() { reset(); }
  void reset() { ls_id_.reset(); tablet_id_.reset(); transfer_scn_.reset(); }
  bool operator==(const ObSSGCTablet &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool is_valid() const;

public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(transfer_scn));
};


// A mark-deleted tablet together with the meta_scn of its mark-delete tablet meta version
// (the version whose delete_commit_scn is valid).
struct ObSSGCMarkDeletedTablet final
{
public:
  ObSSGCMarkDeletedTablet() : gc_tablet_(), mark_delete_meta_scn_() {}
  ObSSGCMarkDeletedTablet(const ObSSGCTablet &gc_tablet, const share::SCN &mark_delete_meta_scn)
    : gc_tablet_(gc_tablet), mark_delete_meta_scn_(mark_delete_meta_scn) {}
  ObSSGCMarkDeletedTablet(const ObSSGCMarkDeletedTablet &other)
    : gc_tablet_(other.gc_tablet_), mark_delete_meta_scn_(other.mark_delete_meta_scn_) {}
  ~ObSSGCMarkDeletedTablet() { reset(); }
  ObSSGCMarkDeletedTablet &operator=(const ObSSGCMarkDeletedTablet &other)
  {
    if (this != &other) {
      gc_tablet_ = other.gc_tablet_;
      mark_delete_meta_scn_ = other.mark_delete_meta_scn_;
    }
    return *this;
  }
  void reset() { gc_tablet_.reset(); mark_delete_meta_scn_.reset(); }
  bool is_valid() const { return gc_tablet_.is_valid() && mark_delete_meta_scn_.is_valid(); }
  TO_STRING_KV(K_(gc_tablet), K_(mark_delete_meta_scn));
public:
  ObSSGCTablet gc_tablet_;
  share::SCN mark_delete_meta_scn_;
};


class ObSSGCTabletWithDelScn final
{
public:
  static const int32_t SS_GC_TABLET_WITH_DEL_SCN_VERSION = 1;
  OB_UNIS_VERSION(SS_GC_TABLET_WITH_DEL_SCN_VERSION);
public:
  ObSSGCTabletWithDelScn() : gc_tablet_(), deleted_scn_() {}
  ObSSGCTabletWithDelScn(const share::ObLSID &ls_id,
                         const common::ObTabletID &tablet_id,
                         const share::SCN &transfer_scn,
                         const share::SCN &deleted_scn)
    : gc_tablet_(ls_id, tablet_id, transfer_scn), deleted_scn_(deleted_scn)
  {}
  ObSSGCTabletWithDelScn(const ObSSGCTablet &gc_tablet,
                         const share::SCN &deleted_scn)
    : gc_tablet_(gc_tablet),
      deleted_scn_(deleted_scn)
  {}
  ~ObSSGCTabletWithDelScn() {}
  bool operator==(const ObSSGCTabletWithDelScn &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool is_valid() const;

public:
  ObSSGCTablet gc_tablet_;
  share::SCN deleted_scn_;
  TO_STRING_KV(K_(gc_tablet), K_(deleted_scn));
};


class ObSSGCTabletWithMetaScn final
{
public:
  static const int32_t SS_GC_TABLET_WITH_META_SCN_VERSION = 1;
  OB_UNIS_VERSION(SS_GC_TABLET_WITH_META_SCN_VERSION);
public:
  ObSSGCTabletWithMetaScn() : gc_tablet_(), meta_scn_() {}
  ObSSGCTabletWithMetaScn(const ObSSGCTablet &gc_tablet,
                         const share::SCN &meta_scn)
    : gc_tablet_(gc_tablet), meta_scn_(meta_scn)
  {}
  ObSSGCTabletWithMetaScn(const share::ObLSID &ls_id,
                         const common::ObTabletID &tablet_id,
                         const share::SCN &transfer_scn,
                         const share::SCN &meta_scn)
    : gc_tablet_(ls_id, tablet_id, transfer_scn), meta_scn_(meta_scn)
  {}
  ~ObSSGCTabletWithMetaScn() {}
  bool is_valid() const;

public:
  ObSSGCTablet gc_tablet_;
  share::SCN meta_scn_;
  TO_STRING_KV(K_(gc_tablet), K_(meta_scn));
};


struct ObSSGCTabletStudyInfo final
{
public:
  ObSSGCTabletStudyInfo() : min_dep_meta_scn_(), min_major_snapshot_(0) {}
  ObSSGCTabletStudyInfo(const share::SCN &min_dep_meta_scn, const int64_t min_major_snapshot)
    : min_dep_meta_scn_(min_dep_meta_scn), min_major_snapshot_(min_major_snapshot)
  {}
  ~ObSSGCTabletStudyInfo() {}
  bool has_finite_rely() const
  {
    return min_dep_meta_scn_.is_valid_and_not_min() && !min_dep_meta_scn_.is_max();
  }
  bool has_finite_major() const
  {
    return 0 < min_major_snapshot_ && min_major_snapshot_ < INT64_MAX;
  }
  bool has_finite() const
  {
    return has_finite_rely() || has_finite_major();
  }
  void set_to_max()
  {
    min_dep_meta_scn_.set_max();
    min_major_snapshot_ = INT64_MAX;
  }
  void set_to_min()
  {
    min_dep_meta_scn_.set_min();
    min_major_snapshot_ = 0;
  }

public:
  share::SCN min_dep_meta_scn_;
  int64_t min_major_snapshot_;
  TO_STRING_KV(K_(min_dep_meta_scn), K_(min_major_snapshot));
};


struct ObSSGCTabletStudyResult final
{
public:
  static const int32_t SS_GC_TABLET_STUDY_RESULT_VERSION = 1;
  OB_UNIS_VERSION(SS_GC_TABLET_STUDY_RESULT_VERSION);
public:
  ObSSGCTabletStudyResult() : gc_tablet_(), study_info_() {}
  ObSSGCTabletStudyResult(const ObSSGCTablet &gc_tablet,
                          const share::SCN &min_dep_meta_scn,
                          const int64_t min_major_snapshot)
    : gc_tablet_(gc_tablet), study_info_(min_dep_meta_scn, min_major_snapshot)
  {}
  ObSSGCTabletStudyResult(const ObSSGCTablet &gc_tablet,
                          const ObSSGCTabletStudyInfo &study_info)
    : gc_tablet_(gc_tablet), study_info_(study_info)
  {}
  ObSSGCTabletStudyResult(const share::ObLSID &ls_id,
                          const common::ObTabletID &tablet_id,
                          const share::SCN &transfer_scn,
                          const share::SCN &min_dep_meta_scn,
                          const int64_t min_major_snapshot)
    : gc_tablet_(ls_id, tablet_id, transfer_scn),
      study_info_(min_dep_meta_scn, min_major_snapshot)
  {}
  ~ObSSGCTabletStudyResult() {}
  bool is_valid() const;

public:
  ObSSGCTablet gc_tablet_;
  ObSSGCTabletStudyInfo study_info_;
  TO_STRING_KV(K_(gc_tablet), K_(study_info));
};


class ObSSGCTabletInfo final
{
public:
  ObSSGCTabletInfo() : min_meta_scn_(), gc_end_meta_scn_(), reserved_meta_scns_() {}
  ObSSGCTabletInfo(const share::SCN &min_meta_scn,
                   const share::SCN &gc_end_meta_scn)
    : min_meta_scn_(min_meta_scn), gc_end_meta_scn_(gc_end_meta_scn), reserved_meta_scns_()
  {}
  ~ObSSGCTabletInfo() {}
  int assign(const ObSSGCTabletInfo &other);
  void reset();

public:
  // min meta_scn used by one tablet
  share::SCN min_meta_scn_;
  // the final meta_scn that guides gc of one tablet. meta_scn < gc_end_meta_scn can really be gc.
  // it considers min_meta_scn, _shared_gc_retention_time and start_service_time
  share::SCN gc_end_meta_scn_;
  // meta_scns that should be reserved by tenant-level reservation users
  common::ObArray<share::SCN> reserved_meta_scns_;
  TO_STRING_KV(K_(min_meta_scn), K_(gc_end_meta_scn), K_(reserved_meta_scns));
};

using ObSSGCTabletWithDelScnArray  = common::ObSEArray<ObSSGCTabletWithDelScn, 16>;
using ObSSGCTabletWithMetaScnArray = common::ObSEArray<ObSSGCTabletWithMetaScn, 16>;
using ObSSGCTabletStudyResultArray = common::ObSEArray<ObSSGCTabletStudyResult, 16>;


// ====================================================================================
// Per-tablet info for deleting rows from __all_shared_meta.
//   max_del_meta_scn_ — the largest meta_scn to delete (= last entry of del_meta_scns).
//   kept_meta_scns_   — kept_meta_scns strictly less than max_del_meta_scn_, i.e. the
//                       reserved meta_scns that fall inside the delete range.
// SortTask produces del_meta_scns; DiffTask iterates (max_del_meta_scn_, kept_meta_scns_) to
// issue range deletes via ObSSMetaService::remove_tablet.
// ====================================================================================
struct ObTabletDelMetaScnInfo final
{
public:
  ObTabletDelMetaScnInfo();
  ~ObTabletDelMetaScnInfo() { reset(); }
  void reset() { gc_tablet_.reset(); max_del_meta_scn_.reset(); kept_meta_scns_.reset(); }
  bool is_valid() const;
  TO_STRING_KV(K_(gc_tablet), K_(max_del_meta_scn), K_(kept_meta_scns));

public:
  ObSSGCTablet gc_tablet_;
  share::SCN max_del_meta_scn_;
  common::ObSEArray<share::SCN, 2> kept_meta_scns_;
};

struct ObSSFailGCTabletMapValue final
{
public:
  ObSSFailGCTabletMapValue();
  ObSSFailGCTabletMapValue(const bool is_mark_deleted, const share::SCN &last_major_snapshot);
  ~ObSSFailGCTabletMapValue() {}
  bool operator==(const ObSSFailGCTabletMapValue &other) const;
  bool is_valid() const;
  int assign(const ObSSFailGCTabletMapValue &other);
  void reset();
  TO_STRING_KV(K_(is_mark_deleted), K_(last_major_snapshot));
public:
  bool is_mark_deleted_;
  share::SCN last_major_snapshot_;
};

typedef hash::ObHashMap<ObSSGCTabletBaseInfo, ObSSFailGCTabletMapValue> ObSSFailGCTabletMap;

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_DEFINE_H_
