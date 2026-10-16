/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_STORAGE_OB_SS_DIAGNOSE_MGR_H_
#define SRC_STORAGE_OB_SS_DIAGNOSE_MGR_H_

#include "storage/incremental/share/ob_ss_diagnose.h"
#include "lib/lock/ob_small_spin_lock.h"

namespace oceanbase
{
namespace storage
{
struct ObSSDiagnoseTablet
{
  ObSSDiagnoseTablet()
    : ls_id_(),
      tablet_id_()
  {}
  ObSSDiagnoseTablet(const share::ObLSID &ls_id,  const ObTabletID &tablet_id)
    : ls_id_(ls_id),
      tablet_id_(tablet_id)
  {}
  ~ObSSDiagnoseTablet() {}
  inline bool operator == (const ObSSDiagnoseTablet &other) const
  {
    return ls_id_ == other.ls_id_ && tablet_id_ == other.tablet_id_;
  }
  inline int hash(uint64_t &hash_value) const
  {
    int ret = common::OB_SUCCESS;
    hash_value = murmurhash(&ls_id_, sizeof(ls_id_), 0);
    hash_value = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_value);
    return ret;
  }
  inline bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid();
  }
  static bool is_flagged(int64_t flag, const ObSSDiagnoseInfo::ObDiagnoseType type)
  {
    return flag & (1 << static_cast<int64_t>(type));
  }
  static void set_flag(int64_t &flag, const ObSSDiagnoseInfo::ObDiagnoseType type)
  {
    flag |= (1 << static_cast<int64_t>(type));
  }
  static void del_flag(int64_t &flag, const ObSSDiagnoseInfo::ObDiagnoseType type)
  {
    flag &= ~(1 << static_cast<int64_t>(type));
  }
  // input_flag = 100100, other_flag = 011100 -> input_flag  = 000100
  static void sub_flag(int64_t &input_flag, const int64_t other_flag)
  {
    int64_t bits = 0;
    while (bits < ObSSDiagnoseInfo::ObDiagnoseType::DIA_TYPE_MAX) {
      const int64_t flag = other_flag & (1 << bits++);
      input_flag &= ~flag;
    }
  }
  TO_STRING_KV(K_(ls_id), K_(tablet_id));

  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
};

#define SIMPLE_TO_STRING(n)                                                                       \
    if (OB_FAIL(ret)) {                                                                          \
    } else if (OB_FAIL(::oceanbase::common::logdata_print_key_obj(buf, buf_size - 1, __pos, key##n, false, obj##n))) { \
    } else if (__pos + 1 >= buf_size) {                                                          \
    } else {                                                                                     \
      buf[__pos++] = ',';                                                                        \
    }

#define SIMPLE_TO_STRING_1  SIMPLE_TO_STRING(1)

#define SIMPLE_TO_STRING_2                                                                    \
    SIMPLE_TO_STRING_1                                                                        \
    SIMPLE_TO_STRING(2)

#define SIMPLE_TO_STRING_3                                                                    \
    SIMPLE_TO_STRING_2                                                                        \
    SIMPLE_TO_STRING(3)

#define SIMPLE_TO_STRING_4                                                                    \
    SIMPLE_TO_STRING_3                                                                        \
    SIMPLE_TO_STRING(4)

#define SIMPLE_TO_STRING_5                                                                    \
    SIMPLE_TO_STRING_4                                                                        \
    SIMPLE_TO_STRING(5)

#define SIMPLE_TO_STRING_6                                                                   \
    SIMPLE_TO_STRING_5                                                                        \
    SIMPLE_TO_STRING(6)

#define SIMPLE_TO_STRING_7                                                                    \
    SIMPLE_TO_STRING_6                                                                        \
    SIMPLE_TO_STRING(7)

#define SIMPLE_TO_STRING_8                                                                    \
    SIMPLE_TO_STRING_7                                                                        \
    SIMPLE_TO_STRING(8)

#define SIMPLE_TO_STRING_9                                                                    \
    SIMPLE_TO_STRING_8                                                                        \
    SIMPLE_TO_STRING(9)

#define SIMPLE_TO_STRING_10                                                                   \
    SIMPLE_TO_STRING_9                                                                        \
    SIMPLE_TO_STRING(10)

#define IMPL_SET_SS_DIAGNOSE_INFO(n)                                                                                  \
template <LOG_TYPENAME_TN##n>                                                                                         \
int SET_SS_DIAGNOSE_INFO(ObSSDiagnoseInfo &diagnose_info,                                                             \
                         const int64_t type,                                                                          \
                         const share::ObLSID ls_id,                                                                   \
                         const common::ObTabletID tablet_id,                                                          \
                         const int64_t timestamp,                                                                     \
                         LOG_PARAMETER_KV##n)                                                                         \
  {                                                                                                                   \
    int64_t __pos = 0;                                                                                                \
    int ret = OB_SUCCESS;                                                                                             \
    if (type <= ObSSDiagnoseInfo::ObDiagnoseType::DIA_TYPE_INVALID                                                    \
        || type >= ObSSDiagnoseInfo::ObDiagnoseType::DIA_TYPE_MAX) {                                                  \
      ret = OB_INVALID_ARGUMENT;                                                                                      \
      STORAGE_LOG(WARN, "invalid type", K(ret), K(type));                                                             \
    } else {                                                                                                          \
        diagnose_info.diagnose_type_ = type;                                                                          \
        diagnose_info.ls_id_ = ls_id.id();                                                                            \
        diagnose_info.tenant_id_ = MTL_ID();                                                                          \
        diagnose_info.tablet_id_ = tablet_id.id();                                                                    \
        diagnose_info.timestamp_ = timestamp;                                                                         \
        char *buf = diagnose_info.diagnose_info_;                                                                     \
        const int64_t buf_size =  ::oceanbase::common::OB_DIAGNOSE_INFO_LENGTH;                                       \
        SIMPLE_TO_STRING_##n                                                                                          \
        if (__pos < buf_size) {                                                                                       \
          buf[__pos-1] = '\0';                                                                                        \
        } else {                                                                                                      \
          buf[buf_size - 1] = '\0';                                                                                   \
        }                                                                                                             \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }
  
IMPL_SET_SS_DIAGNOSE_INFO(1)
IMPL_SET_SS_DIAGNOSE_INFO(2)
IMPL_SET_SS_DIAGNOSE_INFO(3)
IMPL_SET_SS_DIAGNOSE_INFO(4)
IMPL_SET_SS_DIAGNOSE_INFO(5)
IMPL_SET_SS_DIAGNOSE_INFO(6)
IMPL_SET_SS_DIAGNOSE_INFO(7)
IMPL_SET_SS_DIAGNOSE_INFO(8)
IMPL_SET_SS_DIAGNOSE_INFO(9)
IMPL_SET_SS_DIAGNOSE_INFO(10)

#undef IMPL_SET_SS_DIAGNOSE_INFO


class ObSSDiagnoseInfoMgr
{
public:
  using ByteLockGuard = ObSmallSpinLockGuard<common::ObByteLock>;
  static int mtl_init(ObSSDiagnoseInfoMgr *&diagnose_tablet_mgr);
  ObSSDiagnoseInfoMgr();
  virtual ~ObSSDiagnoseInfoMgr() { destroy(); }

  int init();
  void destroy();

  // for diagnose
  int add_diagnose_tablet(
      const share::ObLSID &ls_id, 
      const ObTabletID &tablet_id, 
      const ObSSDiagnoseInfo::ObDiagnoseType type);
  int get_diagnose_tablets(ObIArray<ObSSDiagnoseTablet> &diagnose_tablets);
  int delete_diagnose_tablet(
      const share::ObLSID &ls_id, 
      const ObTabletID &tablet_id,
      const ObSSDiagnoseInfo::ObDiagnoseType type);
  void remove_diagnose_tablets(
      ObIArray<ObSSDiagnoseTablet> &tablets);
  void get_diagnose_info(const ObSSDiagnoseInfo::ObDiagnoseType type,
                         ObSSDiagnoseInfo *info_array,
                         int64_t &pos,
                         const int64_t max_cnt);
#define SS_DIAGNOSE_LOG_KV_0
#define SS_DIAGNOSE_LOG_KV_1 key1, obj1
#define SS_DIAGNOSE_LOG_KV_2 SS_DIAGNOSE_LOG_KV_1, key2, obj2
#define SS_DIAGNOSE_LOG_KV_3 SS_DIAGNOSE_LOG_KV_2, key3, obj3
#define SS_DIAGNOSE_LOG_KV_4 SS_DIAGNOSE_LOG_KV_3, key4, obj4
#define SS_DIAGNOSE_LOG_KV_5 SS_DIAGNOSE_LOG_KV_4, key5, obj5
#define SS_DIAGNOSE_LOG_KV_6 SS_DIAGNOSE_LOG_KV_5, key6, obj6
#define SS_DIAGNOSE_LOG_KV_7 SS_DIAGNOSE_LOG_KV_6, key7, obj7
#define SS_DIAGNOSE_LOG_KV_8 SS_DIAGNOSE_LOG_KV_7, key8, obj8
#define SS_DIAGNOSE_LOG_KV_9 SS_DIAGNOSE_LOG_KV_8, key9, obj9
#define SS_DIAGNOSE_LOG_KV_10 SS_DIAGNOSE_LOG_KV_9, key10, obj10
#define DEFINE_ADD_SS_DIAGNOSE_INFO(n)                                                           \
  template <LOG_TYPENAME_TN##n>                                                                  \
  int add_ss_diagnose_info(const int64_t type,                                                   \
                           const share::ObLSID ls_id, const common::ObTabletID tablet_id,        \
                           const int64_t timestamp,                                              \
                           LOG_PARAMETER_KV##n)                                                  \
  {                                                                                              \
    int ret = OB_SUCCESS;                                                                        \
    if (!is_inited_) {                                                                           \
      ret = OB_NOT_INIT;                                                                         \
      STORAGE_LOG(WARN, "ObSSDiagnoseInfoMgr is not init", K(ret));                              \
    } else {                                                                                     \
      ByteLockGuard guard(diagnose_lock_[type].lock_);                                           \
      if (OB_NOT_NULL(diagnose_infos_[type])) {                                                  \
        const int64_t max_cnt = MAX_DIAGNOSE_CNT[type];                                          \
        int64_t &idx = idx_[type];                                                               \
        ObSSDiagnoseInfo &diagnose_info = diagnose_infos_[type][idx++ % max_cnt];                \
        ret = SET_SS_DIAGNOSE_INFO(diagnose_info, type, ls_id, tablet_id, timestamp, SS_DIAGNOSE_LOG_KV_##n);  \
      } else {                                                                                   \
        ret = OB_ERR_UNEXPECTED;                                                                 \
        STORAGE_LOG(WARN, "diagnose_info is null", K(ret));                                      \
      }                                                                                          \
    }                                                                                            \
    return ret;                                                                                  \
  }
                       
  DEFINE_ADD_SS_DIAGNOSE_INFO(1)
  DEFINE_ADD_SS_DIAGNOSE_INFO(2)
  DEFINE_ADD_SS_DIAGNOSE_INFO(3)
  DEFINE_ADD_SS_DIAGNOSE_INFO(4)
  DEFINE_ADD_SS_DIAGNOSE_INFO(5)
  DEFINE_ADD_SS_DIAGNOSE_INFO(6)
  DEFINE_ADD_SS_DIAGNOSE_INFO(7)
  DEFINE_ADD_SS_DIAGNOSE_INFO(8)
  DEFINE_ADD_SS_DIAGNOSE_INFO(9)
  DEFINE_ADD_SS_DIAGNOSE_INFO(10)
#undef DEFINE_ADD_SS_DIAGNOSE_INFO
#undef SS_DIAGNOSE_LOG_KV_0
#undef SS_DIAGNOSE_LOG_KV_1
#undef SS_DIAGNOSE_LOG_KV_2
#undef SS_DIAGNOSE_LOG_KV_3
#undef SS_DIAGNOSE_LOG_KV_4
#undef SS_DIAGNOSE_LOG_KV_5
#undef SS_DIAGNOSE_LOG_KV_6
#undef SS_DIAGNOSE_LOG_KV_7
#undef SS_DIAGNOSE_LOG_KV_8
#undef SS_DIAGNOSE_LOG_KV_9
#undef SS_DIAGNOSE_LOG_KV_10

public:
  static const int64_t DEFAULT_DIAGNOSE_TABLET_COUNT = 128;
  static const int64_t MAX_DIAGNOSE_TABLET_BUCKET_NUM = 1024;
  typedef common::hash::ObHashMap<ObSSDiagnoseTablet, int64_t, common::hash::NoPthreadDefendMode> DiagnoseTabletMap;
private:
  bool need_diagnose_info_(const int64_t type, ObSSDiagnoseInfo *diagnose_info);
private:
  bool is_inited_;
  lib::ObMutex diagnose_map_lock_;
  DiagnoseTabletMap diagnose_tablet_map_;

  // compaction info storage
  constexpr static int64_t MAX_DIAGNOSE_CNT[ObSSDiagnoseInfo::DIA_TYPE_MAX] = {
      1,   // DIA_TYPE_INVALID
      10,  // DIA_TYPE_META_READ_WRITE
      10,  // DIA_TYPE_SS_GC
      10,  // DIA_TYPE_SSTABLE_UPLOAD
      10,  // DIA_TYPE_SSTABLE_MINOR
      10,  // DIA_TYPE_SSTABLE_MAJOR
      10,  // DIA_TYPE_SSTABLE_ATTACH
      10,  // DIA_TYPE_SSTABLE_GC
      32,   // DIA_TYPE_SCHE_UPLOAD
      32    // DIA_TYPE_SS_CKPT
  };
  struct DiagnoseLockWrapper {
    ObByteLock lock_;
    DiagnoseLockWrapper() : lock_(common::ObLatchIds::SS_DIAGNOSE_INFO_MGR_LOCK) {}
  };
  DiagnoseLockWrapper diagnose_lock_[ObSSDiagnoseInfo::DIA_TYPE_MAX];
  int64_t idx_[ObSSDiagnoseInfo::DIA_TYPE_MAX];
  ObSSDiagnoseInfo *diagnose_infos_[ObSSDiagnoseInfo::DIA_TYPE_MAX];
};

#define ADD_SS_DIAGNOSE_INFO(type, ls_id, tablet_id, timestamp, ...) \
  MTL(ObSSDiagnoseInfoMgr*)->add_ss_diagnose_info(type, ls_id, tablet_id, timestamp, __VA_ARGS__)

}
}

#endif
