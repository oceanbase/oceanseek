/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LA_MICRO_KEY_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LA_MICRO_KEY_MANAGER_H_

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{

class ObLS;

class ObSSLAMicroKeyManager
{
public:
  ObSSLAMicroKeyManager();
  virtual ~ObSSLAMicroKeyManager();
  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }
  void clear();
  int push_la_micro_key_to_hashset(const ObSSMicroPrewarmMeta &micro_prewarm_meta);
  int get_batch_micro_keys(ObLS *ls, ObIArray<ObSSMicroPrewarmMeta> &infos);
  int is_tablet_id_need_filter(ObLS *ls, const uint64_t tablet_id, bool &is_filter);
  int check_stop_record_la_micro_key();

private:
  class ObGetMicroKeyOp
  {
  public:
    ObGetMicroKeyOp(common::ObIArray<ObSSMicroPrewarmMeta> &keys, ObLS *ls,
                    ObSSLAMicroKeyManager &la_micro_key_manager)
      : keys_(keys), ls_(ls), la_micro_key_manager_(la_micro_key_manager) {}
    ~ObGetMicroKeyOp() {}
    int operator()(common::hash::HashSetTypes<ObSSMicroPrewarmMeta>::pair_type &pair);
  public:
    common::ObIArray<ObSSMicroPrewarmMeta> &keys_;
    ObLS *ls_;
    ObSSLAMicroKeyManager &la_micro_key_manager_;
    DISALLOW_COPY_AND_ASSIGN(ObGetMicroKeyOp);
  };
  int batch_erase_micro_keys(const common::ObIArray<ObSSMicroPrewarmMeta> &infos);
  int filter_micro_key_by_object_type(const ObSSMicroPrewarmMeta &micro_meta, bool &is_filtered);
  int filter_micro_key_by_tablet_id(ObLS *ls, const ObSSMicroPrewarmMeta &micro_meta, bool &is_filter);

private:
  static const uint64_t MAX_MICRO_BLOCK_KEY_SET_CAPACITY = 100000; // 10w

private:
  bool is_inited_;
  // store latest accessed micro_meta from ss_micro_cache, used for ls replica prewarm.
  common::hash::ObHashSet<ObSSMicroPrewarmMeta> la_micro_key_set_;
  bool is_stop_record_la_micro_key_; // is ls replica prewarm stop
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LA_MICRO_KEY_MANAGER_H_ */
