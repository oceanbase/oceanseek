/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_CACHE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_CACHE_MANAGER_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "deps/oblib/src/lib/list/ob_list.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "common/ob_tablet_id.h"
#include "storage/shared_storage/task/ob_ss_preread_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_preread_map_clear_task.h"

namespace oceanbase
{
namespace storage
{
bool is_support_preread_meta(const blocksstable::MacroBlockId &file_id);

class ObTenantFileManager;

enum ObPrereadNodeStatus
{
  FAKE = 0,  // FAKE means will read from object storage;
  NORMAL = 1,  // NORMAL means has been read from object storage;
};

struct ObPrereadNode
{
public:
  ObPrereadNode()
    : node_status_(ObPrereadNodeStatus::FAKE), file_length_(0), is_need_preread_(false), timestamp_us_(common::ObTimeUtility::current_time_us())
  {}
  ObPrereadNode(const ObPrereadNodeStatus node_status, const int64_t file_length)
    : node_status_(node_status), file_length_(file_length), is_need_preread_(true), timestamp_us_(common::ObTimeUtility::current_time_us())
  {}
  ObPrereadNode(const ObPrereadNode &other)
  {
    *this = other;
  }
  virtual ~ObPrereadNode() {}
  ObPrereadNode& operator =(const ObPrereadNode &other)
  {
    if (this != &other) {
      node_status_ = other.node_status_;
      file_length_ = other.file_length_;
      is_need_preread_ = other.is_need_preread_;
      timestamp_us_ = other.timestamp_us_;
    }
    return *this;
  }
  TO_STRING_KV(K_(node_status), K_(file_length), K_(is_need_preread));

public:
  // FAKE means will read from object storage.
  // NORMAL means has been read from object storage.
  ObPrereadNodeStatus node_status_;
  // in order to judge whether has been read half.
  int64_t file_length_;
  // when file has been read half, set is_need_preread to false.
  // it means this file is no need to be preread any more.
  bool is_need_preread_;
  int64_t timestamp_us_;
};


using PrereadMapPairType = common::hash::HashMapPair<blocksstable::MacroBlockId, ObPrereadNode>;

class ObPutPrereadMapCallback
{
public:
  ObPutPrereadMapCallback(const blocksstable::MacroBlockId &macro_id,
                          const common::ObTabletID &effective_tablet_id)
    : macro_id_(macro_id), effective_tablet_id_(effective_tablet_id)
  {}
  virtual ~ObPutPrereadMapCallback() {}
  int operator()(const PrereadMapPairType &entry);
  TO_STRING_KV(K_(macro_id), K_(effective_tablet_id));

private:
  blocksstable::MacroBlockId macro_id_;
  common::ObTabletID effective_tablet_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPutPrereadMapCallback);
};


class ObUpdatePrereadStatusCallback
{
public:
  explicit ObUpdatePrereadStatusCallback(const int64_t file_length)
    : ret_(OB_SUCCESS), file_length_(file_length)
  {}
  virtual ~ObUpdatePrereadStatusCallback() {}
  int operator()(PrereadMapPairType &entry);
  int get_ret() const { return ret_; }
  TO_STRING_KV(K_(file_length));

private:
  int ret_;
  int64_t file_length_;
  DISALLOW_COPY_AND_ASSIGN(ObUpdatePrereadStatusCallback);
};


class ObUpdateNeedPrereadCallback
{
public:
  explicit ObUpdateNeedPrereadCallback(const bool is_need_preread)
    : ret_(OB_SUCCESS), is_need_preread_(is_need_preread)
  {}
  virtual ~ObUpdateNeedPrereadCallback() {}
  int operator()(PrereadMapPairType &entry);
  int get_ret() const { return ret_; }
  TO_STRING_KV(K_(is_need_preread));

private:
  int ret_;
  bool is_need_preread_;
  DISALLOW_COPY_AND_ASSIGN(ObUpdateNeedPrereadCallback);
};


class ObPrereadCacheManager
{
public:
  static const int64_t NOT_PREREAD_IO_SIZE = 512L * 1024L; // 512KB
  static const int64_t PREREAD_QUEUE_SIZE_THRESHOLD = 100; // preread queue size threshold
  friend class ObSSPreReadTask;
  ObPrereadCacheManager();
  ~ObPrereadCacheManager();
  int init(ObTenantFileManager *file_manager);
  int start();
  void destroy();
  void stop();
  void wait();
  int push_to_preread_queue(const blocksstable::MacroBlockId &file_id,
                            const common::ObTabletID &effective_tablet_id);
  // for TMP_FILE. if read to the end of one TMP_FILE, then evict this TMP_FILE
  int evict_preread_node(const blocksstable::MacroBlockId &file_id);
  int remove_preread_node(const blocksstable::MacroBlockId &file_id);
  int refresh_preread_node_if_need(const blocksstable::ObStorageObjectReadInfo &read_info);
  int update_to_normal_status(const blocksstable::MacroBlockId &file_id, const int64_t file_length);
  int is_file_id_need_preread(const blocksstable::MacroBlockId &file_id, bool &is_need_preread);
  int is_exist_in_preread_map(const blocksstable::MacroBlockId &file_id, bool &is_exist);
  int set_need_preread(const blocksstable::MacroBlockId &file_id, const bool is_need_preread);
  int do_preread_map_clear_work();
  int batch_erase_preread_entry(const common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  OB_INLINE int64_t get_preread_map_size() { return preread_map_.size(); }
  OB_INLINE int64_t get_preread_queue_size() { return preread_queue_.size(); }

private:
  bool is_read_cache_support_object_type (const blocksstable::MacroBlockId &file_id);
  bool is_need_cancel_preread(const blocksstable::ObStorageObjectReadInfo &read_info);

private:
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t PREREAD_ENTRY_EXPIRATION_TIME_US = 24 * 3600 * 1000L * 1000L; // 24h
  typedef hash::ObHashMap<blocksstable::MacroBlockId, ObPrereadNode> PrereadMap;

private:
  friend class ObPutPrereadMapCallback;
  friend class ObRefreshPrereadMapCallback;
  bool is_inited_;
  volatile bool is_stopped_;
  ObTenantFileManager *file_manager_;
  PrereadMap preread_map_;
  int tg_id_;
  ObSSPreReadTask preread_task_;
  ObSSPrereadMapClearTask preread_map_clear_task_;
  common::ObLinkQueue preread_queue_; // store macro_ids that need preread from remote object storage
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_CACHE_MANAGER_H_ */
