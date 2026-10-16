/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "common/storage/ob_io_device.h"
#include "common/ob_file_common_header.h"
#include "share/io/ob_io_define.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "share/ob_delegate.h"
#include "storage/shared_storage/prewarm/ob_prewarm_parallelism_controller.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
namespace oceanbase
{
namespace blocksstable
{
struct ObStorageObjectReadInfo;
struct ObMicroBlockDesc;
}
namespace storage
{
class ObHotTabletInfoBaseWriter;

class ObHotMicroInfo
{
public:
  static const int64_t OB_HOT_MICRO_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_HOT_MICRO_INFO_VERSION);

public:
  ObHotMicroInfo();
  ObHotMicroInfo(const int64_t offset,
                 const int64_t size,
                 const blocksstable::ObLogicMicroBlockId &logic_micro_id,
                 const int64_t crc);
  virtual ~ObHotMicroInfo() {}
  TO_STRING_KV(K_(offset), K_(size), K_(logic_micro_id), K_(micro_crc));

public:
  int64_t offset_;
  int64_t size_;
  blocksstable::ObLogicMicroBlockId logic_micro_id_;
  int64_t micro_crc_;
};


class ObHotMacroInfo
{
public:
  using HotMicroInfoArr = common::ObSEArray<ObHotMicroInfo, 8>;

  static const int64_t OB_HOT_MACRO_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_HOT_MACRO_INFO_VERSION);

public:
  ObHotMacroInfo();
  virtual ~ObHotMacroInfo() {}
  CONST_DELEGATE_WITH_RET(hot_micro_infos_, count, int64_t);
  CONST_DELEGATE_WITH_RET(hot_micro_infos_, at, ObHotMacroInfo&);
  DELEGATE_WITH_RET(hot_micro_infos_, push_back, int);
  TO_STRING_KV(K_(macro_id), "hot_micro_info_cnt", hot_micro_infos_.count(), "hot_micro_infos",
               ObArrayWrap<ObHotMicroInfo>(hot_micro_infos_.get_data(),
               MIN(10, hot_micro_infos_.count())));

public:
  blocksstable::MacroBlockId macro_id_;
  HotMicroInfoArr hot_micro_infos_;
};


class ObHotTabletInfo
{
public:
  public:
  using HotMacroInfoArr = common::ObSEArray<ObHotMacroInfo, 8>;

  static const int64_t OB_HOT_TABLET_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_HOT_TABLET_INFO_VERSION);

public:
  ObHotTabletInfo();
  virtual ~ObHotTabletInfo() {}
  void reset();
  CONST_DELEGATE_WITH_RET(hot_macro_infos_, count, int64_t);
  DELEGATE_WITH_RET(hot_macro_infos_, at, ObHotMacroInfo&);
  int push_back(const blocksstable::ObMicroBlockDesc &micro_block_desc, ObHotTabletInfoBaseWriter &writer);
  DELEGATE_WITH_RET(hot_macro_infos_, reuse, void);
  TO_STRING_KV("hot_macro_info_cnt", hot_macro_infos_.count(), "hot_macro_infos",
               ObArrayWrap<ObHotMacroInfo>(hot_macro_infos_.get_data(),
               MIN(10, hot_macro_infos_.count())));

public:
  HotMacroInfoArr hot_macro_infos_;
};

class ObHotTabletInfoIndex
{
public:
  static const int64_t OB_HOT_TABLET_INFO_INDEX_VERSION = 1;
  OB_UNIS_VERSION(OB_HOT_TABLET_INFO_INDEX_VERSION);

public:
  ObHotTabletInfoIndex();
  virtual ~ObHotTabletInfoIndex() {}
  void reset();
  // sum of all recorded serialized sizes
  int64_t get_total_size() const;
  TO_STRING_KV("size_cnt", sizes_.count(), "sizes", ObArrayWrap<int64_t>(sizes_.get_data(),
               MIN(10, sizes_.count())));

public:
  // record serialized size of each multipart uploaded hot macro infos
  common::ObSEArray<int64_t, 8> sizes_;
};

// multipart upload data and put index
class ObHotTabletInfoBaseWriter
{
public:
  ObHotTabletInfoBaseWriter(const int64_t tablet_id,
                            const int64_t compaction_scn,
                            const share::SCN &reorganization_scn);
  virtual ~ObHotTabletInfoBaseWriter();
  // Append hot macro infos. When accumulated 2w hot micro, serialize and upload the accumulated hot
  // macro infos to object storage, and record serialized size into ObHotTabletInfoIndex.
  // There exists concurrent call of this function, which requires mutex to ensure safety.
  int append(const common::ObIArray<ObHotMacroInfo> &hot_macro_infos);
  // Call this function after appending all hot macro infos, which will serialize and upload
  // the accumulated hot macro infos that has not yet been uploaded to object storage, and record
  // the serialized size in ObHotTabletInfoIndex. Besides, it will serialize and upload
  // ObHotTabletInfoIndex to object storage.
  int complete();
  VIRTUAL_TO_STRING_KV(K_(hot_tablet_info), K_(hot_micro_cnt), K_(index), K_(tablet_id),
                       K_(compaction_scn), K_(reorganization_scn), K_(object_handle),
                       KP_(device_handle), K_(fd), K_(is_opened), K_(policy_status));

private:
  int open_device_handle_and_fd();
  void close_device_handle_and_fd();
  int upload_hot_tablet_info();
  int upload_hot_tablet_info_index();
  int wait_if_need();
  int write_hot_tablet_info_index(char *buf, const int64_t buf_len, blocksstable::ObStorageObjectHandle &object_handle);
  void abort_on_fail(const int ret);
  virtual blocksstable::ObStorageObjectType get_data_object_type() const = 0;
  virtual blocksstable::ObStorageObjectType get_index_object_type() const = 0;

public:
  static const int64_t INDEX_BUF_SIZE = 1 * 1024 * 1024L; // 1MB

private:
  static const int64_t FLUSH_THRESHOLD = 20000L; // 2w
  ObHotTabletInfo hot_tablet_info_;
  int64_t hot_micro_cnt_;
  ObHotTabletInfoIndex index_;
  int64_t tablet_id_;
  int64_t compaction_scn_;
  share::SCN reorganization_scn_;
  // hold the io handle of the last multipart upload prewarm_data
  blocksstable::ObStorageObjectHandle object_handle_;
  // record the serialize size of the last multipart upload prewarm_data
  int64_t last_serialize_size_;
  common::ObIODevice *device_handle_;
  common::ObIOFd fd_;
  bool is_opened_;
  lib::ObMutex lock_;
  PolicyStatus policy_status_;
};

// prewarm writer for SHARED_MAJOR_DATA_MACRO
class ObHotTabletInfoDataWriter : public ObHotTabletInfoBaseWriter
{
public:
  ObHotTabletInfoDataWriter(const int64_t tablet_id,
                            const int64_t compaction_scn,
                            const share::SCN &reorganization_scn)
    : ObHotTabletInfoBaseWriter(tablet_id, compaction_scn, reorganization_scn)
  {}
  virtual ~ObHotTabletInfoDataWriter() {}

private:
  virtual blocksstable::ObStorageObjectType get_data_object_type() const override
  {
    return blocksstable::ObStorageObjectType::MAJOR_PREWARM_DATA;
  }
  virtual blocksstable::ObStorageObjectType get_index_object_type() const override
  {
    return blocksstable::ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX;
  }
};

// prewarm writer for SHARED_MAJOR_META_MACRO
class ObHotTabletInfoMetaWriter : public ObHotTabletInfoBaseWriter
{
public:
  ObHotTabletInfoMetaWriter(const int64_t tablet_id,
                            const int64_t compaction_scn,
                            const share::SCN &reorganization_scn)
    : ObHotTabletInfoBaseWriter(tablet_id, compaction_scn, reorganization_scn)
  {}
  virtual ~ObHotTabletInfoMetaWriter() {}

private:
  virtual blocksstable::ObStorageObjectType get_data_object_type() const override
  {
    return blocksstable::ObStorageObjectType::MAJOR_PREWARM_META;
  }
  virtual blocksstable::ObStorageObjectType get_index_object_type() const override
  {
    return blocksstable::ObStorageObjectType::MAJOR_PREWARM_META_INDEX;
  }
};

class ObHotTabletInfoWriter final
{
public:
  ObHotTabletInfoWriter(const int64_t tablet_id,
                        const int64_t compaction_scn,
                        const share::SCN &reorganization_scn)
    : data_writer_(tablet_id, compaction_scn, reorganization_scn),
      meta_writer_(tablet_id, compaction_scn, reorganization_scn)
  {}
  ~ObHotTabletInfoWriter() {}
  int complete();
  TO_STRING_KV(K_(data_writer), K_(meta_writer));

public:
  ObHotTabletInfoDataWriter data_writer_;
  ObHotTabletInfoMetaWriter meta_writer_;
};


// Prewarm major SSTable macro blocks into local macro cache.
class ObHotTabletInfoReader : public ObPrewarmParController, public ObSSBasePrewarmer
{
public:
  ObHotTabletInfoReader(const int64_t compaction_scn,
                        const share::SCN &reorganization_scn,
                        const ObTabletHandle &tablet_handle,
                        const blocksstable::ObSSTable &sstable);
  virtual ~ObHotTabletInfoReader();
  int load_hot_macro_infos();

  TO_STRING_KV(K_(tablet_id), K_(compaction_scn), K_(reorganization_scn), K_(object_handles),
               K_(handle_idx));

private:
  int handle_hot_tablet();
  int prewarm_sstable_(const blocksstable::ObSSTable &sstable);
  int handle_non_hot_tablet();
  bool is_vaild_data_object_type(const blocksstable::ObStorageObjectType data_object_type) const;
  bool is_vaild_index_object_type(const blocksstable::ObStorageObjectType index_object_type) const;
  int load_hot_macro_infos(const blocksstable::ObStorageObjectType data_object_type,
                           const blocksstable::ObStorageObjectType index_object_type);
  int check_if_index_file_exist(const blocksstable::ObStorageObjectType object_type, bool &is_exist);
  int read_index_file(const blocksstable::ObStorageObjectType object_type, ObHotTabletInfoIndex &index);
  int read_data_file(const blocksstable::ObStorageObjectType object_type, const ObHotTabletInfoIndex &index);
  static int get_micro_count_from_tablet_info_(
      const ObHotTabletInfo &hot_tablet_info,
      int64_t &total_micro_cnt,
      int64_t &physic_micro_cnt,
      int64_t &logic_micro_cnt);
  int load_hot_macro_block_data(const ObHotTabletInfo &hot_tablet_info,
                                int64_t &total_micro_cnt,
                                int64_t &physic_micro_cnt,
                                int64_t &logic_micro_cnt,
                                bool &is_stop_prewarm);
  int async_read_macro_block(const ObHotMacroInfo &hot_macro_info);
  int async_write_hot_policy_macro_into_local_cache(blocksstable::ObStorageObjectHandle &read_handle);
  int async_write_hot_policy_macro_(blocksstable::ObStorageObjectHandle &read_handle);
  int wait_all_object_handles();
  int wait_all_write_handles();
  int check_if_macro_cache_prewarm_space_enough(bool &is_enough);
  int check_if_stop_prewarm(bool &is_stop);
  void reset_io_resources();

private:
  static const int64_t AGGREGATED_READ_PARALLELISM_MAX = 16;
  static const int64_t TOTAL_READ_PARALLELISM = AGGREGATED_READ_PARALLELISM_MAX;
  int64_t tablet_id_;
  int64_t compaction_scn_;
  share::SCN reorganization_scn_;
  const ObTabletHandle &tablet_handle_;
  const blocksstable::ObSSTable &sstable_;
  common::ObArenaAllocator micro_buf_allocator_;
  blocksstable::ObStorageObjectHandle object_handles_[TOTAL_READ_PARALLELISM];
  blocksstable::ObStorageObjectHandle write_handles_[TOTAL_READ_PARALLELISM];
  int64_t handle_idx_;
  int64_t write_handle_idx_;
  int64_t read_parallelism_;
  int64_t aggregated_read_cnt_;
  int64_t prewarmed_macro_cnt_;
  int64_t skipped_macro_cnt_;
  int64_t fail_wait_handle_cnt_;
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_ */
