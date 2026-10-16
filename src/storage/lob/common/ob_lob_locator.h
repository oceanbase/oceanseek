/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_STORAGE_LOB_LOCATOR_H
#define OB_STORAGE_LOB_LOCATOR_H

#include "lib/charset/ob_charset.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace storage
{

class ObTableAccessContext;
struct ObLobReadContext;

class ObLobDiskLocatorWrapper
{
  static constexpr int64_t DISK_LOB_OUTROW_FULL_SIZE = ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE;
public:
  ObLobDiskLocatorWrapper() { reset(); }
  ~ObLobDiskLocatorWrapper() { reset(); }

  int init(const char *ptr, const int64_t len);
  void reset();

  bool is_valid() const;
  bool is_inrow() const { return nullptr != lob_common_ && lob_common_->in_row_; }
  bool is_outrow() const { return nullptr != lob_common_ && !lob_common_->in_row_; }

  const ObLobCommon *get_lob_common() const { return lob_common_; }
  ObLobId get_lob_id() const { return (nullptr != ptr_ && is_outrow()) ? lob_id_ : ObLobId(); }
  int get_lob_id(ObLobId &lob_id) const;
  const ObLobId *get_lob_id_ptr() const { return (nullptr != ptr_ && is_outrow()) ? &lob_id_ : nullptr; }
  const char *get_inrow_data_ptr() const { return inrow_data_; }
  int64_t get_inrow_data_len() const { return inrow_data_len_; }

  transaction::ObTxSEQ get_seq_no() const { return seq_no_; }
  bool is_ext_info_log() const { return false; }

public:
  TO_STRING_KV(KP_(ptr), K_(len), KPC_(lob_common), K_(lob_id), K_(seq_no),
               KP_(inrow_data), K_(inrow_data_len));

private:
  const char *ptr_;
  int64_t len_;
  const ObLobCommon *lob_common_;
  ObLobId lob_id_;
  transaction::ObTxSEQ seq_no_;
  const char *inrow_data_;
  int64_t inrow_data_len_;
};

class ObMemLobLocatorBuilder
{
public:
  ObMemLobLocatorBuilder();
  ~ObMemLobLocatorBuilder();

  void reset();

  // input must be a valid non-NULL disk LOB locator.
  // SQL NULL/NOP must be filtered by the caller. An empty LOB must be represented
  // by an in-row disk locator containing ObLobCommon with an empty payload.
  int init(common::ObIAllocator &allocator,
          const common::ObString &input,
          const bool need_data);

  int set_extern_info(const uint64_t table_id, 
                      const uint64_t column_idx,
                      const ObCollationType &cs_type);
  int set_location_info(const uint64_t tablet_id,
                        const int64_t ls_id_,
                        const ObAddr &addr);
  int set_retry_info(const bool leader,
                     const bool read_latest,
                     const uint64_t timeout);
  int set_read_snapshot(transaction::ObTxReadSnapshot &tx_read_snapshot,
                        const int64_t ls_id,
                        const share::SCN &fb_snapshot,
                        const int64_t tx_seq_base,
                        bool read_latest);
  int set_rowkey(const common::ObString &rowkey_str);
  int build(common::ObLobLocatorV2 &locator);
  TO_STRING_KV(K(is_inited_), K(input_.length()), K(extern_header_),
              K(location_info_), K(retry_info_), KPC(tx_read_snapshot_), K(read_snapshot_ls_id_), 
              K(fb_snapshot_), K(rowkey_), K(locator_size_), K(extern_body_size_), 
              K(disk_locator_size_), K(read_snapshot_size_), K(payload_offset_), K(payload_size_), 
              K(has_extern_), K(has_location_info_), K(has_retry_info_), K(has_read_snapshot_), 
              K(has_row_key_), K(need_payload_));

private:
  static const int64_t MAX_EXTERN_BODY_SIZE = UINT16_MAX;

  int calc_size_();
  int calc_disk_locator_size_();

  int prepare_mem_lob_common_(common::ObMemLobCommon &mem_lob_common) const;

  int prepare_read_snapshot_(const int64_t read_snapshot_size,
                            common::ObMemLobReadSnapshot &read_snapshot) const;
  int prepare_extern_header_(common::ObMemLobExternHeader &extern_header) const;

  // build a lob access param from members to query outrow data length/content
  int build_query_param_(ObLobReadContext &read_ctx, common::ObLobLocatorV2 &disk_locator) const;
  // query outrow data content into the payload region
  int prepare_payload_(char *payload_buf) const;

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;
  common::ObString input_;
  ObLobDiskLocatorWrapper input_wrapper_;
  common::ObMemLobExternHeader extern_header_;
  common::ObMemLobLocationInfo location_info_;
  common::ObMemLobRetryInfo retry_info_;
  transaction::ObTxReadSnapshot *tx_read_snapshot_;
  int64_t read_snapshot_ls_id_;
  share::SCN fb_snapshot_;
  common::ObString rowkey_;
  
  int64_t locator_size_;
  int64_t extern_body_size_;
  int64_t disk_locator_size_;
  int64_t read_snapshot_size_;
  int64_t payload_offset_;
  int64_t payload_size_;
  bool has_extern_;
  bool has_location_info_;
  bool has_retry_info_;
  bool has_read_snapshot_;
  bool has_row_key_;
  bool need_payload_;
};

class ObLobLocatorHelper
{
public:
  ObLobLocatorHelper();
  virtual ~ObLobLocatorHelper();
  void reset();
  void reuse() {
    locator_allocator_.reuse();
    rowid_objs_.reuse();
    rowkey_str_.reset();
  }
  // used for TableScan
  int init(const ObTableScanParam &scan_param,
           const ObStoreCtx &ctx,
           const share::ObLSID &ls_id,
           const int64_t snapshot_version);
  int init(const uint64_t table_id,
           const uint64_t tablet_id,
           const ObStoreCtx &ctx,
           const share::ObLSID &ls_id,
           const int64_t snapshot_version);
  int fill_lob_locator_v2(blocksstable::ObDatumRow &row,
                          const ObTableAccessContext &access_ctx,
                          const ObTableAccessParam &access_param,
                          const transaction::ObTxSnapshot *read_snapshot = nullptr);
  // @brief fill memory lob locator without rowkey, mainly for internal use
  int fill_lob_locator_v2(common::ObDatum &datum,
                          const ObColumnParam &col_param,
                          const ObTableIterParam &iter_param,
                          const ObTableAccessContext &access_ctx,
                          const int32_t col_offset);
  int fuse_mem_lob_header(ObObj &def_obj, uint64_t col_id, bool is_systable);
  void update_lob_locator_ctx(uint64_t table_id, uint64_t tablet_id, int64_t tx_id) 
  {
    table_id_ = table_id;
    tablet_id_ = tablet_id;
    tx_id_ = tx_id;
  }
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool enable_lob_locator_v2() const { return true; }
  TO_STRING_KV(K_(table_id), K_(ls_id), K_(tx_id), K_(snapshot_version), K_(tx_read_snapshot), 
               K_(rowid_version), KPC(rowid_project_), K_(is_inited), K_(scan_flag), 
               K_(tx_seq_base));
private:
  struct LobReadSnapshotParam
  {
    LobReadSnapshotParam()
      : historical_snapshot_(), snapshot_(nullptr), fb_snapshot_(), read_latest_(false)
    {}
    transaction::ObTxReadSnapshot historical_snapshot_;
    transaction::ObTxReadSnapshot *snapshot_;
    share::SCN fb_snapshot_;
    bool read_latest_;
  };
  int prepare_lob_read_snapshot_(
      const transaction::ObTxSnapshot *read_snapshot,
      const share::ObLSID &ls_id,
      LobReadSnapshotParam &param);
  static const int64_t DEFAULT_LOCATOR_OBJ_ARRAY_SIZE = 8;
  static const int64_t LOB_FORCE_INROW_SIZE = 64 * 1024L; // 64K
  /*
  * @brief build rowid str from row
  * @param [in] row, the row datum
  *        [out] rowid_str, the output rowid str
  *        [in] is_projected_row, whether the row is projected,
  *             if false, use out_project to get real idx
  *        [in] col_descs, the column descriptors
  *        [in] out_project, the output project
  *        [in] tablet_id, the tablet id
  */
  int build_rowid_obj(blocksstable::ObDatumRow &row,
                      common::ObString &rowid_str,
                      bool is_projected_row,
                      const ObColDescIArray &col_descs,
                      const common::ObIArray<int32_t> &out_project,
                      const common::ObTabletID &tablet_id);
  // @brief Inrow lob can can use disk locator to avoid overhead.
private:
  uint64_t table_id_;
  uint64_t tablet_id_;
  int64_t ls_id_;
  int64_t tx_id_;
  int64_t snapshot_version_;
  transaction::ObTxReadSnapshot tx_read_snapshot_;
  share::SCN fb_snapshot_;
  int64_t rowid_version_;
  const common::ObIArray<int32_t> *rowid_project_; //map to projected row
  common::ObSEArray<common::ObObj, DEFAULT_LOCATOR_OBJ_ARRAY_SIZE> rowid_objs_;
  common::ObArenaAllocator locator_allocator_;
  ObString rowkey_str_; // for default values
  bool is_inited_;
  ObQueryFlag scan_flag_;
  int64_t tx_seq_base_;
};



class ObLobDiskLocatorBuilder
{
  static const int64_t DISK_LOB_OUTROW_FULL_SIZE = ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE;
public:
  ObLobDiskLocatorBuilder() { reset(); }
  ~ObLobDiskLocatorBuilder() { reset(); }
  void reset();

  // init outrow
  int init_outrow(common::ObIAllocator &allocator);
  // init inrow
  int init_inrow(common::ObIAllocator &allocator, const char *inrow_data, const int64_t inrow_data_len);
  int set_lob_id(const ObLobId &lob_id);
  int set_seq_no(const transaction::ObTxSEQ &seq_no);
  int to_locator(ObLobLocatorV2 &locator) const;

public:
  TO_STRING_KV(KP_(ptr), K_(len), KPC_(lob_common), K_(seq_no),
               KP_(inrow_data), K_(inrow_data_len));

private:
  char *ptr_;
  int64_t len_;
  ObLobCommon *lob_common_;
  transaction::ObTxSEQ seq_no_;
  char *inrow_data_;
  int64_t inrow_data_len_;
};

} // namespace storage
} // namespace oceanbase
#endif
