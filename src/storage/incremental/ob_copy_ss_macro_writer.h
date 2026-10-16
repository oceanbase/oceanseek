/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COPY_SS_MACRO_WRITER_H_
#define OCEANBASE_COPY_SS_MACRO_WRITER_H_

#include "lib/list/ob_dlist.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{

class CopySSMacroBlockWriter
{
  // read a mini sstable macro block and write it to shared-storage
public:
  CopySSMacroBlockWriter(const share::ObLSID ls_id,
                         const ObITable::TableKey &output_table_key,
                         ObTablet &tablet,
                         blocksstable::ObSSTable &sstable,
                         const ObStorageSchema &schema,
                         const int io_parallel_limit,
                         const int64_t op_id,
                         const uint64_t data_version,
                         const bool is_upload_minor = false)
    : allocator_("IncCopySSMacro", MTL_ID()),
      ls_id_(ls_id),
      tablet_id_(output_table_key.tablet_id_),
      output_table_key_(output_table_key),
      tablet_(tablet),
      sstable_(sstable),
      meta_macro_seq_(0),
      sstable_index_builder_(false),
      index_block_rebuilder_(),
      data_store_desc_(),
      storage_schema_(schema),
      result_sstable_handle_(),
      free_io_ctx_list_(),
      busy_io_ctx_list_(),
      io_parallel_limit_(io_parallel_limit),
      op_id_(op_id),
      data_version_(data_version),
      is_upload_minor_(is_upload_minor)
  {}

  int init();

  ~CopySSMacroBlockWriter() {
    destroy_();
  }

  int process(blocksstable::ObDataMacroBlockMeta &meta);
  int wait();
  int close(ObArenaAllocator &allocator);
public:
  ObTableHandleV2 get_result_sstable_handle() { return result_sstable_handle_; }
private:
  void destroy_();
  int process_append_index_();
  int create_sstable_(blocksstable::ObSSTableMergeRes &res,
                      ObArenaAllocator &allocator,
                      ObTableHandleV2 &sstable_handle);
  int async_read_(const blocksstable::MacroBlockId &macro_id,
                  blocksstable::ObStorageObjectHandle &read_handle,
                  common::ObIOCallback *callback);
  int async_write_(const char *buf,
                   const int64_t size,
                   const blocksstable::ObStorageObjectType obj_type,
                   const int64_t macro_seq,
                   blocksstable::ObStorageObjectHandle &write_handle);
  blocksstable::ObStorageObjectType get_storage_obj_type_() const;
protected:
  int async_write(const char *buf,
                  const int64_t size,
                  const blocksstable::ObStorageObjectType obj_type,
                  const int64_t macro_seq,
                  blocksstable::ObStorageObjectHandle &write_handle) {
    return async_write_(buf, size, obj_type, macro_seq, write_handle);
  }
  common::ObIAllocator &get_allocator() { return allocator_; }
  blocksstable::ObIndexBlockRebuilder &get_index_builder() { return index_block_rebuilder_; }
  void refresh_meta_macro_seq();
public:
  struct IOCtx : public common::ObIOCallback, public common::ObDLinkBase<IOCtx> {
    IOCtx(const int64_t macro_seq_start, const int64_t limit)
      : ObIOCallback(ObIOCallbackType::INC_UPLOAD_ASYNC_MACRO_READ_CALLBACK),
        ObDLinkBase<IOCtx>(),
        host_(NULL),
        obj_type_(blocksstable::ObStorageObjectType::MAX),
        macro_id_(),
        macro_seq_(macro_seq_start),
        macro_seq_limit_(macro_seq_start + limit),
        read_handle_(),
        write_handle_(),
        ret_code_(0),
        macro_buf_(NULL),
        macro_buf_len_(0),
        stage_(Stage::INIT) {}
    int init(CopySSMacroBlockWriter *host,
             const blocksstable::ObStorageObjectType obj_type,
             const blocksstable::MacroBlockId &macro_id);
    ~IOCtx();
    const char *get_cb_name() const override { return "SSUploadMacroBlockIOCallback"; } 
    bool is_callback_valid() const;
    // member of ObIOCallback
    ObIAllocator *get_allocator() override { return NULL; }
    const char *get_data() override { return NULL; }
    int64_t size() const override { return 0; }
    int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override { return OB_NOT_SUPPORTED; }
    int inner_process(const char *data_buffer, const int64_t size) override;
    int try_append_index();
    int wait();
    void reset(bool reset_macro_seq = false);
    bool is_born_out() const { return macro_seq_ >= macro_seq_limit_; }
    CopySSMacroBlockWriter *host_;
    blocksstable::ObStorageObjectType obj_type_;
    blocksstable::MacroBlockId macro_id_;
    int64_t macro_seq_;
    int64_t macro_seq_limit_;
    blocksstable::ObStorageObjectHandle read_handle_;
    blocksstable::ObStorageObjectHandle write_handle_;
    int ret_code_;
    char *macro_buf_;
    int64_t macro_buf_len_;
    enum Stage: int {
      INIT = 1,        // initialized
      READ,            // aio read in progress
      APPEND_INDEX,    // waiting append macro block to index
      WRITE,           // aio write in progress
      FINISH,          // write done
    } stage_;
    TO_STRING_KV(KP_(host), K_(type), K_(obj_type), K_(macro_id),
                 KP_(macro_buf), K_(macro_buf_len), K_(macro_seq), K_(macro_seq_limit), K_(stage));
  };
private:
  int init_io_ctx_list_();
  int alloc_io_ctx_(IOCtx *&io_ctx);
  int return_io_ctx_(IOCtx *io_ctx);
  int init_index_builder_();
  int init_data_store_desc_();
  DefaultPageAllocator allocator_;
  const share::ObLSID ls_id_;
  const common::ObTabletID tablet_id_;
  const ObITable::TableKey &output_table_key_;
  ObTablet &tablet_;
  blocksstable::ObSSTable &sstable_;
  int64_t meta_macro_seq_;
  blocksstable::ObSSTableIndexBuilder sstable_index_builder_;
  blocksstable::ObIndexBlockRebuilder index_block_rebuilder_;
  blocksstable::ObWholeDataStoreDesc data_store_desc_;
  const ObStorageSchema &storage_schema_;
  ObTableHandleV2 result_sstable_handle_;
  common::ObDList<IOCtx> free_io_ctx_list_;
  common::ObDList<IOCtx> busy_io_ctx_list_;
  int32_t io_parallel_limit_;
  int64_t op_id_;
  uint64_t data_version_;
  bool is_upload_minor_;
};
} // storage
} // oceanbase
#endif
