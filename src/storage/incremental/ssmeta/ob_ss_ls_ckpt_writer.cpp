/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_ls_ckpt_writer.h"
#include "ob_ss_meta_define.h"
#include "ob_ss_meta_info_util.h"
#include "observer/ob_server_struct.h"
#include "share/io/ob_io_manager.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/ob_i_table.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_block_header.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_store.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

static int prepare_aligned_write_buf(
    ObIAllocator &allocator,
    const char *buf,
    const int64_t size,
    const bool need_align,
    const char *&write_buf,
    int64_t &write_size);
static int validate_existing_object(
    const blocksstable::MacroBlockId &macro_id,
    const char *expected_buf,
    const int64_t expected_size,
    ObIAllocator &allocator,
    ObBaseFileManager &file_manager);
static int write_object_idempotently(
    const blocksstable::MacroBlockId &macro_id,
    const char *buf,
    const int64_t size,
    const bool need_align,
    const bool need_check_existing_object);

ObSSLsCkptWriter::ObSSLsCkptWriter(const bool need_check_existing_object)
  : need_check_existing_object_(need_check_existing_object),
    tablet_key_(),
    tablet_meta_(),
    tablet_(nullptr),
    macro_info_(nullptr),
    tiered_store_(nullptr),
    allocator_(nullptr)
{
}

int ObSSLsCkptWriter::upload_tablet_meta(const ObSSMetaTabletKey &tablet_key)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObArenaAllocator allocator("LsCkptTblMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObSSMetaTabletRow tablet_row;
  ObTieredMetadataStore tiered_store;
  blocksstable::MacroBlockId tablet_macro_id;

  if (OB_ISNULL(meta_svr) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service or sql proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!tablet_key.is_valid()
                         || !tablet_key.ls_id_.is_sys_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet key", KR(ret), K(tablet_key));
  } else if (OB_FAIL(tiered_store.init(OB_SYS_TENANT_ID, GCTX.sql_proxy_))) {
    LOG_WARN("init tiered metadata store for ls ckpt failed", KR(ret), K(tablet_key));
  } else if (OB_FAIL(meta_svr->read_tablet_meta(tablet_key, allocator, tablet_row))) {
    LOG_WARN("read tablet meta failed", KR(ret), K(tablet_key));
  } else if (OB_FAIL(write_tablet_meta_(
                 tablet_key, tablet_row.value_, tiered_store, allocator, tablet_macro_id))) {
    LOG_WARN("write tablet meta and sub-meta for ls ckpt failed", KR(ret), K(tablet_key));
  }
  return ret;
}

int ObSSLsCkptWriter::collect_ls_meta(
    const ObLSID &ls_id,
    const SCN &meta_scn,
    ObSSLSMeta &ls_meta) const
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObSSMetaReadParamV2 read_param;

  if (OB_ISNULL(meta_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service is null", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_sys_ls()
                         || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id or meta scn", KR(ret), K(ls_id), K(meta_scn));
  } else {
    read_param.set_read_mark_delete(true);
    read_param.set_ls_level_param(
        ObSSMetaReadParamType::LS_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::LS_META,
        ls_id,
        SCN::max_scn(),
        true/*is_in_table*/);
    if (OB_FAIL(meta_svr->get_ls_meta(read_param, meta_scn, ls_meta))) {
      LOG_WARN("get ls meta failed", KR(ret), K(ls_id), K(meta_scn));
    }
  }
  return ret;
}

void ObSSLsCkptWriter::init_write_tablet_context_(
    const ObSSMetaTabletKey &tablet_key,
    ObTieredMetadataStore &tiered_store,
    ObArenaAllocator &allocator)
{
  tablet_key_ = tablet_key;
  tablet_meta_ = ObSSTabletMetaValue();
  tablet_ = nullptr;
  macro_info_ = nullptr;
  tiered_store_ = &tiered_store;
  allocator_ = &allocator;
}

void ObSSLsCkptWriter::reset_write_tablet_context_()
{
  tablet_key_.reset();
  tablet_meta_ = ObSSTabletMetaValue();
  tablet_ = nullptr;
  macro_info_ = nullptr;
  tiered_store_ = nullptr;
  allocator_ = nullptr;
}

int ObSSLsCkptWriter::write_tablet_meta_(
    const ObSSMetaTabletKey &tablet_key,
    const ObString &tablet_meta_value,
    ObTieredMetadataStore &tiered_store,
    ObArenaAllocator &allocator,
    blocksstable::MacroBlockId &obj_macro_id)
{
  int ret = OB_SUCCESS;
  ObString rewritten_value;
  blocksstable::MacroBlockId in_table_macro_id;
  obj_macro_id.reset();
  init_write_tablet_context_(tablet_key, tiered_store, allocator);

  if (OB_UNLIKELY(!tablet_key_.is_valid() || tablet_meta_value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls ckpt tablet write argument", KR(ret), K_(tablet_key),
             K(tablet_meta_value.length()));
  } else {
    ObSSMetaTabletKey tablet_key_copy(tablet_key_);
    in_table_macro_id = tablet_key_copy.convert_to_macro_id(true /*is_in_table*/);
    if (OB_UNLIKELY(!in_table_macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to build in-table ls ckpt tablet id",
               KR(ret), K_(tablet_key), K(in_table_macro_id));
    } else {
      obj_macro_id = in_table_macro_id;
      obj_macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
      if (OB_UNLIKELY(!obj_macro_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to build ls ckpt tablet object id",
                 KR(ret), K_(tablet_key), K(in_table_macro_id), K(obj_macro_id));
      } else if (OB_FAIL(rewrite_tablet_meta_(tablet_meta_value, rewritten_value))) {
        LOG_WARN("rewrite tablet meta for ls ckpt failed", KR(ret), K_(tablet_key));
      } else if (OB_FAIL(write_object_idempotently(
                     obj_macro_id, rewritten_value.ptr(),
                     rewritten_value.length(), false /*need_align*/,
                     need_check_existing_object_))) {
        LOG_WARN("write tablet meta to object storage failed",
                 KR(ret), K(obj_macro_id), K_(tablet_key));
      }
    }
  }
  if (OB_FAIL(ret)) {
    obj_macro_id.reset();
  }
  // 写入上下文只在本函数内有效，返回前清空所有借用调用方对象的成员。
  reset_write_tablet_context_();
  return ret;
}

int ObSSLsCkptWriter::write_checkpoint_object(
    const ObLSID &ls_id,
    const ObSSLsCkptData &ckpt_data) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("LsCkptOss", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  char *buf = nullptr;
  int64_t pos = 0;

  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !ls_id.is_sys_ls()
                  || !ckpt_data.is_valid()
                  || ckpt_data.ls_meta_.ls_id_ != ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid checkpoint write argument", KR(ret), K(ls_id), K(ckpt_data));
  } else {
    const int64_t serialize_size = ckpt_data.get_serialize_size();
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc serialize buffer failed", KR(ret), K(serialize_size));
    } else if (OB_FAIL(ckpt_data.serialize(buf, serialize_size, pos))) {
      LOG_WARN("serialize ckpt data failed", KR(ret), K(serialize_size));
    } else {
      blocksstable::ObStorageObjectOpt opt;
      opt.set_ss_ls_ckpt_opt(
          ls_id.id(),
          ckpt_data.ckpt_scn_.get_val_for_inner_table_field(),
          false /*is_in_table*/);

      blocksstable::ObStorageObjectWriteInfo write_info;
      write_info.buffer_ = buf;
      write_info.offset_ = 0;
      write_info.size_ = pos;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
      write_info.mtl_tenant_id_ = MTL_ID();

      blocksstable::ObStorageObjectHandle handle;
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.write_object(opt, write_info, handle))) {
        LOG_WARN("write checkpoint to object storage failed",
                 KR(ret), K(opt), K(pos), K(ckpt_data.ckpt_scn_));
      }
    }
  }
  return ret;
}

static void redirect_macro_id_to_object_storage(
    blocksstable::MacroBlockId &macro_id)
{
  if (macro_id.is_in_table()) {
    macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  }
}

static int redirect_meta_addr_to_object_storage(ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId macro_id;
  int64_t offset = 0;
  int64_t size = 0;
  if (addr.is_none()) {
    // 空地址不需要处理。
  } else if (OB_FAIL(addr.get_block_addr(macro_id, offset, size))) {
    LOG_WARN("failed to get block address", KR(ret), K(addr));
  } else if (OB_UNLIKELY(macro_id.is_in_table()
                         && !macro_id.is_shared_tablet_sub_meta_in_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected in-table macro id for object storage redirect",
             KR(ret), K(macro_id), K(addr));
  } else {
    redirect_macro_id_to_object_storage(macro_id);
    if (OB_FAIL(addr.set_block_addr(macro_id, offset, size, addr.type()))) {
      LOG_WARN("failed to set object storage block address",
               KR(ret), K(macro_id), K(offset), K(size), K(addr));
    }
  }
  return ret;
}

static int redirect_sstable_addr_to_object_storage(blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (!sstable.get_addr().is_disked()) {
    // 已经驻留内存的 sstable 不需要转换自身地址。
  } else {
    ObMetaDiskAddr addr = sstable.get_addr();
    if (OB_FAIL(redirect_meta_addr_to_object_storage(addr))) {
      LOG_WARN("failed to redirect sstable address", KR(ret), K(sstable));
    } else if (OB_FAIL(sstable.set_addr(addr))) {
      LOG_WARN("failed to set redirected sstable address", KR(ret), K(addr), K(sstable));
    }
  }
  return ret;
}

int ObSSLsCkptWriter::get_tiered_meta_(
    const blocksstable::MacroBlockId &in_table_macro_id,
    ObSSTieredMetaTableIterator &iter,
    ObString &meta_value) const
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId row_macro_id;
  meta_value.reset();

  if (OB_UNLIKELY(!in_table_macro_id.is_shared_tablet_sub_meta_in_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid in-table tiered meta macro id", KR(ret), K(in_table_macro_id));
  } else if (OB_ISNULL(tiered_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tiered metadata store is null", KR(ret), K(in_table_macro_id));
  } else if (OB_FAIL(tiered_store_->get_tiered_metadata_iter(
                 in_table_macro_id, in_table_macro_id, iter))) {
    LOG_WARN("get tiered metadata iterator failed", KR(ret), K(in_table_macro_id));
  // 只按 meta_key 读取 value，不使用可能失真的 meta_type。
  } else if (OB_FAIL(iter.get_next(row_macro_id, meta_value))) {
    if (OB_ITER_END == ret) {
      ret = OB_OBJECT_NOT_EXIST;
    }
    LOG_WARN("get tiered metadata row failed", KR(ret), K(in_table_macro_id));
  } else if (OB_UNLIKELY(row_macro_id != in_table_macro_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tiered metadata row macro id mismatched",
             KR(ret), K(in_table_macro_id), K(row_macro_id));
  }
  return ret;
}

int ObSSLsCkptWriter::upload_table_store_(
    const blocksstable::MacroBlockId &table_store_macro_id)
{
  int ret = OB_SUCCESS;
  ObSSTieredMetaTableIterator iter;
  ObString table_store_value;
  ObString rewritten_value;

  if (OB_FAIL(get_tiered_meta_(
          table_store_macro_id, iter, table_store_value))) {
    LOG_WARN("get table store tiered metadata failed",
             KR(ret), K(table_store_macro_id), K_(tablet_key));
  } else if (OB_FAIL(upload_sstables_and_rewrite_table_store_(
                 table_store_value, rewritten_value))) {
    LOG_WARN("upload sstables and rewrite table store for ls ckpt failed",
             KR(ret), K(table_store_macro_id), K_(tablet_key));
  } else {
    blocksstable::MacroBlockId obj_macro_id = table_store_macro_id;
    obj_macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    if (OB_FAIL(write_object_idempotently(
            obj_macro_id, rewritten_value.ptr(), rewritten_value.length(),
            true /*need_align*/, need_check_existing_object_))) {
      LOG_WARN("write rewritten table store to object storage failed",
               KR(ret), K(obj_macro_id), K_(tablet_key));
    }
  }
  return ret;
}

int ObSSLsCkptWriter::check_table_store_sstables_(
    const ObTabletTableStore &table_store) const
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator iter(false /*is_reverse*/, false /*need_load_sstable*/);
  ObITable *table = nullptr;
  blocksstable::ObSSTable *sstable = nullptr;
  blocksstable::ObSSTableMetaHandle meta_handle;
  // SHARED_MACRO_BLOCKS describes data macro blocks. An sstable without data
  // macro blocks therefore needs no shared-data-macro validation here.
  bool has_data_macro_blocks = false;

  if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store for ls ckpt precheck", KR(ret), K(table_store));
  } else if (OB_FAIL(table_store.get_all_sstable(iter, false /*unpack_co_table*/))) {
    LOG_WARN("get all sstables for ls ckpt precheck failed", KR(ret), K(table_store));
  } else {
    while (OB_SUCC(ret)) {
      table = nullptr;
      sstable = nullptr;
      meta_handle.reset();
      has_data_macro_blocks = false;
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next sstable for ls ckpt precheck failed", KR(ret), K(table_store));
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable is null for ls ckpt precheck", KR(ret), K(table_store));
      } else if (OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-sstable table returned by ls ckpt precheck iterator",
                 KR(ret), KPC(table), K(table_store));
      } else if (ObITable::is_column_store_sstable(table->get_table_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("table store has unsupported column store sstable for ls ckpt",
                  KR(ret), K_(tablet_key), KPC(table));
      } else if (FALSE_IT(sstable = static_cast<blocksstable::ObSSTable *>(table))) {
      } else if (FALSE_IT(has_data_macro_blocks = !sstable->is_empty())) {
      } else if (has_data_macro_blocks && OB_FAIL(sstable->get_meta(meta_handle))) {
        LOG_WARN("get non-empty sstable meta for ls ckpt failed",
                 KR(ret), K_(tablet_key), KPC(sstable));
      } else if (has_data_macro_blocks
                 && OB_UNLIKELY(!meta_handle.get_sstable_meta()
                                      .get_table_shared_flag()
                                      .is_shared_macro_blocks())) {
        ret = OB_EAGAIN;
        LOG_WARN("non-empty sstable still references private macro blocks, retry ls ckpt later",
                 KR(ret), K_(tablet_key),
                 "table_shared_flag",
                 meta_handle.get_sstable_meta().get_table_shared_flag(),
                 KPC(sstable));
      }
    }
  }
  return ret;
}

int ObSSLsCkptWriter::check_object_storage_table_store_() const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;

  if (OB_ISNULL(tablet_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null for ls ckpt precheck", KR(ret), K_(tablet_key));
  } else if (OB_FAIL(tablet_->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fetch object storage table store for ls ckpt precheck failed",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("get object storage table store for ls ckpt precheck failed",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object storage table store is null for ls ckpt precheck",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (OB_FAIL(check_table_store_sstables_(*table_store))) {
    LOG_WARN("check object storage table store for ls ckpt failed",
             KR(ret), K_(tablet_key), KPC(tablet_));
  }
  return ret;
}

int ObSSLsCkptWriter::validate_referenced_sub_meta_() const
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_manager = nullptr;
  if (OB_ISNULL(tablet_) || OB_ISNULL(macro_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet context is incomplete for ls ckpt writer",
             KR(ret), K_(tablet_key), KP_(tablet), KP_(macro_info));
  } else if (OB_FAIL(ObSSObjectAccessUtil::get_file_manager(MTL_ID(), file_manager))) {
    LOG_WARN("get file manager failed", KR(ret), K(MTL_ID()));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(MTL_ID()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_info_->shared_meta_block_info_arr_.cnt_; ++i) {
    blocksstable::MacroBlockId obj_macro_id =
        macro_info_->shared_meta_block_info_arr_.arr_[i];
    bool is_exist = false;
    if (OB_UNLIKELY(!obj_macro_id.is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid shared meta macro id in tablet macro info", KR(ret), K(i), K(obj_macro_id));
    } else {
      redirect_macro_id_to_object_storage(obj_macro_id);
      // 发布前必须直查远端，避免本地 sub-meta 缓存掩盖对象缺失。
      if (OB_FAIL(file_manager->is_exist_remote_file(
              obj_macro_id, 0 /*ls_epoch*/, is_exist))) {
        LOG_WARN("check shared meta object existence failed", KR(ret), K(i), K(obj_macro_id));
      } else if (OB_UNLIKELY(!is_exist)) {
        ret = OB_OBJECT_NOT_EXIST;
        LOG_WARN("shared meta object referenced by tablet was not uploaded",
                 KR(ret), K(i), K(obj_macro_id));
      }
    }
  }
  return ret;
}

int ObSSLsCkptWriter::redirect_tablet_to_object_storage_()
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr table_store_addr;
  ObMetaDiskAddr storage_schema_addr;

  if (OB_ISNULL(tablet_) || OB_ISNULL(macro_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet context is incomplete for ls ckpt redirect",
             KR(ret), K_(tablet_key), KP_(tablet), KP_(macro_info));
  } else {
    table_store_addr = tablet_->table_store_addr_.addr_;
    storage_schema_addr = tablet_->storage_schema_addr_.addr_;
    if (OB_FAIL(redirect_meta_addr_to_object_storage(table_store_addr))) {
      LOG_WARN("failed to redirect table store address for ls ckpt writer",
               KR(ret), K_(tablet_key), KPC(tablet_));
    } else if (OB_FAIL(redirect_meta_addr_to_object_storage(storage_schema_addr))) {
      LOG_WARN("failed to redirect storage schema address for ls ckpt writer",
               KR(ret), K_(tablet_key), KPC(tablet_));
    } else {
      // 地址转换全部成功后再提交，避免失败时留下半转换状态。
      tablet_->table_store_addr_.addr_ = table_store_addr;
      tablet_->storage_schema_addr_.addr_ = storage_schema_addr;
      for (int64_t i = 0; i < macro_info_->shared_meta_block_info_arr_.cnt_; ++i) {
        redirect_macro_id_to_object_storage(macro_info_->shared_meta_block_info_arr_.arr_[i]);
      }
    }
  }
  return ret;
}

int ObSSLsCkptWriter::precheck_and_upload_table_store_(
    const blocksstable::MacroBlockId &table_store_macro_id)
{
  int ret = OB_SUCCESS;

  if (!table_store_macro_id.is_valid()) {
    // 空地址表示当前 tablet 没有 table store。
  } else if (table_store_macro_id.is_shared_tablet_sub_meta_in_table()) {
    if (OB_FAIL(upload_table_store_(table_store_macro_id))) {
      LOG_WARN("upload table store failed",
               KR(ret), K_(tablet_key), K(table_store_macro_id));
    }
  } else if (table_store_macro_id.is_in_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected in-table table store id for ls ckpt precheck",
             KR(ret), K_(tablet_key), K(table_store_macro_id));
  } else if (!table_store_macro_id.is_shared_tablet_sub_meta()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table store id for ls ckpt precheck",
             KR(ret), K_(tablet_key), K(table_store_macro_id));
  } else if (OB_FAIL(check_object_storage_table_store_())) {
    LOG_WARN("check object storage table store for ls ckpt failed",
             KR(ret), K_(tablet_key), K(table_store_macro_id));
  }
  return ret;
}

int ObSSLsCkptWriter::upload_sub_meta_and_redirect_tablet_()
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId table_store_macro_id;
  blocksstable::MacroBlockId storage_schema_macro_id;

  // 这里只处理已经持久化的 sub-meta。空地址表示没有对应对象；内存地址
  // 表示源 tablet 尚未完成持久化，不能伪装成对象存储地址写进 checkpoint。
  if (OB_ISNULL(tablet_) || OB_ISNULL(tablet_->get_allocator()) || OB_ISNULL(macro_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet context is incomplete for ls ckpt writer",
             KR(ret), K_(tablet_key), KP_(tablet), KP_(macro_info));
  } else if (OB_UNLIKELY(tablet_->table_store_addr_.is_memory_object())
             || OB_UNLIKELY(tablet_->storage_schema_addr_.is_memory_object())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected memory sub-meta address in ls ckpt writer",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (tablet_->table_store_addr_.addr_.is_block()
             && OB_FAIL(tablet_->table_store_addr_.addr_.get_macro_block_id(
                    table_store_macro_id))) {
    LOG_WARN("failed to get table store macro id for ls ckpt writer",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (tablet_->storage_schema_addr_.addr_.is_block()
             && OB_FAIL(tablet_->storage_schema_addr_.addr_.get_macro_block_id(
                    storage_schema_macro_id))) {
    LOG_WARN("failed to get storage schema macro id for ls ckpt writer",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (OB_FAIL(precheck_and_upload_table_store_(table_store_macro_id))) {
    LOG_WARN("precheck and upload table store failed",
             KR(ret), K_(tablet_key), K(table_store_macro_id));
  } else if (storage_schema_macro_id.is_shared_tablet_sub_meta_in_table()
             && OB_FAIL(upload_tiered_meta_(storage_schema_macro_id))) {
    LOG_WARN("upload storage schema sub-meta failed",
             KR(ret), K_(tablet_key), K(storage_schema_macro_id));
  } else if (OB_FAIL(validate_referenced_sub_meta_())) {
    LOG_WARN("validate referenced sub-meta failed", KR(ret), K_(tablet_key));
  } else if (OB_FAIL(redirect_tablet_to_object_storage_())) {
    LOG_WARN("redirect tablet to object storage failed", KR(ret), K_(tablet_key));
  }
  return ret;
}

int ObSSLsCkptWriter::build_tablet_meta_(
    const ObString &tablet_meta_value)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr tablet_addr;
  ObTabletBlockHeader header;
  char *tablet_buf = nullptr;
  int64_t tablet_buf_len = 0;
  int64_t header_pos = 0;
  int64_t deserialize_pos = 0;

  // deserialize 不恢复写入侧使用的 data_version 和 macro_info，需要手动设置，
  // 这里统一构造后续改写及重新序列化需要的 tablet_meta_ 上下文。
  if (OB_UNLIKELY(!tablet_key_.is_valid()
                  || tablet_meta_value.empty()
                  || OB_ISNULL(allocator_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls ckpt tablet meta build argument",
             KR(ret), K_(tablet_key), K(tablet_meta_value.length()));
  } else if (OB_FAIL(ObSSTabletMetaValue::get_tablet_raw_buffer(
                 tablet_meta_value.ptr(), tablet_meta_value.length(), *allocator_,
                 tablet_buf, tablet_buf_len))) {
    LOG_WARN("failed to get tablet raw buffer", KR(ret), K(tablet_meta_value.length()));
  } else if (OB_FAIL(header.deserialize(tablet_buf, tablet_buf_len, header_pos))) {
    LOG_WARN("failed to deserialize tablet block header", KR(ret), K(tablet_buf_len));
  } else if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_addr(
                 tablet_key_.ls_id_, tablet_key_.tablet_id_, tablet_key_.reorganization_scn_,
                 0 /*op_id*/, tablet_meta_value.length(), tablet_addr))) {
    LOG_WARN("failed to generate tablet address", KR(ret), K_(tablet_key));
  } else if (OB_FAIL(tablet_meta_.deserialize(
                 *allocator_, tablet_addr, tablet_meta_value.ptr(),
                 tablet_meta_value.length(), deserialize_pos))) {
    LOG_WARN("failed to deserialize tablet meta value", KR(ret), K(tablet_addr));
  } else if (OB_UNLIKELY(deserialize_pos > tablet_meta_value.length()
                         || OB_ISNULL(tablet_meta_.tablet_))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tablet meta value has invalid payload",
             KR(ret), K(deserialize_pos), K(tablet_meta_value.length()),
             KP(tablet_meta_.tablet_));
  } else if (OB_UNLIKELY(!tablet_meta_.tablet_->is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid tablet in ls ckpt payload",
             KR(ret), K_(tablet_key), KPC(tablet_meta_.tablet_));
  } else if (OB_UNLIKELY(tablet_meta_.tablet_->get_ls_id() != tablet_key_.ls_id_
                         || tablet_meta_.tablet_->get_tablet_id() != tablet_key_.tablet_id_
                         || tablet_meta_.tablet_->get_reorganization_scn()
                                != tablet_key_.reorganization_scn_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tablet payload does not match ls ckpt key", KR(ret),
             K_(tablet_key), KPC(tablet_meta_.tablet_));
  } else if (FALSE_IT(tablet_ = tablet_meta_.tablet_)) {
  } else if (FALSE_IT(tablet_meta_.data_version_ = header.data_version_)) {
  } else if (OB_UNLIKELY(!tablet_->is_row_store())) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("column store tablet is unsupported by ls ckpt",
              KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (OB_FAIL(tablet_->load_macro_info(
                 0 /*ls_epoch*/, *allocator_, macro_info_))) {
    LOG_WARN("failed to load tablet macro info", KR(ret), K(tablet_addr));
  } else if (OB_ISNULL(macro_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet macro info is null", KR(ret), K(tablet_addr));
  } else {
    tablet_meta_.macro_info_ = macro_info_;
  }
  return ret;
}

int ObSSLsCkptWriter::serialize_tablet_meta_(
    ObString &rewritten_value)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;

  const int64_t serialize_size = tablet_meta_.get_serialize_size();
  if (OB_UNLIKELY(serialize_size <= 0 || serialize_size > INT32_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("invalid rewritten tablet meta size",
             KR(ret), K_(tablet_key), K(serialize_size));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret), K_(tablet_key));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate rewritten tablet meta buffer",
             KR(ret), K_(tablet_key), K(serialize_size));
  } else if (OB_FAIL(tablet_meta_.serialize(buf, serialize_size, pos))) {
    LOG_WARN("failed to serialize rewritten tablet meta",
             KR(ret), K_(tablet_key), K(serialize_size));
  } else if (OB_UNLIKELY(pos != serialize_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rewritten tablet meta size mismatch",
             KR(ret), K_(tablet_key), K(pos), K(serialize_size));
  } else {
    rewritten_value.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObSSLsCkptWriter::rewrite_tablet_meta_(
    const ObString &tablet_meta_value,
    ObString &rewritten_value)
{
  int ret = OB_SUCCESS;
  rewritten_value.reset();

  if (OB_FAIL(build_tablet_meta_(tablet_meta_value))) {
    LOG_WARN("failed to build tablet meta for ls ckpt", KR(ret), K_(tablet_key));
  } else if (OB_FAIL(upload_sub_meta_and_redirect_tablet_())) {
    LOG_WARN("failed to upload sub-meta and rewrite tablet for ls ckpt",
             KR(ret), K_(tablet_key));
  } else if (OB_FAIL(serialize_tablet_meta_(rewritten_value))) {
    LOG_WARN("failed to serialize tablet meta for ls ckpt",
             KR(ret), K_(tablet_key));
  }
  return ret;
}

int ObSSLsCkptWriter::serialize_table_store_(
    const ObTabletTableStore &table_store,
    ObString &rewritten_value)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  const int64_t serialize_size = table_store.get_serialize_size(tablet_meta_.data_version_);

  if (OB_UNLIKELY(serialize_size <= 0 || serialize_size > INT32_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("invalid rewritten table store size",
             KR(ret), K_(tablet_key), K(serialize_size));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret), K_(tablet_key));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate rewritten table store buffer",
             KR(ret), K_(tablet_key), K(serialize_size));
  } else if (OB_FAIL(table_store.serialize(
                 tablet_meta_.data_version_, buf, serialize_size, pos))) {
    LOG_WARN("failed to serialize rewritten table store",
             KR(ret), K_(tablet_key), K(serialize_size));
  } else if (OB_UNLIKELY(pos != serialize_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rewritten table store size mismatch",
             KR(ret), K_(tablet_key), K(pos), K(serialize_size));
  } else {
    rewritten_value.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObSSLsCkptWriter::upload_sstables_and_rewrite_table_store_(
    const ObString &table_store_value,
    ObString &rewritten_value)
{
  int ret = OB_SUCCESS;
  void *table_store_buf = nullptr;
  ObTabletTableStore *table_store = nullptr;
  int64_t pos = 0;
  rewritten_value.reset();

  if (OB_ISNULL(tablet_) || OB_UNLIKELY(!tablet_->is_valid() || table_store_value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store rewrite argument",
             KR(ret), K_(tablet_key), KP_(tablet), K(table_store_value.length()));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret), K_(tablet_key));
  } else if (OB_ISNULL(table_store_buf = allocator_->alloc(sizeof(ObTabletTableStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table store for ls ckpt", KR(ret));
  } else if (FALSE_IT(table_store = new(table_store_buf) ObTabletTableStore())) {
  } else if (OB_FAIL(table_store->deserialize(*allocator_, *tablet_, table_store_value.ptr(),
                                              table_store_value.length(), pos))) {
    LOG_WARN("failed to deserialize table store for ls ckpt",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (OB_UNLIKELY(pos > table_store_value.length())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("table store payload exceeds buffer", KR(ret), K(pos),
             K(table_store_value.length()));
  } else if (OB_UNLIKELY(!table_store->is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("deserialized table store is invalid",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (OB_FAIL(check_table_store_sstables_(*table_store))) {
    LOG_WARN("precheck table store for ls ckpt failed",
             KR(ret), K_(tablet_key), KPC(tablet_));
  } else if (OB_FAIL(upload_and_redirect_sstables_(
                 table_store->get_major_sstables()))) {
    LOG_WARN("failed to upload major sstable meta for ls ckpt",
             KR(ret), K_(tablet_key));
  } else if (OB_FAIL(upload_and_redirect_sstables_(
                 table_store->get_inc_major_sstables()))) {
    LOG_WARN("failed to upload inc major sstable meta for ls ckpt",
             KR(ret), K_(tablet_key));
  } else if (OB_FAIL(upload_and_redirect_sstables_(
                 table_store->get_minor_sstables()))) {
    LOG_WARN("failed to upload minor sstable meta for ls ckpt",
             KR(ret), K_(tablet_key));
  } else if (OB_FAIL(upload_and_redirect_sstables_(
                 table_store->get_ddl_sstables()))) {
    LOG_WARN("failed to upload ddl sstable meta for ls ckpt",
             KR(ret), K_(tablet_key));
  } else if (OB_FAIL(upload_and_redirect_sstables_(
                 table_store->get_inc_major_ddl_sstables()))) {
    LOG_WARN("failed to upload inc major ddl sstable meta for ls ckpt",
             KR(ret), K_(tablet_key));
  } else if (OB_FAIL(upload_and_redirect_sstables_(
                 table_store->get_meta_major_sstables()))) {
    LOG_WARN("failed to upload meta major sstable meta for ls ckpt",
             KR(ret), K_(tablet_key));
  } else if (OB_FAIL(serialize_table_store_(*table_store, rewritten_value))) {
    LOG_WARN("failed to serialize table store for ls ckpt",
             KR(ret), K_(tablet_key));
  }
  if (OB_NOT_NULL(table_store)) {
    table_store->~ObTabletTableStore();
  }
  return ret;
}

int ObSSLsCkptWriter::upload_and_redirect_sstables_(
    const ObSSTableArray &sstable_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    blocksstable::ObSSTable *sstable = sstable_array.at(i);
    if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is null for ls ckpt", KR(ret), K(i));
    } else if (OB_FAIL(upload_and_redirect_sstable_(*sstable))) {
      LOG_WARN("failed to upload and redirect sstable for ls ckpt",
               KR(ret), K(i), K(*sstable));
    }
  }
  return ret;
}

int ObSSLsCkptWriter::upload_and_redirect_sstable_(
    blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObSSTieredMetaTableIterator iter;
  blocksstable::MacroBlockId in_table_macro_id;
  ObString meta_value;

  if (OB_UNLIKELY(!sstable.get_addr().is_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-block sstable address for ls ckpt",
             KR(ret), K_(tablet_key), K(sstable));
  } else if (OB_UNLIKELY(ObITable::is_column_store_sstable(sstable.get_table_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column store sstable passed ls ckpt precheck unexpectedly",
              KR(ret), K(sstable));
  } else {
    if (OB_FAIL(sstable.get_addr().get_macro_block_id(in_table_macro_id))) {
      LOG_WARN("failed to get sstable macro id for ls ckpt", KR(ret), K(sstable));
    } else if (in_table_macro_id.is_shared_tablet_sub_meta_in_table()) {
      if (OB_FAIL(get_tiered_meta_(in_table_macro_id, iter, meta_value))) {
        LOG_WARN("get sstable tiered metadata failed",
                 KR(ret), K_(tablet_key), K(in_table_macro_id));
      } else {
        blocksstable::MacroBlockId obj_macro_id = in_table_macro_id;
        obj_macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
        if (OB_FAIL(write_object_idempotently(
                       obj_macro_id, meta_value.ptr(), meta_value.length(),
                       true /*need_align*/, need_check_existing_object_))) {
          LOG_WARN("failed to upload sstable sub-meta for ls ckpt",
                   KR(ret), K_(tablet_key), K(obj_macro_id));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sstable macro id for ls ckpt",
               KR(ret), K_(tablet_key), K(in_table_macro_id));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(redirect_sstable_addr_to_object_storage(sstable))) {
    LOG_WARN("failed to redirect sstable address for ls ckpt", KR(ret), K(sstable));
  }
  return ret;
}

int ObSSLsCkptWriter::upload_tiered_meta_(
    const blocksstable::MacroBlockId &in_table_macro_id)
{
  int ret = OB_SUCCESS;
  ObSSTieredMetaTableIterator iter;
  ObString meta_value;
  if (OB_FAIL(get_tiered_meta_(in_table_macro_id, iter, meta_value))) {
    LOG_WARN("get tiered metadata failed",
             KR(ret), K_(tablet_key), K(in_table_macro_id));
  } else {
    blocksstable::MacroBlockId obj_macro_id = in_table_macro_id;
    obj_macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    if (OB_FAIL(write_object_idempotently(
                   obj_macro_id, meta_value.ptr(), meta_value.length(),
                   true /*need_align*/, need_check_existing_object_))) {
      LOG_WARN("failed to upload tiered meta for ls ckpt",
               KR(ret), K_(tablet_key), K(in_table_macro_id), K(obj_macro_id));
    }
  }
  return ret;
}

static int prepare_aligned_write_buf(
    ObIAllocator &allocator,
    const char *buf,
    const int64_t size,
    const bool need_align,
    const char *&write_buf,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_buf = buf;
  write_size = size;
  if (OB_UNLIKELY(OB_ISNULL(buf) || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aligned write buffer argument", KR(ret), KP(buf), K(size));
  } else if (need_align) {
    const int64_t aligned_size = upper_align(size, DIO_READ_ALIGN_SIZE);
    if (aligned_size != size) {
      char *align_buf = nullptr;
      if (OB_ISNULL(align_buf = static_cast<char *>(allocator.alloc(aligned_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc aligned buffer failed", KR(ret), K(size), K(aligned_size));
      } else {
        MEMSET(align_buf, 0, aligned_size);
        MEMCPY(align_buf, buf, size);
        write_buf = align_buf;
        write_size = aligned_size;
      }
    }
  }
  return ret;
}

static int validate_existing_object(
    const blocksstable::MacroBlockId &macro_id,
    const char *expected_buf,
    const int64_t expected_size,
    ObIAllocator &allocator,
    ObBaseFileManager &file_manager)
{
  int ret = OB_SUCCESS;
  int64_t object_size = 0;
  char *read_buf = nullptr;
  if (OB_FAIL(file_manager.get_remote_file_length(
          macro_id, 0 /*ls_epoch*/, object_size))) {
    LOG_WARN("get existing object size failed", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(object_size != expected_size)) {
    ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
    LOG_WARN("existing object size mismatch", KR(ret), K(macro_id),
             K(object_size), K(expected_size));
  } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(object_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate existing object read buffer failed", KR(ret), K(macro_id), K(object_size));
  } else {
    blocksstable::ObStorageObjectReadInfo read_info;
    blocksstable::ObStorageObjectHandle read_handle;
    ObSSObjectStorageReader object_storage_reader;
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = object_size;
    read_info.buf_ = read_buf;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    read_info.set_ls_epoch_id(0);
    read_info.mtl_tenant_id_ = MTL_ID();
    // 一致性校验必须直读对象存储，避免本地缓存掩盖远端残留或损坏内容。
    if (OB_FAIL(object_storage_reader.aio_read(read_info, read_handle))) {
      LOG_WARN("read existing object failed", KR(ret), K(macro_id), K(read_info));
    } else if (OB_FAIL(read_handle.wait())) {
      LOG_WARN("wait existing object read failed", KR(ret), K(macro_id), K(read_info));
    } else if (OB_UNLIKELY(OB_ISNULL(read_handle.get_buffer())
                           || read_handle.get_data_size() != expected_size)) {
      ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
      LOG_WARN("existing object read size mismatch", KR(ret), K(macro_id), K(expected_size),
               "read_size", read_handle.get_data_size(), KP(read_handle.get_buffer()));
    } else if (OB_UNLIKELY(0 != MEMCMP(
                   read_handle.get_buffer(), expected_buf, expected_size))) {
      ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
      LOG_WARN("existing object content mismatch", KR(ret), K(macro_id), K(expected_size));
    }
  }
  return ret;
}

// 非 bootstrap 路径在对象不存在时写入；已存在时只有完整落盘 payload 一致才幂等成功。
// 对齐与幂等校验使用单对象本地 allocator 管理临时 buffer，写入结束后立即释放，
// 避免同一 tablet 引用大量 sub-meta 时跨对象累积临时内存。
static int write_object_idempotently(
    const blocksstable::MacroBlockId &macro_id,
    const char *buf,
    const int64_t size,
    const bool need_align,
    const bool need_check_existing_object)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  bool need_write = !need_check_existing_object;
  ObBaseFileManager *file_manager = nullptr;
  const char *write_buf = nullptr;
  int64_t write_size = 0;
  ObArenaAllocator allocator("LsCkptObjTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  if (OB_UNLIKELY(!macro_id.is_valid() || OB_ISNULL(buf) || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object write argument", KR(ret), K(macro_id), KP(buf), K(size));
  } else if (OB_FAIL(prepare_aligned_write_buf(
          allocator, buf, size, need_align, write_buf, write_size))) {
    LOG_WARN("prepare aligned write buffer failed",
             KR(ret), K(macro_id), K(size), K(need_align));
  } else if (!need_check_existing_object) {
    // Bootstrap allows overwriting objects.
  } else if (OB_FAIL(ObSSObjectAccessUtil::get_file_manager(
                 MTL_ID(), file_manager))) {
    LOG_WARN("get file manager failed", KR(ret), K(macro_id), K(MTL_ID()));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(macro_id), K(MTL_ID()));
  } else if (OB_FAIL(file_manager->is_exist_remote_file(
                 macro_id, 0 /*ls_epoch*/, is_exist))) {
    LOG_WARN("check object existence failed", KR(ret), K(macro_id));
  } else if (is_exist) {
    if (OB_FAIL(validate_existing_object(
            macro_id, write_buf, write_size, allocator, *file_manager))) {
      LOG_WARN("existing object payload validation failed", KR(ret), K(macro_id),
               K(size), K(write_size));
    } else {
      LOG_TRACE("object already exists with identical payload", K(macro_id), K(write_size));
    }
  } else {
    need_write = true;
  }

  if (OB_SUCC(ret) && need_write) {
    blocksstable::ObStorageObjectWriteInfo write_info;
    blocksstable::ObStorageObjectHandle handle;
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = write_size;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_unsealed();
    write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.set_ls_epoch_id(0);
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(macro_id, write_info, handle))) {
      LOG_WARN("async write object failed", KR(ret), K(macro_id), K(write_info));
    } else if (OB_FAIL(handle.wait())) {
      LOG_WARN("wait object write failed", KR(ret), K(macro_id), K(write_info));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
