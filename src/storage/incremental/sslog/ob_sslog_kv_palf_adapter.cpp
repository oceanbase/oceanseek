/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "src/logservice/libpalf/libpalf_kv_instance.h"
#include "ob_sslog_kv_palf_adapter.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_sslog_kv_id_service.h"
#include "storage/tx/ob_ts_mgr.h"

namespace oceanbase
{
namespace sslog
{

using namespace libpalf;

int ObSSLogKVPalfAdapter::init(const uint64_t cluster_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  tenant_id_str_.reset();
  cluster_id_str_.reset();

  allocator_.set_tenant_id(tenant_id);
  allocator_.set_label("PalfKVAdapter");
  cluster_id_str_.set_allocator(&allocator_);
  tenant_id_str_.set_allocator(&allocator_);

  char tmp_str_buf[64];
  memset(tmp_str_buf, 0, sizeof(char) * 64);
  int64_t pos = 0;

  if (OB_SUCC(ret)) {
    pos = 0;
    if (OB_FAIL(databuff_printf(tmp_str_buf, 64, pos, "%ld", cluster_id))) {
      STORAGE_LOG(WARN, "convert into str failed", K(ret), K(cluster_id), K(pos));
    } else if (OB_FAIL(cluster_id_str_.append(tmp_str_buf, pos))) {
      STORAGE_LOG(WARN, "append cluster_id str into StringBuffer failed", K(ret),
                  K(cluster_id_str_), K(pos), K(cluster_id));
    }
  }

  if (OB_SUCC(ret)) {
    pos = 0;
    if (OB_FAIL(databuff_printf(tmp_str_buf, 64, pos, "%ld", tenant_id))) {
      STORAGE_LOG(WARN, "convert into str failed", K(ret), K(tenant_id), K(pos));
    } else if (OB_FAIL(tenant_id_str_.append(tmp_str_buf, pos))) {
      STORAGE_LOG(WARN, "append tenant_id str into StringBuffer failed", K(ret), K(tenant_id_str_),
                  K(pos), K(tenant_id));
    }
  }

  return ret;
}

int ObSSLogKVPalfAdapter::get(const common::ObString &key, common::ObString &value)
{
  int ret = OB_SUCCESS;
  common::ObStringBuffer value_buffer(&allocator_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().get(
                 cluster_id_str_.string(), tenant_id_str_.string(), key, value_buffer))) {
    STORAGE_LOG(WARN, "get palf value failed", K(ret), KPC(this), K(key), K(value_buffer));
  } else if (value_buffer.length() > value.length()) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "the buf of value is not enough", K(ret), KPC(this), K(key), K(key), K(value),
                K(value_buffer));
  } else {
    memcpy(value.ptr(), value_buffer.ptr(), value_buffer.length());
  }

  return ret;
}

int ObSSLogKVPalfAdapter::get(const common::ObString &key, common::ObStringBuffer &value_buffer)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", KR(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().get(
                 cluster_id_str_.string(), tenant_id_str_.string(), key, value_buffer))) {
    STORAGE_LOG(WARN, "get palf value failed", KR(ret), KPC(this), K(key), K(value_buffer));
  }
  return ret;
}

int ObSSLogKVPalfAdapter::get_decrypted(const common::ObString &key, common::ObStringBuffer &value_buffer)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", KR(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().get_decrypted(
                 cluster_id_str_.string(), tenant_id_str_.string(), key, value_buffer))) {
    STORAGE_LOG(WARN, "get palf value failed", KR(ret), KPC(this), K(key), K(value_buffer));
  }
  return ret;
}

int ObSSLogKVPalfAdapter::put_encrypted(const common::ObString &key, const common::ObString &value)
{

  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", KR(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().insert_encrypted(
                 cluster_id_str_.string(), tenant_id_str_.string(), key, value))) {
    STORAGE_LOG(WARN, "insert palf key-value failed", KR(ret), KPC(this), K(key), K(value));
  }

  return ret;
}

int ObSSLogKVPalfAdapter::put(const common::ObString &key, const common::ObString &value)
{

  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().insert(
                 cluster_id_str_.string(), tenant_id_str_.string(), key, value))) {
    STORAGE_LOG(WARN, "insert palf key-value failed", K(ret), KPC(this), K(key), K(value));
  }

  return ret;
}

int ObSSLogKVPalfAdapter::set(const common::ObString &key, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().update(
                 cluster_id_str_.string(), tenant_id_str_.string(), key, value))) {
    STORAGE_LOG(WARN, "update palf key-value failed", K(ret), KPC(this), K(key), K(value));
  }

  return ret;
}

int ObSSLogKVPalfAdapter::cas(const common::ObString &key,
                              const common::ObString &old_value,
                              const common::ObString &new_value,
                              bool &expected)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().cas(cluster_id_str_.string(),
                                                           tenant_id_str_.string(), key, old_value,
                                                           new_value, &expected))) {
    STORAGE_LOG(WARN, "cas palf key-value failed", K(ret), KPC(this), K(key), K(old_value),
                K(new_value));
  }

  return ret;
}

int ObSSLogKVPalfAdapter::watch_cas(const common::ObString &key,
                                    const common::ObString &watch_key,
                                    const common::ObString &old_value,
                                    const common::ObString &new_value,
                                    bool &expected)
{
  int ret = OB_SUCCESS;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().watch_cas(
                 cluster_id_str_.string(), tenant_id_str_.string(), key, watch_key, old_value,
                 new_value, &expected))) {
    STORAGE_LOG(WARN, "watch cas key-value failed", K(ret), KPC(this), K(key), K(watch_key),
                K(old_value), K(new_value), K(expected));
  }

  return ret;
}

int ObSSLogKVPalfAdapter::delete_kv(const common::ObString &key)
{
  int ret = OB_SUCCESS;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSLogKVPalfAdapter is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(LibPalfKVInstance::get_instance().del(cluster_id_str_.string(),
                                                           tenant_id_str_.string(), key))) {
    STORAGE_LOG(WARN, "del palf key-value failed", K(ret), KPC(this), K(key));
  }
  return ret;
}

int ObSSLogKVPalfAdapter::list(const common::ObString &prefix,
                               const int32_t limit,
                               ObSSLogKVTempCache &kv_pairs)
{
  int ret = OB_SUCCESS;

  ObString cluster_id_res = cluster_id_str_.string();
  ObString tenant_id_res = tenant_id_str_.string();
  ObString prefix_res = prefix;
  libpalf::LibPalfKVIteraotr lib_palf_iter(allocator_, cluster_id_res, tenant_id_res, prefix_res);

  if (OB_FAIL(lib_palf_iter.init())) {
    STORAGE_LOG(WARN, "init palf kv iter failed", K(ret), K(prefix), KPC(this));
  } else {

    while (ret == OB_SUCCESS) {
      if (OB_FAIL(lib_palf_iter.next(kv_pairs.get_cur_key_buf(), kv_pairs.get_cur_val_buf()))) {
        STORAGE_LOG(WARN, "get next kv in the palf kv", K(ret), K(kv_pairs));
      } else if (OB_FAIL(kv_pairs.store_into_cache())) {
        STORAGE_LOG(WARN, "store kv into tmp cache failed", K(ret), K(kv_pairs));
      }
    }

    if (OB_ITER_END == ret) {
      STORAGE_LOG(INFO, "iter kv in the palf", K(ret), K(prefix), K(cluster_id_res),
                  K(tenant_id_res), K(kv_pairs));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSSLogKVPalfAdapter::get_gts(share::SCN &gts_scn)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 1 * 1000 * 1000; // 1_s
  if (!is_sys_tenant(MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    SSLOG_LOG(WARN, "unexepected tenant id", KR(ret));
  } else {
    if (OB_FAIL(ObSSLogKVIDService::get_gts(OB_SYS_TENANT_ID, this, timeout_us, gts_scn))) {
      SSLOG_LOG(WARN, "get gts for kv failed", K(ret));
    } else {
      // do nothing
    }
    SSLOG_LOG(INFO, "get gts for kv", K(ret), K(gts_scn));
  }
  return ret;
}

int ObPalfKVFirstVersionIterator::inner_get_next_kv_(ObString &key_buf, ObString &val_buf)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && OB_ISNULL(palf_iter_)) {
    char tmp_str_buf[64];
    memset(tmp_str_buf, 0, sizeof(char) * 64);
    int64_t pos = 0;

    uint64_t cluster_id = GCONF.cluster_id;
    uint64_t tenant_id = MTL_ID();

    if (OB_SUCC(ret)) {
      pos = 0;
      if (OB_FAIL(databuff_printf(tmp_str_buf, 64, pos, "%ld", cluster_id))) {
        STORAGE_LOG(WARN, "convert into str failed", K(ret), K(cluster_id), K(pos));
      } else if (OB_FAIL(cluster_id_str_.append(tmp_str_buf, pos))) {
        STORAGE_LOG(WARN, "append cluster_id str into StringBuffer failed", K(ret),
                    K(cluster_id_str_), K(pos), K(cluster_id));
      }
    }

    if (OB_SUCC(ret)) {
      pos = 0;
      if (OB_FAIL(databuff_printf(tmp_str_buf, 64, pos, "%ld", tenant_id))) {
        STORAGE_LOG(WARN, "convert into str failed", K(ret), K(tenant_id), K(pos));
      } else if (OB_FAIL(tenant_id_str_.append(tmp_str_buf, pos))) {
        STORAGE_LOG(WARN, "append tenant_id str into StringBuffer failed", K(ret),
                    K(tenant_id_str_), K(pos), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      ObString dir = cluster_id_str_.string();
      ObString sub_dir = tenant_id_str_.string();
      palf_iter_ = static_cast<libpalf::LibPalfKVIteraotr *>(
          allocator_.alloc(sizeof(libpalf::LibPalfKVIteraotr)));
      if (OB_ISNULL(palf_iter_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "alloc palf kv iterator failed", K(ret), KP(palf_iter_), K(dir),
                    K(sub_dir));
      } else {
        new (palf_iter_) libpalf::LibPalfKVIteraotr(
            allocator_, dir, sub_dir, ObPrefixFirstVersionBaseIterator::common_prefix_);
        if (OB_FAIL(palf_iter_->init())) {
          STORAGE_LOG(WARN, "init palf iterator failed", K(ret), KP(palf_iter_), K(cluster_id),
                      K(tenant_id));
          palf_iter_->~LibPalfKVIteraotr();
          allocator_.free(palf_iter_);
          palf_iter_ = nullptr;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    key_str_buf_.reuse();
    val_str_buf_.reuse();
    if (OB_FAIL(palf_iter_->next(key_str_buf_, val_str_buf_))) {
      STORAGE_LOG(WARN, "get next kv buf failed", K(ret), KP(palf_iter_), K(key_str_buf_),
                  K(val_str_buf_));
    } else {
      key_buf = key_str_buf_.string();
      val_buf = val_str_buf_.string();
    }
  }

  return ret;
}

int ObPalfKVGcVersionIterator::inner_get_next_kv_(ObString &key_buf, ObString &val_buf)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && OB_ISNULL(palf_iter_)) {
    char tmp_str_buf[64];
    memset(tmp_str_buf, 0, sizeof(char) * 64);
    int64_t pos = 0;

    uint64_t cluster_id = GCONF.cluster_id;
    uint64_t tenant_id = MTL_ID();

    if (OB_SUCC(ret)) {
      pos = 0;
      if (OB_FAIL(databuff_printf(tmp_str_buf, 64, pos, "%ld", cluster_id))) {
        STORAGE_LOG(WARN, "convert into str failed", K(ret), K(cluster_id), K(pos));
      } else if (OB_FAIL(cluster_id_str_.append(tmp_str_buf, pos))) {
        STORAGE_LOG(WARN, "append cluster_id str into StringBuffer failed", K(ret),
                    K(cluster_id_str_), K(pos), K(cluster_id));
      }
    }

    if (OB_SUCC(ret)) {
      pos = 0;
      if (OB_FAIL(databuff_printf(tmp_str_buf, 64, pos, "%ld", tenant_id))) {
        STORAGE_LOG(WARN, "convert into str failed", K(ret), K(tenant_id), K(pos));
      } else if (OB_FAIL(tenant_id_str_.append(tmp_str_buf, pos))) {
        STORAGE_LOG(WARN, "append tenant_id str into StringBuffer failed", K(ret),
                    K(tenant_id_str_), K(pos), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      ObString dir = cluster_id_str_.string();
      ObString sub_dir = tenant_id_str_.string();
      palf_iter_ = static_cast<libpalf::LibPalfKVIteraotr *>(
          allocator_.alloc(sizeof(libpalf::LibPalfKVIteraotr)));
      if (OB_ISNULL(palf_iter_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "alloc palf kv iterator failed", K(ret), KP(palf_iter_), K(dir),
                    K(sub_dir));
      } else {
        new (palf_iter_) libpalf::LibPalfKVIteraotr(allocator_, dir, sub_dir,
                                                    ObUserKeyAllVersionBaseIterator::user_key_buf_);
        if (OB_FAIL(palf_iter_->init())) {
          STORAGE_LOG(WARN, "init palf iterator failed", K(ret), KP(palf_iter_), K(cluster_id),
                      K(tenant_id));
          palf_iter_->~LibPalfKVIteraotr();
          allocator_.free(palf_iter_);
          palf_iter_ = nullptr;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    key_str_buf_.reuse();
    val_str_buf_.reuse();
    if (OB_FAIL(palf_iter_->next(key_str_buf_, val_str_buf_))) {
      STORAGE_LOG(WARN, "get next kv buf failed", K(ret), KP(palf_iter_), K(key_str_buf_),
                  K(val_str_buf_));
    } else {
      key_buf = key_str_buf_.string();
      val_buf = val_str_buf_.string();
    }
  }
  return ret;
}

int ObPalfKVGcTask::make_palf_kv_gc_task(const uint64_t tenant_id, ObPalfKVGcTask *&gc_task)
{
  int ret = OB_SUCCESS;

  void *task_buf = share::mtl_malloc(sizeof(ObPalfKVGcTask), "PALF_KV_GC_TASK");

  if (OB_ISNULL(task_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc memory failed", K(ret), KP(task_buf), KP(gc_task), K(tenant_id));
  } else {
    new (task_buf) ObPalfKVGcTask(tenant_id);
    ObPalfKVGcTask *tmp_gc_task = static_cast<ObPalfKVGcTask *>(task_buf);
    if (OB_FAIL(tmp_gc_task->get_palf_kv_adapter().init(GCONF.cluster_id, tenant_id))) {
      STORAGE_LOG(WARN, "init palf kv adapter failed", K(ret), K(tenant_id), KP(tmp_gc_task));
      tmp_gc_task->~ObPalfKVGcTask();
      share::mtl_free(tmp_gc_task);
      tmp_gc_task = nullptr;
    } else {
      gc_task = tmp_gc_task;
    }
  }

  return ret;
}

int ObPalfKVGcTask::free_palf_kv_gc_task(ObPalfKVGcTask *&gc_task)
{
  int ret = OB_SUCCESS;

  if (gc_task != nullptr) {
    gc_task->~ObPalfKVGcTask();
    share::mtl_free(gc_task);
    gc_task = nullptr;
  }

  return ret;
}

} // namespace sslog

} // namespace oceanbase
