/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef OB_BUILD_JNI_ODPS
#define USING_LOG_PREFIX SQL_ENG
#include "ob_odps_jni_upload_session_mgr.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObOdpsJniUploadSessionMgr::create_writer_params_map(ObIAllocator &alloc, const sql::ObODPSGeneralFormat &odps_format,
    const ObString &external_partition, bool is_overwrite, common::hash::ObHashMap<ObString, ObString> &odps_params_map)
{
  int ret = OB_SUCCESS;
  /* create one uploader and get session id */
  ObString access_id;
  ObString access_key;
  ObString project;
  ObString table;
  ObString region;
  ObString external_partition_c_style;
  ObString endpoint;
  ObString tunnel_endpoint;

  // The bundled Java tunnel writer only addresses the default schema.
  // Reject other schemas before it can open an upload on the wrong table.
  if (!odps_format.schema_.empty() && 0 != odps_format.schema_.case_compare("default")) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "ODPS JNI writes to a non-default schema; use opt_param('_use_odps_jni_connector', false)");
    LOG_WARN("ODPS JNI writer does not support non-default schema", K(ret), K(odps_format.schema_));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.access_id_, access_id, true))) {
    LOG_WARN("failed to write access id", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.access_key_, access_key, true))) {
    LOG_WARN("failed to write access key", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.project_, project, true))) {
    LOG_WARN("failed to write project", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.table_, table, true))) {
    LOG_WARN("failed to write table", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.region_, region, true))) {
    LOG_WARN("failed to write region", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, external_partition, external_partition_c_style, true))) {
    LOG_WARN("failed to write external partition", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.endpoint_, endpoint, true))) {
    LOG_WARN("failed to write endpoint", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.tunnel_endpoint_, tunnel_endpoint, true))) {
    LOG_WARN("failed to write tunnel endpoint", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (!odps_params_map.created() &&
        OB_FAIL(odps_params_map.create(MAX_PARAMS_SIZE, "IntoOdps", "IntoOdps", MTL_ID()))) {
      LOG_WARN("failed to create odps params map", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("access_id"), access_id))) {
      LOG_WARN("failed to add access id to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("access_key"), access_key))) {
      LOG_WARN("failed to add access key to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("project"), project))) {
      LOG_WARN("failed to add project to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("table_name"), table))) {
      LOG_WARN("failed to add table name to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("region"), region))) {
      LOG_WARN("failed to add region to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("odps_url"), endpoint))) {
      LOG_WARN("failed to add endpoint to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("tunnel_url"), tunnel_endpoint))) {
      LOG_WARN("failed to add tunnel endpoint to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(
                   ObString::make_string("public_access"), ObString::make_string("true")))) {
      LOG_WARN("failed to add public access to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(
                   ObString::make_string("use_epoch_offset"), ObString::make_string("true")))) {
      // Note: set this param to make java side return the date/datetime format which use
      // epoch interval day/microseconds.
      LOG_WARN("failed to add use_epoch_day to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(
                   ObString::make_string("overwrite"), ObString::make_string(is_overwrite ? "true" : "false")))) {
      LOG_WARN("failed to add overwrite to params", K(ret));
    } else { /* do nothing */
    }
  }
  if (OB_SUCC(ret)) {
    if (!external_partition.empty() &&
        OB_FAIL(odps_params_map.set_refactored(ObString::make_string("partition_spec"), external_partition_c_style))) {
      LOG_WARN("failed to add partition spec to params", K(ret));
    }
  }
  return ret;
}

int ObOdpsJniUploadSessionMgr::init_impl(
    const sql::ObODPSGeneralFormat &odps_format, const ObString &external_partition, bool is_overwrite, int64_t parallel)
{
  int ret = OB_SUCCESS;
  // This entry is first time to setup java env
  if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to env init", K(ret));
  } else if (OB_FAIL(create_writer_params_map(write_arena_alloc_,
                odps_format,
                external_partition,
                is_overwrite,
                odps_params_map_))) {
    LOG_WARN("failed to init writer params", K(ret));
  } else if (OB_FAIL(get_writer_sid(write_arena_alloc_, odps_params_map_, sid_))) {
    LOG_WARN("failed to session id of the writer");
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("session_id"), sid_))) {
    LOG_WARN("failed to add session id to params", K(ret));
  }

  // The manager owns the session holder and releases it on destruction.
  return ret;
}

int ObOdpsJniUploadSessionMgr::get_writer_sid(
    ObIAllocator &alloc, const common::hash::ObHashMap<ObString, ObString> &odps_params_map, ObString &sid)
{
  int ret = OB_SUCCESS;
  session_holder_ptr_ = create_odps_jni_writer();
  ObString tmp_sid;
  if (OB_ISNULL(session_holder_ptr_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create odps jni writer", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->init_params(odps_params_map))) {
    LOG_WARN("failed to init params", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->do_open())) {
    LOG_WARN("failed to do open", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->get_session_id(alloc, tmp_sid))) {
    LOG_WARN("failed to get session id", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, tmp_sid, sid, true))) {
    LOG_WARN("failed to write string", K(ret));
  }
  // Writer handles are released by their owning uploader or manager.
  return ret;
}

int ObOdpsJniUploadSessionMgr::get_odps_uploader_in_px(
    int task_id, OdpsUploader &odps_uploader)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init write sid should after writer params odps uploader", K(ret));
  } else {
    JNIWriterPtr writer_ptr = create_odps_jni_writer();
    if (OB_ISNULL(writer_ptr.get())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create ODPS JNI writer", K(ret));
    } else if (OB_FAIL(writer_ptr->init_params(odps_params_map_))) {
      LOG_WARN("failed to init params", K(ret));
    } else if (OB_FAIL(writer_ptr->do_open())) {
      LOG_WARN("failed to do open", K(ret));
    } else if (OB_FAIL(writer_ptr->do_open_record(task_id))) {
      LOG_WARN("failed to do open record", K(ret));
    }
    if (OB_SUCC(ret)) {
      odps_uploader.writer_ptr = writer_ptr;
    }
    // Writer handles are released by their owning uploader or manager.
  }
  return ret;
}

int ObOdpsJniUploadSessionMgr::append_block_id(long block_id) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_holder_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no session holder ", K(ret));
  } else {
    if (OB_FAIL(session_holder_ptr_->append_block_id(block_id))) {
      LOG_WARN("failed to commit ", K(ret));
    }
  }
  return ret;
}

int ObOdpsJniUploadSessionMgr::finalize_impl(bool can_commit) {
  int ret= OB_SUCCESS;
  if (!can_commit) {
    // A failed task suppresses the commit; the base class still releases resources.
  } else if (OB_ISNULL(session_holder_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no session holder ", K(ret));
  } else {
    if (OB_FAIL(session_holder_ptr_->commit_session(0))) {
      LOG_WARN("failed to commit ", K(ret));
    }
  }
  return ret;
}

void ObOdpsJniUploadSessionMgr::release_resources()
{
  // Only the last task, or SQC teardown after all workers stop, releases the holder.
  if (OB_NOT_NULL(session_holder_ptr_.get())) {
    (void)session_holder_ptr_->do_close();
    session_holder_ptr_.reset();
  }
  odps_params_map_.destroy();
  sid_.reset();
  // Task writers borrow parameter strings. Keep their storage until SQC
  // teardown, after those writers have been destroyed.
}

} // sql
} // oceanbase
#endif // OB_BUILD_JNI_ODPS
