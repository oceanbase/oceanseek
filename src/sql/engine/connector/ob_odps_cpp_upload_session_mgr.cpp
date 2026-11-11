/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef OB_BUILD_CPP_ODPS
#define USING_LOG_PREFIX SQL_ENG
#include "ob_odps_cpp_upload_session_mgr.h"
#include <odps/odps_api.h>
#include "lib/alloc/alloc_assist.h"
#include "lib/string/ob_sql_string.h"
#include "lib/worker.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObOdpsCppUploadSessionMgr::create_upload_session(const sql::ObODPSGeneralFormat &odps_format,
                                                      const ObString &external_partition,
                                                      bool is_overwrite,
                                                      apsara::odps::sdk::IUploadPtr &upload)
{
  int ret = OB_SUCCESS;
  apsara::odps::sdk::Configuration conf;
  apsara::odps::sdk::OdpsTunnel tunnel;
  const char* account_type = "";
  bool need_create_partition = false;
  try {
    conf.SetEndpoint(std::string(odps_format.endpoint_.ptr(), odps_format.endpoint_.length()));
    if (!odps_format.tunnel_endpoint_.empty()) {
      conf.SetTunnelEndpoint(std::string(odps_format.tunnel_endpoint_.ptr(), odps_format.tunnel_endpoint_.length()));
    }
    conf.SetUserAgent("OB_ACCESS_ODPS");
    conf.SetTunnelQuotaName(std::string(odps_format.quota_.ptr(), odps_format.quota_.length()));
    if (0 == odps_format.compression_code_.case_compare("zlib")) {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::ZLIB_COMPRESS);
    } else if (0 == odps_format.compression_code_.case_compare("zstd")) {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::ZSTD_COMPRESS);
    } else if (0 == odps_format.compression_code_.case_compare("lz4")) {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::LZ4_COMPRESS);
    } else if (0 == odps_format.compression_code_.case_compare("odps_lz4")) {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::ODPS_LZ4_COMPRESS);
    } else {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::NO_COMPRESS);
    }
    if (0 == odps_format.access_type_.case_compare("aliyun") ||
        odps_format.access_type_.empty()) {
      account_type = apsara::odps::sdk::ACCOUNT_ALIYUN;
    } else if (0 == odps_format.access_type_.case_compare("sts")) {
      account_type = apsara::odps::sdk::ACCOUNT_STS;
    } else if (0 == odps_format.access_type_.case_compare("app")) {
      account_type = apsara::odps::sdk::ACCOUNT_APPLICATION;
    } else if (0 == odps_format.access_type_.case_compare("token")
               || 0 == odps_format.access_type_.case_compare("domain")
               || 0 == odps_format.access_type_.case_compare("taobao")) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported access type", K(ret), K(odps_format.access_type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "this ODPS access type");
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported access type", K(ret), K(odps_format.access_type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "this ODPS access type");
    }

    if (OB_SUCC(ret)) {
      apsara::odps::sdk::Account account(std::string(account_type),
          std::string(odps_format.access_id_.ptr(), odps_format.access_id_.length()),
          std::string(odps_format.access_key_.ptr(), odps_format.access_key_.length()));
      conf.SetAccount(account);
      tunnel.Init(conf);
      try {
        upload = tunnel.CreateUpload(
            std::string(odps_format.project_.ptr(), odps_format.project_.length()),
            std::string(odps_format.table_.ptr(), odps_format.table_.length()),
            std::string(external_partition.ptr(), external_partition.length()),
            "",
            is_overwrite,
            std::string(odps_format.schema_.ptr(), odps_format.schema_.length()));
      } catch (apsara::odps::sdk::OdpsException& ex) {
        if (ex.GetErrorCode() == "NoSuchPartition") {
          need_create_partition = true;
        } else {
          throw ex;
        }
      }
    }
    if (OB_SUCC(ret) && need_create_partition) {
      apsara::odps::sdk::IODPSPtr odps = apsara::odps::sdk::IODPS::Create(
            conf, std::string(odps_format.project_.ptr(), odps_format.project_.length()));
      apsara::odps::sdk::ISQLTaskPtr sqlTaskPtr = apsara::odps::sdk::ISQLTask::Create();
      ObSqlString ddl_sql;
      ObSqlString table_name;
      if (!odps_format.schema_.empty() && 0 != odps_format.schema_.case_compare("default")) {
        std::map<std::string, std::string> hints;
        hints["odps.namespace.schema"] = "true";
        hints["odps.sql.allow.namespace.schema"] = "true";
        sqlTaskPtr->SetDefaultHints(hints);
        if (OB_FAIL(table_name.append_fmt("`%.*s`.`%.*s`.`%.*s`",
                                          odps_format.project_.length(), odps_format.project_.ptr(),
                                          odps_format.schema_.length(), odps_format.schema_.ptr(),
                                          odps_format.table_.length(), odps_format.table_.ptr()))) {
          LOG_WARN("failed to qualify ODPS table name", K(ret));
        }
      } else if (OB_FAIL(table_name.append_fmt("`%.*s`",
                                              odps_format.table_.length(), odps_format.table_.ptr()))) {
        LOG_WARN("failed to append ODPS table name", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ddl_sql.append_fmt("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%.*s);",
                                            table_name.ptr(),
                                            external_partition.length(), external_partition.ptr()))) {
        LOG_WARN("fail to append format", K(ret));
      } else {
        apsara::odps::sdk::IODPSInstancePtr instancePtr = sqlTaskPtr->Run(odps, ddl_sql.ptr());
        apsara::odps::sdk::InstanceStatus status;
        uint32_t sleepTimeMs = 0;

        while (OB_SUCC(ret) && (status = instancePtr->AcquireStatus(false)) != apsara::odps::sdk::InstanceStatus::TERMINATED) {
          if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("session is invalid", K(ret));
          } else {
            sleepTimeMs += 1000;
            ob_usleep(1000 * 1000);
          }
        }
        if (OB_SUCC(ret)) {
          instancePtr->WaitForSuccess(0);
          upload = tunnel.CreateUpload(
              std::string(odps_format.project_.ptr(), odps_format.project_.length()),
              std::string(odps_format.table_.ptr(), odps_format.table_.length()),
              std::string(external_partition.ptr(), external_partition.length()),
              "",
              is_overwrite,
              std::string(odps_format.schema_.ptr(), odps_format.schema_.length()));
        }
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when create odps upload session", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when create odps upload session", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when create odps upload session", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!(upload))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
    }
  }
  return ret;
}

int ObOdpsCppUploadSessionMgr::init_impl(const sql::ObODPSGeneralFormat &odps_format,
                                              const ObString &external_partition,
                                              bool is_overwrite,
                                              int64_t parallel)
{
  int ret = OB_SUCCESS;
  apsara::odps::sdk::IUploadPtr upload;
  apsara::odps::sdk::IRecordWriterPtr record_writer;
  void *ptr;
  OdpsUploader *uploader;
  if (!odps_mgr_map_.created() &&
             OB_FAIL(odps_mgr_map_.create(parallel, "IntoOdps"))) {
    LOG_WARN("create hash table failed", K(ret), K(parallel));
  } else  {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
    try {
      if (OB_FAIL(create_upload_session(odps_format,
                                        external_partition,
                                        is_overwrite,
                                        upload))) {
        LOG_WARN("failed to create upload session", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < parallel; i++) {
          if (OB_UNLIKELY(!(record_writer = upload->OpenWriter(i, true)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexcepted null ptr", K(ret));
          } else if (OB_ISNULL(ptr = arena_alloc_.alloc(sizeof(OdpsUploader)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate uploader", K(ret), K(sizeof(OdpsUploader)));
          } else {
            uploader = new(ptr) OdpsUploader();
            uploader->record_writer_ = record_writer;
            uploader->upload_ = upload;
          }
          if (OB_SUCC(ret)
              && OB_FAIL(odps_mgr_map_.set_refactored(i, reinterpret_cast<int64_t>(uploader)))) {
            LOG_WARN("failed to set refactored", K(ret), K(i));
            uploader->~OdpsUploader();
          }
        }
      }
    } catch (apsara::odps::sdk::OdpsException& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret));
      }
    }
  }
  return ret;
}

int ObOdpsCppUploadSessionMgr::get_odps_uploader(int64_t task_id,
                                                    apsara::odps::sdk::IUploadPtr &upload,
                                                    apsara::odps::sdk::IRecordWriterPtr &record_writer)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  OdpsUploader *uploader = NULL;
  if (OB_FAIL(odps_mgr_map_.get_refactored(task_id, value))) {
    LOG_WARN("failed to get uploader", K(ret), K(task_id));
  } else if (OB_ISNULL(uploader = reinterpret_cast<OdpsUploader *>(value))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(ret), K(value));
  } else if (OB_UNLIKELY(!uploader->upload_ || !uploader->record_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  } else {
    upload = uploader->upload_;
    record_writer = uploader->record_writer_;
  }
  return ret;
}

int ObOdpsCppUploadSessionMgr::finalize_impl(bool can_commit)
{
  int ret = OB_SUCCESS;
  std::vector<uint32_t> blocks;
  uint32_t block_id = 0;
  uint32_t task_count = static_cast<uint32_t>(odps_mgr_map_.size());
  OdpsUploader *uploader = NULL;
  try {
    for (OdpsMgrMap::iterator iter = odps_mgr_map_.begin();
         OB_SUCC(ret) && iter != odps_mgr_map_.end(); iter++) {
      if (OB_ISNULL(uploader = reinterpret_cast<OdpsUploader *>(iter->second))
          || OB_UNLIKELY(!uploader->record_writer_ || !uploader->upload_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      } else {
        uploader->record_writer_->Close();
        blocks.push_back(block_id);
        block_id++;
        // 所有线程都成功才commit
        if (block_id == task_count && can_commit) {
          uploader->upload_->Commit(blocks);
        }
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when commit", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when commit", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when commit", K(ret));
    }
  }
  return ret;
}

void ObOdpsCppUploadSessionMgr::release_resources()
{
  // The map owns placement-new uploaders, including those registered before
  // initialization failed. Release SDK objects before freeing their storage.
  if (odps_mgr_map_.created()) {
    for (OdpsMgrMap::iterator iter = odps_mgr_map_.begin();
         iter != odps_mgr_map_.end(); iter++) {
      OdpsUploader *uploader = reinterpret_cast<OdpsUploader *>(iter->second);
      if (OB_NOT_NULL(uploader)) {
        uploader->~OdpsUploader();
      }
    }
    odps_mgr_map_.destroy();
  }
  arena_alloc_.clear();
}

} // sql
} // oceanbase
#endif // OB_BUILD_CPP_ODPS
