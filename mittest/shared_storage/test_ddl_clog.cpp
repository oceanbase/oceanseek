/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <string>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "lib/utility/ob_tracepoint.h"
#include "src/storage/ob_i_table.h"
#include "src/storage/ddl/ob_ddl_merge_task.h"
#include "src/storage/ddl/ob_ddl_merge_task_utils.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/mock_ob_log_handler.h"
#include "test_ss_common_util.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/compaction_v2/ob_ss_link_major_task.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace common
{
bool is_tenant_has_sslog(const uint64_t tenant_id)
{
  return false;
}
}

namespace
{
class FailingFinishLogHandler : public MockObLogHandler
{
public:
  explicit FailingFinishLogHandler(const int error_code)
    : error_code_(error_code), append_count_(0)
  {}

  int append(const void *buffer, const int64_t nbytes, const share::SCN &ref_scn,
      const bool need_nonblock, const bool allow_compress, logservice::AppendCb *cb,
      palf::LSN &lsn, share::SCN &scn) override
  {
    UNUSEDx(buffer, nbytes, ref_scn, need_nonblock, allow_compress, cb, lsn, scn);
    ++append_count_;
    return error_code_;
  }

  int64_t get_append_count() const { return append_count_; }

private:
  int error_code_;
  int64_t append_count_;
};

class ScopedLinkMajorResult
{
public:
  explicit ScopedLinkMajorResult(const int result)
    : previous_(active_), result_(result), call_count_(0)
  {
    active_ = this;
  }
  ~ScopedLinkMajorResult() { active_ = previous_; }

  static ScopedLinkMajorResult *get_active() { return active_; }
  int invoke()
  {
    ++call_count_;
    return result_;
  }
  int64_t get_call_count() const { return call_count_; }

private:
  static thread_local ScopedLinkMajorResult *active_;
  ScopedLinkMajorResult *previous_;
  int result_;
  int64_t call_count_;
  DISALLOW_COPY_AND_ASSIGN(ScopedLinkMajorResult);
};

thread_local ScopedLinkMajorResult *ScopedLinkMajorResult::active_ = nullptr;

class ScopedNamedEvent
{
public:
  ScopedNamedEvent() : name_(nullptr), is_inited_(false) {}
  ~ScopedNamedEvent() { reset(); }

  int init(const char *name, const int error_code, const int64_t occur, const int64_t cond)
  {
    int ret = OB_SUCCESS;
    common::EventItem item;
    if (OB_ISNULL(name) || OB_SUCCESS == error_code || occur <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      item.error_code_ = error_code;
      item.occur_ = occur;
      item.trigger_freq_ = 0;
      item.cond_ = cond;
      if (OB_FAIL(common::EventTable::set_event(name, item))) {
      } else {
        name_ = name;
        is_inited_ = true;
      }
    }
    return ret;
  }

  void reset()
  {
    if (is_inited_) {
      common::EventItem item;
      (void) common::EventTable::set_event(name_, item);
      name_ = nullptr;
      is_inited_ = false;
    }
  }

private:
  const char *name_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ScopedNamedEvent);
};
}

namespace share
{
// Keep the real LINK implementation unless this test thread scripts its result.
int SSCompactHelper::link_major(const ObLSID &ls_id, const ObTabletID &tablet_id,
    const compaction::ObMergeType merge_type, const int64_t snapshot_version,
    const int64_t merge_base_version, const bool prewarm, const bool report)
{
  int ret = OB_SUCCESS;
  ScopedLinkMajorResult *script = ScopedLinkMajorResult::get_active();
  if (nullptr != script) {
    ret = script->invoke();
  } else {
    compaction::ObTabletSSLinkMajorTask task(ls_id, tablet_id, snapshot_version,
        merge_base_version, merge_type, prewarm, report);
    ret = task.process();
  }
  return ret;
}
}

class TestDDLClogCase : public ::testing::Test
{
  public:
  TestDDLClogCase();
  virtual ~TestDDLClogCase();
  static void TearDownTestCase();
  static void SetUpTestCase();
  static void create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static void prepare_ddl_finish_log(ObTabletID &tablet_id,ObDDLFinishLog &finish_log);
  static void get_test_ls_handle();
  static void mock_sstable(ObTableHandleV2 &table_handle);

  int64_t get_next_table_id() {
    static int64_t inc_val = 0;
    inc_val += 1;
    return table_id_ + inc_val;
  }

  int64_t get_next_tablet_id() {
    static int64_t inc_val = 0;
    inc_val += 1;
    return tablet_id_ + inc_val;
  }

  void prepare_schema();

  /* basic info for mack tablet */
  static const uint64_t tenant_id_ = 1;
  static const uint64_t tablet_id_ = 300000;
  static const uint64_t table_id_ = 12345;
  static const uint64_t ls_id_ = 1001;
  static const int64_t mock_snapshot_version = 100;
  static const int64_t column_count_ = ObExtendType - 1;
  static const int64_t rowkey_count_ = 8;
  static const uint64_t mock_data_format_version = DATA_VERSION_4_3_3_0;

  static const uint64_t mock_start_scn_ = 100;

  static ObArenaAllocator allocator_;
  static ObTableSchema table_schema_;
};


ObArenaAllocator TestDDLClogCase::allocator_;
ObTableSchema TestDDLClogCase::table_schema_;


TestDDLClogCase::TestDDLClogCase()
{
}
TestDDLClogCase::~TestDDLClogCase()
{
}

void TestDDLClogCase::create_ls(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;

  ObLSService *ls_svr = MTL(ObLSService*);
  bool b_exist = false;
  ObLS *ls = nullptr;
  obrpc::ObCreateLSArg create_ls_arg;
  ASSERT_NE(nullptr, ls_svr);
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, create_ls_arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(create_ls_arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(ls_id, b_exist));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());
  ls = ls_handle.get_ls();

  // set member list
  ObMemberList member_list;
  const int64_t paxos_replica_num = 1;
  (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
  GlobalLearnerList learner_list;
  ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(member_list, paxos_replica_num, learner_list));

  ObRole role;
  for (int i = 0; OB_SUCC(ret) && i < 15; i++) {
    int64_t proposal_id = 0;
    if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
      STORAGE_LOG(WARN, "failed to get role", K(ret));
    } else if (role == ObRole::LEADER) {
      break;
    }
    ::sleep(1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, role is not leader", K(ret), K(role));
  }
  return ;
}

void TestDDLClogCase::mock_sstable(ObTableHandleV2 &table_handle)
{
  void *buf = nullptr;
  ObSSTable *sstable = nullptr;
  ObTabletCreateSSTableParam param;
  ObStorageSchema storage_schema;
  ObITable::TableKey table_key;

  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  table_key.version_range_.base_version_ = mock_snapshot_version;
  table_key.version_range_.snapshot_version_ = mock_snapshot_version;

  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(ObTabletID(tablet_id_), storage_schema, 100, -1, false, false));
  ASSERT_NE(nullptr, buf = allocator_.alloc(sizeof(ObSSTable)));
  ASSERT_NE(nullptr, sstable = new(buf)ObSSTable());
  ASSERT_EQ(OB_SUCCESS, sstable->init(param, &allocator_));
  ASSERT_EQ(OB_SUCCESS, table_handle.set_sstable(sstable, &allocator_));
}

void TestDDLClogCase::prepare_ddl_finish_log(ObTabletID &tablet_id, ObDDLFinishLog &finish_log)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  mock_sstable(table_handle);
  if (table_handle.is_valid()) {
    ObSSTable *sstable = NULL;
    ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(sstable));
    int64_t buf_len = 0;
    if (sstable) {
      const uint64_t data_version = DATA_CURRENT_VERSION;
      char *buf = (char*)mtl_malloc((buf_len = sstable->get_serialize_size(data_version)), "MajorSSTable");
      if (buf) {
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, sstable->serialize(data_version, buf, buf_len, pos));
        ASSERT_EQ(OB_SUCCESS, finish_log.init(tenant_id_, ObLSID(ls_id_), sstable->key_, buf, pos, mock_data_format_version));
      } else {
        ASSERT_EQ(true, false) <<"buf is null";
      }
    }
  }
  return ;
}

void set_column_type(const common::ObObjType obj_type, share::schema::ObColumnSchemaV2 &column)
{
  ObObjMeta meta_type;
  meta_type.set_type(obj_type);
  column.set_meta_type(meta_type);
  if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
    meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
    meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_meta_type(meta_type);
  }
}

int generate_table(const uint64_t table_id, const int64_t column_count,
                    const int64_t rowkey_count, share::schema::ObTableSchema &table_schema)
{
  int ret = common::OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::schema::ObColumnSchemaV2 column;
  table_schema.reset();
  table_schema.set_table_name("test_table");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(tenant_id);
  table_schema.set_database_id(tenant_id);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_count);
  table_schema.set_max_used_column_id(column_count);
  table_schema.set_block_size(2L * 1024);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_micro_index_clustered(true);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  int64_t rowkey_pos = 0;
  for(int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    if (OB_FAIL(column.set_column_name(name))) {
      STORAGE_LOG(WARN, "set_column_name failed", K(ret));
    } else {
      set_column_type(obj_type, column);
      column.set_data_length(1);
      if (obj_type >= common::ObIntType && rowkey_pos < rowkey_count) {
        ++rowkey_pos;
        column.set_rowkey_position(rowkey_pos);
      } else {
        column.set_rowkey_position(0);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_schema.add_column(column))) {
        STORAGE_LOG(WARN, "add_column failed", K(ret), K(column));
      }
    }
  }
  return ret;
}

void TestDDLClogCase::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;

  /* mock mtl env*/
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;

  /* create ls*/
  ObLSHandle ls_handle;
  create_ls(tenant_id_, ObLSID(ls_id_), ls_handle);

  /* create tablet */
  ASSERT_EQ(OB_SUCCESS, generate_table(table_id_, column_count_, rowkey_count_, table_schema_));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ObTabletID(tablet_id_), table_schema_, allocator_));
}

void TestDDLClogCase::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestDDLClogCase, test_finish_log_write)
{
  // A successful LINK must precede a successful FINISH log submission.
  ScopedLinkMajorResult link_result(OB_SUCCESS);
  /* preppare test*/
  ObDDLFinishLog finish_log;

  int64_t new_table_id = get_next_table_id();
  ObTabletID new_tablet_id(get_next_tablet_id());
  prepare_ddl_finish_log(new_tablet_id, finish_log);

  ObLSService* ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));

  /* create new tablet to test*/
  ObTableSchema tmp_schema;
  ASSERT_EQ(OB_SUCCESS, generate_table(new_table_id, column_count_, rowkey_count_, tmp_schema));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ObTabletID(new_tablet_id), tmp_schema, allocator_));

  /* create ddl kv mgr*/
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLKVHandle ddl_kv_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(new_tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*try create */));

  /* test for local write */
  ObDDLRedoLogWriter writer;
  // MockLogHandler mock_log_handler;

  // ON_CALL(mock_log_handler, append(testing::_, testing::_, testing::_,testing::_, testing::_, testing::_,testing::_))
  // .WillByDefault(testing::Return(OB_SUCCESS));


  bool is_remote_write = false;
  ASSERT_EQ(OB_SUCCESS, writer.init(ObLSID(ls_id_), new_tablet_id));
  ASSERT_EQ(OB_SUCCESS, writer.write_finish_log(false /*disable remote write*/,finish_log, is_remote_write));
  EXPECT_EQ(1, link_result.get_call_count());
}

TEST_F(TestDDLClogCase, test_finish_log_link_failure_does_not_submit)
{
  // A failed LINK must prevent appending FINISH and handing off a callback.
  ScopedLinkMajorResult link_result(OB_TIMEOUT);
  ObDDLFinishLog finish_log;
  ObTabletID finish_tablet_id(tablet_id_);
  prepare_ddl_finish_log(finish_tablet_id, finish_log);
  FailingFinishLogHandler log_handler(OB_NOT_MASTER);
  ObDDLRedoLogWriter writer;
  ObDDLFinishLogHandle finish_handle;
  ASSERT_EQ(OB_SUCCESS, writer.init(ObLSID(ls_id_), finish_tablet_id));
  EXPECT_EQ(OB_TIMEOUT, writer.local_write_ddl_finish_log(
      finish_log, ObLSID(ls_id_), &log_handler, finish_handle));
  EXPECT_EQ(1, link_result.get_call_count());
  EXPECT_EQ(0, log_handler.get_append_count());
  EXPECT_EQ(nullptr, finish_handle.cb_);
}

TEST_F(TestDDLClogCase, test_finish_cleanup_does_not_link_again)
{
  // Cleanup after FINISH must not introduce a second fallible LINK operation.
  ScopedLinkMajorResult link_result(OB_TIMEOUT);
  ObDDLFinishLog finish_log;
  ObTabletID finish_tablet_id(tablet_id_);
  prepare_ddl_finish_log(finish_tablet_id, finish_log);
  ObDDLRedoLogWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(ObLSID(ls_id_), finish_tablet_id));
  EXPECT_EQ(OB_SUCCESS, writer.wait_finish_log(
      ObLSID(ls_id_), finish_log.get_table_key(), mock_data_format_version));
  EXPECT_EQ(0, link_result.get_call_count());
}

TEST_F(TestDDLClogCase, test_finish_log_retry_releases_failed_callback)
{
  // A FINISH callback failure must retry FINISH even after LINK has succeeded.
  ScopedLinkMajorResult link_result(OB_SUCCESS);
  ObDDLFinishLog finish_log;
  ObTabletID finish_tablet_id(tablet_id_);
  prepare_ddl_finish_log(finish_tablet_id, finish_log);

  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      finish_tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  const int64_t ref_cnt_before = tablet_handle.get_obj()->get_ref();

  {
    ScopedNamedEvent event;
    ASSERT_EQ(OB_SUCCESS, event.init(
        "ERRSIM_DDL_FINISH_CLOG_CALLBACK_FAILED", OB_NOT_MASTER, 1, finish_tablet_id.id()));
    ObDDLRedoLogWriter writer;
    bool is_remote_write = false;
    ASSERT_EQ(OB_SUCCESS, writer.init(ObLSID(ls_id_), finish_tablet_id));
    ASSERT_EQ(OB_SUCCESS, writer.write_finish_log_with_retry(
        false /*allow_remote_write*/, finish_log, is_remote_write));
    EXPECT_FALSE(is_remote_write);
    EXPECT_GE(link_result.get_call_count(), 2);
  }

  for (int64_t i = 0;
       i < 1000 && ref_cnt_before != tablet_handle.get_obj()->get_ref();
       ++i) {
    ob_usleep(1000L);
  }
  EXPECT_EQ(ref_cnt_before, tablet_handle.get_obj()->get_ref());
}

TEST_F(TestDDLClogCase, test_finish_log_replay)
{
  ObLSHandle ls_handle;
  ObDDLFinishLog finish_log;
  ObTabletHandle tablet_handle;
  ObDDLRedoLogReplayer redo_replayer;
  int64_t new_table_id = get_next_table_id();

  ObTabletID new_tablet_id(get_next_tablet_id());
  prepare_ddl_finish_log(new_tablet_id, finish_log);

  ObLSService* ls_service  = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ObTabletID(tablet_id_), tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));

  /* create new tablet to test*/
  ObTableSchema tmp_schema;
  ASSERT_EQ(OB_SUCCESS, generate_table(new_table_id, column_count_, rowkey_count_, tmp_schema));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ObTabletID(new_tablet_id), tmp_schema, allocator_));

  /* create ddl kv mgr*/
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLKVHandle ddl_kv_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(new_tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*try create */));

  share::SCN scn;
  scn.convert_from_ts(mock_start_scn_);
  ASSERT_EQ(OB_SUCCESS, redo_replayer.init(ls_handle.get_ls()));
  ASSERT_EQ(OB_SUCCESS, redo_replayer.replay_finish(finish_log, scn));
}

TEST_F(TestDDLClogCase, test_create_empty_ddl_dump_sstable)
{
  ObLSService* ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      ObTabletID(tablet_id_), tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));

  ObTabletDDLParam ddl_param;
  ddl_param.direct_load_type_ = ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DDL;
  ddl_param.ls_id_ = ObLSID(ls_id_);
  ASSERT_EQ(OB_SUCCESS, ddl_param.start_scn_.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, ddl_param.commit_scn_.convert_for_tx(200));
  ddl_param.data_format_version_ = mock_data_format_version;
  ddl_param.snapshot_version_ = 200;
  ddl_param.rec_scn_ = ddl_param.start_scn_;
  ddl_param.table_key_.table_type_ = ObITable::TableType::DDL_DUMP_SSTABLE;
  ddl_param.table_key_.tablet_id_ = ObTabletID(tablet_id_);
  ddl_param.table_key_.scn_range_.start_scn_ = ddl_param.start_scn_;
  ddl_param.table_key_.scn_range_.end_scn_ = ddl_param.commit_scn_;
  ASSERT_TRUE(ddl_param.is_valid());

  ObStorageSchema storage_schema;
  ObArray<ObDDLBlockMeta> empty_meta_array;
  ObArray<MacroBlockId> empty_macro_id_array;
  ObTableHandleV2 sstable_handle;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, ObTabletDDLUtil::create_ddl_sstable(
      *tablet_handle.get_obj(), ddl_param, empty_meta_array, empty_macro_id_array,
      nullptr/*first_ddl_sstable*/, &storage_schema, nullptr/*alloc_mutex*/, allocator_, sstable_handle));

  ObSSTable *sstable = nullptr;
  blocksstable::ObSSTableMetaHandle meta_handle;
  ASSERT_TRUE(sstable_handle.is_valid());
  ASSERT_EQ(OB_SUCCESS, sstable_handle.get_sstable(sstable));
  ASSERT_NE(nullptr, sstable);
  ASSERT_EQ(OB_SUCCESS, sstable->get_meta(meta_handle));
  ASSERT_TRUE(meta_handle.get_sstable_meta().get_table_shared_flag().is_shared_macro_blocks());
}

TEST_F(TestDDLClogCase, test_add_macro_ids_from_all_ddl_memtables)
{
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      ObTabletID(tablet_id_), tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));

  share::SCN ddl_start_scn;
  ASSERT_EQ(OB_SUCCESS, ddl_start_scn.convert_for_tx(mock_start_scn_));
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObDDLKVHandle ddl_kv_handle;
  ASSERT_NE(nullptr, t3m);
  ASSERT_EQ(OB_SUCCESS, t3m->acquire_ddl_kv(ddl_kv_handle));
  ASSERT_EQ(OB_SUCCESS, ddl_kv_handle.get_obj()->init(
      ObLSID(ls_id_), ObTabletID(tablet_id_), ddl_start_scn, mock_snapshot_version,
      ddl_start_scn, mock_data_format_version));

  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  table_key.tablet_id_ = ObTabletID(tablet_id_);
  table_key.version_range_.base_version_ = mock_snapshot_version;
  table_key.version_range_.snapshot_version_ = mock_snapshot_version;

  // The first frozen KV contains two slice memtables with 3 and 2 macro blocks.
  ObDDLMemtable *ddl_memtables[2] = { nullptr, nullptr };
  const int64_t macro_counts[2] = { 3, 2 };
  ObArray<MacroBlockId> expected_macro_ids;
  int64_t macro_seq = 0;
  for (int64_t i = 0; i < ARRAYSIZEOF(ddl_memtables); ++i) {
    table_key.slice_range_.start_slice_idx_ = i;
    table_key.slice_range_.end_slice_idx_ = i;
    ASSERT_EQ(OB_SUCCESS, ddl_kv_handle.get_obj()->create_ddl_memtable(
        *tablet_handle.get_obj(), table_key, ddl_memtables[i]));
    ASSERT_NE(nullptr, ddl_memtables[i]);
    for (int64_t j = 0; j < macro_counts[i]; ++j) {
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(++macro_seq);
      ObDDLMacroHandle macro_handle;
      ASSERT_EQ(OB_SUCCESS, macro_handle.set_block_id(macro_id));
      ASSERT_EQ(OB_SUCCESS, ddl_memtables[i]->insert_block_meta_tree(
          macro_handle, nullptr, 0 /* co_sstable_row_offset */));
      ASSERT_EQ(OB_SUCCESS, expected_macro_ids.push_back(macro_id));
    }
  }

  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  // Simulate 24 macro blocks carried by an existing DDL dump: 24 + 3 + 2 = 29.
  ObArray<MacroBlockId> carried_macro_ids;
  for (int64_t i = 0; i < 24; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(++macro_seq);
    ASSERT_EQ(OB_SUCCESS, carried_macro_ids.push_back(macro_id));
    ASSERT_EQ(OB_SUCCESS, expected_macro_ids.push_back(macro_id));
  }
  ASSERT_EQ(29, expected_macro_ids.count());

  ASSERT_EQ(OB_SUCCESS, frozen_ddl_kvs.push_back(ddl_kv_handle));
  auto verify_macro_ids = [&expected_macro_ids](
      const hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode> &actual_macro_ids) {
    ASSERT_EQ(expected_macro_ids.count(), actual_macro_ids.size());
    for (int64_t i = 0; i < expected_macro_ids.count(); ++i) {
      EXPECT_EQ(OB_HASH_EXIST, actual_macro_ids.exist_refactored(expected_macro_ids.at(i)));
    }
  };

  hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode> actual_macro_ids;
  ASSERT_EQ(OB_SUCCESS, actual_macro_ids.create(37));
  for (int64_t i = 0; i < carried_macro_ids.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, actual_macro_ids.set_refactored(carried_macro_ids.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, ObDDLMergeTaskUtils::add_macro_ids_from_ddl_kvs(
      frozen_ddl_kvs, actual_macro_ids));
  verify_macro_ids(actual_macro_ids);

  // Different replay/insertion order must produce the same complete macro set.
  ObIArray<ObDDLMemtable *> &kv_memtables = ddl_kv_handle.get_obj()->get_ddl_memtables();
  ASSERT_EQ(2, kv_memtables.count());
  ObDDLMemtable *tmp_memtable = kv_memtables.at(0);
  kv_memtables.at(0) = kv_memtables.at(1);
  kv_memtables.at(1) = tmp_memtable;
  hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode> reversed_macro_ids;
  ASSERT_EQ(OB_SUCCESS, reversed_macro_ids.create(37));
  for (int64_t i = 0; i < carried_macro_ids.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, reversed_macro_ids.set_refactored(carried_macro_ids.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, ObDDLMergeTaskUtils::add_macro_ids_from_ddl_kvs(
      frozen_ddl_kvs, reversed_macro_ids));
  verify_macro_ids(reversed_macro_ids);
}
TEST_F(TestDDLClogCase, test_finish_append_failure_releases_callback)
{
  // Synchronous timeout and role errors must release the callback's Tablet reference.
  ObDDLFinishLog finish_log;
  ObTabletID tablet_id(tablet_id_);
  prepare_ddl_finish_log(tablet_id, finish_log);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
      ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));

  const int append_errors[] = {OB_TIMEOUT, OB_NOT_MASTER};
  for (int64_t i = 0; i < ARRAYSIZEOF(append_errors); ++i) {
    ScopedLinkMajorResult link_result(OB_SUCCESS);
    FailingFinishLogHandler log_handler(append_errors[i]);
    ObDDLRedoLogWriter writer;
    ObDDLFinishLogHandle finish_handle;
    ASSERT_EQ(OB_SUCCESS, writer.init(ObLSID(ls_id_), tablet_id));
    const int64_t ref_cnt_before = tablet_handle.get_obj()->get_ref();
    EXPECT_EQ(append_errors[i], writer.local_write_ddl_finish_log(
        finish_log, ObLSID(ls_id_), &log_handler, finish_handle));
    EXPECT_EQ(1, link_result.get_call_count());
    EXPECT_EQ(1, log_handler.get_append_count());
    EXPECT_EQ(nullptr, finish_handle.cb_);
    for (int64_t retry = 0;
         retry < 1000 && ref_cnt_before != tablet_handle.get_obj()->get_ref();
         ++retry) {
      ob_usleep(1000L);
    }
    EXPECT_EQ(ref_cnt_before, tablet_handle.get_obj()->get_ref()) << append_errors[i];
  }
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -r -f ./test_ddl_clog.log*");
  OB_LOGGER.set_file_name("./test_ddl_clog.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
