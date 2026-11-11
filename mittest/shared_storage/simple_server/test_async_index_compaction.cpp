/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

/**
 * 基础组用例目录（按源码定义顺序；搜索完整用例名可定位实现）
 * 范围：DDL/schema、合并选表/选范围、元数据和 medium info 构造/提交。
 * 真实 DAG 执行、refresh/DML 配合及破坏性 GC 场景放在 integration 文件。
 * 共 39 个测试定义，参数展开后 52 个启用 case。
 * 默认 fixture 为 ObAsyncIndexCompactionTest；特殊参数化 fixture 如下：
 *   多索引：ObMultiAsyncIndexCompactionTest，/0、/1、/2 分别为 2、3、5 个全文索引。
 *   分区：ObPartitionAsyncIndexCompactionTest，/0=HASH 4 分区，/1=RANGE 3 分区，
 *         /2=LIST 3 分区，/3=HASH 3 分区且每分区 2 个全文索引；其余类型为单全文索引。
 * 说明依据当前调用链和断言；历史名称与当前行为不同的项单独注明。
 *
 * 01. create_async_fts_table_ddl
 *     创建带异步全文索引的堆表，检查主表的异步索引标志，以及全文索引表、删除索引表的 schema 元数据。
 * 02. async_index_tablet_and_storage_schema
 *     检查主表、全文索引和删除索引 tablet 的身份标志，以及对应 storage schema 的异步索引类型。
 * 03. async_index_compaction_schedule
 *     单个全文索引：构造 Major 和增量段，从主表调度合并，验证选定区间及注册的 medium info。
 * 04. async_index_tablet_direct_schedule_not_supported
 *     拒绝直接对全文索引 tablet 发起用户合并或主表专用调度；删除索引 tablet 仍可走普通 medium 调度入口。
 * 05. async_index_inc_major_medium_info
 *     构造多个 IncMajor 输入，验证策略选出 base=0 的区间并注册对应 medium info；当前断言验证的是 Medium/Major 输出。
 * 06. async_index_delete_percentage_medium_info
 *     检查删除比例计算，并验证删除比例触发的 Medium/Major 调度使用共同发布快照、写入正确的 medium info。
 * 07. multi_major_sstable_table_store
 *     连续安装多个 Major，验证 table store 保留多个版本且 latest major snapshot 指向最新版本。
 * 08. multi_major_async_index_compaction_schedule
 *     存在多个历史 Major 和后续增量段时，验证从最新 Major 边界选择并注册新的合并区间。
 * 09. schedule_for_async_index_invalid_handle
 *     检查未绑定有效 tablet 的调度器、无效 tablet handle，以及将索引 tablet 误传给主表入口时的错误返回。
 * 10. get_async_index_tablet_ids
 *     从主表解析异步全文索引 tablet 列表和删除索引 tablet，核对返回的数量及 ID。
 * 11. submit_medium_info_medium_major_no_inc
 *     没有 IncMajor、只有 base Major 时，验证强制 Medium/Major 合并原因仍能推进目标快照并提交 medium info。
 * 12. submit_medium_info_inc_major_merge
 *     段数未达到合并阈值时，验证普通调度返回 OB_NO_NEED_MERGE；当前并不执行 IncMajor 合并。
 * 13. collect_merge_infos_multi_major_only_latest_boundary
 *     收集合并层级信息时只使用最新 Major 边界，避免把历史 Major 计为额外合并层级。
 * 14. collect_merge_infos_stale_narrow_inc_skipped
 *     存在覆盖旧窄区间的宽 IncMajor 时，验证旧段不重复计入层级，也不会虚增段数触发合并。
 * 15. submit_medium_info_medium_major_with_inc
 *     存在 IncMajor 输入时，验证租户级 Major 选择已发布目标快照，并构造、提交对应 medium info。
 * 16. tenant_major_submit_skips_satisfied_index
 *     多个索引中，跳过已完成目标 Major 或已有目标 medium 的成员，仅提交待处理成员；重试不重复提交。
 * 17. tenant_major_submit_waits_for_unfinished_medium
 *     任一索引存在较低版本的未完成 medium 时，整批新目标返回 OB_EAGAIN，不能留下部分提交。
 * 18. choose_merge_range_inc_major
 *     验证 TENANT_MAJOR 选择 base=0、终点为共同发布快照的区间；当前断言针对 Medium/Major，而非纯 IncMajor。
 * 19. choose_merge_range_multi_index_varying_cnts
 *     四个索引具有不同增量段数时，验证合并终点受共同发布进度限制，选范围不会修改各索引发布版本。
 * 20. choose_merge_range_multi_index_min_snapshot
 *     三个索引具有不同发布进度时，验证选出的合并终点不超过最慢成员的可发布快照。
 * 21. choose_merge_range_no_need_merge
 *     在初始表状态下发起 ASYNC_INDEX_FORCE_MERGE，验证成功选出 base=0 的有效区间；历史名称与当前成功断言不同。
 * 22. inc_major_info_init
 *     用三个增量 SSTable 初始化 ObIncMajorSSTableInfo，验证有效性、条目数及起止 SCN。
 * 23. lookup_base_zero_falls_back_to_inc_inv
 *     base=0 的逻辑区间查找可回退到可见的 IncInv；提交版本不可见时拒绝返回，同区间存在 Major 时优先 Major。
 * 24. inv_sstable_upper_trans_version_from_meta_cache
 *     检查倒排 Major、已提交/未提交 IncMajor 的事务上界，并验证填充提交版本后能读到更新的元数据值。
 * 25. inc_inv_continuous_with_unfilled_gap
 *     三个逻辑相邻的 IncInv 中间段尚未填充提交版本时，验证异步索引 table store 的连续性检查仍通过。
 * 26. collect_merge_infos_skips_uncommitted_inc_inv
 *     收集合并层级及计算最大发布版本时跳过未提交 IncInv，只保留已提交 Major 的边界。
 * 27. inc_major_info_build_from_sstable_matches_table_key
 *     从 SSTable 的物理 table key 和元数据构造 IncMajorInfo，核对起止 SCN、checksum 和行数。
 * 28. inc_major_info_try_gene
 *     对 INC_MAJOR_COMPACTION 调用 try_gene_inc_major_info，验证当前路径不生成附加的 inc_major_info，也不设置包含标志。
 * 29. create_multi_async_fts_table_ddl
 *     [多索引参数化] 创建 2/3/5 个全文索引，验证辅助表数量、类型及主表关联。
 * 30. multi_async_index_compaction_schedule
 *     [多索引参数化] 对 2/3/5 个全文索引统一选择合并区间，验证各索引注册的 medium info。
 * 31. create_partition_async_fts_table_ddl
 *     [分区参数化] 验证分区主表及其全文索引、删除索引辅助表的创建和 schema 对应关系。
 * 32. partition_async_index_tablet_and_storage_schema
 *     [分区参数化] 逐分区检查主表、全文索引、删除索引的 tablet 标志和 storage schema 类型。
 * 33. partition_async_index_compaction_schedule
 *     [分区参数化] 按主表分区解析对应索引 tablet，并逐分区验证合并选范围和 medium info 注册。
 * 34. async_index_inc_major_merge_base_version
 *     大 Major 加小增量段时选择 base>0 的局部 IncMajor 合并窗口，验证输入表范围及 medium info 的 merge_base_version。
 * 35. async_index_suffix_first_prefix_orphan_blocked
 *     大中间段阻隔时先选后缀合并；后缀 medium 尚未完成时，阻止重复提交以及回头合并遗留前缀段。
 * 36. async_index_multi_inc_major_medium_info_key_order
 *     在一批 MDS 中乱序提交三个 IncMajor medium info，验证读取列表按 medium_snapshot 排序且保留对应 base version。
 * 37. async_inv_merge_selects_available_ranges
 *     表驱动检查倒排合并选表：允许缺少前后缀及区间空洞，过滤未提交/后续段，拒绝跨边界和交叉重叠等不可用范围。
 * 38. async_index_merge_selection_allows_range_holes
 *     同时检查全文索引和删除索引：物理 SCN 空洞/空区间、逻辑空洞、缺少前后缀或无增量时仍可选择现有合法输入。
 * 39. unpublished_delete_baseline_limits_group_merge
 *     共享删除索引尚未发布时以已安装Major约束组目标，选范围不改变逻辑/查询发布水位。
 */
#include "test_async_index_compaction_common.h"

const char *get_per_file_test_name()
{
  return "test_async_index_compaction_";
}

namespace oceanbase
{
namespace unittest
{

class ObMultiAsyncIndexCompactionTest
    : public ObAsyncIndexCompactionTest,
      public ::testing::WithParamInterface<int64_t>
{
public:
  static void TearDownTestCase() {}
};

class ObPartitionAsyncIndexCompactionTest
    : public ObAsyncIndexCompactionTest,
      public ::testing::WithParamInterface<PartitionTableType>
{
public:
  // The shared runner tears down the cluster after all parameterized cases.
  static void TearDownTestCase() {}
};

void ObAsyncIndexCompactionTest::create_async_fts_partition_table(
    const char *table_name, const PartitionTableType part_type)
{
  ASSERT_NO_FATAL_FAILURE(enable_async_index_flags());
  ObSqlString sql;
  switch (part_type) {
  case PART_HASH_4:
    OK(sql.assign_fmt("create table %s (c1 int, c2 varchar(256), "
                      "async fulltext index idx_ft (c2)) "
                      "organization heap partition by hash(c1) partitions 4",
                      table_name));
    break;
  case PART_RANGE_3:
    OK(sql.assign_fmt("create table %s (c1 int, c2 varchar(256), "
                      "async fulltext index idx_ft (c2)) "
                      "organization heap partition by range(c1) ("
                      "partition p0 values less than (100), "
                      "partition p1 values less than (200), "
                      "partition p2 values less than maxvalue)",
                      table_name));
    break;
  case PART_LIST_3:
    OK(sql.assign_fmt("create table %s (c1 int, c2 varchar(256), "
                      "async fulltext index idx_ft (c2)) "
                      "organization heap partition by list(c1) ("
                      "partition p0 values in (1, 2, 3), "
                      "partition p1 values in (4, 5, 6), "
                      "partition p2 values in (default))",
                      table_name));
    break;
  case PART_HASH_MULTI_IDX_3:
    OK(sql.assign_fmt("create table %s (id int, title varchar(255), content text, "
                      "async fulltext index idx_title (title), "
                      "async fulltext index idx_content (content)) "
                      "organization heap partition by hash(id) partitions 3",
                      table_name));
    break;
  default:
    FAIL() << "unknown partition table type: " << part_type;
    break;
  }
  OK(exe_sql(sql.ptr()));
}

int64_t ObAsyncIndexCompactionTest::get_expected_part_num(const PartitionTableType part_type) const
{
  int64_t part_num = 0;
  switch (part_type) {
  case PART_HASH_4:
    part_num = 4;
    break;
  case PART_RANGE_3:
  case PART_LIST_3:
  case PART_HASH_MULTI_IDX_3:
    part_num = 3;
    break;
  default:
    part_num = 0;
    break;
  }
  return part_num;
}

int64_t ObAsyncIndexCompactionTest::get_expected_fts_index_cnt(const PartitionTableType part_type) const
{
  return PART_HASH_MULTI_IDX_3 == part_type ? 2 : 1;
}

int ObAsyncIndexCompactionTest::count_aux_tables_by_index_type(
    sqlclient::ObISQLConnection &conn,
    const int64_t data_table_id,
    const int64_t index_type,
    int64_t &table_cnt)
{
  int ret = OB_SUCCESS;
  table_cnt = 0;
  ObSqlString sql;
  sql.assign_fmt("select count(*) as val from oceanbase.__all_virtual_table "
                 "where data_table_id = %ld and index_type = %ld",
                 data_table_id, index_type);
  if (OB_FAIL(SimpleServerHelper::select_int64(&conn, sql.ptr(), table_cnt))) {
    LOG_WARN("failed to count aux tables", KR(ret), K(sql));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::get_tablet_and_ls_id_by_table_name_and_part_name(
    common::ObMySQLProxy &sql_proxy,
    const char *table_name,
    const char *part_name,
    ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_ENTRY_NOT_EXIST;
  int64_t tmp_tablet_id = 0;
  int64_t tmp_ls_id = 0;
  ObSqlString sql;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < 60; ++i) {
    tmp_tablet_id = 0;
    tmp_ls_id = 0;
    sql.reuse();
    sql.assign_fmt("select p.tablet_id as val from oceanbase.__all_part p "
                   "inner join oceanbase.__all_table t on p.table_id = t.table_id "
                   "where t.table_name = '%s' and p.part_name = '%s' limit 1",
                   table_name, part_name);
    if (OB_FAIL(SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tmp_tablet_id))) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      sql.reuse();
      sql.assign_fmt("select ls_id as val from oceanbase.__all_tablet_to_ls "
                     "where tablet_id = %ld limit 1", tmp_tablet_id);
      if (OB_FAIL(SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tmp_ls_id))) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        tablet_id = ObTabletID(tmp_tablet_id);
        ls_id = ObLSID(tmp_ls_id);
        ret = OB_SUCCESS;
      }
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      sleep(1);
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::get_tablet_and_ls_id_by_table_id_and_part_name(
    common::ObMySQLProxy &sql_proxy,
    const int64_t table_id,
    const char *part_name,
    ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_ENTRY_NOT_EXIST;
  int64_t tmp_tablet_id = 0;
  int64_t tmp_ls_id = 0;
  ObSqlString sql;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < 60; ++i) {
    tmp_tablet_id = 0;
    tmp_ls_id = 0;
    sql.reuse();
    sql.assign_fmt("select tablet_id as val from oceanbase.__all_part "
                   "where table_id = %ld and part_name = '%s' limit 1",
                   table_id, part_name);
    if (OB_FAIL(SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tmp_tablet_id))) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      sql.reuse();
      sql.assign_fmt("select ls_id as val from oceanbase.__all_tablet_to_ls "
                     "where tablet_id = %ld limit 1", tmp_tablet_id);
      if (OB_FAIL(SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tmp_ls_id))) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        tablet_id = ObTabletID(tmp_tablet_id);
        ls_id = ObLSID(tmp_ls_id);
        ret = OB_SUCCESS;
      }
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      sleep(1);
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::get_aux_part_info(
    common::ObMySQLProxy &sql_proxy,
    const int64_t data_table_id,
    const int64_t index_type,
    int64_t &part_level,
    int64_t &part_num)
{
  int ret = OB_SUCCESS;
  part_level = 0;
  part_num = 0;
  ObSqlString sql;
  sql.assign_fmt("select part_level, part_num from oceanbase.__all_virtual_table "
                 "where data_table_id = %ld and index_type = %ld limit 1",
                 data_table_id, index_type);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("failed to get aux part info", KR(ret), K(sql));
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get aux part info row", KR(ret), K(sql));
      } else if (OB_FAIL(result->get_int("part_level", part_level))) {
        LOG_WARN("failed to get part_level", KR(ret));
      } else if (OB_FAIL(result->get_int("part_num", part_num))) {
        LOG_WARN("failed to get part_num", KR(ret));
      }
    }
  }
  return ret;
}

void ObAsyncIndexCompactionTest::resolve_async_index_tablet_ctx(
    const char *table_name, AsyncIndexTabletCtx &ctx)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS));
  });

  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, ctx.data_table_id_));
  ASSERT_GT(ctx.data_table_id_, 0);
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(*conn, ctx.data_table_id_, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, ctx.fts_index_table_id_));
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(*conn, ctx.data_table_id_, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, ctx.delete_index_table_id_));
  ASSERT_GT(ctx.fts_index_table_id_, 0);
  ASSERT_GT(ctx.delete_index_table_id_, 0);

  ObLSID fts_ls_id;
  ObLSID delete_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(sql_proxy, ctx.data_table_id_, ctx.data_tablet_id_, ctx.ls_id_));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(sql_proxy, ctx.fts_index_table_id_, ctx.fts_index_tablet_id_, fts_ls_id));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(sql_proxy, ctx.delete_index_table_id_, ctx.delete_index_tablet_id_, delete_ls_id));
  ASSERT_EQ(ctx.ls_id_, fts_ls_id);
  ASSERT_EQ(ctx.ls_id_, delete_ls_id);
  remove_async_index_refresh_produced_sstables(ctx);
  LOG_INFO("resolve async index tablet ctx", K(table_name), K(ctx));
}

void ObAsyncIndexCompactionTest::resolve_partition_multi_async_index_tablet_ctx(
    const char *table_name, const char *part_name, MultiAsyncIndexTabletCtx &ctx)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS));
  });

  ctx.fts_index_table_ids_.reuse();
  ctx.fts_index_tablet_ids_.reuse();
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, ctx.data_table_id_));
  ASSERT_GT(ctx.data_table_id_, 0);
  ASSERT_EQ(OB_SUCCESS, get_fts_index_aux_table_ids(sql_proxy, ctx.data_table_id_, ctx.fts_index_table_ids_));
  ASSERT_GT(ctx.fts_index_table_ids_.count(), 0);
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(*conn, ctx.data_table_id_, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, ctx.delete_index_table_id_));
  ASSERT_GT(ctx.delete_index_table_id_, 0);

  ObLSID delete_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_name_and_part_name(
      sql_proxy, table_name, part_name, ctx.data_tablet_id_, ctx.ls_id_));
  for (int64_t i = 0; i < ctx.fts_index_table_ids_.count(); ++i) {
    ObTabletID fts_tablet_id;
    ObLSID fts_ls_id;
    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id_and_part_name(
        sql_proxy, ctx.fts_index_table_ids_.at(i), part_name, fts_tablet_id, fts_ls_id));
    ASSERT_EQ(ctx.ls_id_, fts_ls_id);
    ASSERT_EQ(OB_SUCCESS, ctx.fts_index_tablet_ids_.push_back(fts_tablet_id));
  }
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id_and_part_name(
      sql_proxy, ctx.delete_index_table_id_, part_name, ctx.delete_index_tablet_id_, delete_ls_id));
  ASSERT_EQ(ctx.ls_id_, delete_ls_id);
  remove_async_index_refresh_produced_sstables(ctx);
  LOG_INFO("resolve partition multi async index tablet ctx", K(table_name), K(part_name), K(ctx));
}

void ObAsyncIndexCompactionTest::check_multi_async_fts_table_ddl(
    const char *table_name, const int64_t fts_index_cnt)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS));
  });

  int64_t data_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, data_table_id));
  ASSERT_GT(data_table_id, 0);

  int64_t has_async_index_flag = 0;
  ObSqlString sql;
  sql.assign_fmt("select (table_flags & %ld) as val from oceanbase.__all_virtual_table "
                 "where table_id = %ld",
                 HAS_ASYNC_INDEX_FLAG, data_table_id);
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn, sql.ptr(), has_async_index_flag));
  ASSERT_NE(0, has_async_index_flag);

  int64_t fts_index_cnt_in_schema = 0;
  int64_t delete_index_cnt_in_schema = 0;
  ASSERT_EQ(OB_SUCCESS, count_aux_tables_by_index_type(
      *conn, data_table_id, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, fts_index_cnt_in_schema));
  ASSERT_EQ(OB_SUCCESS, count_aux_tables_by_index_type(
      *conn, data_table_id, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, delete_index_cnt_in_schema));
  ASSERT_EQ(fts_index_cnt, fts_index_cnt_in_schema);
  ASSERT_EQ(1, delete_index_cnt_in_schema);

  ObSEArray<int64_t, 8> fts_index_table_ids;
  ASSERT_EQ(OB_SUCCESS, get_fts_index_aux_table_ids(sql_proxy, data_table_id, fts_index_table_ids));
  ASSERT_EQ(fts_index_cnt, fts_index_table_ids.count());
  LOG_INFO("multi async fts table created",
           K(table_name), K(data_table_id), K(fts_index_cnt), K(fts_index_table_ids));
}

void ObAsyncIndexCompactionTest::check_partition_async_fts_table_ddl(
    const char *table_name, const PartitionTableType part_type)
{
  const int64_t fts_index_cnt = get_expected_fts_index_cnt(part_type);
  const int64_t expected_part_num = get_expected_part_num(part_type);
  check_multi_async_fts_table_ddl(table_name, fts_index_cnt);

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS));
  });

  int64_t data_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, data_table_id));
  ASSERT_GT(data_table_id, 0);

  int64_t fts_part_level = 0;
  int64_t fts_part_num = 0;
  int64_t delete_part_level = 0;
  int64_t delete_part_num = 0;
  ASSERT_EQ(OB_SUCCESS, get_aux_part_info(
      sql_proxy, data_table_id, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, fts_part_level, fts_part_num));
  ASSERT_EQ(OB_SUCCESS, get_aux_part_info(
      sql_proxy, data_table_id, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, delete_part_level, delete_part_num));
  ASSERT_EQ(1, fts_part_level);
  ASSERT_EQ(expected_part_num, fts_part_num);
  ASSERT_EQ(1, delete_part_level);
  ASSERT_EQ(expected_part_num, delete_part_num);
  LOG_INFO("partition async fts table created",
           K(table_name), K(part_type), K(data_table_id), K(fts_index_cnt), K(expected_part_num));
}

void ObAsyncIndexCompactionTest::run_async_index_compaction_schedule(MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_index_tablet_id, mock_major_snapshot));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    const int64_t major_snapshot = fts_index_handle.get_obj()->get_last_major_snapshot_version();
    ASSERT_EQ(mock_major_snapshot, major_snapshot);
    ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
        ls_handle, fts_index_tablet_id, major_snapshot,
        DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
  }

  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));

  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(run_ctx_.tenant_id_, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(fts_index_cnt, index_tablet_ids.count());
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);

  ASSERT_NO_FATAL_FAILURE(prepare_delete_index_publish_for_merge(ctx, ls_handle));

  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));
  const int64_t expected_schedule_snapshot = calc_schedule_snapshot_version(mock_major_snapshot);
  ASSERT_GT(merge_version_range.snapshot_version_, mock_major_snapshot);
  ASSERT_GT(expected_schedule_snapshot, merge_version_range.snapshot_version_);
  ASSERT_EQ(0, merge_version_range.base_version_);

  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ASSERT_NE(nullptr, GCTX.sql_proxy_);

  ensure_ls_reserved_snapshot_ready(ctx.ls_id_);

  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));

  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, &schedule_cnt,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_tablet_handle));
  const int schedule_ret = schedule_func.schedule_async_index();
  ASSERT_TRUE(OB_SUCCESS == schedule_ret || OB_NO_NEED_MERGE == schedule_ret);
  ASSERT_EQ(OB_SUCCESS == schedule_ret ? fts_index_cnt : 0, schedule_cnt.submit_clog_cnt_);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    ObTabletHandle reload_fts_handle;
    const ObMediumCompactionInfoList *registered_medium_list = nullptr;
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
    ASSERT_NE(nullptr, registered_medium_list);

    const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
        registered_medium_list, merge_version_range.snapshot_version_);
    ASSERT_NE(nullptr, actual_info) << "fts index tablet index=" << i;
    ASSERT_EQ(merge_version_range.snapshot_version_, actual_info->medium_snapshot_);
    verify_medium_info_medium_major(*actual_info);
  }
  LOG_INFO("multi async index compaction schedule verified",
           K(fts_index_cnt), K(merge_version_range), K(schedule_cnt), K(ctx));
}

void ObAsyncIndexCompactionTest::assert_async_index_version_range_cnt_at_least(
    const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t expect_min)
{
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSEArray<ObNewVersionRange, 16> version_ranges;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());
  ASSERT_EQ(OB_SUCCESS, table_store_wrapper.get_member()->get_async_index_sstable_version_ranges(*tablet_handle.get_obj(), version_ranges));
  ASSERT_GE(version_ranges.count(), expect_min);
}

int ObAsyncIndexCompactionTest::count_inc_major_segments_with_base_lt(
    const ObTablet &tablet, const int64_t base_upper_bound, int64_t &cnt)
{
  int ret = OB_SUCCESS;
  cnt = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret));
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is null", KR(ret));
  } else {
    const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_majors.count(); ++i) {
      ObITable *table = inc_majors.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc major table is null", KR(ret), K(i));
      } else if (static_cast<ObSSTable *>(table)->get_async_index_base_version() < base_upper_bound) {
        ++cnt;
      }
    }
  }
  return ret;
}

void ObAsyncIndexCompactionTest::run_multi_major_async_index_compaction_schedule(
    MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t mock_second_major_snapshot = calc_schedule_snapshot_version(mock_major_snapshot);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    assert_major_sstable_cnt_at_least(ctx.ls_id_, fts_index_tablet_id, 1);

    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_index_tablet_id, mock_major_snapshot));
    assert_major_sstable_cnt_at_least(ctx.ls_id_, fts_index_tablet_id, 2);

    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_index_tablet_id, mock_second_major_snapshot));
    assert_major_sstable_cnt_at_least(ctx.ls_id_, fts_index_tablet_id, 3);
    assert_async_index_version_range_cnt_at_least(ctx.ls_id_, fts_index_tablet_id, 3);

    ObTabletHandle fts_index_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    const int64_t latest_major = fts_index_handle.get_obj()->get_last_major_snapshot_version();
    ASSERT_EQ(mock_second_major_snapshot, latest_major);
    ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
        ls_handle, fts_index_tablet_id, latest_major,
        DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));

    int64_t max_published_version = 0;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->get_async_index_max_published_version(max_published_version));
    ASSERT_GT(max_published_version, mock_second_major_snapshot);
  }

  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));

  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(run_ctx_.tenant_id_, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(fts_index_cnt, index_tablet_ids.count());
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);

  ASSERT_NO_FATAL_FAILURE(prepare_delete_index_publish_for_merge(ctx, ls_handle));

  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));
  const int64_t expected_schedule_snapshot = calc_schedule_snapshot_version(mock_second_major_snapshot);
  ASSERT_GT(merge_version_range.snapshot_version_, mock_second_major_snapshot);
  ASSERT_GT(expected_schedule_snapshot, merge_version_range.snapshot_version_);
  ASSERT_EQ(0, merge_version_range.base_version_);

  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);

  ensure_ls_reserved_snapshot_ready(ctx.ls_id_);

  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));

  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, &schedule_cnt,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_tablet_handle));
  const int schedule_ret = schedule_func.schedule_async_index();
  ASSERT_TRUE(OB_SUCCESS == schedule_ret || OB_NO_NEED_MERGE == schedule_ret);
  ASSERT_EQ(OB_SUCCESS == schedule_ret ? fts_index_cnt : 0, schedule_cnt.submit_clog_cnt_);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    ObTabletHandle reload_fts_handle;
    const ObMediumCompactionInfoList *registered_medium_list = nullptr;
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
    ASSERT_NE(nullptr, registered_medium_list);

    const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
        registered_medium_list, merge_version_range.snapshot_version_);
    ASSERT_NE(nullptr, actual_info) << "fts index tablet index=" << i;
    ASSERT_EQ(merge_version_range.snapshot_version_, actual_info->medium_snapshot_);
    verify_medium_info_medium_major(*actual_info);
  }
  LOG_INFO("multi major async index compaction schedule verified",
           K(fts_index_cnt), K(merge_version_range), K(schedule_cnt), K(ctx));
}

void ObAsyncIndexCompactionTest::check_storage_schema_async_index_flags(
    const ObTablet &tablet,
    const bool expect_is_async_index,
    const bool expect_is_async_delete_index,
    const ObIndexType expect_index_type)
{
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet.load_storage_schema(allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(expect_is_async_index, storage_schema->is_async_index());
  ASSERT_EQ(expect_is_async_delete_index, storage_schema->is_async_delete_index());
  ASSERT_EQ(expect_index_type, storage_schema->index_type_);
}

void ObAsyncIndexCompactionTest::verify_collect_merge_infos(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t schedule_snapshot_version,
    const int64_t expected_tier_cnt,
    const ObAsyncIndexSSTableMergeInfo *expected_tiers,
    const int64_t expected_tier_spec_cnt)
{
  ObTabletHandle tablet_handle;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_handle.get_ls()->get_ls_id(), tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::collect_merge_infos(
      *ls_handle.get_ls(), *tablet_handle.get_obj(), schedule_snapshot_version, merge_infos));
  ASSERT_EQ(expected_tier_cnt, merge_infos.count());
  if (nullptr != expected_tiers) {
    ASSERT_EQ(expected_tier_cnt, expected_tier_spec_cnt);
    for (int64_t i = 0; i < expected_tier_spec_cnt; ++i) {
      ASSERT_EQ(expected_tiers[i].start_snapshot_version_, merge_infos.at(i).start_snapshot_version_)
          << "tier " << i << " start mismatch";
      ASSERT_EQ(expected_tiers[i].end_snapshot_version_, merge_infos.at(i).end_snapshot_version_)
          << "tier " << i << " end mismatch";
    }
  }
}

int ObAsyncIndexCompactionTest::mock_inc_major_sstable_with_meta(
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t data_checksum,
    const int64_t row_count,
    ObSSTable *&sstable,
    const bool committed)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  sstable = nullptr;
  const ObTabletID tablet_id(50001);
  ObTabletCreateSSTableParam param;
  param.table_key_.tablet_id_ = tablet_id;
  param.table_key_.table_type_ = ObITable::TableType::INC_INV_INDEX_SSTABLE;
  param.table_key_.scn_range_.start_scn_.convert_for_tx(base_version);
  param.table_key_.scn_range_.end_scn_.convert_for_tx(snapshot_version);
  param.min_merged_trans_version_ = base_version;
  param.max_merged_trans_version_ = snapshot_version;
  param.upper_trans_version_ = committed ? snapshot_version : INT64_MAX;
  param.schema_version_ = ObTimeUtility::current_time();
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;
  param.index_type_ = INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
  param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
  param.rowkey_column_cnt_ = 2;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = data_checksum;
  param.occupy_size_ = 1000;
  param.original_size_ = 1000;
  param.ddl_scn_.set_base();
  param.filled_tx_scn_ = param.table_key_.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_ = SCN::plus(param.table_key_.get_start_scn(), 1);
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = row_count;
  param.recycle_version_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
  param.full_column_cnt_ = 0;
  param.is_co_table_without_cgs_ = false;
  param.co_base_snapshot_version_ = 0;
  param.column_cnt_ = param.rowkey_column_cnt_;
  compaction::ObUncommitTxDesc tx_desc(100, 200);
  param.uncommit_tx_info_.push_back(tx_desc);
  param.uncommit_tx_info_.set_info_status_record_seq();
  ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_);
  ObInvIdxStatsMeta inv_stats;
  inv_stats.index_type_ = INV_IDX_TYPE_FTS;
  inv_stats.has_relevance_params_ = true;
  inv_stats.has_position_list_ = false;
  inv_stats.has_nested_key_ = false;
  inv_stats.doc_id_obj_meta_.set_uint64();
  inv_stats.min_doc_id_ = common::OB_INVALID_ID;
  inv_stats.max_doc_id_ = 0;
  if (OB_FAIL(param.inv_meta_.init(inv_stats))) {
    LOG_WARN("failed to initialize inverted sstable metadata", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObInvSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sstable", K(ret));
  } else if (OB_ISNULL(sstable = new (buf) ObInvSSTable())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(sstable->init(param, &allocator_))) {
    LOG_WARN("failed to init inc major sstable", KR(ret));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::add_sstable_to_handle(ObTablesHandleArray &handle, ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(table_handle.set_sstable(sstable, &allocator_))) {
    LOG_WARN("failed to set sstable handle", KR(ret));
  } else if (OB_FAIL(handle.add_table(table_handle))) {
    LOG_WARN("failed to add table to handle", KR(ret));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::build_inc_major_table_array(
    const int64_t seg_cnt,
    const int64_t major_snapshot,
    const int64_t step,
    ObSEArray<ObITable *, 8> &tables)
{
  int ret = OB_SUCCESS;
  tables.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < seg_cnt; ++i) {
    ObSSTable *sstable = nullptr;
    const int64_t base = major_snapshot + i * step;
    const int64_t snap = base + step;
    if (OB_FAIL(mock_inc_major_sstable_with_meta(base, snap, 1000 + i, 100 + i, sstable))) {
      LOG_WARN("failed to mock inc major sstable", KR(ret), K(i));
    } else if (OB_FAIL(tables.push_back(sstable))) {
      LOG_WARN("failed to push inc major table", KR(ret), K(i));
    }
  }
  return ret;
}

void ObAsyncIndexCompactionTest::run_choose_merge_range_tenant_major(MultiAsyncIndexTabletCtx &ctx)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ObTabletHandle fts_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_TRUE(fts_handle.get_obj()->is_async_index_refresh_target());

  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());
  ObSEArray<ObNewVersionRange, 8> sstable_version_ranges;
  ASSERT_EQ(OB_SUCCESS, table_store_wrapper.get_member()->get_async_index_sstable_version_ranges(*fts_handle.get_obj(), sstable_version_ranges));
  ASSERT_GT(sstable_version_ranges.count(), 0);
  const int64_t major_snapshot = fts_handle.get_obj()->get_last_major_snapshot_version();
  ASSERT_EQ(mock_major_snapshot, major_snapshot);

  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, major_snapshot,
      DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
  verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, DEFAULT_INC_MAJOR_CNT);

  int64_t max_published_version = 0;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->get_async_index_max_published_version(max_published_version));
  ASSERT_EQ(major_snapshot + DEFAULT_INC_MAJOR_CNT * DEFAULT_INC_MAJOR_STEP, max_published_version);

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  int64_t common_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(
      *ls_handle.get_ls(), index_tablet_ids,
      delete_index_tablet_id, common_published_snapshot));
  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, async_index_params, merge_version_range));
  ASSERT_TRUE(merge_version_range.is_valid());
  ASSERT_EQ(0, merge_version_range.base_version_);
  ASSERT_EQ(common_published_snapshot, merge_version_range.snapshot_version_);
  ASSERT_LE(common_published_snapshot, max_published_version);
}

void ObAsyncIndexCompactionTest::run_choose_merge_range_no_need_merge(MultiAsyncIndexTabletCtx &ctx)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));

  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_GT(index_tablet_ids.count(), 0);

  ObNewVersionRange merge_version_range;
  const int ret = ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, async_index_params, merge_version_range);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, merge_version_range.base_version_);
  ASSERT_GT(merge_version_range.snapshot_version_, 0);
}

void ObAsyncIndexCompactionTest::run_choose_merge_range_multi_index_varying_cnts(const int64_t fts_index_cnt)
{
  static const int64_t INC_MAJOR_CNTS[] = {11, 15, 12, 14};
  ASSERT_GE(fts_index_cnt, 1);
  ASSERT_LE(fts_index_cnt, 4);

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_merge_vary_%ld", fts_index_cnt));
  create_async_fts_table_with_index_cnt(table_name.ptr(), fts_index_cnt);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name.ptr(), ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ObSEArray<int64_t, 8> max_published_versions;
  int64_t min_max_published_version = INT64_MAX;
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle index_handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, index_handle));
    ASSERT_TRUE(index_handle.get_obj()->is_async_index_refresh_target());
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, index_handle));
    const int64_t major_snapshot = index_handle.get_obj()->get_last_major_snapshot_version();
    ASSERT_GT(major_snapshot, 0);
    ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
        ls_handle, fts_tablet_id, major_snapshot,
        INC_MAJOR_CNTS[i], DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
    verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, INC_MAJOR_CNTS[i]);
    const int64_t max_published = major_snapshot + INC_MAJOR_CNTS[i] * DEFAULT_INC_MAJOR_STEP;
    ASSERT_EQ(OB_SUCCESS, max_published_versions.push_back(max_published));
    min_max_published_version = MIN(min_max_published_version, max_published);
  }

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(fts_index_cnt, index_tablet_ids.count());

  int64_t common_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(
      *ls_handle.get_ls(), index_tablet_ids,
      delete_index_tablet_id, common_published_snapshot));
  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, async_index_params, merge_version_range));
  ASSERT_TRUE(merge_version_range.is_valid());
  ASSERT_EQ(0, merge_version_range.base_version_);
  ASSERT_EQ(common_published_snapshot, merge_version_range.snapshot_version_);
  ASSERT_LE(common_published_snapshot, min_max_published_version);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    int64_t published_version = 0;
    ObTabletHandle reload_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(index_tablet_ids.at(i), reload_handle));
    ASSERT_EQ(OB_SUCCESS, reload_handle.get_obj()->get_async_index_max_published_version(published_version));
    ASSERT_EQ(max_published_versions.at(i), published_version);
  }
}

void ObAsyncIndexCompactionTest::run_choose_merge_range_multi_index_min_snapshot(const int64_t fts_index_cnt)
{
  static const int64_t INC_MAJOR_CNTS[] = {12, 14, 14};
  ASSERT_GE(fts_index_cnt, 1);
  ASSERT_LE(fts_index_cnt, 3);

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_merge_min_%ld", fts_index_cnt));
  create_async_fts_table_with_index_cnt(table_name.ptr(), fts_index_cnt);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name.ptr(), ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  int64_t min_max_published_version = INT64_MAX;
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle index_handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, index_handle));
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, index_handle));
    const int64_t major_snapshot = index_handle.get_obj()->get_last_major_snapshot_version();
    ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
        ls_handle, fts_tablet_id, major_snapshot,
        INC_MAJOR_CNTS[i], DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
    verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, INC_MAJOR_CNTS[i]);
    min_max_published_version = MIN(min_max_published_version,
        major_snapshot + INC_MAJOR_CNTS[i] * DEFAULT_INC_MAJOR_STEP);
  }

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  int64_t common_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(
      *ls_handle.get_ls(), index_tablet_ids,
      delete_index_tablet_id, common_published_snapshot));
  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, async_index_params, merge_version_range));
  ASSERT_TRUE(merge_version_range.is_valid());
  ASSERT_EQ(0, merge_version_range.base_version_);
  ASSERT_EQ(common_published_snapshot, merge_version_range.snapshot_version_);
  ASSERT_LE(common_published_snapshot, min_max_published_version);
}

void ObAsyncIndexCompactionTest::fill_inc_major_info_if_needed_(
    ObMediumCompactionInfo &medium_info,
    const ObNewVersionRange &merge_version_range,
    ObLS &ls,
    const ObTablet &tablet)
{
  if (medium_info.contain_inc_major_info_) {
    return;
  }
  ObGetMergeTablesParam merge_param;
  merge_param.merge_type_ = INC_MAJOR_MERGE;
  merge_param.merge_version_ = merge_version_range.snapshot_version_;
  merge_param.merge_base_version_ = merge_version_range.base_version_;
  ASSERT_TRUE(merge_param.is_valid());

  ObGetMergeTablesResult merge_tables_result;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::get_inc_major_merge_tables(
      merge_param, ls, tablet, merge_tables_result));
  ObSEArray<ObITable *, 16> inc_tables;
  ASSERT_EQ(OB_SUCCESS, merge_tables_result.handle_.get_inc_major_tables(inc_tables));
  ASSERT_FALSE(inc_tables.empty());
  ASSERT_EQ(OB_SUCCESS, medium_info.inc_major_info_.init(
      allocator_, merge_version_range.snapshot_version_, inc_tables));
  medium_info.contain_inc_major_info_ = medium_info.inc_major_info_.is_valid();
  ASSERT_TRUE(medium_info.contain_inc_major_info_);
}

void ObAsyncIndexCompactionTest::submit_cloned_inc_major_medium_info_batch_(
    ObLSHandle &ls_handle,
    const ObTabletID &index_tablet_id,
    const ObMediumCompactionInfo &template_info,
    const int64_t *medium_snapshots,
    const int64_t *merge_base_versions,
    const int64_t *submit_order,
    const int64_t item_cnt,
    const int64_t tx_id)
{
  ObTabletMediumCompactionInfoMdsArg mds_arg;
  ensure_ls_reserved_snapshot_ready(ls_handle.get_ls()->get_ls_id());
  mds_arg.ls_id_ = ls_handle.get_ls()->get_ls_id();
  for (int64_t i = 0; i < item_cnt; ++i) {
    const int64_t idx = submit_order[i];
    ObMediumCompactionInfo medium_info(allocator_);
    ASSERT_EQ(OB_SUCCESS, medium_info.init(allocator_, template_info));
    medium_info.medium_snapshot_ = medium_snapshots[idx];
    medium_info.merge_base_version_ = merge_base_versions[idx];
    ASSERT_TRUE(medium_info.is_valid());
    ASSERT_TRUE(medium_info.is_inc_major_compaction());
    ASSERT_EQ(OB_SUCCESS, mds_arg.add_item(index_tablet_id, medium_info));
  }
  ASSERT_EQ(OB_SUCCESS, register_mds_in_transaction(mds_arg, tx_id));
}

void ObAsyncIndexCompactionTest::assert_medium_info_list_sorted_by_key_(
    const ObMediumCompactionInfoList *medium_list,
    const int64_t *expected_merge_base_versions,
    const int64_t *expected_medium_snapshots,
    const int64_t expected_cnt) const
{
  ASSERT_NE(nullptr, medium_list);
  ASSERT_NE(nullptr, expected_merge_base_versions);
  ASSERT_NE(nullptr, expected_medium_snapshots);
  ASSERT_GT(expected_cnt, 0);

  ObSEArray<int64_t, 4> actual_medium_snapshots;
  ObSEArray<int64_t, 4> actual_merge_base_versions;
  DLIST_FOREACH_NORET(info, medium_list->get_list()) {
    ASSERT_NE(nullptr, info);
    ASSERT_TRUE(info->is_inc_major_compaction());
    ASSERT_EQ(ObMediumCompactionInfo::INC_MAJOR_COMPACTION, info->compaction_type_);
    const int64_t medium_snapshot = info->medium_snapshot_;
    const int64_t merge_base_version = info->merge_base_version_;
    if (actual_medium_snapshots.empty()
        || actual_medium_snapshots.at(actual_medium_snapshots.count() - 1) != medium_snapshot) {
      ASSERT_EQ(OB_SUCCESS, actual_medium_snapshots.push_back(medium_snapshot));
      ASSERT_EQ(OB_SUCCESS, actual_merge_base_versions.push_back(merge_base_version));
    }
  }
  ASSERT_EQ(expected_cnt, actual_medium_snapshots.count());
  for (int64_t i = 1; i < actual_medium_snapshots.count(); ++i) {
    ASSERT_LT(actual_medium_snapshots.at(i - 1), actual_medium_snapshots.at(i));
  }
  for (int64_t i = 0; i < expected_cnt; ++i) {
    bool found = false;
    for (int64_t j = 0; j < actual_medium_snapshots.count(); ++j) {
      if (expected_medium_snapshots[i] == actual_medium_snapshots.at(j)
          && expected_merge_base_versions[i] == actual_merge_base_versions.at(j)) {
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found) << "missing medium info idx=" << i
                       << " medium_snapshot=" << expected_medium_snapshots[i]
                       << " merge_base_version=" << expected_merge_base_versions[i];
  }
}

void ObAsyncIndexCompactionTest::run_async_index_inc_major_merge_base_version(
    MultiAsyncIndexTabletCtx &ctx)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);

  // Large base major + many small inc majors: policy merges a partial inc-major window
  // instead of folding from snapshot 0, so base_version_ (merge_base_version_) > 0.
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, fts_tablet_id, mock_major_snapshot,
      100, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, LARGE_MAJOR_OCCUPY_SIZE));

  ObTabletHandle fts_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  const int64_t major_snapshot = fts_handle.get_obj()->get_last_major_snapshot_version();
  ASSERT_EQ(mock_major_snapshot, major_snapshot);
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, major_snapshot,
      DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
  verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, DEFAULT_INC_MAJOR_CNT);

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  ASSERT_NO_FATAL_FAILURE(prepare_delete_index_publish_for_merge(ctx, ls_handle));

  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));
  ASSERT_GT(merge_version_range.base_version_, 0);
  ASSERT_EQ(major_snapshot, merge_version_range.base_version_);
  ASSERT_GT(merge_version_range.snapshot_version_, merge_version_range.base_version_);
  ASSERT_GT(calc_schedule_snapshot_version(major_snapshot), merge_version_range.snapshot_version_);

  ObGetMergeTablesParam merge_param;
  merge_param.merge_type_ = INC_MAJOR_MERGE;
  merge_param.merge_version_ = merge_version_range.snapshot_version_;
  merge_param.merge_base_version_ = merge_version_range.base_version_;
  ASSERT_TRUE(merge_param.is_valid());

  ObGetMergeTablesResult merge_tables_result;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::get_inc_major_merge_tables(
      merge_param, *ls_handle.get_ls(), *fts_handle.get_obj(), merge_tables_result));
  ASSERT_FALSE(merge_tables_result.handle_.empty());
  ASSERT_EQ(merge_version_range.base_version_, merge_tables_result.version_range_.base_version_);
  ASSERT_EQ(merge_version_range.snapshot_version_, merge_tables_result.version_range_.snapshot_version_);
  for (int64_t i = 0; i < merge_tables_result.handle_.get_count(); ++i) {
    const ObSSTable *sstable = static_cast<const ObSSTable *>(merge_tables_result.handle_.get_table(i));
    ASSERT_NE(nullptr, sstable);
    ASSERT_TRUE(sstable->is_inc_major_type_sstable());
    ObNewVersionRange table_range;
    ASSERT_EQ(OB_SUCCESS, sstable->get_async_index_version_range(table_range));
    ASSERT_GE(table_range.base_version_, merge_version_range.base_version_);
    ASSERT_LE(table_range.snapshot_version_, merge_version_range.snapshot_version_);
  }

  ensure_ls_reserved_snapshot_ready(ctx.ls_id_);
  SCN weak_read_scn;
  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(6000 + 4201));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ObMediumCompactionScheduleFunc index_schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, nullptr,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.init_tablet_handle(fts_handle));

  ObMediumCompactionInfo medium_info(allocator_);
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.build_async_index_medium_info(merge_version_range, medium_info));
  ASSERT_TRUE(medium_info.is_inc_major_compaction());
  ASSERT_EQ(ObMediumCompactionInfo::INC_MAJOR_COMPACTION, medium_info.compaction_type_);
  ASSERT_EQ(merge_version_range.base_version_, medium_info.merge_base_version_);

  LOG_INFO("async index partial inc major merge start version verified",
           K(merge_version_range), "merge_table_cnt", merge_tables_result.handle_.get_count());
}

void ObAsyncIndexCompactionTest::run_async_index_suffix_first_prefix_orphan_blocked(
    MultiAsyncIndexTabletCtx &ctx)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t step = DEFAULT_INC_MAJOR_STEP;

  // Layout mirrors TestAsyncIndexMergePolicy.huge_middle_segment:
  // large base major + small prefix + huge middle blocker + uniform tail inc majors.
  // find_merges should pick a suffix window and leave prefix segments as orphans.
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, fts_tablet_id, mock_major_snapshot,
      100, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, LARGE_MAJOR_OCCUPY_SIZE));

  ObTabletHandle fts_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  const int64_t major_snapshot = fts_handle.get_obj()->get_last_major_snapshot_version();
  ASSERT_EQ(mock_major_snapshot, major_snapshot);
  const int64_t prefix_small_end = major_snapshot + step;
  const int64_t huge_middle_end = major_snapshot + 2 * step;
  ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
      ls_handle, fts_tablet_id, major_snapshot, prefix_small_end, DEFAULT_SEG_SIZE));
  ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
      ls_handle, fts_tablet_id, prefix_small_end, huge_middle_end, LARGE_MAJOR_OCCUPY_SIZE));
  for (int64_t i = 2; i < DEFAULT_INC_MAJOR_CNT; ++i) {
    const int64_t base = major_snapshot + i * step;
    const int64_t snap = base + step;
    ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
        ls_handle, fts_tablet_id, base, snap, DEFAULT_SEG_SIZE));
  }
  verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, DEFAULT_INC_MAJOR_CNT);

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  ASSERT_NO_FATAL_FAILURE(prepare_delete_index_publish_for_merge(ctx, ls_handle));

  ObNewVersionRange suffix_merge_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, suffix_merge_range));
  ASSERT_GE(suffix_merge_range.base_version_, huge_middle_end);
  ASSERT_GT(suffix_merge_range.base_version_, major_snapshot);
  ASSERT_LE(suffix_merge_range.snapshot_version_, calc_schedule_snapshot_version(major_snapshot));

  ensure_ls_reserved_snapshot_ready(ctx.ls_id_);
  SCN weak_read_scn;
  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ObMediumCompactionInfo suffix_medium_info(allocator_);
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(6000 + 4401));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ObMediumCompactionScheduleFunc index_schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, nullptr,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.init_tablet_handle(fts_handle));
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.build_async_index_medium_info(
      suffix_merge_range, suffix_medium_info));
  fill_inc_major_info_if_needed_(
      suffix_medium_info, suffix_merge_range, *ls_handle.get_ls(), *fts_handle.get_obj());
  ASSERT_TRUE(suffix_medium_info.is_valid());
  ASSERT_TRUE(suffix_medium_info.is_inc_major_compaction());

  ObTabletMediumCompactionInfoMdsArg mds_arg;
  mds_arg.ls_id_ = ls_handle.get_ls()->get_ls_id();
  ASSERT_EQ(OB_SUCCESS, mds_arg.add_item(fts_tablet_id, suffix_medium_info));
  ASSERT_EQ(OB_SUCCESS, register_mds_in_transaction(mds_arg, 4401));

  const ObMediumCompactionInfoList *registered_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
  ASSERT_NE(nullptr, registered_medium_list);
  const int64_t inc_major_snapshot = fts_handle.get_obj()->get_tablet_meta().inc_major_snapshot_;
  ASSERT_FALSE(registered_medium_list->could_schedule_next_round(major_snapshot, inc_major_snapshot));

  int64_t prefix_orphan_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, count_inc_major_segments_with_base_lt(
      *fts_handle.get_obj(), suffix_merge_range.base_version_, prefix_orphan_cnt));
  ASSERT_GE(prefix_orphan_cnt, 2);

  SCN retry_weak_read_scn;
  const ObMediumCompactionInfoList *retry_medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, retry_weak_read_scn.convert_for_tx(6000 + 4402));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, retry_medium_list_ptr));
  ASSERT_NE(nullptr, retry_medium_list_ptr);
  ObMediumCompactionScheduleFunc retry_index_schedule_func(
      *ls_handle.get_ls(), retry_weak_read_scn, *retry_medium_list_ptr, nullptr,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, retry_index_schedule_func.init_tablet_handle(fts_handle));
  ObMediumCompactionInfo retry_medium_info(allocator_);
  ASSERT_EQ(OB_NO_NEED_MERGE, retry_index_schedule_func.build_async_index_medium_info(
      suffix_merge_range, retry_medium_info));

  // Prefix orphan segments remain in table store; second submit must be blocked while pending.
  ObNewVersionRange retry_merge_range;
  const int choose_ret = ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, retry_merge_range);
  if (OB_SUCCESS == choose_ret) {
    ObMediumCompactionInfo prefix_medium_info(allocator_);
    ASSERT_EQ(OB_NO_NEED_MERGE, retry_index_schedule_func.build_async_index_medium_info(
        retry_merge_range, prefix_medium_info));
  } else {
    ASSERT_EQ(OB_NO_NEED_MERGE, choose_ret);
  }

  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
  ASSERT_NE(nullptr, registered_medium_list);
  ASSERT_FALSE(registered_medium_list->could_schedule_next_round(major_snapshot, inc_major_snapshot));

  LOG_INFO("async index suffix-first prefix-orphan schedule blocked verified",
           K(suffix_merge_range), K(prefix_orphan_cnt), K(choose_ret), K(retry_merge_range));
}

void ObAsyncIndexCompactionTest::run_async_index_multi_inc_major_medium_info_key_order(
    MultiAsyncIndexTabletCtx &ctx)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t step = DEFAULT_INC_MAJOR_STEP;

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, fts_tablet_id, mock_major_snapshot,
      100, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, LARGE_MAJOR_OCCUPY_SIZE));

  ObTabletHandle fts_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  const int64_t major_snapshot = fts_handle.get_obj()->get_last_major_snapshot_version();
  ASSERT_EQ(mock_major_snapshot, major_snapshot);
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, major_snapshot,
      DEFAULT_INC_MAJOR_CNT, step, DEFAULT_SEG_SIZE));
  verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, DEFAULT_INC_MAJOR_CNT);

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  ASSERT_NO_FATAL_FAILURE(prepare_delete_index_publish_for_merge(ctx, ls_handle));

  ObNewVersionRange policy_merge_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, policy_merge_range));
  ASSERT_GT(policy_merge_range.base_version_, 0);
  ASSERT_GT(policy_merge_range.snapshot_version_, policy_merge_range.base_version_);

  ensure_ls_reserved_snapshot_ready(ctx.ls_id_);
  SCN weak_read_scn;
  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ObMediumCompactionInfo template_info(allocator_);
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(6000 + 4201));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ObMediumCompactionScheduleFunc index_schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, nullptr,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.init_tablet_handle(fts_handle));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.init_tablet_handle(fts_handle));
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.build_async_index_medium_info(
      policy_merge_range, template_info));
  fill_inc_major_info_if_needed_(
      template_info, policy_merge_range, *ls_handle.get_ls(), *fts_handle.get_obj());
  ASSERT_TRUE(template_info.is_valid());
  ASSERT_TRUE(template_info.is_inc_major_compaction());

  const int64_t base_medium_snapshot = template_info.medium_snapshot_;
  const int64_t base_merge_base_version = template_info.merge_base_version_;
  const int64_t expected_medium_snapshots[3] = {
      base_medium_snapshot,
      base_medium_snapshot + 1,
      base_medium_snapshot + 2};
  const int64_t expected_merge_base_versions[3] = {
      base_merge_base_version,
      base_merge_base_version + 1,
      base_merge_base_version + 2};

  // Add items in shuffled order inside one MDS batch. Reader/list should still
  // return ObMediumCompactionInfoKey (medium_snapshot_) in ascending order.
  const int64_t submit_order[3] = {1, 2, 0};
  submit_cloned_inc_major_medium_info_batch_(
      ls_handle, fts_tablet_id, template_info,
      expected_medium_snapshots, expected_merge_base_versions,
      submit_order, 3, 4300);

  const ObMediumCompactionInfoList *registered_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
  assert_medium_info_list_sorted_by_key_(
      registered_medium_list,
      expected_merge_base_versions,
      expected_medium_snapshots,
      3);

  LOG_INFO("async index multi inc major medium info key order verified",
           K(major_snapshot),
           "snap0", expected_medium_snapshots[0],
           "snap1", expected_medium_snapshots[1],
           "snap2", expected_medium_snapshots[2]);
}

// 最小场景：共享存储模式下 DDL 创建 async fulltext 索引表，并校验 schema 元数据。
TEST_F(ObAsyncIndexCompactionTest, create_async_fts_table_ddl)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  const char *table_name = "t_async_fts";
  create_async_fts_table(table_name);

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS));
  });

  int64_t data_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, data_table_id));
  ASSERT_GT(data_table_id, 0);

  int64_t has_async_index_flag = 0;
  ObSqlString sql;
  sql.assign_fmt("select (table_flags & %ld) as val from oceanbase.__all_virtual_table "
                 "where table_id = %ld",
                 HAS_ASYNC_INDEX_FLAG, data_table_id);
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn, sql.ptr(), has_async_index_flag));
  ASSERT_NE(0, has_async_index_flag);

  int64_t fts_index_table_id = 0;
  int64_t delete_index_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(*conn, data_table_id, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, fts_index_table_id));
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(*conn, data_table_id, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, delete_index_table_id));
  ASSERT_GT(fts_index_table_id, 0);
  ASSERT_GT(delete_index_table_id, 0);
  LOG_INFO("async fts table created", K(table_name), K(data_table_id), K(fts_index_table_id), K(delete_index_table_id));
}

TEST_F(ObAsyncIndexCompactionTest, async_index_tablet_and_storage_schema)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  const char *table_name = "t_async_fts_schema";
  create_async_fts_table(table_name);

  AsyncIndexTabletCtx ctx;
  resolve_async_index_tablet_ctx(table_name, ctx);

  ObTabletHandle data_tablet_handle;
  ObTabletHandle fts_index_tablet_handle;
  ObTabletHandle delete_index_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.fts_index_tablet_id_, fts_index_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_index_tablet_handle));

  const ObTablet *data_tablet = data_tablet_handle.get_obj();
  const ObTablet *fts_index_tablet = fts_index_tablet_handle.get_obj();
  const ObTablet *delete_index_tablet = delete_index_tablet_handle.get_obj();
  ASSERT_NE(nullptr, data_tablet);
  ASSERT_NE(nullptr, fts_index_tablet);
  ASSERT_NE(nullptr, delete_index_tablet);

  bool has_async_index = false;
  ASSERT_EQ(OB_SUCCESS, data_tablet->get_has_async_index(has_async_index));
  ASSERT_TRUE(has_async_index);
  ASSERT_FALSE(data_tablet->is_async_index_refresh_target());
  ASSERT_FALSE(data_tablet->is_async_delete_index());

  ASSERT_EQ(OB_SUCCESS, fts_index_tablet->get_has_async_index(has_async_index));
  ASSERT_FALSE(has_async_index);
  ASSERT_TRUE(fts_index_tablet->is_async_index_refresh_target());
  ASSERT_FALSE(fts_index_tablet->is_async_delete_index());

  ASSERT_EQ(OB_SUCCESS, delete_index_tablet->get_has_async_index(has_async_index));
  ASSERT_FALSE(has_async_index);
  ASSERT_TRUE(delete_index_tablet->is_async_index_refresh_target());
  ASSERT_TRUE(delete_index_tablet->is_async_delete_index());

  check_storage_schema_async_index_flags(
      *data_tablet, false/*is_async_index*/, false/*is_async_delete_index*/,
      INDEX_TYPE_IS_NOT);
  check_storage_schema_async_index_flags(
      *fts_index_tablet, true, false, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  check_storage_schema_async_index_flags(
      *delete_index_tablet, true, true, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_compaction_schedule)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_schedule";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  run_async_index_compaction_schedule(ctx);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_tablet_direct_schedule_not_supported)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_direct_schedule";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  scheduler->resume_major_merge();
  ASSERT_EQ(OB_NOT_SUPPORTED, scheduler->user_request_schedule_medium_merge(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), false /*is_rebuild_column_group*/,
      false /*is_async_index_force_merge*/));
  // async delete index tablet follows normal medium merge flow when scheduled directly
  ASSERT_EQ(OB_SUCCESS, scheduler->user_request_schedule_medium_merge(
      ctx.ls_id_, ctx.delete_index_tablet_id_, false /*is_rebuild_column_group*/,
      false /*is_async_index_force_merge*/));

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ObTabletHandle fts_index_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), fts_index_handle));
  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, nullptr,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(fts_index_handle));
  ASSERT_EQ(OB_INVALID_ARGUMENT, schedule_func.schedule_async_index());
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_medium_info)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  scheduler->stop_major_merge();
  DEFER(scheduler->resume_major_merge());
  exe_prepare_sql();

  const char *table_name = "t_async_fts_inc_major_info";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  ObTabletHandle fts_handle;
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  const int64_t major_snapshot = fts_handle.get_obj()->get_last_major_snapshot_version();
  ASSERT_EQ(mock_major_snapshot, major_snapshot);
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, major_snapshot,
      DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));

  int64_t tablet_inc_major_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*fts_handle.get_obj(), tablet_inc_major_cnt));
  ASSERT_EQ(DEFAULT_INC_MAJOR_CNT, tablet_inc_major_cnt);

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  ASSERT_NO_FATAL_FAILURE(prepare_delete_index_publish_for_merge(ctx, ls_handle));

  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));
  ASSERT_EQ(0, merge_version_range.base_version_);
  ASSERT_GT(calc_schedule_snapshot_version(mock_major_snapshot), merge_version_range.snapshot_version_);

  build_and_submit_medium_info(
      ls_handle, fts_handle, fts_tablet_id, merge_version_range,
      ObAdaptiveMergePolicy::NONE, 4102);

  const ObMediumCompactionInfoList *registered_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
  ASSERT_NE(nullptr, registered_medium_list);
  const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
      registered_medium_list, merge_version_range.snapshot_version_);
  ASSERT_NE(nullptr, actual_info);
  verify_medium_info_medium_major(*actual_info);
  LOG_INFO("async index medium major medium info verified",
           K(merge_version_range), K(tablet_inc_major_cnt), KPC(actual_info));
}

TEST_F(ObAsyncIndexCompactionTest, async_index_delete_percentage_medium_info)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_delete_pct";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t mock_row_count_major_snapshot = get_mock_row_count_major_snapshot(mock_major_snapshot);

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.data_tablet_id_, mock_row_count_major_snapshot,
      DELETE_PERCENT_DATA_ROW_COUNT, INDEX_TYPE_IS_NOT));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, mock_row_count_major_snapshot,
      DELETE_PERCENT_DELETE_ROW_COUNT, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));

  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, mock_major_snapshot,
      DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  int64_t delete_percentage = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::calc_delete_percentage(
      *ls_handle.get_ls(), *data_handle.get_obj(), ctx.delete_index_tablet_id_, delete_percentage));
  ASSERT_GE(delete_percentage, 20);
  ASSERT_EQ(DELETE_PERCENT_DELETE_ROW_COUNT * 100 / DELETE_PERCENT_DATA_ROW_COUNT, delete_percentage);

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(1, index_tablet_ids.count());
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);

  int64_t schedule_snapshot_version = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
      *ls_handle.get_ls(), index_tablet_ids, schedule_snapshot_version));
  ASSERT_EQ(calc_schedule_snapshot_version(mock_major_snapshot), schedule_snapshot_version);

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, schedule_snapshot_version,
      DELETE_PERCENT_DELETE_ROW_COUNT, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));

  int64_t common_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(
      *ls_handle.get_ls(), index_tablet_ids,
      delete_index_tablet_id, common_published_snapshot));
  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));
  ASSERT_EQ(0, merge_version_range.base_version_);
  ASSERT_EQ(common_published_snapshot, merge_version_range.snapshot_version_);
  ASSERT_LE(common_published_snapshot, schedule_snapshot_version);

  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);

  ensure_ls_reserved_snapshot_ready(ctx.ls_id_);

  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));
  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, &schedule_cnt,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_handle));
  const int schedule_ret = schedule_func.schedule_async_index();
  ASSERT_TRUE(OB_SUCCESS == schedule_ret || OB_NO_NEED_MERGE == schedule_ret);
  ASSERT_EQ(OB_SUCCESS == schedule_ret ? 1 : 0, schedule_cnt.submit_clog_cnt_);

  ObTabletHandle fts_handle;
  const ObMediumCompactionInfoList *registered_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
  ASSERT_NE(nullptr, registered_medium_list);
  const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
      registered_medium_list, merge_version_range.snapshot_version_);
  ASSERT_NE(nullptr, actual_info);
  ASSERT_EQ(merge_version_range.snapshot_version_, actual_info->medium_snapshot_);
  verify_medium_info_medium_major(*actual_info);
  LOG_INFO("async index delete percentage medium info verified",
           K(delete_percentage), K(merge_version_range), K(schedule_snapshot_version), KPC(actual_info));
}

TEST_F(ObAsyncIndexCompactionTest, multi_major_sstable_table_store)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_multi_major_ts";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t mock_second_major_snapshot = calc_schedule_snapshot_version(mock_major_snapshot);

  assert_major_sstable_cnt_at_least(ctx.ls_id_, fts_tablet_id, 1);
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  assert_major_sstable_cnt_at_least(ctx.ls_id_, fts_tablet_id, 2);
  assert_async_index_version_range_cnt_at_least(ctx.ls_id_, fts_tablet_id, 2);

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_second_major_snapshot));
  assert_major_sstable_cnt_at_least(ctx.ls_id_, fts_tablet_id, 3);
  assert_async_index_version_range_cnt_at_least(ctx.ls_id_, fts_tablet_id, 3);

  ObTabletHandle fts_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(mock_second_major_snapshot, fts_handle.get_obj()->get_last_major_snapshot_version());
}

TEST_F(ObAsyncIndexCompactionTest, multi_major_async_index_compaction_schedule)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_multi_major_sched";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  run_multi_major_async_index_compaction_schedule(ctx);
}

TEST_F(ObAsyncIndexCompactionTest, schedule_for_async_index_invalid_handle)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_invalid_handle";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ObMediumCompactionInfoList medium_list;
  ObScheduleTabletCnt schedule_cnt;
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ASSERT_EQ(OB_SUCCESS, medium_list.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(1000));

  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(),
      weak_read_scn,
      medium_list,
      &schedule_cnt,
      ObAdaptiveMergePolicy::NONE,
      0,
      nullptr);
  ASSERT_EQ(OB_ERR_UNEXPECTED, schedule_func.schedule_async_index());

  ObTabletHandle invalid_handle;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObMediumCompactionScheduleFunc::schedule_for_async_index_only(
      *ls_handle.get_ls(), weak_read_scn, invalid_handle, &schedule_cnt));
  ObTabletHandle fts_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), fts_tablet_handle));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObMediumCompactionScheduleFunc::schedule_for_async_index_only(
      *ls_handle.get_ls(), weak_read_scn, fts_tablet_handle, &schedule_cnt));
}

TEST_F(ObAsyncIndexCompactionTest, get_async_index_tablet_ids)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_get_idx_ids";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(1, index_tablet_ids.count());
  ASSERT_EQ(ctx.fts_index_tablet_ids_.at(0), index_tablet_ids.at(0));
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);
}

TEST_F(ObAsyncIndexCompactionTest, unpublished_delete_baseline_limits_group_merge)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_unpublished_delete_baseline";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t base = get_mock_major_snapshot(ctx);
  const int64_t published = calc_schedule_snapshot_version(base);
  const int64_t delete_baseline = base + DEFAULT_INC_MAJOR_STEP;
  ASSERT_GT(published, delete_baseline);
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, base));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(ls_handle, fts_tablet_id,
      base, DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, fts_tablet_id, published));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, ctx.delete_index_tablet_id_,
      delete_baseline, 0 /*row_count*/, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, ctx.delete_index_tablet_id_, 0));

  ObTabletHandle delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  ASSERT_TRUE(delete_handle.get_obj()->is_async_delete_index());
  int64_t logical_published = -1;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->get_latest_async_index_published_version(logical_published));
  ASSERT_EQ(0, logical_published);

  int64_t group_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(*ls_handle.get_ls(),
      ctx.fts_index_tablet_ids_, ctx.delete_index_tablet_id_, group_snapshot));
  ASSERT_EQ(delete_baseline, group_snapshot);
  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  share::schema::ObAsyncIndexParams params;
  ObNewVersionRange range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), ctx.fts_index_tablet_ids_,
      ctx.delete_index_tablet_id_, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, params, range));
  ASSERT_EQ(0, range.base_version_);
  ASSERT_EQ(delete_baseline, range.snapshot_version_);

  share::SCN read_scn;
  ASSERT_EQ(OB_SUCCESS, read_scn.convert_for_tx(published));
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->get_async_index_query_published_version(
      read_scn, logical_published));
  ASSERT_EQ(0, logical_published);
}

TEST_F(ObAsyncIndexCompactionTest, missing_baseline_skips_group_merge)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_missing_baseline";
  create_async_fts_table_with_index_cnt(table_name, 2);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t base = get_mock_major_snapshot(ctx);
  const int64_t newer_major_snapshot = base + (DEFAULT_INC_MAJOR_CNT + 1) * DEFAULT_INC_MAJOR_STEP;
  const int64_t published_snapshot = newer_major_snapshot + DEFAULT_INC_MAJOR_CNT * DEFAULT_INC_MAJOR_STEP;
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, ctx.data_tablet_id_, base,
      DELETE_PERCENT_DATA_ROW_COUNT, INDEX_TYPE_IS_NOT));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, ctx.delete_index_tablet_id_, base,
      DELETE_PERCENT_DELETE_ROW_COUNT, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));
  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, tablet_id, 0 == i ? base : newer_major_snapshot));
    if (0 == i) {
      ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
          ls_handle, tablet_id, base, 2 * DEFAULT_INC_MAJOR_CNT + 1, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
    }
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, tablet_id, published_snapshot));
  }
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
      ls_handle, ctx.delete_index_tablet_id_, published_snapshot));

  share::schema::ObAsyncIndexParams params;
  params.delete_purge_threshold_ = 20;
  int64_t delete_percentage = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::calc_delete_percentage(
      *ls_handle.get_ls(), *data_handle.get_obj(), ctx.delete_index_tablet_id_, delete_percentage));
  ASSERT_GE(delete_percentage, params.delete_purge_threshold_);
  ObNewVersionRange major_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), ctx.fts_index_tablet_ids_,
      ctx.delete_index_tablet_id_, ObAdaptiveMergePolicy::NONE, params, major_range));
  ASSERT_EQ(0, major_range.base_version_);

  // The shared delete index participates in the baseline check as well.
  ObTabletHandle delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  const int64_t delete_major_snapshot = delete_handle.get_obj()->get_last_major_snapshot_version();
  delete_handle.get_obj()->table_store_cache_.last_major_snapshot_version_ = 0;
  ObNewVersionRange inc_range;
  const int choose_ret = ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), ctx.fts_index_tablet_ids_,
      ctx.delete_index_tablet_id_, ObAdaptiveMergePolicy::TENANT_MAJOR, params, inc_range);
  delete_handle.get_obj()->table_store_cache_.last_major_snapshot_version_ = delete_major_snapshot;
  ASSERT_EQ(OB_NO_NEED_MERGE, choose_ret);

  // A building index without a baseline blocks the entire group.
  ObTabletHandle index_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), index_handle));
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->fetch_table_store(wrapper));
  ASSERT_NE(nullptr, wrapper.get_member());
  const_cast<ObTabletTableStore *>(wrapper.get_member())->major_tables_.reset();
  index_handle.get_obj()->table_store_cache_.last_major_snapshot_version_ = 0;

  const ObAdaptiveMergePolicy::AdaptiveMergeReason reasons[] = {
      ObAdaptiveMergePolicy::NONE, ObAdaptiveMergePolicy::TENANT_MAJOR,
      ObAdaptiveMergePolicy::WINDOW_COMPACTION, ObAdaptiveMergePolicy::USER_REQUEST,
      ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE};
  for (const auto reason : reasons) {
    SCOPED_TRACE(static_cast<int>(reason));
    ObNewVersionRange range;
    ASSERT_EQ(OB_NO_NEED_MERGE, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
        *ls_handle.get_ls(), *data_handle.get_obj(), ctx.fts_index_tablet_ids_,
        ctx.delete_index_tablet_id_, reason, params, range));
  }
}

TEST_F(ObAsyncIndexCompactionTest, submit_medium_info_medium_major_no_inc)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  struct ForceMediumCase {
    const char *name_;
    const char *table_name_;
    ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason_;
    int64_t tx_id_;
  };
  const ForceMediumCase cases[] = {
      {"tenant_major", "t_async_fts_tenant_major_no_inc",
       ObAdaptiveMergePolicy::TENANT_MAJOR, 4101},
      {"user_request", "t_async_fts_user_request_no_inc",
       ObAdaptiveMergePolicy::USER_REQUEST, 4102},
      {"async_index_force_merge", "t_async_fts_force_merge_no_inc",
       ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, 4103},
  };
  for (const ForceMediumCase &test_case : cases) {
    SCOPED_TRACE(test_case.name_);
    create_async_fts_table(test_case.table_name_);
    MultiAsyncIndexTabletCtx ctx;
    resolve_multi_async_index_tablet_ctx(test_case.table_name_, ctx);

    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
    const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
    const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
    const int64_t force_medium_snapshot = calc_schedule_snapshot_version(mock_major_snapshot);
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));

    ObTabletHandle index_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, index_handle));
    const int64_t major_snapshot = index_handle.get_obj()->get_last_major_snapshot_version();
    ASSERT_EQ(mock_major_snapshot, major_snapshot);
    verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, 0);

    int64_t max_sync_medium_scn = 0;
    ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->get_max_sync_medium_scn(max_sync_medium_scn));
    ASSERT_EQ(mock_major_snapshot, max_sync_medium_scn);

    // Every forced medium-major reason must be able to advance the published
    // snapshot with only the base major when this index has no IncMajor input.
    ObNewVersionRange merge_version_range;
    merge_version_range.base_version_ = 0;
    merge_version_range.snapshot_version_ = force_medium_snapshot;
    ASSERT_GT(merge_version_range.snapshot_version_, max_sync_medium_scn);
    build_and_submit_medium_info(
        ls_handle, index_handle, fts_tablet_id, merge_version_range,
        test_case.merge_reason_, test_case.tx_id_);
  }
}

TEST_F(ObAsyncIndexCompactionTest, submit_medium_info_inc_major_merge)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_inc_major_merge";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  // collect_merge_infos counts base major + inc majors; keep total below threshold.
  const int64_t below_threshold_inc_cnt = async_index_params.merge_segment_threshold_ - 2;
  ASSERT_GE(below_threshold_inc_cnt, 1);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, mock_major_snapshot,
      below_threshold_inc_cnt, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
  verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, below_threshold_inc_cnt);

  ObNewVersionRange merge_version_range;
  const int ret = ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(ObAsyncIndexCompactionTest, collect_merge_infos_multi_major_only_latest_boundary)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_collect_multi_major";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t mock_second_major_snapshot = calc_schedule_snapshot_version(mock_major_snapshot);

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_second_major_snapshot));
  assert_major_sstable_cnt_at_least(ctx.ls_id_, fts_tablet_id, 2);

  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, fts_tablet_id, mock_second_major_snapshot));

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  int64_t schedule_snapshot_version = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
      *ls_handle.get_ls(), index_tablet_ids, schedule_snapshot_version));
  ASSERT_EQ(mock_second_major_snapshot, schedule_snapshot_version);

  const ObAsyncIndexSSTableMergeInfo expected_tiers[] = {
      make_expected_merge_tier(0, mock_second_major_snapshot),
  };
  verify_collect_merge_infos(
      ls_handle, fts_tablet_id, schedule_snapshot_version, 1, expected_tiers, 1);
  // This tablet has already merged beyond the limit and needs no scheduling input.
  verify_collect_merge_infos(ls_handle, fts_tablet_id, mock_major_snapshot, 0);
}

TEST_F(ObAsyncIndexCompactionTest, collect_merge_infos_stale_narrow_inc_skipped)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_collect_stale_inc";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t narrow_inc_cnt = 10;
  const int64_t wide_snap = mock_major_snapshot + narrow_inc_cnt * DEFAULT_INC_MAJOR_STEP;

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, mock_major_snapshot,
      narrow_inc_cnt, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, fts_tablet_id, wide_snap));
  verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, narrow_inc_cnt);

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  int64_t schedule_snapshot_version = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
      *ls_handle.get_ls(), index_tablet_ids, schedule_snapshot_version));
  ASSERT_EQ(wide_snap, schedule_snapshot_version);

  verify_collect_merge_infos(
      ls_handle, fts_tablet_id, schedule_snapshot_version, 1 + narrow_inc_cnt);

  // Install the wide segment as an IncMajor merge output. Its logical window
  // [mock_major_snapshot, wide_snap] fully covers every narrow segment, so the
  // merge-output path recycles them and only the wide segment remains. A plain
  // refresh result (merge type MAJOR_MERGE) would instead append a segment whose
  // base falls behind the previous snapshot, which the table store rejects as a
  // discontinuous IncMajor range.
  ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
      ls_handle, fts_tablet_id, mock_major_snapshot, wide_snap,
      narrow_inc_cnt * DEFAULT_SEG_SIZE, true /*committed*/,
      true /*as_inc_major_merge_result*/));
  verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, 1);

  const ObAsyncIndexSSTableMergeInfo expected_tiers[] = {
      make_expected_merge_tier(0, mock_major_snapshot),
      make_expected_merge_tier(mock_major_snapshot, wide_snap),
  };
  verify_collect_merge_infos(
      ls_handle, fts_tablet_id, schedule_snapshot_version, 2, expected_tiers, 2);

  // The wide IncMajor must constrain alignment even when it crosses the scheduling limit.
  verify_collect_merge_infos(
      ls_handle, fts_tablet_id, mock_major_snapshot + DEFAULT_INC_MAJOR_STEP,
      2, expected_tiers, 2);

  ObNewVersionRange merge_version_range;
  const int ret = ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret)
      << "stale narrow segments must not inflate tier count above merge_segment_threshold";
}

TEST_F(ObAsyncIndexCompactionTest, submit_medium_info_medium_major_with_inc)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_major_with_inc";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, mock_major_snapshot,
      DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));

  ObTabletHandle data_handle;
  ObTabletHandle index_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, index_handle));

  int64_t max_published_version = 0;
  ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->get_async_index_max_published_version(max_published_version));
  ASSERT_EQ(calc_schedule_snapshot_version(mock_major_snapshot), max_published_version);

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, delete_index_tablet_id, max_published_version,
      0 /*row_count*/, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));

  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::TENANT_MAJOR,
      async_index_params, merge_version_range));
  ASSERT_EQ(max_published_version, merge_version_range.snapshot_version_);

  build_and_submit_medium_info(
      ls_handle, index_handle, fts_tablet_id, merge_version_range,
      ObAdaptiveMergePolicy::TENANT_MAJOR, 4103);
}

TEST_F(ObAsyncIndexCompactionTest, tenant_major_submit_skips_satisfied_index)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  scheduler->stop_major_merge();
  DEFER(scheduler->resume_major_merge());
  exe_prepare_sql();

  const char *table_name = "t_async_tenant_major_partial_submit";
  create_async_fts_table_with_index_cnt(table_name, 3);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(3, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &major_satisfied_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const ObTabletID &medium_satisfied_tablet_id = ctx.fts_index_tablet_ids_.at(1);
  const ObTabletID &pending_tablet_id = ctx.fts_index_tablet_ids_.at(2);
  const int64_t old_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t target_snapshot = calc_schedule_snapshot_version(old_major_snapshot);
  ASSERT_GT(target_snapshot, old_major_snapshot);

  // 第一个索引已完成 S，第二个索引已有 S 的 medium，第三个索引仍需提交 S。
  // 两类已满足成员都不能取消待处理成员对应的批量提交项。
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, major_satisfied_tablet_id, target_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, medium_satisfied_tablet_id, old_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, pending_tablet_id, old_major_snapshot));

  ObTabletHandle data_tablet_handle;
  const ObMediumCompactionInfoList *data_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(
      allocator_, data_medium_list));
  ASSERT_NE(nullptr, data_medium_list);

  ObTabletHandle major_satisfied_handle;
  ObTabletHandle medium_satisfied_handle;
  ObTabletHandle pending_handle;
  const ObMediumCompactionInfoList *major_satisfied_medium_list = nullptr;
  const ObMediumCompactionInfoList *medium_satisfied_medium_list = nullptr;
  const ObMediumCompactionInfoList *pending_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, major_satisfied_tablet_id, major_satisfied_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, medium_satisfied_tablet_id, medium_satisfied_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, pending_tablet_id, pending_handle));
  ASSERT_EQ(target_snapshot,
      major_satisfied_handle.get_obj()->get_last_major_snapshot_version());
  ASSERT_EQ(old_major_snapshot,
      medium_satisfied_handle.get_obj()->get_last_major_snapshot_version());
  ASSERT_EQ(old_major_snapshot,
      pending_handle.get_obj()->get_last_major_snapshot_version());
  ASSERT_EQ(OB_SUCCESS, major_satisfied_handle.get_obj()->read_medium_info_list(
      allocator_, major_satisfied_medium_list));
  ASSERT_EQ(OB_SUCCESS, medium_satisfied_handle.get_obj()->read_medium_info_list(
      allocator_, medium_satisfied_medium_list));
  ASSERT_EQ(OB_SUCCESS, pending_handle.get_obj()->read_medium_info_list(
      allocator_, pending_medium_list));
  ASSERT_EQ(nullptr, find_medium_info_by_snapshot(
      major_satisfied_medium_list, target_snapshot));
  ASSERT_EQ(nullptr, find_medium_info_by_snapshot(
      medium_satisfied_medium_list, target_snapshot));
  ASSERT_EQ(nullptr, find_medium_info_by_snapshot(
      pending_medium_list, target_snapshot));

  ensure_ls_reserved_snapshot_ready(ctx.ls_id_);
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));
  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *data_medium_list, &schedule_cnt,
      ObAdaptiveMergePolicy::TENANT_MAJOR, 0, nullptr);
  ObNewVersionRange merge_version_range;
  merge_version_range.base_version_ = 0;
  merge_version_range.snapshot_version_ = target_snapshot;
  ObSEArray<ObTabletID, 1> pre_submit_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, pre_submit_tablet_ids.push_back(medium_satisfied_tablet_id));
  ASSERT_EQ(OB_SUCCESS, schedule_func.submit_async_index_medium_info(
      merge_version_range, pre_submit_tablet_ids));
  ASSERT_EQ(1, schedule_cnt.submit_clog_cnt_);
  auto wait_medium_sync = [&](const ObTabletID &tablet_id) {
    bool medium_synced = false;
    for (int64_t i = 0; !medium_synced && i < 100; ++i) {
      ObTabletHandle tablet_handle;
      int64_t max_sync_medium_scn = 0;
      ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, tablet_handle));
      ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->update_medium_compaction_info(
          tablet_id, tablet_handle));
      ASSERT_EQ(OB_SUCCESS,
          tablet_handle.get_obj()->get_max_sync_medium_scn(max_sync_medium_scn));
      medium_synced = max_sync_medium_scn >= target_snapshot;
      if (!medium_synced) {
        ::ob_usleep(100 * 1000);
      }
    }
    ASSERT_TRUE(medium_synced) << "medium info is not visible on tablet " << tablet_id.id_;
  };
  wait_medium_sync(medium_satisfied_tablet_id);

  ObSEArray<ObTabletID, 3> index_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(major_satisfied_tablet_id));
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(medium_satisfied_tablet_id));
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(pending_tablet_id));
  ASSERT_EQ(OB_SUCCESS, schedule_func.submit_async_index_medium_info(
      merge_version_range, index_tablet_ids));
  ASSERT_EQ(2, schedule_cnt.submit_clog_cnt_);

  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, major_satisfied_tablet_id, major_satisfied_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, medium_satisfied_tablet_id, medium_satisfied_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, pending_tablet_id, pending_handle));
  ASSERT_EQ(OB_SUCCESS, major_satisfied_handle.get_obj()->read_medium_info_list(
      allocator_, major_satisfied_medium_list));
  ASSERT_EQ(OB_SUCCESS, medium_satisfied_handle.get_obj()->read_medium_info_list(
      allocator_, medium_satisfied_medium_list));
  ASSERT_EQ(OB_SUCCESS, pending_handle.get_obj()->read_medium_info_list(
      allocator_, pending_medium_list));
  ASSERT_EQ(nullptr, find_medium_info_by_snapshot(
      major_satisfied_medium_list, target_snapshot));
  ASSERT_NE(nullptr, find_medium_info_by_snapshot(
      medium_satisfied_medium_list, target_snapshot));
  ASSERT_NE(nullptr, find_medium_info_by_snapshot(
      pending_medium_list, target_snapshot));

  wait_medium_sync(pending_tablet_id);
  const int64_t submitted_before_retry = schedule_cnt.submit_clog_cnt_;
  ASSERT_EQ(OB_NO_NEED_MERGE, schedule_func.submit_async_index_medium_info(
      merge_version_range, index_tablet_ids));
  ASSERT_EQ(submitted_before_retry, schedule_cnt.submit_clog_cnt_);
}

TEST_F(ObAsyncIndexCompactionTest, tenant_major_submit_waits_for_unfinished_medium)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  scheduler->stop_major_merge();
  DEFER(scheduler->resume_major_merge());
  exe_prepare_sql();

  const char *table_name = "t_async_tenant_major_wait_medium";
  create_async_fts_table_with_index_cnt(table_name, 2);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(2, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &pending_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const ObTabletID &blocked_tablet_id = ctx.fts_index_tablet_ids_.at(1);
  const int64_t old_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t unfinished_snapshot = old_major_snapshot + 1;
  const int64_t target_snapshot = calc_schedule_snapshot_version(old_major_snapshot);
  ASSERT_LT(unfinished_snapshot, target_snapshot);
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, pending_tablet_id, old_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, blocked_tablet_id, old_major_snapshot));

  ObTabletHandle data_tablet_handle;
  const ObMediumCompactionInfoList *data_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(
      allocator_, data_medium_list));
  ASSERT_NE(nullptr, data_medium_list);
  ensure_ls_reserved_snapshot_ready(ctx.ls_id_);
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));
  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *data_medium_list, &schedule_cnt,
      ObAdaptiveMergePolicy::TENANT_MAJOR, 0, nullptr);

  ObNewVersionRange unfinished_range;
  unfinished_range.base_version_ = 0;
  unfinished_range.snapshot_version_ = unfinished_snapshot;
  ObSEArray<ObTabletID, 1> blocked_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, blocked_tablet_ids.push_back(blocked_tablet_id));
  ASSERT_EQ(OB_SUCCESS, schedule_func.submit_async_index_medium_info(
      unfinished_range, blocked_tablet_ids));
  ASSERT_EQ(1, schedule_cnt.submit_clog_cnt_);

  // 先处理可提交成员，再遇到低版本未完成任务；整批目标不能留下部分提交。
  ObNewVersionRange target_range;
  target_range.base_version_ = 0;
  target_range.snapshot_version_ = target_snapshot;
  ObSEArray<ObTabletID, 2> index_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(pending_tablet_id));
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(blocked_tablet_id));
  ASSERT_EQ(OB_EAGAIN, schedule_func.submit_async_index_medium_info(
      target_range, index_tablet_ids));
  ASSERT_EQ(1, schedule_cnt.submit_clog_cnt_);

  ObTabletHandle pending_handle;
  ObTabletHandle blocked_handle;
  const ObMediumCompactionInfoList *pending_medium_list = nullptr;
  const ObMediumCompactionInfoList *blocked_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, pending_tablet_id, pending_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, blocked_tablet_id, blocked_handle));
  ASSERT_EQ(OB_SUCCESS, pending_handle.get_obj()->read_medium_info_list(
      allocator_, pending_medium_list));
  ASSERT_EQ(OB_SUCCESS, blocked_handle.get_obj()->read_medium_info_list(
      allocator_, blocked_medium_list));
  ASSERT_EQ(nullptr, find_medium_info_by_snapshot(
      pending_medium_list, target_snapshot));
  ASSERT_EQ(nullptr, find_medium_info_by_snapshot(
      blocked_medium_list, target_snapshot));
  ASSERT_NE(nullptr, find_medium_info_by_snapshot(
      blocked_medium_list, unfinished_snapshot));
}

TEST_F(ObAsyncIndexCompactionTest, choose_merge_range_inc_major)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_merge_inc_major";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  run_choose_merge_range_tenant_major(ctx);
}

TEST_F(ObAsyncIndexCompactionTest, choose_merge_range_multi_index_varying_cnts)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  run_choose_merge_range_multi_index_varying_cnts(4);
}

TEST_F(ObAsyncIndexCompactionTest, choose_merge_range_multi_index_min_snapshot)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  run_choose_merge_range_multi_index_min_snapshot(3);
}

TEST_F(ObAsyncIndexCompactionTest, choose_merge_range_no_need_merge)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_no_need_merge";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  run_choose_merge_range_no_need_merge(ctx);
}

TEST_F(ObAsyncIndexCompactionTest, inc_major_info_init)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObIncMajorSSTableInfo inc_major_info;
  ObSEArray<ObITable *, 8> tables;
  const int64_t major_snapshot = 100;
  const int64_t step = 10;
  ASSERT_EQ(OB_SUCCESS, build_inc_major_table_array(3, major_snapshot, step, tables));
  const int64_t boundary_snapshot = major_snapshot + 3 * step;
  ASSERT_EQ(OB_SUCCESS, inc_major_info.init(allocator_, boundary_snapshot, tables));
  ASSERT_TRUE(inc_major_info.is_valid());
  ASSERT_EQ(3, inc_major_info.get_size());
  ASSERT_EQ(major_snapshot, inc_major_info.info_list_[0].start_scn_);
  ASSERT_EQ(major_snapshot + 3 * step, inc_major_info.info_list_[2].end_scn_);
  inc_major_info.destroy(allocator_);
}

TEST_F(ObAsyncIndexCompactionTest, lookup_base_zero_falls_back_to_inc_inv)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_lookup_base0_inc";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  const ObTabletID &tablet_id = ctx.fts_index_tablet_ids_.at(0);
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, tablet_handle));

  const int64_t logical_base = ASYNC_INDEX_MIN_INC_MAJOR_BASE_VERSION;
  const int64_t logical_snapshot = 200;
  const int64_t physical_start = 220;
  const int64_t physical_end = 240;
  const int64_t commit_version = 250;
  ObSSTable *inc_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable(
      tablet_id, logical_base, logical_snapshot, DEFAULT_SEG_SIZE, inc_sstable));
  ASSERT_NE(nullptr, inc_sstable);
  ASSERT_EQ(OB_SUCCESS,
      inc_sstable->key_.scn_range_.start_scn_.convert_for_tx(physical_start));
  ASSERT_EQ(OB_SUCCESS,
      inc_sstable->key_.scn_range_.end_scn_.convert_for_tx(physical_end));
  inc_sstable->meta_->basic_meta_.upper_trans_version_ = commit_version;
  inc_sstable->meta_cache_.upper_trans_version_ = commit_version;

  ObNewVersionRange logical_range;
  logical_range.base_version_ = logical_base;
  logical_range.snapshot_version_ = logical_snapshot;
  ObNewVersionRange actual_range;
  ASSERT_EQ(OB_SUCCESS, inc_sstable->get_async_index_version_range(actual_range));
  ASSERT_EQ(logical_range, actual_range);
  ASSERT_NE(logical_range, inc_sstable->get_key().version_range_);

  ObSEArray<ObITable *, 1> inc_tables;
  ASSERT_EQ(OB_SUCCESS, inc_tables.push_back(inc_sstable));
  ObTabletTableStore inc_only_store;
  inc_only_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, inc_only_store.inc_major_tables_.init(allocator_, inc_tables));

  ObStorageMetaHandle table_store_handle;
  ObTableHandleV2 selected_handle;
  ASSERT_EQ(OB_SUCCESS, inc_only_store.get_async_index_sstable(
      table_store_handle, *tablet_handle.get_obj(), ObITable::INV_INDEX_SSTABLE,
      logical_range, selected_handle));
  // ObSSTableArray::init() deep-copies each table, so the handle returned by the
  // lookup points at the store's own copy, not at the object built here. Compare
  // the table identity instead of the pointer.
  ASSERT_NE(nullptr, selected_handle.get_table());
  ASSERT_EQ(inc_sstable->get_key(), selected_handle.get_table()->get_key());
  ASSERT_EQ(inc_sstable->get_key(), inc_only_store.inc_major_tables_.at(0)->get_key());
  selected_handle.reset();

  ObInvSSTable *major_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      mock_major_sstable(tablet_id, logical_snapshot, major_sstable));
  ObSEArray<ObITable *, 1> major_tables;
  ASSERT_EQ(OB_SUCCESS, major_tables.push_back(major_sstable));
  ObTabletTableStore major_and_inc_store;
  major_and_inc_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS,
      major_and_inc_store.major_tables_.init(allocator_, major_tables));
  ASSERT_EQ(OB_SUCCESS,
      major_and_inc_store.inc_major_tables_.init(allocator_, inc_tables));

  ASSERT_EQ(OB_SUCCESS, major_and_inc_store.get_async_index_sstable(
      table_store_handle, *tablet_handle.get_obj(), ObITable::INV_INDEX_SSTABLE,
      logical_range, selected_handle));
  // Same deep-copy caveat as above: once a Major is present at the same logical
  // range it must win over the IncMajor, so assert on the selected table type and
  // key rather than on the pointer identity of the locally built object.
  ASSERT_NE(nullptr, selected_handle.get_table());
  ASSERT_EQ(major_sstable->get_key(), selected_handle.get_table()->get_key());
  ASSERT_TRUE(ObITable::is_major_sstable(selected_handle.get_table()->get_table_type()));
}

TEST_F(ObAsyncIndexCompactionTest, inv_sstable_upper_trans_version_from_meta_cache)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  const ObTabletID tablet_id(50001);
  const int64_t major_snapshot = 100;
  const int64_t inc_base = 100;
  const int64_t inc_snapshot = 110;
  const int64_t commit_version = 115;

  ObInvSSTable *major_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_major_sstable(tablet_id, major_snapshot, major_sstable));
  ASSERT_TRUE(major_sstable->is_inv_sstable());
  ASSERT_FALSE(major_sstable->is_inc_inv_sstable());
  ASSERT_EQ(major_snapshot, major_sstable->get_upper_trans_version());
  ASSERT_EQ(major_snapshot, major_sstable->get_snapshot_version());

  ObSSTable *uncommitted_inc = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable(
      tablet_id, inc_base, inc_snapshot, DEFAULT_SEG_SIZE, uncommitted_inc, false));
  ObInvSSTable *inc_sstable = static_cast<ObInvSSTable *>(uncommitted_inc);
  ASSERT_TRUE(inc_sstable->is_inc_inv_sstable());
  ASSERT_EQ(INT64_MAX, inc_sstable->get_upper_trans_version());
  ASSERT_NE(inc_snapshot, inc_sstable->get_upper_trans_version());

  ObSSTable *committed_inc = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable(
      tablet_id, inc_base, inc_snapshot, DEFAULT_SEG_SIZE, committed_inc, true));
  ASSERT_EQ(inc_snapshot, static_cast<ObInvSSTable *>(committed_inc)->get_upper_trans_version());

  ASSERT_EQ(OB_SUCCESS, inc_sstable->set_upper_trans_version(allocator_, commit_version));
  ASSERT_EQ(commit_version, inc_sstable->get_upper_trans_version());
}

TEST_F(ObAsyncIndexCompactionTest, inc_inv_continuous_with_unfilled_gap)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  const ObTabletID tablet_id(50001);
  const int64_t base_versions[] = {100, 200, 300};
  const int64_t snapshot_versions[] = {200, 300, 400};
  const bool committed[] = {true, false, true};
  ObSEArray<ObITable *, 3> inc_inv_tables;

  for (int64_t i = 0; i < ARRAYSIZEOF(base_versions); ++i) {
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable(
        tablet_id,
        base_versions[i],
        snapshot_versions[i],
        DEFAULT_SEG_SIZE,
        sstable,
        committed[i]));
    ASSERT_EQ(OB_SUCCESS, inc_inv_tables.push_back(sstable));
  }

  ObTabletTableStore table_store;
  table_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, table_store.inc_major_tables_.init(allocator_, inc_inv_tables));
  EXPECT_EQ(OB_SUCCESS, table_store.check_continuous(true /* is_async_index_tablet */));
}

TEST_F(ObAsyncIndexCompactionTest, collect_merge_infos_skips_uncommitted_inc_inv)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_collect_uncommitted_inc";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t uncommitted_inc_snapshot = mock_major_snapshot + DEFAULT_INC_MAJOR_STEP;

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
      ls_handle, fts_tablet_id, mock_major_snapshot, uncommitted_inc_snapshot,
      DEFAULT_SEG_SIZE, false/*committed*/));
  verify_inc_major_cnt(ctx.ls_id_, fts_tablet_id, 1);

  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, tablet_handle));
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_EQ(1, table_store_wrapper.get_member()->get_inc_major_sstables().count());
  ObSSTable *inc_sstable = table_store_wrapper.get_member()->get_inc_major_sstables().at(0);
  ASSERT_NE(nullptr, inc_sstable);
  ASSERT_TRUE(inc_sstable->is_inc_inv_sstable());
  ASSERT_EQ(INT64_MAX, inc_sstable->get_upper_trans_version());

  const ObAsyncIndexSSTableMergeInfo expected_tiers[] = {
      make_expected_merge_tier(0, mock_major_snapshot),
  };
  verify_collect_merge_infos(
      ls_handle, fts_tablet_id, uncommitted_inc_snapshot, 1, expected_tiers, 1);

  int64_t max_published_version = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_max_published_version(max_published_version));
  ASSERT_EQ(mock_major_snapshot, max_published_version);
}

TEST_F(ObAsyncIndexCompactionTest, inc_major_info_build_from_sstable_matches_table_key)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable_with_meta(200, 300, 4321, 77, sstable));
  const ObIncMajorSSTableInfo::IncMajorInfo info(
      sstable->get_key().get_start_scn().get_val_for_tx(),
      sstable->get_key().get_end_scn().get_val_for_tx(),
      sstable->is_co_sstable()
          ? static_cast<const ObCOSSTableV2 &>(*sstable).get_cs_meta().data_checksum_
          : sstable->get_data_checksum(),
      sstable->get_row_count());
  ASSERT_EQ(200, info.start_scn_);
  ASSERT_EQ(300, info.end_scn_);
  ASSERT_EQ(4321, info.data_checksum_);
  ASSERT_EQ(77, info.row_count_);
}

TEST_F(ObAsyncIndexCompactionTest, inc_major_info_try_gene)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObMediumCompactionInfo medium_info(allocator_);
  ObSEArray<ObITable *, 8> tables;
  ObTablesHandleArray handle;
  const int64_t major_snapshot = 100;
  const int64_t step = 10;
  const int64_t boundary_snapshot = major_snapshot + 3 * step;
  ASSERT_EQ(OB_SUCCESS, build_inc_major_table_array(3, major_snapshot, step, tables));
  for (int64_t i = 0; i < tables.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, add_sstable_to_handle(handle, static_cast<ObSSTable *>(tables.at(i))));
  }
  medium_info.set_basic_info(
      ObMediumCompactionInfo::INC_MAJOR_COMPACTION,
      ObAdaptiveMergePolicy::NONE,
      boundary_snapshot);
  ASSERT_EQ(OB_SUCCESS, medium_info.try_gene_inc_major_info(handle));
  ASSERT_FALSE(medium_info.contain_inc_major_info_);
  ASSERT_FALSE(medium_info.inc_major_info_.is_valid());
}

TEST_P(ObMultiAsyncIndexCompactionTest, create_multi_async_fts_table_ddl)
{
  const int64_t fts_index_cnt = GetParam();
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_multi_%ld", fts_index_cnt));
  create_async_fts_table_with_index_cnt(table_name.ptr(), fts_index_cnt);
  check_multi_async_fts_table_ddl(table_name.ptr(), fts_index_cnt);
}

TEST_P(ObMultiAsyncIndexCompactionTest, multi_async_index_compaction_schedule)
{
  const int64_t fts_index_cnt = GetParam();
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_sched_%ld", fts_index_cnt));
  create_async_fts_table_with_index_cnt(table_name.ptr(), fts_index_cnt);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name.ptr(), ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());
  run_async_index_compaction_schedule(ctx);
}

TEST_P(ObPartitionAsyncIndexCompactionTest, create_partition_async_fts_table_ddl)
{
  const PartitionTableType part_type = GetParam();
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_part_ddl_%ld", static_cast<int64_t>(part_type)));
  create_async_fts_partition_table(table_name.ptr(), part_type);
  check_partition_async_fts_table_ddl(table_name.ptr(), part_type);
}

TEST_P(ObPartitionAsyncIndexCompactionTest, partition_async_index_tablet_and_storage_schema)
{
  const PartitionTableType part_type = GetParam();
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_part_schema_%ld", static_cast<int64_t>(part_type)));
  create_async_fts_partition_table(table_name.ptr(), part_type);

  const int64_t fts_index_cnt = get_expected_fts_index_cnt(part_type);
  MultiAsyncIndexTabletCtx ctx;
  resolve_partition_multi_async_index_tablet_ctx(table_name.ptr(), "p0", ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());

  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));
  const ObTablet *data_tablet = data_tablet_handle.get_obj();
  ASSERT_NE(nullptr, data_tablet);
  bool has_async_index = false;
  ASSERT_EQ(OB_SUCCESS, data_tablet->get_has_async_index(has_async_index));
  ASSERT_TRUE(has_async_index);
  ASSERT_FALSE(data_tablet->is_async_index_refresh_target());
  ASSERT_FALSE(data_tablet->is_async_delete_index());
  check_storage_schema_async_index_flags(
      *data_tablet, false/*is_async_index*/, false/*is_async_delete_index*/,
      INDEX_TYPE_IS_NOT);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    ObTabletHandle fts_index_tablet_handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.fts_index_tablet_ids_.at(i), fts_index_tablet_handle));
    const ObTablet *fts_index_tablet = fts_index_tablet_handle.get_obj();
    ASSERT_NE(nullptr, fts_index_tablet);
    ASSERT_EQ(OB_SUCCESS, fts_index_tablet->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
    ASSERT_TRUE(fts_index_tablet->is_async_index_refresh_target());
    ASSERT_FALSE(fts_index_tablet->is_async_delete_index());
    check_storage_schema_async_index_flags(
        *fts_index_tablet, true, false, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  }

  ObTabletHandle delete_index_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_index_tablet_handle));
  const ObTablet *delete_index_tablet = delete_index_tablet_handle.get_obj();
  ASSERT_NE(nullptr, delete_index_tablet);
  ASSERT_EQ(OB_SUCCESS, delete_index_tablet->get_has_async_index(has_async_index));
  ASSERT_FALSE(has_async_index);
  ASSERT_TRUE(delete_index_tablet->is_async_index_refresh_target());
  ASSERT_TRUE(delete_index_tablet->is_async_delete_index());
  check_storage_schema_async_index_flags(
      *delete_index_tablet, true, true, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
}

TEST_P(ObPartitionAsyncIndexCompactionTest, partition_async_index_compaction_schedule)
{
  const PartitionTableType part_type = GetParam();
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_part_sched_%ld", static_cast<int64_t>(part_type)));
  create_async_fts_partition_table(table_name.ptr(), part_type);

  const int64_t fts_index_cnt = get_expected_fts_index_cnt(part_type);
  MultiAsyncIndexTabletCtx ctx;
  resolve_partition_multi_async_index_tablet_ctx(table_name.ptr(), "p0", ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());
  run_async_index_compaction_schedule(ctx);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_merge_base_version)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_inc_major_start_ver";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  run_async_index_inc_major_merge_base_version(ctx);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_suffix_first_prefix_orphan_blocked)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_suffix_prefix_orphan";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  run_async_index_suffix_first_prefix_orphan_blocked(ctx);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_multi_inc_major_medium_info_key_order)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_multi_inc_major_key";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  run_async_index_multi_inc_major_medium_info_key_order(ctx);
}

TEST_F(ObAsyncIndexCompactionTest, async_inv_merge_selects_available_ranges)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_inv_logical_coverage";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &tablet_id = ctx.fts_index_tablet_ids_.at(0);
  struct LogicalRangeCase {
    const char *name_;
    int64_t ranges_[4][2];
    int64_t range_count_;
    bool committed_;
    int expected_ret_;
    int64_t expected_table_count_;
  };
  const LogicalRangeCase cases[] = {
      {"no_increment", {}, 0, true, OB_SUCCESS, 1},
      {"crosses_base", {{50, 200}}, 1, true, OB_NO_NEED_MERGE, 0},
      {"missing_prefix", {{150, 200}}, 1, true, OB_SUCCESS, 2},
      {"missing_suffix", {{100, 150}}, 1, true, OB_SUCCESS, 2},
      {"gap", {{100, 150}, {160, 200}}, 2, true, OB_SUCCESS, 3},
      {"crossed_overlap", {{100, 160}, {150, 200}}, 2, true, OB_NO_NEED_MERGE, 0},
      {"over_target_only", {{100, 250}}, 1, true, OB_NO_NEED_MERGE, 0},
      {"target_inside_later_segment", {{100, 150}, {150, 250}}, 2, true, OB_NO_NEED_MERGE, 0},
      {"exact_target", {{100, 200}}, 1, true, OB_SUCCESS, 2},
      {"uncommitted", {{100, 200}}, 1, false, OB_SUCCESS, 1},
      {"later_range", {{100, 200}, {200, 300}}, 2, true, OB_SUCCESS, 2},
      {"physical_commit_after_target", {{100, 200}}, 1, true, OB_SUCCESS, 2},
      {"retained_children", {{100, 250}, {100, 150}, {150, 200}}, 3, true, OB_NO_NEED_MERGE, 0},
      {"retained_left_children", {{50, 200}, {100, 150}, {150, 200}}, 3, true, OB_NO_NEED_MERGE, 0},
      {"contained_children", {{100, 200}, {100, 150}, {150, 200}}, 3, true, OB_SUCCESS, 2},
  };
  for (const LogicalRangeCase &test_case : cases) {
    SCOPED_TRACE(test_case.name_);
    ObTabletTableStore table_store;
    table_store.is_inited_ = true;
    ObTablet tablet;
    tablet.table_store_addr_.ptr_ = &table_store;
    DEFER(tablet.table_store_addr_.ptr_ = nullptr);
    tablet.table_store_cache_.is_async_index_refresh_target_ = true;
    tablet.table_store_cache_.is_async_delete_index_ = false;
    tablet.tablet_meta_.tablet_id_ = tablet_id;
    tablet.tablet_meta_.snapshot_version_ = 300;
    tablet.tablet_meta_.multi_version_start_ = 200;
    ObInvSSTable *major = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_major_sstable(tablet_id, 100, major));
    ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major));
    ObSEArray<ObITable *, 4> inc_tables;
    for (int64_t i = 0; i < test_case.range_count_; ++i) {
      ObSSTable *inc = nullptr;
      ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable(tablet_id,
          test_case.ranges_[i][0], test_case.ranges_[i][1], DEFAULT_SEG_SIZE, inc,
          test_case.committed_));
      if (0 == STRCMP("physical_commit_after_target", test_case.name_)) {
        inc->meta_->basic_meta_.upper_trans_version_ = 300;
        inc->meta_cache_.upper_trans_version_ = 300;
      }
      ASSERT_EQ(OB_SUCCESS, inc_tables.push_back(inc));
    }
    ASSERT_EQ(OB_SUCCESS, table_store.inc_major_tables_.init(allocator_, inc_tables));
    if (!test_case.committed_) {
      // Reserved tx id 100 is deterministically RUNNING in ObTxTable.
      int64_t tx_state = ObTxData::UNKOWN;
      int64_t commit_version = 0;
      ASSERT_EQ(OB_SUCCESS, ObIncMajorTxHelper::get_inc_major_commit_version(
          *ls_handle.get_ls(), *table_store.inc_major_tables_.at(0),
          SCN::max_scn(), tx_state, commit_version));
      ASSERT_EQ(ObTxData::RUNNING, tx_state);
      ASSERT_EQ(INT64_MAX, commit_version);
    }
    for (int64_t need_check_tablet = 0; need_check_tablet <= 1; ++need_check_tablet) {
      SCOPED_TRACE(need_check_tablet);
      ObGetMergeTablesResult result;
      ASSERT_EQ(test_case.expected_ret_, ObPartitionMergePolicy::get_result_by_snapshot(
          *ls_handle.get_ls(), tablet, 200, result, need_check_tablet));
      if (OB_SUCCESS == test_case.expected_ret_) {
        ASSERT_EQ(test_case.expected_table_count_, result.handle_.get_count());
        for (int64_t i = 1; i < result.handle_.get_count(); ++i) {
          ObITable *inc = result.handle_.get_table(i);
          ASSERT_TRUE(inc->is_inc_inv_sstable());
          ObNewVersionRange logical_range;
          ASSERT_EQ(OB_SUCCESS,
              static_cast<ObSSTable *>(inc)->get_async_index_version_range(logical_range));
          ASSERT_GT(logical_range.snapshot_version_, 100);
          ASSERT_LT(logical_range.base_version_, 200);
        }
      }
    }
  }
}

TEST_F(ObAsyncIndexCompactionTest, async_index_merge_selection_allows_range_holes)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_logical_physical_range";
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table(table_name));
  MultiAsyncIndexTabletCtx ctx;
  ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(table_name, ctx));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t major_snapshot = get_mock_major_snapshot(ctx);
  ASSERT_GT(major_snapshot, 0);
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), major_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, major_snapshot));

  struct CoverageCase {
    const char *name_;
    int64_t logical_offsets_[2][2];
    int64_t physical_ranges_[2][2];
    int64_t count_;
  };
  const CoverageCase cases[] = {
      {"physical_gap", {{0, 50}, {50, 100}}, {{1000, 1100}, {1200, 1300}}, 2},
      {"physical_empty", {{0, 50}, {50, 100}}, {{1000, 1000}, {1200, 1200}}, 2},
      {"logical_gap", {{0, 50}, {60, 100}}, {{1000, 1100}, {1100, 1200}}, 2},
      {"missing_prefix", {{10, 100}}, {{1000, 1100}}, 1},
      {"missing_suffix", {{0, 90}}, {{1000, 1100}}, 1},
      {"no_increment", {}, {}, 0},
  };
  for (const bool is_delete_index : {false, true}) {
    SCOPED_TRACE(is_delete_index);
    const ObTabletID &tablet_id = is_delete_index
        ? ctx.delete_index_tablet_id_ : ctx.fts_index_tablet_ids_.at(0);
    ObTabletHandle real_tablet;
    ObTabletMemberWrapper<ObTabletTableStore> real_store;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, real_tablet));
    ASSERT_EQ(OB_SUCCESS, real_tablet.get_obj()->fetch_table_store(real_store));
    ASSERT_NE(nullptr, real_store.get_member());
    ObSSTable *major = static_cast<ObSSTable *>(
        real_store.get_member()->get_major_sstables().get_boundary_table(true));
    ASSERT_NE(nullptr, major);
    const int64_t base = major->get_snapshot_version();
    const int64_t target = base + 100;
    for (const CoverageCase &test_case : cases) {
      SCOPED_TRACE(test_case.name_);
      ObTabletTableStore table_store;
      table_store.is_inited_ = true;
      ObTablet tablet;
      tablet.table_store_addr_.ptr_ = &table_store;
      DEFER(tablet.table_store_addr_.ptr_ = nullptr);
      tablet.table_store_cache_.is_async_index_refresh_target_ = true;
      tablet.table_store_cache_.is_async_delete_index_ = is_delete_index;
      tablet.tablet_meta_.tablet_id_ = tablet_id;
      tablet.tablet_meta_.snapshot_version_ = target;
      tablet.tablet_meta_.multi_version_start_ = target;
      ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major));
      ObSEArray<ObITable *, 2> inc_tables;
      for (int64_t i = 0; i < test_case.count_; ++i) {
        ObScnRange physical_range;
        ASSERT_EQ(OB_SUCCESS, physical_range.start_scn_.convert_for_tx(test_case.physical_ranges_[i][0]));
        ASSERT_EQ(OB_SUCCESS, physical_range.end_scn_.convert_for_tx(test_case.physical_ranges_[i][1]));
        ObSSTable *inc = nullptr;
        ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable(tablet_id,
            base + test_case.logical_offsets_[i][0], base + test_case.logical_offsets_[i][1],
            DEFAULT_SEG_SIZE, inc, true /*committed*/, is_delete_index, &physical_range));
        ASSERT_EQ(is_delete_index ? ObITable::INC_MAJOR_SSTABLE : ObITable::INC_INV_INDEX_SSTABLE,
            inc->get_table_type());
        ASSERT_EQ(physical_range.start_scn_, inc->get_start_scn());
        ASSERT_EQ(physical_range.end_scn_, inc->get_end_scn());
        ASSERT_EQ(OB_SUCCESS, inc_tables.push_back(inc));
      }
      ASSERT_EQ(OB_SUCCESS, table_store.inc_major_tables_.init(allocator_, inc_tables));
      for (const bool need_check_tablet : {false, true}) {
        ObGetMergeTablesResult result;
        ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::get_result_by_snapshot(
            *ls_handle.get_ls(), tablet, target, result, need_check_tablet));
        ASSERT_EQ(1 + test_case.count_, result.handle_.get_count());
      }
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    AsyncIndexCnt,
    ObMultiAsyncIndexCompactionTest,
    ::testing::Values(2, 3, 5));

INSTANTIATE_TEST_CASE_P(
    PartitionType,
    ObPartitionAsyncIndexCompactionTest,
    ::testing::Values(PART_HASH_4, PART_RANGE_3, PART_LIST_3, PART_HASH_MULTI_IDX_3));

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_async_index_compaction.log*");
  return run_async_index_compaction_tests(argc, argv);
}
