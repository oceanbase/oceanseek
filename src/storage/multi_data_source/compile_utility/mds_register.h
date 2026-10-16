/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// ################################### 使用多源事务兼容性占位须知 ##################################
// # 占位代码需要书写于宏定义块【NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION】中
// # 占位方式: 通过【注释】占位，占位需要信息：Helper类型名/Ctx类型名/Enum数值/Enum命名
// #
// # 注意：
// # 0. 在‘余留位置’之前占位
// # 1. 始终先在master占位，保证master分支是其他所有分支的超集
// # 2. master占位之后，开发分支上不可修改注册宏中的对应信息，否则FARM会认为占位冲突，如果有这种场景，需要先修改master占位
// # 3. 所有类型名的书写从全局命名空间'::oceanbase'开始
// # 4. Enum数值采用递增方式占位
// # 5. Enum命名不可同前文重复
// # 6. 由于采用注释方式占位，因此【不需要】书写对应的类型定义并包含在【NEED_MDS_REGISTER_DEFINE】中
// ############################################################################################

// ################################### 使用多源数据兼容性占位须知 ##################################
// # 占位代码需要书写于宏定义块【GENERATE_MDS_UNIT】中，更近一步的:
// # 1. 业务 MDS 单元统一注册在【GENERATE_LS_INNER_MDS_TABLE】（数据落在 LS_MDS_TABLET / ObMdsTable 路径）
// # 占位方式: 通过【定义】占位，占位需要信息：Key类型名/Value类型名/多版本语义支持
// #
// # 注意：
// # 0. 在‘余留位置’之前占位
// # 1. 始终先在master占位，保证master分支是其他所有分支的超集
// # 2. master占位之后，开发分支上不可修改注册宏中的对应信息，否则FARM会认为占位冲突，如果有这种场景，需要先修改master占位
// # 3. 所有类型名的书写从全局命名空间'::oceanbase'开始
// # 4. 若Key类型名非'::oceanbase::storage::mds::DummyKey'，则需要提供对应的Key类型定义，并将对应头文件包含在【NEED_MDS_REGISTER_DEFINE】中
// # 5. 需要提供对应的Value类型定义，并将对应头文件包含在【NEED_MDS_REGISTER_DEFINE】中
// # 6. Key/Value的类型仅需要空定义，不需要定义成员方法和变量，但需要实现框架所需的接口，以通过框架的编译期检查，包括(占位时实现为空)：
// #    a. 打印函数：[int64_t T::to_string(char *, const int64_t) const]
// #    b. 比较函数的某种实现，例如：[bool T::operator==(const T &) const] and [bool T::operator<(const T &) const]
// #    c. 序列化函数的某种实现，例如：[int serialize(char *, const int64_t, int64_t &) const] and [int deserialize(const char *, const int64_t, int64_t &)] and [int64_t get_serialize_size() const]
// #    d. 拷贝/移动函数的某种实现，例如：[int T::assign(const T &)]
// ############################################################################################

// the MDS FRAME must know the defination of some class type to generate legal CPP codes, including:
// 1. DATA type defination if you need multi source data support.
//    1.a. KEY type defination if you need multi source data support with multi key support.
// 2. HELPER FUNCTION type and inherited class of BufferCtx definations if you need multi source
//    transaction support.
// inlcude those classes definations header file in below MACRO BLOCK
// CAUTION: MAKE SURE your header file is as CLEAN as possible to avoid recursive dependencies!
#if defined (NEED_MDS_REGISTER_DEFINE) && !defined (NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION)
  #include "src/storage/ddl/ob_ddl_change_tablet_to_table_helper.h"
  #include "src/storage/multi_data_source/compile_utility/mds_dummy_key.h"
  #include "src/storage/multi_data_source/mds_ctx.h"
  #include "src/storage/multi_data_source/test/mds_test_framework_helper.h"
  #include "src/storage/mds_table/ob_test_mds_types.h"
  #include "src/storage/tablet/ob_tablet_create_delete_mds_user_data.h"
  #include "src/storage/tablet/ob_tablet_create_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_delete_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_binding_helper.h"
  #include "src/storage/tablet/ob_tablet_binding_mds_user_data.h"
  #include "src/storage/tablet/ob_tablet_split_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_split_mds_user_data.h"
  #include "src/share/ob_tablet_autoincrement_param.h"
  #include "src/storage/compaction/ob_medium_compaction_info.h"
  #include "src/storage/compaction/ob_medium_compaction_info_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_start_transfer_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_finish_transfer_mds_helper.h"
  #include "src/share/balance/ob_balance_task_table_operator.h"
  #include "src/storage/tablet/ob_tablet_transfer_tx_ctx.h"
  #include "src/storage/multi_data_source/ob_tablet_create_mds_ctx.h"
  #include "src/share/ob_standby_upgrade.h"
  #include "src/storage/tablet/ob_tablet_abort_transfer_mds_helper.h"
  #include "src/storage/multi_data_source/ob_tablet_create_mds_ctx.h"
  #include "src/storage/multi_data_source/ob_start_transfer_in_mds_ctx.h"
  #include "src/storage/multi_data_source/ob_finish_transfer_in_mds_ctx.h"
  #include "src/storage/multi_data_source/ob_abort_transfer_in_mds_ctx.h"
  #include "src/storage/multi_data_source/ob_tablet_mds_info_cache_ctx.h"
  #include "src/share/ob_standby_upgrade.h"
  #include "src/storage/mview/ob_major_mv_merge_info.h"
  #include "src/storage/truncate_info/ob_truncate_info.h"
  #include "src/storage/truncate_info/ob_truncate_info_mds_helper.h"
  #include "src/storage/mds_table/ob_ls_mds_tablet_reloc_mds_helper.h"
  #include "src/storage/compaction_ttl/ob_ttl_filter_info.h"
  #include "src/storage/compaction_ttl/ob_ttl_filter_info_mds_helper.h"
  #include "src/storage/mview/ob_mview_mds.h"
  #include "src/storage/tablet/ob_tablet_ddl_complete_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_ddl_complete_mds_data.h"
  #include "src/storage/tablet/ob_tablet_split_info_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_split_info_mds_user_data.h"
  #include "src/storage/tablet/ob_async_index_refresh_info_mds_helper.h"
  #include "src/storage/tablet/ob_async_index_refresh_info.h"
  #include "src/storage/mds_table/ob_ls_mds_tablet_reloc_mds_helper.h"
  #include "src/storage/compaction/ob_medium_compaction_info_mds_helper.h"
  #include "src/share/ob_fork_table_util.h"
  #include "src/storage/tablet/ob_tablet_fork_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_fork_src_mds_data.h"
  #include "src/storage/tablet/ob_tablet_mds_reloc_status.h"
#endif

/**************************************************************************************************/

/**********************generate mds frame code with multi source transaction***********************/
// GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME) is an
// interface MACRO to transfer necessary information to MDS FRAME to generate codes in transaction
// layer, mostly in ob_multi_data_source.cpp and ob_trans_part_ctx.cpp.
//
// @param HELPER_CLASS the class must has two static method signatures(or COMPILE ERROR):
//                     1. static int on_register(const char* buf,
//                                               const int64_t len,
//                                               storage::mds::BufferCtx &ctx);// on leader
//                     2. static int on_replay(const char* buf,
//                                             const int64_t len,
//                                             const share::SCN &scn,
//                                             storage::mds::BufferCtx &ctx);// on follower
//                     the actual ctx's type is BUFFER_CTX_TYPE user registered.
// @param BUFFER_CTX_TYPE must inherited from storage::mds::BufferCtx.
// @param ID for FRAME code reflection reason and compat reason, write the number by INC logic.
// @param ENUM_NAME transaction layer needed, will be defined in enum class
//                  ObTxDataSourceType(ob_multi_data_source.h)



// 注意!!!
// 多源事务类型不能通过注释占位, 必须用真实代码占位.
// 为了编译通过, 可以在下面PLACE_DEFINE代码块内, 定义需要的类型: Helper类和Ctx类(如果不是默认MdsCtx的话).
// 等正式代码合入后, 再删除占位的Helper类和Ctx类定义.


#define GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME) \
_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME)
#ifdef NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
  // Test MDS transaction types: IDs 14-16 (legacy enum uses 0-13, see ObTxDataSourceType)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::unittest::MDSTestFrameworkHelper,\
                                          ::oceanbase::unittest::MDSTestFrameworkCtx,\
                                          14,\
                                          TEST_FRAMEWORK)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::mds::ObTestMdsSingleRowHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          15,\
                                          TEST_SINGLE_ROW)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::mds::ObTestMdsMultiRowHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          16,\
                                          TEST_MULTI_ROW)
  // Production MDS transaction types: IDs start from 19 (17-18 reserved, see ObTxDataSourceType)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletCreateMdsHelper,\
                                          ::oceanbase::storage::mds::ObTabletCreateMdsCtx,\
                                          19,\
                                          CREATE_TABLET_NEW_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletDeleteMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          20,\
                                          DELETE_TABLET_NEW_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletUnbindMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          21,\
                                          UNBIND_TABLET_NEW_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletStartTransferOutHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          22,\
                                          START_TRANSFER_OUT)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletStartTransferInHelper,\
                                          ::oceanbase::storage::mds::ObStartTransferInMdsCtx,\
                                          23,\
                                          START_TRANSFER_IN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletFinishTransferOutHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          24,\
                                          FINISH_TRANSFER_OUT)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletFinishTransferInHelper,\
                                          ::oceanbase::storage::mds::ObFinishTransferInMdsCtx,\
                                          25,\
                                          FINISH_TRANSFER_IN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::share::ObBalanceTaskMDSHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          26,\
                                          TRANSFER_TASK)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletStartTransferOutPrepareHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          27,\
                                          START_TRANSFER_OUT_PREPARE)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletStartTransferOutV2Helper,\
                                          ::oceanbase::storage::ObTransferOutTxCtx,\
                                          28,\
                                          START_TRANSFER_OUT_V2)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObStartTransferMoveTxHelper,\
                                          ::oceanbase::storage::ObTransferMoveTxCtx,\
                                          29,\
                                          TRANSFER_MOVE_TX_CTX)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObStartTransferDestPrepareHelper,\
                                          ::oceanbase::storage::ObTransferDestPrepareTxCtx,\
                                          30,\
                                          TRANSFER_DEST_PREPARE)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletUnbindLobMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          31,\
                                          UNBIND_LOB_TABLET)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObChangeTabletToTableHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          32,\
                                          CHANGE_TABLET_TO_TABLE_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletSplitMdsHelper,\
                                           ::oceanbase::storage::mds::MdsCtx,\
                                           33,\
                                           TABLET_SPLIT)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletAbortTransferHelper,\
                                          ::oceanbase::storage::mds::ObAbortTransferInMdsCtx,\
                                          34,\
                                          TRANSFER_IN_ABORTED)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::share::ObUpgradeDataVersionMDSHelper, \
                                          ::oceanbase::storage::mds::MdsCtx, \
                                          35,\
                                          STANDBY_UPGRADE_DATA_VERSION)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletBindingMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          36,\
                                          TABLET_BINDING)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObMVPublishSCNHelper,\
                                          ::oceanbase::storage::ObUnUseCtx, \
                                          37,\
                                          MV_PUBLISH_SCN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObMVNoticeSafeHelper,\
                                          ::oceanbase::storage::ObUnUseCtx, \
                                          38,\
                                          MV_NOTICE_SAFE)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObMVUpdateSCNHelper,\
                                          ::oceanbase::storage::ObUnUseCtx, \
                                          39,\
                                          MV_UPDATE_SCN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTruncateInfoMdsHelper,\
                                          ::oceanbase::storage::mds::ObTabletMdsInfoCacheCtx, \
                                          40,\
                                          SYNC_TRUNCATE_INFO) // FARM COMPAT WHITELIST
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObMVMergeSCNHelper,\
                                          ::oceanbase::storage::ObUnUseCtx, \
                                          41,\
                                          MV_MERGE_SCN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObMViewMdsOpHelper,\
                                          ::oceanbase::storage::ObMViewMdsOpCtx, \
                                          42,\
                                          MVIEW_MDS_OP)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletDDLCompleteMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          43,\
                                          DDL_COMPLETE_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletSplitInfoMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          44,\
                                          TABLET_SPLIT_INFO)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTTLFilterInfoMdsHelper,\
                                          ::oceanbase::storage::mds::ObTabletMdsInfoCacheCtx, \
                                          45,\
                                          SYNC_TTL_FILTER_INFO) // FARM COMPAT WHITELIST
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletRandomMdsHelper,\
                                           ::oceanbase::storage::mds::MdsCtx,\
                                           47,\
                                           TABLET_RANDOM)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::share::ObSyncStandbyStatusMDSHelper, \
                                       ::oceanbase::storage::mds::MdsCtx, \
                                       48,\
                                       SYNC_STANDBY_STATUS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletAsyncIndexRefreshInfoMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          49,\
                                          ASYNC_INDEX_REFRESH_INFO)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObLsMdsTabletRelocMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          50,\
                                          LS_MDS_TABLET_RELOC_BATCH)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletMediumCompactionInfoMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          51,\
                                          MEDIUM_COMPACTION_INFO)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletForkSrcMdsHelper, \
                                          ::oceanbase::storage::mds::MdsCtx, \
                                          52,\
                                          FORK_TABLE_SRC_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletForkDestMdsHelper, \
                                          ::oceanbase::storage::mds::MdsCtx, \
                                          53,\
                                          FORK_TABLE_DEST_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletForkFinishMdsHelper, \
                                          ::oceanbase::storage::mds::MdsCtx, \
                                          54,\
                                          FORK_TABLE_FINISH_MDS)
  // # 余留位置（此行之前占位）
#undef GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
#endif
/**************************************************************************************************/

#if defined (NEED_MDS_REGISTER_DEFINE) && !defined (NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION)
#ifndef PLACE_DEFINE
#define PLACE_DEFINE
namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{
namespace mds
{
struct BufferCtx;
}
}

#define DEFINE_EMPTY(HELPER_TYPE) \
class HELPER_TYPE \
{ \
public: \
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx) \
  { \
    (void)buf; \
    (void)len; \
    (void)ctx; \
    return 0; \
  } \
  static int on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx) \
  { \
    (void)buf; \
    (void)len; \
    (void)scn; \
    (void)ctx; \
    return 0; \
  } \
};

namespace storage
{
  DEFINE_EMPTY(ObTabletRandomMdsHelper)
}
namespace share
{
  DEFINE_EMPTY(ObSyncStandbyStatusMDSHelper)
}

#undef DEFINE_EMPTY

} // namespace oceanbase
#endif // PLACE_DEFINE
#endif

/**********************generate mds frame code with multi source data******************************/
// GENERATE_MDS_UNIT(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION) is an
// interface MACRO to transfer necessary information to MDS FRAME to generate codes on MdsTable.
// 1. There are some requirements about class type KEY_TYPE and VALUE_TYPE, if your type is not
//    satisfied with below requirments, COMPILE ERROR will occured.
// 2. Register production Key/Value pairs in GENERATE_LS_INNER_MDS_TABLE (LS-level MDS tablet).
//
// @param KEY_TYPE if you do not need multi row specfied by key, use DummyKey, otherwise, your Key
//                 type must support:
//                 1. has both [bool T::operator==(const T &)] and [bool T::operator<(const T &)],
//                    or has [int T::compare(const Key &, bool &)]
//                 2. be able to_string: [int64_t T::to_string(char *, const int64_t)]
//                 3. serializeable(the classic 3 serialize methods)
//                 4. copiable, Frame code will try call(ordered):
//                    a. copy construction: [T::T(const T &)]
//                    b. assign operator: [T &T::operator=(const T &)]
//                    c. assign method: [int T::assign(const T &)]
//                    d. COMPILE ERROR if neither of above method exists.
// @param VALUE_TYPE must support:
//                   1. be able to string: [int64_t to_string(char *, const int64_t)]
//                   2. serializeable(the classic 3 serialize methods)
//                   3. copiable as KEY_TYPE
//                   4. (optional) if has [void on_redo(const share::SCN&)] will be called.
//                      (optional) if has [void on_prepare(const share::SCN&)] will be called.
//                      (optional) if has [void on_commit(const share::SCN&)] will be called.
//                      (optional) if has [void on_abort()] will be called.
//                      CAUTION: DO NOT do heavy action in these optinal functions!
//                   5. (optional) optimized if VALUE_TYPE support rvalue sematic.
// @param NEED_MULTI_VERSION if setted true, will remain multi version data,
//                           or just remain uncommitted and the last committed data.
// @param TRANSFER_RELOC whether this type participates in cross-LS transfer reloc.
// @param SPLIT_RELOC whether this type participates in tablet split reloc.
#define GENERATE_MDS_UNIT(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
_GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC)
#ifdef GENERATE_LS_INNER_MDS_TABLE
  // production mds types
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::ObTabletCreateDeleteMdsUserData,\
                    false, false, false)
  // Test MDS types for DBMS_TEST package
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::mds::ObTestMdsSingleRowValue,\
                    false, true, true)
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::ObTestMdsMultiRowKey,\
                    ::oceanbase::storage::mds::ObTestMdsMultiRowValue,\
                    false, true, true)
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::ObTabletBindingMdsUserData,\
                    false, true, false)
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::share::ObTabletAutoincSeq,\
                    false, true, false)
  GENERATE_MDS_UNIT(::oceanbase::compaction::ObMediumCompactionInfoKey,\
                    ::oceanbase::compaction::ObMediumCompactionInfo,\
                    false, true, true)
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::ObTabletSplitMdsUserData,\
                    false, true, false)
  GENERATE_MDS_UNIT(::oceanbase::storage::ObTruncateInfoKey,\
                    ::oceanbase::storage::ObTruncateInfo,\
                    false, true, true)
  GENERATE_MDS_UNIT(::oceanbase::storage::ObTabletDDLCompleteMdsUserDataKey,\
                    ::oceanbase::storage::ObTabletDDLCompleteMdsUserData,\
                    false, true, false) // FARM COMPAT WHITELIST
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::ObTabletSplitInfoMdsUserData,\
                    false, true, false)
  GENERATE_MDS_UNIT(::oceanbase::storage::ObTTLFilterInfoKey,\
                    ::oceanbase::storage::ObTTLFilterInfo,\
                    false, true, true)
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::ObTabletAsyncIndexRefreshInfo,\
                    false, true, false)
  GENERATE_MDS_UNIT(::oceanbase::storage::ObTabletForkSrcMdsKey,\
                    ::oceanbase::storage::ObTabletForkSrcMdsData,\
                    false, false, false) // FARM COMPAT WHITELIST
  // reloc final marker：与 TabletStatus 解耦的独立 unit，代际由 rowkey 第二列承担；
  // marker 本身不参与 transfer/split 搬迁
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::ObTabletMdsRelocStatus,\
                    false, false, false)
  // # 余留位置（此行之前占位）
#endif
/**************************************************************************************************/
