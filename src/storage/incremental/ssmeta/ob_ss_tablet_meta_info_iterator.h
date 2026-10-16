/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_SS_TABLET_META_INFO_ITERATOR_H_
#define OCEANBASE_SHARED_STORAGE_SS_TABLET_META_INFO_ITERATOR_H_

#include "share/ob_define.h"
#include "common/ob_tablet_id.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
class ObSSTable;
class ObSSTableMacroInfo;
}

namespace storage
{
class ObITable;
class ObMetaDiskAddr;
class ObSSMetaHeadValueInfo;
class ObTieredMetadataStore;
class ObTablet;
class ObTabletHandle;
class ObTabletTableStore;
struct ObSSUpdateTabletLog;

// ObSSTabletMetaInfoIterator 产出的每一行 tablet 共享存储 meta：
//   level=1 "tablet meta"    -> tablet meta 摘要（ObTablet + head_info + data_version）
//   level=2 "table store"    -> ObTabletTableStore 摘要（读自 __all_shared_tiered_meta）
//   level=2 "storage schema" -> ObStorageSchema 摘要
//   level=3 "sstable"        -> ObSSTable 摘要（每个 sstable 一行）
//   level=4 "cg sstable"     -> ObCOSSTableV2 摘要（每个 cg sstable 一行）
// type_ / meta_value_ 借用 iterator arena 的内存，reset/析构前有效。
struct ObSSTabletMetaInfoRow
{
  ObSSTabletMetaInfoRow() : level_(0), type_(), meta_value_() {}
  int64_t level_;
  common::ObString type_;
  common::ObString meta_value_;
  TO_STRING_KV(K_(level), K_(type), K_(meta_value));
};

// ObSSTabletMetaInfoIterator : 对应 __all_virtual_shared_tablet_meta_info 虚表的行生成器。
// 给定 5 元组 (tenant_id, ls_id, tablet_id, transfer_scn, meta_scn)，定位 __all_shared_meta 里
// 的那一条 tablet-meta 行，再按 tablet meta / table store / storage schema / 各 sstable / 各 cg sstable
// 依次产出一行。
// init() 须在目标租户 MTL 上下文内调用（与虚表一致：底层 ObSSMetaRawRowIter /
// ObTieredMetadataStore 依赖 MTL_ID() 和传入的 sql_client）。tablet meta 不存在不算错，
// 迭代器为空、get_next 返回 OB_ITER_END。
//
// 用法：
//   ObSSTabletMetaInfoIterator iter;
//   MTL_SWITCH(tenant_id) {
//     if (OB_FAIL(iter.init(*GCTX.sql_proxy_, tenant_id, ls_id, tablet_id,
//                           transfer_scn, meta_scn,
//                           obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE))) { ... }
//   }
//   ObSSTabletMetaInfoRow row;
//   while (OB_SUCC(iter.get_next(row))) { /* use row */ }
class ObSSTabletMetaInfoIterator
{
public:
  static const int64_t ROWKEY_COL_COUNT = 5;
  // 每行格式化 meta_value 的复用 scratch buffer。16K 够大以承载 ob_admin RPC 的丰富摘要；
  // init() 中堆分配，避免在 512K worker 栈上构造 iterator 时栈溢出。
  static const int64_t META_VALUE_BUF_LEN = 16 * 1024;
  // FULL 模式格式化 buffer 的扩张上限（单行上限），防止冗长 to_string() 吃光租户内存；
  // SIMPLE 用 META_VALUE_BUF_LEN。
  static const int64_t FULL_MAX_BUF_LEN = 64 * 1024 * 1024;
  // FULL/DETAIL 模式 str_alloc_ 总分配上限（所有行 meta_value_ 深拷贝之和），防止大 tablet
  // 在 FULL 下产生过多行导致单租户内存尖峰。512M ≈ 8 行满配 64M 行。
  static const int64_t FULL_STR_ALLOC_MAX_LEN = 512 * 1024 * 1024;

  ObSSTabletMetaInfoIterator();
  ~ObSSTabletMetaInfoIterator();

  // 定位 tablet meta 并收集所有 sub-meta 行。须在 MTL_SWITCH(tenant_id) 内调用。
  // tablet meta 不存在不算错：迭代器为空，get_next 返回 OB_ITER_END。
  // mode==FULL 时 sstable/cg sstable 行追加实际宏块 id；DETAIL 只 to_string；SIMPLE 摘要。
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &transfer_scn,
           const share::SCN &meta_scn,
           const obrpc::ObDumpSSTabletMetaInfoMode mode);

  // 返回下一行；结束时返回 OB_ITER_END。
  int get_next(ObSSTabletMetaInfoRow &row);

  bool is_inited() const { return is_inited_; }
  void reset();

private:
  // 前置声明：emit_table_store_sub_meta_ / emit_schema_sub_meta_ 的声明早于下面的完整定义。
  struct SubMetaEmitCtx;
  // 驱动整个收集流程（对应虚表 generate_virtual_rows_ 的本地分支 + collect_and_fill_sub_meta_），填充 rows_。
  int generate_rows_();
  int collect_and_fill_sub_meta_(const common::ObString &tablet_meta_value,
                                 const ObSSMetaHeadValueInfo &head_info);
  // 反序列化 tablet meta_value 得到 ObTablet handle，并取出其内嵌的 storage schema /
  // table store 两个 sub-meta addr，以及同行的 ObSSUpdateTabletLog。
  // 老行可能不带 update_log，此时保持 invalid，调用方打印前需 is_valid() 判断。
  int build_tablet_handle_and_sub_meta_addrs_(const common::ObString &tablet_meta_value,
                                              ObTabletHandle &tablet_handle,
                                              ObMetaDiskAddr &schema_addr,
                                              ObMetaDiskAddr &table_store_addr,
                                              ObSSUpdateTabletLog &update_log);
  // 从 tablet meta_value 反推 data_version（取自 ObTabletBlockHeader），仅用于 SIMPLE 摘要行。
  void get_tablet_data_version_(const common::ObString &tablet_meta_value,
                                uint64_t &data_version);
  // 把 tablet-meta 行格式化并产出（add_row_）。SIMPLE 调 format_tablet_meta_summary_（单层 {}，已内嵌
  // head_info 与 data_version）；DETAIL/FULL 拼成 {<tablet>,<head_info>[,<update_log>]}，update_log 仅
  // valid 时才加入。DETAIL/FULL 经 full_scratch_ 扩容，buffer 可能搬到 full_scratch_，故 add_row_（深拷贝）
  // 在本函数内完成、用扩容后的指针。
  int emit_tablet_meta_row_(const ObTablet &tablet,
                            const ObSSMetaHeadValueInfo &head_info,
                            const uint64_t data_version,
                            const ObSSUpdateTabletLog &update_log);
  // 读+反序列化 table-store sub-meta blob，产 "table store" 摘要行，再从同一 ObTabletTableStore 经
  // emit_sstable_rows_from_table_store_ 逐个产出 sstable/cg sstable 行。sstable meta 内联在此 blob，
  // 无需二次 tiered 读；须在此 blob 存活期内（sub_alloc->reuse() 释放它之前）产出 sstable 行。
  int emit_table_store_sub_meta_(const SubMetaEmitCtx &ctx,
                                 const ObMetaDiskAddr &table_store_addr,
                                 const ObTablet &tablet);
  // 读+反序列化+格式化 storage schema sub-meta，产 "storage schema" 摘要行。与
  // emit_table_store_sub_meta_ 同形（read->deserialize->format->add_row）。
  int emit_schema_sub_meta_(const SubMetaEmitCtx &ctx,
                            const ObMetaDiskAddr &schema_addr);
  int read_tiered_meta_value_(ObTieredMetadataStore &tiered_store,
                              const ObMetaDiskAddr &addr,
                              common::ObArenaAllocator &allocator,
                              common::ObString &sub_meta_value);
  // grow buffer 公共逻辑：cap 翻倍并 clamp 到 FULL_MAX_BUF_LEN。preserve_prefix=true 时另开 block
  // 并拷贝 [0,len)（append_* 用）；false 时 reuse alloc（obj_to_string_grow_ 用）。已达上限则
  // 截断 len=cap-1 并返回 OB_SIZE_OVERFLOW，调用方停止扩容重试。
  int grow_buf_cap_(common::ObArenaAllocator &alloc,
                    char *&buf, int64_t &cap, int64_t &len,
                    const bool preserve_prefix);
  // FULL 模式：把对象的完整 to_string() 写入可扩张 buffer（不截断）。buf/cap/len 为 in-out：
  // buf 初始用 meta_value_buf_(16K)，截断时在 alloc(full_scratch_) 上翻倍扩容直到写完或到
  // FULL_MAX_BUF_LEN；add_row_ 会先深拷贝结果再让下一行复用 arena。
  template <typename T>
  int obj_to_string_grow_(common::ObArenaAllocator &alloc,
                          const T &obj,
                          char *&buf, int64_t &cap, int64_t &len)
  {
    int ret = OB_SUCCESS;
    len = 0;
    if (OB_ISNULL(buf) || cap <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (OB_SUCC(ret)) {
        int64_t n = obj.to_string(buf, cap);
        if (n < 0) {
          n = 0;
        }
        // OB to_string() 返回值最多截到 buf_len-1（溢出时 databuff_vprintf 把 pos 置为 buf_len-1），
        // 不会报真实长度。因此返回 cap-1 时无法区分「正好写满」还是「被截断」，一律按截断扩容；
        // 只有 n < cap-1 才确定写完。极少数正好写满的情况最多多扩一次。
        if (n < cap - 1) {
          buf[n] = '\0';
          len = n;
          break;
        }
        if (OB_FAIL(grow_buf_cap_(alloc, buf, cap, len, false))) {
          if (OB_SIZE_OVERFLOW == ret) {
            ret = OB_SUCCESS;
            break;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      len = 0;
      if (OB_NOT_NULL(buf) && cap > 0) {
        buf[0] = '\0';
      }
    }
    return ret;
  }
  // FULL 模式：在已开始的可扩张 buffer 偏移 len 处追加一个对象的 to_string()，用于拼装
  // tablet-meta 行（ObTablet + head_info + update_log）。与 obj_to_string_grow_ 不同，这里
  // 绝不 reuse alloc（前缀 [0,len) 必须在扩容后存活）：截断时另开更大 block、拷贝前缀、重试，
  // 旧 block 等 reset() 回收。buf/cap/len 为 in-out，调用前先 reuse alloc 一次释放上一行残留。
  template <typename T>
  int append_obj_to_string_grow_(common::ObArenaAllocator &alloc,
                                 const T &obj,
                                 char *&buf, int64_t &cap, int64_t &len)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || cap <= 0 || len < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (OB_SUCC(ret)) {
        const int64_t avail = cap - len;
        int64_t n = (avail > 0) ? obj.to_string(buf + len, avail) : 0;
        if (n < 0) {
          n = 0;
        }
        // OB to_string() 返回值最多截到 avail-1，n == avail-1 时无法区分（正好写满 vs 截断），
        // 一律扩容；只有 n < avail-1 才确定写完。详见 obj_to_string_grow_。
        if (n < avail - 1) {
          buf[len + n] = '\0';
          len += n;
          break;
        }
        if (OB_FAIL(grow_buf_cap_(alloc, buf, cap, len, true))) {
          if (OB_SIZE_OVERFLOW == ret) {
            ret = OB_SUCCESS;
            break;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(buf) && cap > 0) {
        buf[0] = '\0';
      }
      len = 0;
    }
    return ret;
  }
  // FULL 模式：在 buf+len 处追加字面量 C 串（把各对象 to_string() 拼成 {<o1>,<o2>,<o3>} 的括号/逗号），
  // 截断时同样扩容（不复用 alloc，另开 block 拷前缀重试）。buf/cap/len 为 in-out。
  int append_cstr_grow_(common::ObArenaAllocator &alloc,
                        const char *s,
                        char *&buf, int64_t &cap, int64_t &len);
  // 格式化一个 sub-meta 对象并追加一行。SIMPLE 调 summary_fn(obj, buf, cap)（截到 cap）；
  // DETAIL/FULL 经 obj_to_string_grow_ 调对象完整 to_string()（在 full_scratch_ 上扩容，可超 META_VALUE_BUF_LEN）。
  // buf/cap 为 in-out，DETAIL/FULL 扩容后 buf 会指向更大 block，add_row_ 再深拷贝进 str_alloc_。
  template <typename T>
  int format_and_add_row_(const int64_t level,
                          const common::ObString &type,
                          const T &obj,
                          int64_t (*summary_fn)(const T &, char *, const int64_t))
  {
    int ret = OB_SUCCESS;
    char *buf = meta_value_buf_;
    int64_t cap = META_VALUE_BUF_LEN;
    int64_t len = 0;
    if (obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE != mode_) {
      if (OB_FAIL(obj_to_string_grow_(full_scratch_, obj, buf, cap, len))) {
        STORAGE_LOG(WARN, "grow to_string failed", KR(ret), K(level), K(type));
      }
    } else {
      len = summary_fn(obj, buf, cap);
      if (len < 0) {
        len = 0;
      } else if (len >= cap) {
        len = cap - 1;
      }
      buf[len] = '\0';
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_row_(level, type, common::ObString(len, buf)))) {
        STORAGE_LOG(WARN, "add row failed", KR(ret), K(level), K(type));
      }
    }
    return ret;
  }
  // collect_and_fill_sub_meta_ 中读 table-store / storage-schema sub-meta 的上下文：
  // tiered reader、两个复用的 scratch arena。在 collect_and_fill_sub_meta_ 中一次性构造。
  struct SubMetaEmitCtx
  {
    SubMetaEmitCtx() : tiered_store(nullptr),
                       tiered_alloc(nullptr), sub_alloc(nullptr) {}
    ObTieredMetadataStore *tiered_store;
    common::ObArenaAllocator *tiered_alloc;
    common::ObArenaAllocator *sub_alloc;
  };
  // 从 tiered store 读一条 sub-meta blob 到 `blob`，按 addr.size() 从 ctx.tiered_alloc 分配。
  // 仅 OBJECT_NOT_EXIST 记日志后吞掉；SIZE_OVERFLOW 转为 OB_INVALID_DATA，其他错误原样向上返回。
  // 读前 reuse ctx.tiered_alloc。成功或对象不存在时返回 OB_SUCCESS（调用方据 `blob` 是否空区分）。
  int read_sub_meta_blob_(const SubMetaEmitCtx &ctx,
                          const ObMetaDiskAddr &addr,
                          common::ObString &blob);
  // 对反序列化后的 table store 里每个 sstable 产出一行 "sstable"/"cg sstable"，直接用已反序列化的
  // sstable 对象格式化（不再二次 tiered 读）。须在任何 sub_alloc->reuse() 释放 table store 之前调用。
  // mode==FULL 时，行 = {<sst.to_string()>, macro_block_ids:{...}}，追加段由 append_macro_block_ids_ 生成。
  int emit_sstable_rows_from_table_store_(const ObTabletTableStore &ts);
  // 对 table store 里的单个 sstable 产出一行 "sstable"/"cg sstable"，直接用已反序列化的对象
  // 格式化（不二次 tiered 读）。mode==FULL 时行 = {<sst.to_string()>, macro_block_ids:{...}}；
  // DETAIL 经 to_string；SIMPLE 调对应 curated 摘要。co 与非 co 决定对象 + 格式化器 + level/type。
  int emit_one_sstable_row_(const ObITable &table);
  // FULL 模式：把 sst 的 ObSSTableMacroInfo 三数组实际宏块 id 追加到 buf+len，形如
  // {data_block_ids:{count:N,ids:[...]},other_block_ids:{...},linked_block_ids:{...}}。
  // get_meta 失败或某数组迭代失败：记 WARN 打印，行仍产出。buf/cap/len 为 in-out。
  int append_macro_block_ids_(const blocksstable::ObSSTable &sst,
                              common::ObArenaAllocator &alloc,
                              char *&buf, int64_t &cap, int64_t &len);
  // 辅助：把 mi 的某个数组（type: ObMacroIdIterator::DATA_BLOCK/OTHER_BLOCK/LINKED_BLOCK）
  // 的实际宏块 id 追加为 name:{count:N,ids:[...]}。count 经 get_*_block_count 取（失败置 -1），ids 经迭代器遍历。
  int append_block_id_array_(const char *name, int type,
                             const blocksstable::ObSSTableMacroInfo &mi,
                             common::ObArenaAllocator &alloc,
                             char *&buf, int64_t &cap, int64_t &len);
  // 把格式化好的 (level, type, meta_value) 三元组深拷贝进 str_alloc_ 并追加到 rows_。
  // FULL 模式 meta_value 可能很长（格式化器自身可扩到 FULL_MAX_BUF_LEN），整体拷贝，不截到 META_VALUE_BUF_LEN。
  int add_row_(const int64_t level, const common::ObString &type,
               const common::ObString &meta_value);

private:
  bool is_inited_;
  bool collected_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  share::SCN meta_scn_;
  obrpc::ObDumpSSTabletMetaInfoMode mode_;
  common::ObISQLClient *sql_client_;
  // 整个收集周期持有反序列化 tablet + tiered 读 buffer。
  common::ObArenaAllocator allocator_;
  // 持有每行的 type_ / meta_value_ 字符串，存活到 reset()/析构。
  common::ObArenaAllocator str_alloc_;
  // FULL 模式 scratch，用于把 sstable/cg to_string() 扩过 META_VALUE_BUF_LEN；
  // 每行复用，add_row_ 时深拷贝进 str_alloc_。
  common::ObArenaAllocator full_scratch_;
  common::ObSEArray<ObSSTabletMetaInfoRow, 16> rows_;
  int64_t cur_idx_;
  // str_alloc_ 达到 FULL_STR_ALLOC_MAX_LEN 后置 true，后续 add_row_ 直接跳过并打一次 WARN。
  bool str_alloc_limit_reached_;
  // 每行格式化复用的 scratch buffer，add_row_ 时拷进 str_alloc_。
  // init() 中从 allocator_ 堆分配（16K 不放栈上），allocator_ reset 时释放。
  char *meta_value_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletMetaInfoIterator);
};

} // storage
} // oceanbase
#endif // OCEANBASE_SHARED_STORAGE_SS_TABLET_META_INFO_ITERATOR_H_
