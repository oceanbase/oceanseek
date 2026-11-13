/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_JSON_TABLE_OP_H_
#define OCEANBASE_BASIC_OB_JSON_TABLE_OP_H_

#include "sql/engine/ob_operator.h"
#include "lib/charset/ob_charset.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_path.h"
#include "lib/json_type/ob_json_bin.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_json_param_type.h"
#include "sql/engine/expr/ob_expr_json_utils.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "deps/oblib/src/lib/xml/ob_xml_util.h"
#include "share/search_index/ob_search_index_generator.h"
#include "lib/ai_split_document/ob_ai_split_document.h"
#include "lib/ai_split_document/ob_ai_split_document_util.h"

namespace oceanbase
{
namespace sql
{
struct JtScanCtx;
class ObExpr;
struct JtScanCtx;
class ObJsonTableSpec;
class MulModeTableFunc;
class JoinNode;
class ObRegCol;
class ScanNode;
class UnionNode;
class ObMultiModeTableNode;

enum table_type : int8_t {
  OB_INVALID_TABLE = 0,
  OB_JSON_TABLE = 1,
  OB_XML_TABLE = 2,
  OB_RB_ITERATE_TABLE = 3,
  OB_UNNEST_TABLE = 4,
  OB_AI_SPLIT_DOCUMENT_TABLE = 5,
  OB_INDEX_DATA_GEN_TABLE = 6,
};

typedef enum JtNodeType {
  REG_TYPE, // ordinality && reg type
  SCAN_TYPE, // scan node type
  JOIN_TYPE, // join node
  UNION_TYPE, // scan node
} JtNodeType;

typedef enum ResultType {
  NOT_DATUM,
  EMPTY_DATUM,
  ERROR_DATUM,
  // curr_ points to ObJsonWrapper* (non-datum result), parallel to NOT_DATUM for ObIJsonBase*.
  NOT_DATUM_WRAPPER
} ResultType;

typedef enum ScanType{  // symbol of execute stage, scan node or col node
  SCAN_NODE_TYPE,
  COL_NODE_TYPE,
} ScanType;

typedef struct ObJtColInfo
{
  ObJtColInfo();
  ObJtColInfo(const ObJtColInfo& info);

  int32_t col_type_;
  int32_t truncate_;
  int32_t format_json_;
  int32_t wrapper_;
  int32_t allow_scalar_;
  int64_t output_column_idx_;
  int64_t empty_expr_id_;
  int64_t error_expr_id_;
  ObString col_name_;
  ObString path_;
  int32_t on_empty_;
  int32_t on_error_;
  int32_t on_mismatch_;
  int32_t on_mismatch_type_;
  int64_t res_type_;
  ObDataType data_type_;
  int32_t parent_id_;
  int32_t id_;

  int from_JtColBaseInfo(const ObJtColBaseInfo& info);
  int deep_copy(const ObJtColInfo& src, ObIAllocator* allocator);
  int serialize(char *buf, int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(col_type), K_(format_json), K_(wrapper), K_(allow_scalar),
   K_(output_column_idx), K_(col_name), K_(path), K_(parent_id), K_(id));
} ObJtColInfo;

struct JtColTreeNode {
  JtColTreeNode(const ObJtColInfo& info)
    : col_base_info_(info),
      regular_cols_(),
      nested_cols_() {}
  void destroy();
  ObJtColInfo col_base_info_;
  common::ObSEArray<JtColTreeNode*, 4, common::ModulePageAllocator, true> regular_cols_;
  common::ObSEArray<JtColTreeNode*, 4, common::ModulePageAllocator, true> nested_cols_;

  TO_STRING_KV(K_(col_base_info));
} ;

class ObJsonTableSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObJsonTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      value_exprs_(alloc),
      column_exprs_(alloc),
      emp_default_exprs_(alloc),
      err_default_exprs_(alloc),
      has_correlated_expr_(false),
      alloc_(&alloc),
      cols_def_(alloc),
      table_type_(MulModeTableType::OB_ORA_JSON_TABLE_TYPE),
      namespace_def_(alloc),
      inc_pk_proj_(0),
      index_column_cnt_(0),
      search_idx_included_cid_idxes_(alloc),
      column_comments_(alloc),
      search_index_target_(share::SEARCH_DATA) {}

  int dup_origin_column_defs(common::ObIArray<ObJtColBaseInfo*>& columns);
  int construct_tree(common::ObArray<ObMultiModeTableNode*> all_nodes, JoinNode* parent);
  int construct_tree(common::ObArray<ObMultiModeTableNode*> all_nodes, UnionNode* parent);
  int gen_gin_included_cid_idxes();
  common::ObFixedArray<ObExpr*, common::ObIAllocator> value_exprs_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_; // 列输出表达式
  common::ObFixedArray<ObExpr*, common::ObIAllocator> emp_default_exprs_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> err_default_exprs_;
  bool has_correlated_expr_; //是否是变量输入，用在算子rescan中，同function table
  ObIAllocator* alloc_;

  common::ObFixedArray<ObJtColInfo*, common::ObIAllocator> cols_def_;
  MulModeTableType table_type_;
  common::ObFixedArray<ObString, common::ObIAllocator> namespace_def_;
  int64_t inc_pk_proj_;
  int64_t index_column_cnt_;
  common::ObFixedArray<int32_t, common::ObIAllocator> search_idx_included_cid_idxes_;
  common::ObFixedArray<ObString, common::ObIAllocator> column_comments_;
  share::ObSearchIndexRowTarget search_index_target_;
};

class ObJsonTableOp;

struct JtScanCtx {
    JtScanCtx()
      : row_alloc_(),
        mem_ctx_(nullptr),
        op_exec_alloc_(nullptr),
        context(nullptr),
        table_func_(nullptr),
        default_ns() {}
  
  bool is_xml_table_func() {
    return spec_ptr_->table_type_ == OB_ORA_XML_TABLE_TYPE;
  }
  bool is_json_table_func() {
    return spec_ptr_->table_type_ == OB_ORA_JSON_TABLE_TYPE;
  }

  bool is_rb_iterate_table_func() {
    return spec_ptr_->table_type_ == OB_RB_ITERATE_TABLE_TYPE;
  }

  bool is_unnest_table_func() {
    return spec_ptr_->table_type_ == OB_UNNEST_TABLE_TYPE;
  }

  bool is_index_data_gen_table_func() {
    return spec_ptr_->table_type_ == OB_INDEX_DATA_GEN_TABLE_TYPE;
  }

  bool is_ai_split_document_table_func() {
    return spec_ptr_->table_type_ == OB_AI_SPLIT_DOCUMENT_TABLE_TYPE;
  }

  ObJsonTableSpec* spec_ptr_;
  ObEvalCtx* eval_ctx_;
  ObExecContext* exec_ctx_;
  common::ObArenaAllocator row_alloc_;
  ObMulModeMemCtx *mem_ctx_;
  ObMulModeMemCtx *xpath_ctx_;
  ObIAllocator *op_exec_alloc_;
  ObJsonTableOp* jt_op_;
  void *context;      // xml ns prefix_ns
  MulModeTableFunc *table_func_;
  ObString default_ns;

  bool is_evaled_;
  bool is_cover_error_;
  bool is_need_end_; // parse input json doc fail will affect by column 
  bool is_charset_converted_;
  bool is_const_input_;
  int error_code_; 
  int32_t ord_val_;
  ObDatum* res_obj_;
  ObDatum** data_;
  char buf[OB_MAX_DECIMAL_PRECISION];

};

class ObJsonTableOp : public ObOperator
{
public:
  static const int64_t DEFAULT_GEN_ROW_COUNT = 32;
  typedef common::ObSEArray<blocksstable::ObDatumRow*, DEFAULT_GEN_ROW_COUNT> ObIndexGenRow;
  ObJsonTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      def_root_(nullptr),
      allocator_(&exec_ctx.get_allocator()),
      is_inited_(false),
      is_evaled_(false),
      root_(nullptr),
      input_(nullptr),
      input_wrapper_(),
      enable_json_bin_view_(true),
      j_null_(),
      j_arr_(allocator_),
      j_obj_(allocator_),
      row_idx_(0),
      row_(),
      rows_(),
      row_generator_(),
      index_gen_skip_cnt_(0)
  {
    const ObJsonTableSpec* spec_ptr = reinterpret_cast<const ObJsonTableSpec*>(&spec);
    col_count_ = spec_ptr->column_exprs_.count();
  }

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  //virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
  ObJsonNull* get_js_null() { return &j_null_; }
  ObJsonArray* get_js_array() { return &j_arr_; }
  ObJsonObject* get_js_object() { return &j_obj_; }
  void* get_root_param() { return input_; }
  JoinNode* get_root_entry() { return root_; }
  TO_STRING_KV(K_(is_inited),
               K_(col_count));

private:
  int init();
  int reset_variable();  // reset var in rescan 
  int init_data_obj(); // allocate data_ array
  int generate_column_trees(JtColTreeNode*& root);
  int find_column(int32_t id, JtColTreeNode* root, JtColTreeNode*& col);
  int generate_table_exec_tree();
  int generate_table_exec_tree(ObIAllocator* allocator, const JtColTreeNode& orig_col,
                               JoinNode*& join_col);
  int inner_get_next_row_jt();  // json table function inner
  int get_next_index_gen_row();
  int init_row_generator();
  int generate_index_gen_rows();
  int fill_index_gen_column_exprs(blocksstable::ObDatumRow &row);
  bool is_async_refresh_skip_row_error_(const int err) const;
  void log_async_refresh_skip_row_(const int err);

private:
  JtColTreeNode* def_root_;
  common::ObIAllocator *allocator_;
  uint32_t col_count_;
  bool is_inited_;
  bool is_evaled_;
  JoinNode* root_;  // mul mode table
  JtScanCtx jt_ctx_;

public:
  void *input_;   // mul mode table
  common::ObJsonWrapper input_wrapper_;
  bool enable_json_bin_view_;
  void reset_columns();

private:
  ObJsonNull j_null_;
  ObJsonArray j_arr_;
  ObJsonObject j_obj_;

private:
  uint32_t row_idx_;
  blocksstable::ObDatumRow row_;
  ObIndexGenRow rows_;
  share::ObSearchIndexRowGenerator row_generator_;
  int64_t index_gen_skip_cnt_;  // 增量刷新跳过行计数
};

class MulModeTableFunc {
public:
  using InitCtxFunc = int (*)(ObRegCol &scan_node, JtScanCtx*& ctx);
  using EvalDefaultValueFunc = int (*)(JtScanCtx*& ctx,
                                       ObExpr*& default_expr,
                                       void*& res,
                                       bool need_datum);
  using ContainerAtFunc = int (*)(void* in, void *&out, int32_t pos);
  using EvalInputFunc = int (*)(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx);
  using ResetPathIterFunc = int (*)(ObRegCol &scan_node,
                                    void* in,
                                    JtScanCtx*& ctx,
                                    ScanType init_flag,
                                    bool &is_null_value);
  using GetIterValueFunc = int (*)(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value);
  using EvalSeekColFunc = int (*)(ObRegCol &col_node,
                                  void* in,
                                  JtScanCtx* ctx,
                                  bool &is_null_value,
                                  bool &need_cast_res);
  using ColResTypeCheckFunc = int (*)(ObRegCol &col_node, JtScanCtx* ctx);
  using CheckDefaultValueFunc = int (*)(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr);
  using SetOnEmptyFunc = int (*)(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null);
  using SetOnErrorFunc = int (*)(ObRegCol& col_node, JtScanCtx* ctx, int& ret);
  using CastToResultFunc = int (*)(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error, bool is_pack_result);
  using SetExprExecParamFunc = int (*)(ObRegCol& col_node, JtScanCtx* ctx);
  using ResetCtxFunc = int (*)(ObRegCol &scan_node, JtScanCtx*& ctx);

  MulModeTableFunc()
    : init_ctx_func_(nullptr),
      eval_default_value_func_(nullptr),
      container_at_func_(nullptr),
      eval_input_func_(nullptr),
      reset_path_iter_func_(nullptr),
      get_iter_value_func_(nullptr),
      eval_seek_col_func_(nullptr),
      col_res_type_check_func_(nullptr),
      check_default_value_func_(nullptr),
      set_on_empty_func_(nullptr),
      set_on_error_func_(nullptr),
      cast_to_result_func_(nullptr),
      set_expr_exec_param_func_(nullptr),
      reset_ctx_func_(nullptr)
  {}
  ~MulModeTableFunc() {}

  OB_INLINE int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx)
  {
    return OB_ISNULL(init_ctx_func_)
             ? OB_SUCCESS : init_ctx_func_(scan_node, ctx);
  }
  OB_INLINE int eval_default_value(JtScanCtx*& ctx,
                                   ObExpr*& default_expr,
                                   void*& res,
                                   bool need_datum)
  {
    return OB_ISNULL(eval_default_value_func_)
             ? OB_SUCCESS : eval_default_value_func_(ctx, default_expr, res, need_datum);
  }
  OB_INLINE int container_at(void* in, void *&out, int32_t pos)
  {
    return OB_ISNULL(container_at_func_)
             ? OB_SUCCESS : container_at_func_(in, out, pos);
  }
  OB_INLINE int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx)
  {
    return OB_ISNULL(eval_input_func_)
             ? OB_SUCCESS : eval_input_func_(jt, ctx, eval_ctx);
  }
  OB_INLINE int reset_path_iter(ObRegCol &scan_node,
                                void* in,
                                JtScanCtx*& ctx,
                                ScanType init_flag,
                                bool &is_null_value)
  {
    return OB_ISNULL(reset_path_iter_func_)
             ? OB_SUCCESS : reset_path_iter_func_(scan_node, in, ctx, init_flag, is_null_value);
  }
  OB_INLINE int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value)
  {
    return OB_ISNULL(get_iter_value_func_)
             ? OB_SUCCESS : get_iter_value_func_(col_node, ctx, is_null_value);
  }
  OB_INLINE int eval_seek_col(ObRegCol &col_node,
                              void* in,
                              JtScanCtx* ctx,
                              bool &is_null_value,
                              bool &need_cast_res)
  {
    return OB_ISNULL(eval_seek_col_func_)
             ? OB_SUCCESS : eval_seek_col_func_(col_node, in, ctx, is_null_value, need_cast_res);
  }
  OB_INLINE int col_res_type_check(ObRegCol &col_node, JtScanCtx* ctx)
  {
    return OB_ISNULL(col_res_type_check_func_)
             ? OB_SUCCESS : col_res_type_check_func_(col_node, ctx);
  }
  OB_INLINE int check_default_value(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr)
  {
    return OB_ISNULL(check_default_value_func_)
             ? OB_SUCCESS : check_default_value_func_(ctx, col_node, expr);
  }
  OB_INLINE int set_on_empty(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null)
  {
    return OB_ISNULL(set_on_empty_func_)
             ? OB_SUCCESS : set_on_empty_func_(col_node, ctx, need_cast, is_null);
  }
  OB_INLINE int set_on_error(ObRegCol& col_node, JtScanCtx* ctx, int& ret)
  {
    return OB_ISNULL(set_on_error_func_)
             ? OB_SUCCESS : set_on_error_func_(col_node, ctx, ret);
  }
  OB_INLINE int cast_to_result(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error = false, bool is_pack_result = true)
  {
    return OB_ISNULL(cast_to_result_func_)
             ? OB_SUCCESS : cast_to_result_func_(col_node, ctx, enable_error, is_pack_result);
  }
  OB_INLINE int set_expr_exec_param(ObRegCol& col_node, JtScanCtx* ctx)
  {
    return OB_ISNULL(set_expr_exec_param_func_)
             ? OB_SUCCESS : set_expr_exec_param_func_(col_node, ctx);
  }
  OB_INLINE int reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx)
  {
    return OB_ISNULL(reset_ctx_func_)
             ? OB_SUCCESS : reset_ctx_func_(scan_node, ctx);
  }

protected:
  InitCtxFunc init_ctx_func_;
  EvalDefaultValueFunc eval_default_value_func_;
  ContainerAtFunc container_at_func_;
  EvalInputFunc eval_input_func_;
  ResetPathIterFunc reset_path_iter_func_;
  GetIterValueFunc get_iter_value_func_;
  EvalSeekColFunc eval_seek_col_func_;
  ColResTypeCheckFunc col_res_type_check_func_;
  CheckDefaultValueFunc check_default_value_func_;
  SetOnEmptyFunc set_on_empty_func_;
  SetOnErrorFunc set_on_error_func_;
  CastToResultFunc cast_to_result_func_;
  SetExprExecParamFunc set_expr_exec_param_func_;
  ResetCtxFunc reset_ctx_func_;
};

class JsonTableFunc : public MulModeTableFunc {
public:
  JsonTableFunc()
    : MulModeTableFunc()
  {
    init_ctx_func_ = &JsonTableFunc::init_ctx;
    eval_default_value_func_ = &JsonTableFunc::eval_default_value;
    container_at_func_ = &JsonTableFunc::container_at;
    eval_input_func_ = &JsonTableFunc::eval_input;
    reset_path_iter_func_ = &JsonTableFunc::reset_path_iter;
    get_iter_value_func_ = &JsonTableFunc::get_iter_value;
    eval_seek_col_func_ = &JsonTableFunc::eval_seek_col;
    col_res_type_check_func_ = &JsonTableFunc::col_res_type_check;
    check_default_value_func_ = &JsonTableFunc::check_default_value;
    set_on_empty_func_ = &JsonTableFunc::set_on_empty;
    set_on_error_func_ = &JsonTableFunc::set_on_error;
    cast_to_result_func_ = &JsonTableFunc::cast_to_result;
    set_expr_exec_param_func_ = &JsonTableFunc::set_expr_exec_param;
  }
  ~JsonTableFunc() {}

  static int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
  static int eval_default_value(JtScanCtx*& ctx, ObExpr*& default_expr, void*& res, bool need_datum);
  static int container_at(void* in, void *&out, int32_t pos);
  static int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx);
  static int reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value);
  static int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value);
  static int eval_seek_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, bool &is_null_value, bool &need_cast_res);
  static int col_res_type_check(ObRegCol &col_node, JtScanCtx* ctx);
  static int check_default_value(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr);
  static int set_on_empty(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null);
  static int set_on_error(ObRegCol& col_node, JtScanCtx* ctx, int& ret);
  static int cast_to_result(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error = false, bool is_pack_result = true);
  static int set_expr_exec_param(ObRegCol& col_node, JtScanCtx* ctx);
};

class XmlTableFunc : public MulModeTableFunc {
public:
  XmlTableFunc()
    : MulModeTableFunc()
  {
    init_ctx_func_ = &XmlTableFunc::init_ctx;
    eval_default_value_func_ = &XmlTableFunc::eval_default_value;
    container_at_func_ = &XmlTableFunc::container_at;
    eval_input_func_ = &XmlTableFunc::eval_input;
    reset_path_iter_func_ = &XmlTableFunc::reset_path_iter;
    get_iter_value_func_ = &XmlTableFunc::get_iter_value;
    eval_seek_col_func_ = &XmlTableFunc::eval_seek_col;
    col_res_type_check_func_ = &XmlTableFunc::col_res_type_check;
    check_default_value_func_ = &XmlTableFunc::check_default_value;
    set_on_empty_func_ = &XmlTableFunc::set_on_empty;
    set_on_error_func_ = &XmlTableFunc::set_on_error;
    cast_to_result_func_ = &XmlTableFunc::cast_to_result;
    reset_ctx_func_ = &XmlTableFunc::reset_ctx;
  }
  ~XmlTableFunc() {}

  static int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
  static int eval_default_value(JtScanCtx*& ctx, ObExpr*& default_expr, void*& res, bool need_datum);
  static int container_at(void* in, void *&out, int32_t pos);
  static int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx);
  static int reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value);
  static int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value);
  static int eval_seek_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, bool &is_null_value, bool &need_cast_res);
  static int col_res_type_check(ObRegCol &col_node, JtScanCtx* ctx);
  static int check_default_value(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr);
  static int set_on_empty(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null);
  static int set_on_error(ObRegCol& col_node, JtScanCtx* ctx, int& ret);
  static int cast_to_result(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error = false, bool is_pack_result = true);
  static int reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);  // reset variable if variable is not const
};

class RbIterateTableFunc : public MulModeTableFunc {
public:
  RbIterateTableFunc()
    : MulModeTableFunc()
  {
    init_ctx_func_ = &RbIterateTableFunc::init_ctx;
    eval_input_func_ = &RbIterateTableFunc::eval_input;
    reset_path_iter_func_ = &RbIterateTableFunc::reset_path_iter;
    get_iter_value_func_ = &RbIterateTableFunc::get_iter_value;
    reset_ctx_func_ = &RbIterateTableFunc::reset_ctx;
  }
  ~RbIterateTableFunc() {}

  static int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
  static int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx);
  static int reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value);
  static int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value);
  static int reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
};

class UnnestTableFunc : public MulModeTableFunc {
public:
  UnnestTableFunc()
    : MulModeTableFunc()
  {
    init_ctx_func_ = &UnnestTableFunc::init_ctx;
    eval_input_func_ = &UnnestTableFunc::eval_input;
    reset_path_iter_func_ = &UnnestTableFunc::reset_path_iter;
    get_iter_value_func_ = &UnnestTableFunc::get_iter_value;
    reset_ctx_func_ = &UnnestTableFunc::reset_ctx;
  }
  ~UnnestTableFunc() {}

  static int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
  static int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx);
  static int reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value);
  static int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value);
  static int reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
};


class AiSplitDocumentTableFunc : public MulModeTableFunc {
public:
  AiSplitDocumentTableFunc()
    : MulModeTableFunc()
  {
    init_ctx_func_ = &AiSplitDocumentTableFunc::init_ctx;
    eval_input_func_ = &AiSplitDocumentTableFunc::eval_input;
    reset_path_iter_func_ = &AiSplitDocumentTableFunc::reset_path_iter;
    get_iter_value_func_ = &AiSplitDocumentTableFunc::get_iter_value;
    reset_ctx_func_ = &AiSplitDocumentTableFunc::reset_ctx;
  }
  ~AiSplitDocumentTableFunc() {}

  static int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
  static int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx);
  static int reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value);
  static int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value);
  static int reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
};



class ObMultiModeTableNode {
public:
  ObMultiModeTableNode()
    : in_(nullptr),
      is_evaled_(false),
      is_null_result_(false) {
      node_type_ = REG_TYPE;
    }
  virtual void destroy();
  virtual int reset(JtScanCtx* ctx) {
    UNUSED(ctx);
    is_evaled_ = false;
    is_null_result_ = false;
    return OB_SUCCESS;
  }
  JtNodeType node_type() { return node_type_; }
  virtual int get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value) { return 0; }
  bool is_null_result() { return is_null_result_; }
  virtual int open(JtScanCtx* ctx) {
    return ObMultiModeTableNode::reset(ctx);
  }
  virtual void* get_curr_iter_value() { return nullptr; }

  JtNodeType node_type_;
  void* in_;
  bool is_evaled_;  // 节点是否被解析过
  bool is_null_result_;  // 节点是否返回空值

  TO_STRING_KV(K_(node_type),
               K_(is_evaled),
               K_(node_type));
};

class ObRegCol final
{
public:
  ObRegCol(const ObJtColInfo& col_info)
    : total_(0),
      path_(nullptr),
      curr_(nullptr),
      iter_(nullptr),
      cur_pos_(-1),
      ord_val_(-1),
      is_path_evaled_(false),
      is_emp_evaled_(false),
      is_err_evaled_(false),
      emp_val_(nullptr),
      err_val_(nullptr),
      value_wrapper_(),
      res_flag_(NOT_DATUM),
      expr_param_() {
      new (&col_info_) ObJtColInfo(col_info);
      node_type_ = REG_TYPE;
      is_ord_node_ = col_info_.col_type_ == COL_TYPE_ORDINALITY;
    }
  void destroy();
  int reset(JtScanCtx* ctx);
  bool is_ord_node() { return is_ord_node_;  }
  const ObJtColInfo& get_column_node_def() { return col_info_; }
  ObJtColInfo& get_column_def() { return col_info_; }
  int open(JtScanCtx* ctx);
  int eval_regular_col(void *in, JtScanCtx* ctx, bool& is_null_value); // process regular column
  JtColType type() { return static_cast<JtColType>(col_info_.col_type_); }
  JtNodeType node_type() { return node_type_; }
  
  int32_t total_;
  void *path_;
  void* curr_;
  void* iter_;
  int32_t cur_pos_;
  int32_t ord_val_;
  bool is_path_evaled_;
  ObJtColInfo col_info_;
  bool is_ord_node_;
  bool is_emp_evaled_;
  bool is_err_evaled_;
  void *emp_val_;
  void *err_val_;
  common::ObJsonWrapper value_wrapper_;
  // curr_ 的显式值类型，避免把 wrapper、Datum 和 ObIJsonBase 相互误转。
  ResultType res_flag_; 
  MulModeTableType tab_type_; // distinct xml path or json path , due to not common root
  JtNodeType node_type_;  // distinct regcol in scan node or regcol
  ObJsonExprParam expr_param_;
  TO_STRING_KV(K_(node_type),
               K_(cur_pos),
               K_(ord_val),
               K_(is_ord_node),
               K_(node_type),
               K_(col_info));

};

class ScanNode : public ObMultiModeTableNode
{
  public:
  ScanNode(const ObJtColInfo& col_info)
    : ObMultiModeTableNode(),
      reg_col_defs_(),
      child_idx_(),
      seek_node_(col_info),
      row_hits_()
      {
        new (&seek_node_) ObRegCol(col_info);
        seek_node_.node_type_ = SCAN_TYPE;
        node_type_ = SCAN_TYPE;
      }
  void destroy();
  int reset(JtScanCtx* ctx);
  int open(JtScanCtx* ctx);
  int get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value);
  int get_next_iter(void* in, JtScanCtx* ctx, bool& is_null_value);
  int assign(const ScanNode& other);
  int add_reg_column_node(ObRegCol* node, bool add_idx = false);
  size_t reg_column_count() { return reg_col_defs_.count(); }
  ObRegCol* reg_col_node(size_t i) { return reg_col_defs_.at(i); }
  ObIArray<int64_t>& child_node_ref() { return child_idx_; }
  void reset_reg_columns(JtScanCtx* ctx);
  void* get_curr_iter_value() { 
    return seek_node_.iter_; 
  }

  TO_STRING_KV(K_(node_type),
              K(reg_col_defs_.count()));
  common::ObSEArray<ObRegCol*, 4, common::ModulePageAllocator, true> reg_col_defs_;
  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> child_idx_;
  ObRegCol seek_node_;
  common::ObSEArray<common::ObJsonWrapper, 8> row_hits_;
};

class UnionNode : public ObMultiModeTableNode
{
public:
  UnionNode()
    : ObMultiModeTableNode(), // useless
      left_(nullptr),
      right_(nullptr),
      is_left_iter_end_(false),
      is_right_iter_end_(true) {
        node_type_ = UNION_TYPE;
      }
  void destroy();
  int reset(JtScanCtx* ctx);
  int open(JtScanCtx* ctx);
  int get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value);
  void set_left(ObMultiModeTableNode* node) { 
    left_ = node; 
  }
  void set_right(ObMultiModeTableNode* node) { 
    right_ = node; 
  }
  ObMultiModeTableNode* left() { return left_; }
  ObMultiModeTableNode* right() { return right_; }
  void* get_curr_iter_value() { 
    return left_->get_curr_iter_value(); 
  }

  TO_STRING_KV(K_(node_type),
               KP_(left),
               KP_(right));

  ObMultiModeTableNode *left_;
  ObMultiModeTableNode *right_;
  bool is_left_iter_end_;    // judge left child whether evaled_,  
  bool is_right_iter_end_;  // judge right child whether evaled_,  
};

class JoinNode : public UnionNode
{
  public:
  JoinNode()
    : UnionNode()
      {}
  int get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value);
  int assign(const JoinNode& other);
  ScanNode* get_scan_node() { return dynamic_cast<ScanNode*>(left_); }
  // void* get_curr_iter_value() { return get_scan_node()->get_curr_iter_value(); }

  TO_STRING_KV(K_(node_type));
};

class RegularCol final{
public:
  static int eval_query_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null);
  static int eval_value_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null);
  static int eval_exist_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null);
  static int eval_xml_scalar_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr);
  static int eval_xml_type_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr);
  static int eval_unnest_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, ObExpr* col_expr);
  static int eval_ai_split_document_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, ObExpr* col_expr);
  static void proc_query_on_error(JtScanCtx* ctx, ObRegCol &col_node, int& ret, bool& is_null);
  static int check_default_val_cast_allowed(JtScanCtx* ctx, ObMultiModeTableNode &col_node, ObExpr* expr)  { return 0; }  // check type of default value
  static int set_val_on_empty(JtScanCtx* ctx, ObRegCol &col_node, bool& need_cast_res, bool& is_null);
  static bool check_cast_allowed(const ObObjType orig_type,
                                const ObCollationType orig_cs_type,
                                const ObObjType expect_type,
                                const ObCollationType expect_cs_type,
                                const bool is_explicit_cast);
  static int check_default_value_oracle(JtScanCtx* ctx, ObJtColInfo &col_info, ObExpr* expr);
  static int check_default_value_inner_oracle(JtScanCtx* ctx,
                                              ObJtColInfo &col_info,
                                              ObExpr* col_expr,
                                              ObExpr* default_expr);
  static int check_item_method_json(ObRegCol &col_node, JtScanCtx* ctx);
  static int check_default_value_inner_mysql(JtScanCtx* ctx,
                                             ObRegCol &col_node,
                                             ObExpr* default_expr,
                                             ObExpr* expr,
                                             ObIJsonBase* j_base);
  static int parse_default_value_2json(ObExpr* default_expr, 
                                       JtScanCtx* ctx,
                                       ObDatum*& tmp_datum,
                                       ObIJsonBase *&res);
  static int check_default_value_mysql(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* expr);
};


} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_JSON_TABLE_OP_H_ */
