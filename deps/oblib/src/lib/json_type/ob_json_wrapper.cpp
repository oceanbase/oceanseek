/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX LIB

#include "ob_json_wrapper.h"
#include "ob_json_path.h"
#include "ob_json_base.h"
#include "ob_json_compare.h"
#include "ob_json_tree.h"            // for ObJsonBaseFactory (DOM seek fallback)

namespace oceanbase {
namespace common {

ObJsonSeekIterator::ObJsonSeekIterator(ObIAllocator &allocator)
    : path_(NULL),
      stack_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      current_path_(&allocator, 256), need_dedup_(false)
{
}

int ObJsonSeekIterator::init(const ObJsonWrapper &root, const ObJsonPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(path_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("json seek iterator is initialized twice", K(ret));
  } else if (!root.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json seek iterator root is invalid", K(ret));
  } else if (!root.is_bin() && root.get_dom()->is_bin()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("json seek iterator requires legacy binary to be converted to tree", K(ret));
  } else {

    path_ = &path;
    need_dedup_ = ObJsonPathUtil::path_gives_duplicates(path.begin(), path.end(), false);
    if (OB_FAIL(current_path_.append("$"))) {
      LOG_WARN("append root path failed", K(ret));
    } else if (OB_FAIL(push_frame(root, 0))) {
      LOG_WARN("push root frame failed", K(ret));
    }
  }
  return ret;
}

int ObJsonSeekIterator::push_frame(const ObJsonWrapper &node, int64_t path_node_idx)
{
  int ret = OB_SUCCESS;
  // Copy before growing stack_: node may refer to a frame in that stack.
  Frame frame(node, path_node_idx, current_path_.length());
  if (OB_FAIL(stack_.push_back(frame))) {
    LOG_WARN("push iterator frame failed", K(ret), K(path_node_idx));
  }
  return ret;
}

int ObJsonSeekIterator::enter_frame(Frame &frame)
{
  int ret = OB_SUCCESS;
  const ObJsonPathNode *path_node = *(path_->begin() + frame.path_node_idx_);
  const ObJsonPathNodeType type = path_node->get_node_type();
  const ObJsonNodeType json_type = frame.node_.json_type();
  const bool is_array = json_type == ObJsonNodeType::J_ARRAY
      || json_type == ObJsonNodeType::J_SEMI_HETE_COL;
  frame.phase_ = DONE;
  switch (type) {
    case JPN_MEMBER: {
      if (json_type == ObJsonNodeType::J_OBJECT) {
        const ObJsonPathBasicNode *member = static_cast<const ObJsonPathBasicNode *>(path_node);
        ObString key(member->get_object().len_, member->get_object().object_name_);
        ObJsonWrapper child;
        if (OB_SEARCH_NOT_FOUND == (ret = frame.node_.lookup(key, child))) {
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("lookup member failed", K(ret));
        } else if (OB_FAIL(ObJsonPathUtil::append_member_key(current_path_, key))) {
          LOG_WARN("append member path failed", K(ret));
        } else if (OB_FAIL(push_frame(child, frame.path_node_idx_ + 1))) {
          LOG_WARN("push member frame failed", K(ret));
        }
      }
      break;
    }
    case JPN_ARRAY_CELL: {
      if (is_array) {
        const ObJsonPathBasicNode *cell = static_cast<const ObJsonPathBasicNode *>(path_node);
        ObJsonArrayIndex index;
        if (OB_FAIL(cell->get_first_array_index(frame.node_.element_count(), index))) {
          LOG_WARN("get array index failed", K(ret));
        } else if (index.is_within_bounds()) {
          ObJsonWrapper child;
          if (OB_FAIL(frame.node_.element(index.get_array_index(), child))) {
            LOG_WARN("get array cell failed", K(ret));
          } else if (OB_FAIL(ObJsonPathUtil::append_bracketed_index(current_path_, index.get_array_index()))) {
            LOG_WARN("append array index failed", K(ret));
          } else if (OB_FAIL(push_frame(child, frame.path_node_idx_ + 1))) {
            LOG_WARN("push array cell frame failed", K(ret));
          }
        }
      }
      break;
    }
    case JPN_MEMBER_WILDCARD: {
      if (json_type == ObJsonNodeType::J_OBJECT) {
        frame.phase_ = CHILDREN;
        frame.end_child_ = frame.node_.element_count();
      }
      break;
    }
    case JPN_ARRAY_RANGE:
    case JPN_ARRAY_CELL_WILDCARD: {
      if (is_array) {
        const ObJsonPathBasicNode *range_node = static_cast<const ObJsonPathBasicNode *>(path_node);
        ObArrayRange range;
        if (OB_FAIL(range_node->get_array_range(frame.node_.element_count(), range))) {
          LOG_WARN("get array range failed", K(ret));
        } else {
          frame.phase_ = CHILDREN;
          frame.next_child_ = range.array_begin_;
          frame.end_child_ = min(range.array_end_, static_cast<uint64_t>(frame.node_.element_count()));
        }
      }
      break;
    }
    case JPN_WILDCARD_ELLIPSIS: {
      frame.phase_ = CHILDREN;
      frame.end_child_ = (is_array || json_type == ObJsonNodeType::J_OBJECT)
          ? frame.node_.element_count() : 0;
      // Match zero levels first. On resumption, descend one child at a time
      // with the same ** leg, preserving the recursive seek's DFS order.
      if (OB_FAIL(push_frame(frame.node_, frame.path_node_idx_ + 1))) {
        LOG_WARN("push ellipsis continuation failed", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported iterator path node", K(ret), K(type));
      break;
    }
  }
  return ret;
}

int ObJsonSeekIterator::next_child(Frame &frame)
{
  int ret = OB_SUCCESS;
  const uint64_t index = frame.next_child_++;
  const bool ellipsis = (*(path_->begin() + frame.path_node_idx_))->get_node_type()
      == JPN_WILDCARD_ELLIPSIS;
  ObJsonWrapper child;
  if (frame.node_.json_type() == ObJsonNodeType::J_OBJECT) {
    ObString key;
    if (OB_FAIL(frame.node_.get_key_value(index, key, child))) {
      LOG_WARN("get object child failed", K(ret));
    } else if (OB_FAIL(ObJsonPathUtil::append_member_key(current_path_, key))) {
      LOG_WARN("append member path failed", K(ret));
    }
  } else {
    if (OB_FAIL(frame.node_.element(index, child))) {
      LOG_WARN("get array child failed", K(ret));
    } else if (OB_FAIL(ObJsonPathUtil::append_bracketed_index(current_path_, index))) {
      LOG_WARN("append array index failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(push_frame(child, frame.path_node_idx_ + (ellipsis ? 0 : 1)))) {
    LOG_WARN("push child frame failed", K(ret));
  }
  return ret;
}

int ObJsonSeekIterator::next(ObJsonWrapper &node, ObJsonBuffer *&path)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(path_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("json seek iterator is not initialized", K(ret));
  }
  while (OB_SUCC(ret) && !found && !stack_.empty()) {
    Frame &frame = stack_.at(stack_.count() - 1);
    if (OB_FAIL(current_path_.set_length(frame.path_length_))) {
      LOG_WARN("restore iterator path failed", K(ret));
    } else if (frame.path_node_idx_ == path_->path_node_cnt()) {
      bool duplicate = false;
      if (need_dedup_) {
        const uint64_t identity = frame.node_.node_identity();
        if (!seen_.created() && OB_FAIL(seen_.create(32, "JsonSeek", "JsonSeek"))) {
          LOG_WARN("create iterator dedup set failed", K(ret));
        } else if (OB_HASH_EXIST == (ret = seen_.set_refactored(identity, 0))) {
          ret = OB_SUCCESS;
          duplicate = true;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("remember iterator hit failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (!duplicate) {
          node = frame.node_;
          path = &current_path_;
          found = true;
        }
        // Keep the yielded path intact until next() is called again.
        stack_.pop_back();
      }
    } else if (frame.phase_ == INITIAL) {
      if (OB_FAIL(enter_frame(frame))) {
        LOG_WARN("enter iterator frame failed", K(ret));
      }
    } else if (frame.phase_ == CHILDREN && frame.next_child_ < frame.end_child_) {
      if (OB_FAIL(next_child(frame))) {
        LOG_WARN("advance iterator child failed", K(ret));
      }
    } else {
      stack_.pop_back();
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObJsonWrapper::seek(const ObJsonPath &path,
                        ObIAllocator &alloc,
                        ObIArray<ObJsonWrapper> &hits,
                        bool auto_wrap,
                        bool only_need_one) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json wrapper is invalid", K(ret));
  } else if (OB_UNLIKELY(0 == path.path_node_cnt())) {
    // Empty path ($): push self to preserve doc context for zero-copy get_raw_binary
    ret = hits.push_back(*this);
  } else if (is_bin_) {
    if (OB_FAIL(bin_.seek(path, hits, auto_wrap, only_need_one))) {
      LOG_WARN("bin view seek failed", K(ret));
    }
  } else {
    // res_json must be heap-allocated: add_if_missing stores res_point_ as the first-result
    // backing store (via clone_new_node), so the pointer must outlive this seek() call.
    void *res_json_buf = alloc.alloc(sizeof(ObJsonBin));
    if (OB_ISNULL(res_json_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ObJsonBin for DOM seek failed", K(ret));
    } else {
      ObJsonBin *res_json = new (res_json_buf) ObJsonBin(&alloc);
      ObJsonSeekResult dom_hit;
      dom_hit.res_point_ = res_json;
      if (OB_FAIL(dom_->seek(path, path.path_node_cnt(), auto_wrap, only_need_one, dom_hit))) {
        LOG_WARN("dom seek failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < dom_hit.size(); i++) {
          if (OB_FAIL(hits.push_back(ObJsonWrapper(dom_hit[i])))) {
            LOG_WARN("push dom wrapper hit failed", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonWrapper::element(uint64_t index, ObJsonWrapper &out) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json wrapper is invalid", K(ret));
  } else if (is_bin_) {
    ObJsonBinView child_view;
    if (OB_FAIL(bin_.element(index, child_view))) {
      LOG_WARN("bin element fail", K(ret), K(index));
    } else {
      out = ObJsonWrapper(child_view);
    }
  } else {
    ObIJsonBase *child_base = nullptr;
    if (json_type() == ObJsonNodeType::J_OBJECT) {
      if (OB_FAIL(dom_->get_object_value(index, child_base))) {
        LOG_WARN("dom get_object_value fail", K(ret), K(index));
      }
    } else {
      if (OB_FAIL(dom_->get_array_element(index, child_base))) {
        LOG_WARN("dom get_array_element fail", K(ret), K(index));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(child_base)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("child is null", K(ret), K(index));
      } else {
        out = ObJsonWrapper(child_base);
      }
    }
  }
  return ret;
}

int ObJsonWrapper::lookup(const ObString &key, ObJsonWrapper &out) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json wrapper is invalid", K(ret));
  } else if (is_bin_) {
    ObJsonBinView sub_view;
    if (OB_FAIL(bin_.lookup(key, sub_view))) {
      // OB_SEARCH_NOT_FOUND is an expected miss code; let the caller handle it.
      if (ret != OB_SEARCH_NOT_FOUND) {
        LOG_WARN("bin view lookup failed", K(ret));
      }
    } else {
      out = ObJsonWrapper(sub_view);
    }
  } else {
    ObIJsonBase *child = NULL;
    if (OB_FAIL(dom_->get_object_value(key, child))) {
      if (ret != OB_SEARCH_NOT_FOUND) {
        LOG_WARN("dom get_object_value by key failed", K(ret));
      }
    } else {
      out = ObJsonWrapper(child);
    }
  }
  return ret;
}

int ObJsonWrapper::get_key(uint64_t index, ObString &key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json wrapper is invalid", K(ret));
  } else if (is_bin_) {
    if (OB_FAIL(bin_.get_key(index, key))) {
      LOG_WARN("bin get_key fail", K(ret), K(index));
    }
  } else {
    if (OB_FAIL(dom_->get_key(index, key))) {
      LOG_WARN("dom get_key fail", K(ret), K(index));
    }
  }
  return ret;
}

int ObJsonWrapper::get_key_value(uint64_t index, ObString &key, ObJsonWrapper &out) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json wrapper is invalid", K(ret));
  } else if (is_bin_) {
    ObJsonBinView child_view;
    if (OB_FAIL(bin_.get_key_value(index, key, child_view))) {
      LOG_WARN("bin get_key_value fail", K(ret), K(index));
    } else {
      out = ObJsonWrapper(child_view);
    }
  } else {
    ObIJsonBase *child_base = nullptr;
    if (OB_FAIL(dom_->get_object_value(index, key, child_base))) {
      LOG_WARN("dom get_object_value fail", K(ret), K(index));
    } else if (OB_ISNULL(child_base)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("child is null", K(ret), K(index));
    } else {
      out = ObJsonWrapper(child_base);
    }
  }
  return ret;
}

int ObJsonWrapper::get_value(uint64_t index, ObJsonWrapper &value) const
{
  return element(index, value);
}

int ObJsonWrapper::compare(const ObJsonWrapper &a, const ObJsonWrapper &b, int &result)
{
  int ret = OB_SUCCESS;
  result = -1;
  if (OB_UNLIKELY(!a.is_valid() || !b.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json wrapper is invalid", K(ret), KP(a.dom_), KP(b.dom_));
  } else if (a.is_bin_ && b.is_bin_) {
    // both bin: use the ObJsonBinView overload to keep the zero-virtual fast path.
    if (OB_FAIL(ObJsonCompare::compare(a.bin_, b.bin_, result, false))) {
      LOG_WARN("compare bin vs bin fail", K(ret));
    }
  } else if (!a.is_bin_ && !b.is_bin_) {
    if (OB_FAIL(ObJsonCompare::compare(*a.dom_, *b.dom_, result, false))) {
      LOG_WARN("compare dom vs dom fail", K(ret));
    }
  } else if (a.is_bin_) {
    // a is bin, b is dom. Compare dom-vs-bin via the mixed overload (which
    // keeps b on the bin fast path) and negate, since result is a<=>b.
    if (OB_FAIL(ObJsonCompare::compare(*b.dom_, a.bin_, result, false))) {
      LOG_WARN("compare dom vs bin fail", K(ret));
    } else {
      result = -result;
    }
  } else {
    // a is dom, b is bin.
    if (OB_FAIL(ObJsonCompare::compare(*a.dom_, b.bin_, result, false))) {
      LOG_WARN("compare dom vs bin fail", K(ret));
    }
  }
  return ret;
}


int ObJsonWrapper::binary_search(const ObIArray<ObJsonWrapper> &sorted_arr,
                                 const ObJsonWrapper &target,
                                 bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  int64_t low = 0;
  int64_t high = static_cast<int64_t>(sorted_arr.count()) - 1;
  while (OB_SUCC(ret) && low <= high) {
    int64_t mid = low + (high - low) / 2;
    int cmp = 0;
    if (OB_FAIL(ObJsonWrapper::compare(sorted_arr.at(mid), target, cmp))) {
      LOG_WARN("compare failed during binary search", K(ret), K(mid), K(low), K(high));
    } else if (cmp == 0) {
      found = true;
      break;
    } else if (cmp > 0) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  return ret;
}

int ObJsonWrapper::get_raw_binary(ObIAllocator &alloc, ObString &out, bool has_lob_header) const
{
  int ret = OB_SUCCESS;
  if (is_bin_) {
    ret = bin_.get_raw_binary(alloc, out, has_lob_header);
  } else {
    ret = dom_->get_raw_binary(out, &alloc);
  }
  return ret;
}

int ObJsonWrapper::get_value_payload(ObIAllocator &alloc,
                                     ObString &value,
                                     bool &is_raw_binary) const
{
  int ret = OB_SUCCESS;
  is_raw_binary = false;
  if (is_bin_) {
    // bin view path: already encoded value-area bytes
    value = bin_.value_binary();
  } else if (dom_->is_bin()) {
    // json bin path
    ObJsonBin *json_bin = static_cast<ObJsonBin *>(dom_);
    if (OB_FAIL(json_bin->get_value_binary(value))) {
      if (ret == OB_NOT_SUPPORTED) {
        // partial update
        ret = OB_SUCCESS;
        uint64_t serialize_size = 0;
        if (OB_FAIL(json_bin->get_serialize_size(serialize_size))) {
          LOG_WARN("get serialize size fail", K(ret));
        } else {
          ObStringBuffer tmp(&alloc, serialize_size);
          if (OB_FAIL(json_bin->get_total_value(tmp))) {
            LOG_WARN("get_total_value fail", K(ret));
          } else if (OB_FAIL(tmp.get_result_string(value))) {
            LOG_WARN("get_result_string fail", K(ret));
          }
        }
      } else {
        LOG_WARN("get_value_binary fail", K(ret));
      }
    }
  } else if (json_type() == ObJsonNodeType::J_SEMI_BIN) {
    // json tree path（J_SEMI_BIN）: already encoded value-area bytes
    ObJsonNode *json_tree = static_cast<ObJsonNode *>(dom_);
    const uint64_t data_len = json_tree->get_data_length();
    if (OB_UNLIKELY(data_len > UINT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("json value is too large", K(ret), K(data_len));
    } else {
      value.assign_ptr(json_tree->get_data(), static_cast<int32_t>(data_len));
    }
  } else if (json_type() != ObJsonNodeType::J_STRING) {
    // json tree path: serialize into allocator-owned stable storage
    ObJsonNode *json_tree = static_cast<ObJsonNode *>(dom_);
    ObStringBuffer tmp(&alloc, json_tree->get_serialize_size());
    if (OB_FAIL(ObJsonBinSerializer::serialize_json_value(json_tree, tmp))) {
      LOG_WARN("serialize_json_value fail", K(ret));
    } else if (OB_FAIL(tmp.get_result_string(value))) {
      LOG_WARN("get_result_string fail", K(ret));
    }
  } else {
    // plain tree J_STRING: no encoded form, fall back to raw bytes
    value = get_string();
    is_raw_binary = true;
  }
  return ret;
}

int ObJsonWrapper::to_json_base(ObIAllocator &alloc, ObIJsonBase *&base) const
{
  int ret = OB_SUCCESS;
  if (!is_bin_) {
    base = dom_;
  } else {
    // Get raw binary and create ObJsonBin from it
    ObString raw_bin;
    if (OB_FAIL(bin_.get_raw_binary(alloc, raw_bin))) {
      LOG_WARN("get_raw_binary fail", K(ret));
    } else {
      void *buf = alloc.alloc(sizeof(ObJsonBin));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc ObJsonBin failed", K(ret));
      } else {
        ObJsonBin *j_bin = new (buf) ObJsonBin(raw_bin.ptr(), raw_bin.length(), &alloc);
        if (OB_FAIL(j_bin->reset_iter())) {
          LOG_WARN("reset_iter fail", K(ret));
        } else {
          base = j_bin;
        }
      }
    }
  }
  return ret;
}

int ObJsonWrapper::parse_bin(const char *data, int64_t len)
{
  int ret = OB_SUCCESS;
  is_bin_ = true;
  if (OB_FAIL(bin_.init(data, len))) {
    LOG_WARN("init json binary view fail", K(ret));
  }
  return ret;
}

int get_json_wrapper(const ObString &data,
                     ObJsonInType in_type,
                     ObIAllocator &alloc,
                     ObJsonWrapper &wrapper,
                     uint32_t parse_flag,
                     uint32_t max_depth,
                     bool enable_json_bin_view)
{
  int ret = OB_SUCCESS;
  bool input_bin_type = (in_type == ObJsonInType::JSON_BIN);
  bool parse_success = false;
  if (input_bin_type && enable_json_bin_view) {
    if (OB_FAIL(wrapper.parse_bin(data.ptr(), data.length()))) {
      if (ret == OB_NOT_SUPPORTED) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("parse json binary via view fail", K(ret));
      }
    } else {
      parse_success = true;
    }
  }
  if (OB_SUCC(ret) && !parse_success) {
    ObIJsonBase *j_base = nullptr;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&alloc, data, in_type, in_type, 
                                                 j_base, parse_flag, max_depth))) {
      LOG_WARN("get_json_base fail", K(ret), K(in_type));
    } else {
      wrapper = ObJsonWrapper(j_base);
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
