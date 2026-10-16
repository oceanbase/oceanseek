# -*- coding: utf-8 -*-
# SHARED_MINI_*_MACRO configurations

def create_shared_macro_config(obj_type, data_type, type_id, include_write_back=True):
    """Create shared macro configuration with common parameters"""
    write_strategy = ["WRITE_THROUGH", "WRITE_BACK", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"] if include_write_back else ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"]

    base_config = {
        'obj_type': obj_type,
        'owner': 'yunxing.cyx',
        'id': type_id,
        'access_mode': 'shared',
        'data_type': data_type,
        'read_odirect': False,
        'write_strategy': write_strategy,
        'is_support_fd_cache': True,
        'is_read_out_of_bounds': False,
        'is_path_include_inner_tablet': True,
    }

    return base_config


# Common function templates
def create_common_functions():
    """Create common functions used by all shared macro configurations"""
    return {
        'is_valid': '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}
''',
        'get_effective_tablet_id': '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
        'get_object_id': f'''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}}
''',
    }


def create_path_functions(cache_dir_str, remote_dir_str, macro_dir_str,
                          create_parent_dir_func_str):
    """Create path-related functions for specific macro types"""
    local_path_to_macro_id = f'''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{{
  int ret = OB_SUCCESS;
  char format[512] = {{0}};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {{0}};
  char tablet_name_part2[256] = {{0}};
  char tablet_name[512] = {{0}};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  }} else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {{
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  }} else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {{
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    }} else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }} else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1,
                                      tablet_name_part2, &op_id, &seq_id))) {{
    }} else if (OB_UNLIKELY(5 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    }} else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s",
                                        tablet_name_part1, tablet_name_part2))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }} else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {{
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    }} else {{
      is_inner_tablet = true;
    }}
  }} else {{
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    }} else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                       TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }} else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                      &op_id, &seq_id))) {{
    }} else if (OB_UNLIKELY(4 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    }}
  }}
  if (OB_SUCC(ret)) {{
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }}
  return ret;
}}
'''

    relative_path_body = f'''
  if (file_id.meta_is_inner_tablet()) {{
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        {remote_dir_str}, SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), {macro_dir_str},
        SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }} else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), {remote_dir_str},
      SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), {macro_dir_str}, SEQ_KEY_STR,
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {{
    LOG_WARN("fail to databuff printf", KR(ret));
  }}
'''
    remote_parse_body = f'''
  }} else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {{
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  }} else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {{
    if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%s/%%ld/%%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
        LS_DIR_STR, {remote_dir_str}, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
        {macro_dir_str}, SEQ_KEY_STR, (uint8_t)type_))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }} else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name,
                                      &op_id, &seq_id))) {{
    }} else if (OB_UNLIKELY(4 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    }} else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {{
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    }} else {{
      is_inner_tablet = true;
    }}
  }} else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
      TABLET_DIR_STR, {remote_dir_str}, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
      {macro_dir_str}, SEQ_KEY_STR, (uint8_t)type_))) {{
    LOG_WARN("fail to databuff printf", KR(ret));
  }} else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn,
                                    &op_id, &seq_id))) {{
  }} else if (OB_UNLIKELY(4 != num)) {{
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
'''

    return {
        'to_local_path_format': f'''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.meta_is_inner_tablet()) {{
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, {cache_dir_str},
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }} else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, {cache_dir_str},
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {{
    LOG_WARN("fail to databuff printf", KR(ret));
  }}
  return ret;
}}
''',
        'local_path_to_macro_id': local_path_to_macro_id,
        'to_relative_remote_path_format': f'''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  }} else {{
{relative_path_body}  }}
  return ret;
}}
''',
        'to_remote_path_format': '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}
''',
        'remote_path_to_macro_id': f'''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{{
  int ret = OB_SUCCESS;
  char format[512] = {{0}};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {{0}};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
{remote_parse_body}  }}
  if (OB_SUCC(ret)) {{
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }}
  return ret;
}}
''',
        'opt_to_string': f'''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{{
  int ret = OB_SUCCESS;
  const char *third_name = opt.ss_share_opt_.is_ls_inner_tablet_
      ? "ls_id" : "reorganization_scn";
  const int64_t third_value = opt.ss_share_opt_.is_ls_inner_tablet_
      ? opt.ss_share_opt_.ls_id_ : opt.ss_share_opt_.reorganization_scn_;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,%s=%ld)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, third_name, third_value))) {{
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }}
  return ret;
}}
''',
        'get_parent_dir': f'''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {{
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
        {cache_dir_str}, LS_DIR_STR, file_id.ls_id()))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }} else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, {cache_dir_str},
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {{
    LOG_WARN("fail to databuff printf", KR(ret));
  }}
  return ret;
}}
''',
        'create_parent_dir': f'''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()
      && OB_FAIL(OB_DIR_MGR.create_{create_parent_dir_func_str}_ls_id_dir(
          tenant_id, tenant_epoch_id, file_id.ls_id()))) {{
    LOG_WARN("fail to create shared macro ls id dir", KR(ret));
  }}
  return ret;
}}
''',
    }

# SHARED_MINI_DATA_MACRO configuration
def_storage_object_type_cfg(
    **create_shared_macro_config('SHARED_MINI_DATA_MACRO', 'macro_data', 2, include_write_back=True),
    **create_common_functions(),
    **create_path_functions(
      'SHARED_MINI_MACRO_CACHE_DIR_STR',
      'MINI_DIR_STR',
      'DATA_MACRO_DIR_STR',
      "shared_mini")
)

# SHARED_MINI_META_MACRO configuration
def_storage_object_type_cfg(
    **create_shared_macro_config('SHARED_MINI_META_MACRO', 'macro_meta', 3, include_write_back=True),
    **create_common_functions(),
    **create_path_functions(
      'SHARED_MINI_MACRO_CACHE_DIR_STR',
      'MINI_DIR_STR',
      'META_DIR_STR',
      "shared_mini")
)

# SHARED_MINOR_DATA_MACRO configuration
def_storage_object_type_cfg(
    **create_shared_macro_config('SHARED_MINOR_DATA_MACRO', 'macro_data', 4, include_write_back=False),
    **create_common_functions(),
    **create_path_functions(
      'SHARED_MINOR_MACRO_CACHE_DIR_STR',
      'MINOR_DIR_STR',
      'DATA_MACRO_DIR_STR',
      "shared_minor")
)

# SHARED_MINOR_META_MACRO configuration
def_storage_object_type_cfg(
    **create_shared_macro_config('SHARED_MINOR_META_MACRO', 'macro_meta', 5, include_write_back=False),
    **create_common_functions(),
    **create_path_functions(
      'SHARED_MINOR_MACRO_CACHE_DIR_STR',
      'MINOR_DIR_STR',
      'META_DIR_STR',
      "shared_minor")
)
