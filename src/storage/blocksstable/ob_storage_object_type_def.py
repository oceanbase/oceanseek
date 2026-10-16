# -*- coding: utf-8 -*-
# Copyright 2025 - 2025 Alibaba Inc. All Rights Reserved.
# author: zhaomiao
# this file is used to define the storage object type
# you can add new storage object type here
# and then run `python3 generate_shared_object_type.py` to generate the code
# the generated code will be in the `ob_storage_object_type.h` and `ob_storage_object_type.cpp` files
# if you need to modify the code in these files, please modify this file or generate_shared_object_type.py.
# the following is the parameter description:
#
# required parameters:
# obj_type: the name of the storage object type
# id: explicitly set the enum value
# owner: the owner of the storage object type
# data_type: the type of the storage object data, include macro_data, tenant_data, macro_meta, tablet_meta, tenant_meta, others
# access_mode: the access mode of the storage object, include private, shared
# read_odirect: whether to read the storage object directly, True or False
# write_strategy: write strategy of this type object, including WRITE_THROUGH, WRITE_BACK and WRITE_THROUGH_AND_TRY_WRITE_LCACHE
#
# optional parameters: All parameters are optional, if you don't need to modify, just not add it.If you want modify, please contact zhaomiao.
# is_pin_local: the ObjetType only store in local cache, True or False. If is_pin_local is True, is_overwrite must be true.
# need_fsync: whether to need fsync, True or False, default is True
# use_reserved_disk_space: whether to use reserved disk space, True or False
# can_append_write: whether to can append write, True or False
# is_support_fd_cache: whether use fd cache when reading local cache file of this type, True or False.
#                       note: only MACRO BLOCK (e.g., PRIVATE_DATA_MACRO, SHARED_MAJOR_DATA_MACRO,
#                       SHARED_MAJOR_DATA_MACRO...) need fd cache to improve read performance.
# is_overwrite: whether this type of object exists overwrite with 'different content', True or False
# is_read_out_of_bounds: whether to read out of bounds, True or False, default is True
# is_major: whether it is major, True or False
# is_tmp: whether it is tmp file, True or False
# is_support_sn: whether it is support sn mode, True or False
# server_tenant_can_have: whether it is 500 tenant can have, True or False
# is_path_include_inner_tablet: whether to is path include inner tablet, True or False.
#   a normal tablet's id is unique in tenant, but the id of a log stream internal tablet is the same in each log stream.
#   so if the path include inner tablet, you need to set this parameter to True.
#
# complex function implementation:
# support_store_in_table: whether this type supports being stored in table, distinguished by
# MacroBlockId.location(). The same type may represent object or table storage.
# always_store_in_table: whether this type can only be stored in table. When True,
# support_store_in_table() and is_store_in_table() both always return true. It is
# mutually exclusive with support_store_in_table.
# ss_meta_ctrl_family: SS Meta Ctrl key family, one of TABLET/SSTABLE/LS, default None.
# ss_meta_ctrl_should_backup: whether this SS Meta Ctrl type should be backed up.
# ss_meta_ctrl_is_lock_row: whether this SS Meta Ctrl type represents a lock row.
# ss_meta_ctrl_lock_type: lock-row ObStorageObjectType name corresponding to a data-row type.
# ss_meta_ctrl_dir: SS Meta Ctrl path directory token, META_DIR_STR or LOCK_DIR_STR.
# is_valid: the MacroBlockId is valid check logic
# to_local_path_format: the MacroBlockId convert to local cache path logic, if does not exist return OB_NOT_SUPPORTED
# local_path_to_macro_id: logic of convert from local cache path to MacroBlockId, if does not exist, fill ret = OB_NOT_SUPPORTED;
# to_remote_path_format: the MacroBlockId convert to object storage path logic, if does not exist return OB_NOT_SUPPORTED
# to_relative_remote_path_format: only shared macro support, remote path relative to object storage root dir/cluster_id/tenant_id logic, if does not exist return OB_NOT_SUPPORTED
# get_parent_dir: get parent local dir logic according to MacroBlockId, if does not exist return OB_NOT_SUPPORTED
# create_parent_dir: create parent local dir logic according to MacroBlockId, if does not exist return OB_NOT_SUPPORTED
# opt_to_string: the MacroBlockId convert to string logic, if does not exist return OB_NOT_SUPPORTED
# get_object_id: the MacroBlockId get object id logic, if does not exist return OB_NOT_SUPPORTED
# get_effective_tablet_id: the MacroBlockId get effective tablet id logic, if does not exist return OB_NOT_SUPPORTED

all_storage_object_types = []


def create_ss_meta_ctrl_prefix_path_format(family, dir_str, include_inner_tablet=False):
    """Build the ctrl-table prefix formatter from the same metadata as the full key path."""
    if family == 'LS':
        return '''
int to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/",
              LS_DIR_STR, file_id.second_id()))) {
    LOG_WARN("fail to format ss meta ctrl ls prefix path", KR(ret), K(file_id));
  }
  return ret;
}
'''
    elif family in ('TABLET', 'SSTABLE'):
        if include_inner_tablet:
            return f'''
int to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  }} else if (file_id.meta_is_inner_tablet()) {{
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {{
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    }} else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {{
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }}
  }} else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {{
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }}
  return ret;
}}
'''
        return '''
int to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}
'''
    else:
        raise ValueError(f"invalid SS Meta Ctrl path family '{family}'")


def def_storage_object_type_cfg(
        _create_ctrl_prefix_path_format=create_ss_meta_ctrl_prefix_path_format,
        **kwargs):
    # check obj_type is required parameter
    if 'obj_type' not in kwargs:
        raise ValueError("obj_type is required parameter but not provided")

    obj_type = kwargs['obj_type']
    if obj_type is None or obj_type == 'none':
        raise ValueError("obj_type cannot be None or 'none'")

     # check id is required parameter
    if 'id' not in kwargs:
        raise ValueError("id is required parameter but not provided")

    id = kwargs['id']
    if id is None:
        raise ValueError("id cannot be None")

    # check data_type is required parameter
    if 'data_type' not in kwargs:
        raise ValueError("data_type is required parameter but not provided")

    data_type = kwargs['data_type']
    if data_type is None:
        raise ValueError("data_type cannot be None")

    # check access_mode is required parameter
    if 'access_mode' not in kwargs:
        raise ValueError("access_mode is required parameter but not provided")

    owner = 'zhaomiao',
    access_mode = kwargs['access_mode']
    if access_mode is None:
        raise ValueError("access_mode cannot be None")

    # check read_odirect is required parameter
    if 'read_odirect' not in kwargs:
        raise ValueError("read_odirect is required parameter but not provided")

    read_odirect = kwargs['read_odirect']
    if read_odirect is None:
        raise ValueError("read_odirect cannot be None")

    if 'write_strategy' not in kwargs:
        raise ValueError("write_strategy is required parameter but not provided")

    write_strategy = kwargs['write_strategy']
    if write_strategy is None:
        raise ValueError("write_strategy cannot be None")

    # check owner is required parameter
    if 'owner' not in kwargs:
        raise ValueError("owner is required parameter but not provided")

    always_store_in_table = kwargs.get('always_store_in_table', False)
    support_store_in_table = kwargs.get('support_store_in_table', False)
    if always_store_in_table and support_store_in_table:
        raise ValueError(
            f"{obj_type}: always_store_in_table and support_store_in_table are mutually exclusive")

    ss_meta_ctrl_family = kwargs.get('ss_meta_ctrl_family')
    valid_ss_meta_ctrl_families = (None, 'TABLET', 'SSTABLE', 'LS')
    if ss_meta_ctrl_family not in valid_ss_meta_ctrl_families:
        raise ValueError(
            f"{obj_type}: invalid ss_meta_ctrl_family '{ss_meta_ctrl_family}'")

    ss_meta_ctrl_lock_type = kwargs.get('ss_meta_ctrl_lock_type')
    if kwargs.get('ss_meta_ctrl_is_lock_row', False) and ss_meta_ctrl_lock_type is not None:
        raise ValueError(
            f"{obj_type}: lock row cannot configure ss_meta_ctrl_lock_type")

    ss_meta_ctrl_dir = kwargs.get('ss_meta_ctrl_dir')
    if ss_meta_ctrl_family is not None:
        if ss_meta_ctrl_dir not in ('META_DIR_STR', 'LOCK_DIR_STR'):
            raise ValueError(
                f"{obj_type}: SS Meta Ctrl type must configure a valid ss_meta_ctrl_dir")
        kwargs['to_ss_meta_ctrl_prefix_path_format'] = _create_ctrl_prefix_path_format(
            ss_meta_ctrl_family,
            ss_meta_ctrl_dir,
            kwargs.get('is_path_include_inner_tablet', False))
    elif ss_meta_ctrl_dir is not None:
        raise ValueError(f"{obj_type}: ss_meta_ctrl_dir requires ss_meta_ctrl_family")

    all_storage_object_types.append(kwargs)

def_storage_object_type_default_cfg = {
    'obj_type': None,
    'owner': None,
    'data_type': 'others',# {macro_data, tenant_data, macro_meta, tablet_meta, tenant_meta, others}
    'access_mode':'private', #{private, shared}
    'read_odirect': False,
    'write_strategy': ["WRITE_THROUGH"],
    # low frequency modify parameters, if you need to modify, please contact zhaomiao
    'is_pin_local': False,
    'need_fsync': True,
    'use_reserved_disk_space': False,
    'can_append_write': False,
    'is_support_fd_cache': False,
    'is_overwrite': False,
    'is_read_out_of_bounds': True,
    'is_mds': False,
    'is_major': False,
    'is_tmp': False,
    'server_tenant_can_have': False,
    'is_path_include_inner_tablet': False,
    'support_store_in_table': False,
    'always_store_in_table': False,
    'ss_meta_ctrl_family': None,
    'ss_meta_ctrl_should_backup': False,
    'ss_meta_ctrl_is_lock_row': False,
    'ss_meta_ctrl_lock_type': None,
    'ss_meta_ctrl_dir': None,
    # complex function implementation
    'is_valid': False,
    'to_local_path_format': 'OB_NOT_SUPPORTED',
    'local_path_to_macro_id': 'OB_NOT_SUPPORTED',
    'to_remote_path_format': 'OB_NOT_SUPPORTED',
    'to_ss_meta_ctrl_prefix_path_format': 'OB_NOT_SUPPORTED',
    'get_parent_dir': 'OB_NOT_SUPPORTED',
    'create_parent_dir': 'OB_NOT_SUPPORTED',
    'opt_to_string':'OB_NOT_SUPPORTED',
    'get_object_id':'OB_NOT_SUPPORTED',
    'get_effective_tablet_id':'OB_NOT_SUPPORTED',
}

# PRIVATE_DATA_MACRO
def_storage_object_type_cfg(
    obj_type = 'PRIVATE_DATA_MACRO',  #ObPrivateDataMacroType
    id = 0,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_BACK"],
    is_support_fd_cache = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.tenant_seq() >= 0;
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
    const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%lu%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
      TABLET_DATA_DIR_STR, file_id.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM,
      file_id.second_id(), file_id.third_id(), DATA_MACRO_DIR_STR, SVR_KEY_STR, server_id,
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.tenant_seq(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format private macro local path", KR(ret), K(file_id), K(server_id));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  const char *sub_path = nullptr;
  char format[512] = {0};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t third_id = 0;
  int64_t server_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s%%ld%s%%ld%s%%ld.T%hhu", DATA_MACRO_DIR_STR, SVR_KEY_STR,
      OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &third_id, &server_id,
                                    &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id(third_id);
    macro_id.set_op_id(op_id);
    macro_id.set_tenant_seq(seq_id);
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%ld/%s/%s%lu%s%ld%s%ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
      tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id.second_id(), file_id.third_id(),
      DATA_MACRO_DIR_STR, SVR_KEY_STR, server_id, OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.tenant_seq(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id,
      file_id.second_id(), file_id.third_id(), ObMacroType::DATA_MACRO);
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(
      tenant_id, tenant_epoch_id, file_id.second_id(), file_id.third_id());
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id,
    uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%ld,op_id=%ld,reorganization_scn=%ld)", get_type_str(),
      opt.private_opt_.tablet_id_, opt.private_opt_.op_id_,
      opt.private_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;
  set_ss_object_first_id_(0, object_id);
  object_id.set_second_id(opt.private_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.private_opt_.is_ls_inner_tablet_,
      opt.private_opt_.ls_id_, opt.private_opt_.reorganization_scn_);
  object_id.set_op_id(opt.private_opt_.op_id_);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
    LOG_WARN("fail to get private object seq", K(ret), K(opt));
  } else {
    object_id.set_tenant_seq(seq);
  }
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'PRIVATE_META_MACRO',  #ObPrivateMetaMacroType
    id = 1,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_BACK"],
    is_support_fd_cache = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.tenant_seq() >= 0;
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
    const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%lu%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
      TABLET_DATA_DIR_STR, file_id.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM,
      file_id.second_id(), file_id.third_id(), META_DIR_STR, SVR_KEY_STR, server_id,
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.tenant_seq(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format private macro local path", KR(ret), K(file_id), K(server_id));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  const char *sub_path = nullptr;
  char format[512] = {0};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t third_id = 0;
  int64_t server_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s%%ld%s%%ld%s%%ld.T%hhu", META_DIR_STR, SVR_KEY_STR,
      OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &third_id, &server_id,
                                    &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id(third_id);
    macro_id.set_op_id(op_id);
    macro_id.set_tenant_seq(seq_id);
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%ld/%s/%s%lu%s%ld%s%ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
      tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id.second_id(), file_id.third_id(),
      META_DIR_STR, SVR_KEY_STR, server_id, OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.tenant_seq(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id,
      file_id.second_id(), file_id.third_id(), ObMacroType::META_MACRO);
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(
      tenant_id, tenant_epoch_id, file_id.second_id(), file_id.third_id());
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id,
    uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%ld,op_id=%ld,reorganization_scn=%ld)", get_type_str(),
      opt.private_opt_.tablet_id_, opt.private_opt_.op_id_,
      opt.private_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;
  set_ss_object_first_id_(0, object_id);
  object_id.set_second_id(opt.private_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.private_opt_.is_ls_inner_tablet_,
      opt.private_opt_.ls_id_, opt.private_opt_.reorganization_scn_);
  object_id.set_op_id(opt.private_opt_.op_id_);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
    LOG_WARN("fail to get private object seq", K(ret), K(opt));
  } else {
    object_id.set_tenant_seq(seq);
  }
  return ret;
}
'''
)
# Legacy object type definitions are kept for reference and intentionally disabled.
r"""
def_storage_object_type_cfg(
    obj_type = 'SHARED_MINI_DATA_MACRO',  #ObSharedMiniDataMacroType
    id = 2,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // inner_tablet: tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld.T%hhu
  // user_tablet: tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/t%ldrs%ldop%ldseq%ld.T%hhu
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.meta_ls_id(),
              get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
              TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
              OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // inner_tablet: ls/ls_id/tablet_name/mini/sstable/op_id/data/seq%ld.T%hhu
  // user_tablet: tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/seq%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINI_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
                TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  // inner_tablet: cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/data/seq%ld.T%hhu
  // user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/seq%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  // inner_tablet: /ls/ls_id/tablet_name/mini/sstable/op_id/data/seq%ld
  // user_tablet: /tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id
  // user_tablet: tenant_id_epoch_id/shared_mini_macro_cache/scatter_id
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.meta_ls_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM)))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_mini_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared mini ls id dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MINI_META_MACRO',  #ObSharedMiniMetaMacroType
    id = 3,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // inner_tablet:tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld.T%hhu
  // user_tablet:tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/t%ldrs%ldop%ldseq%ld.T%hhu
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.meta_ls_id(),
              get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
              TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
              OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // inner_tablet:ls/ls_id/tablet_name/mini/sstable/op_id/meta/seq%ld.T%hhu
  // user_tablet:tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/seq%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINI_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, META_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
                TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, META_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/meta/seq%ld.T%hhu
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/seq%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  // inner_tablet: /ls/ls_id/tablet_name/mini/sstable/op_id/meta/seq%ld
  // user_tablet: /tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id
  // user_tablet: tenant_id_epoch_id/shared_mini_macro_cache/scatter_id
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.meta_ls_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM)))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_mini_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared mini ls id dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''
)
def_storage_object_type_cfg(
    obj_type = 'SHARED_MINOR_DATA_MACRO',  #ObSharedMinorDataMacroType
    id = 4,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // inner_tablet: tenant_id_epoch_id/shared_minor_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld.T%hhu
  // user_tablet: tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/t%ldrs%ldop%ldseq%ld.T%hhu
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                        TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // inner_tablet:ls/ls_id/tablet_name/minor/sstable/op_id/data/seq%ld.T%hhu
  // user_tablet:tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/seq%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINOR_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
                TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/data/seq%ld.T%hhu
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/seq%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  // inner_tablet: /ls/ls_id/tablet_name/minor/sstable/op_id/data/seq%ld
  // user_tablet: /tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_minor_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared minor ls id dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MINOR_META_MACRO',  #ObSharedMinorMetaMacroType
    id = 5,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // inner_tablet:tenant_id_epoch_id/shared_minor_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld.T%hhu
  // user_tablet:tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/t%ldrs%ldop%ldseq%ld.T%hhu
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                        OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                        TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // inner_tablet:ls/ls_id/tablet_name/minor/sstable/op_id/meta/seq%ld.T%hhu
  // user_tablet:tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/seq%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINOR_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, META_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
                TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, META_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/, OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/meta/seq%ld.T%hhu
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/seq%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  // inner_tablet: /ls/ls_id/tablet_name/minor/sstable/op_id/meta/seq%ld
  // user_tablet: /tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_minor_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared minor ls id dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
''',
)
"""

def_storage_object_type_cfg(
    obj_type = 'SHARED_MAJOR_DATA_MACRO',  #ObSharedMajorDataMacroType
    id = 6,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_major = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.major_snapshot() >= 0 && file_id.seq_id() >= 0;
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.major_snapshot(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t major_snapshot = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &major_snapshot, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_major_snapshot(major_snapshot);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.major_snapshot(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t major_snapshot = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, DATA_MACRO_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &major_snapshot, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_major_snapshot(major_snapshot);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,major_snapshot=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_major_snapshot(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}
''',
    is_path_include_inner_tablet = True,
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MAJOR_META_MACRO',  #ObSharedMajorMetaMacroType
    id = 7,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_major = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.major_snapshot() >= 0 && file_id.seq_id() >= 0;
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.major_snapshot(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t major_snapshot = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &major_snapshot, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_major_snapshot(major_snapshot);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.major_snapshot(), META_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t major_snapshot = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, META_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &major_snapshot, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_major_snapshot(major_snapshot);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,major_snapshot=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_major_snapshot(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}
''',
    is_path_include_inner_tablet = True,
)

def_storage_object_type_cfg(
    obj_type = 'TMP_FILE',  #ObTmpFileType
    id = 8,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tenant_data',
    read_odirect = False,
    write_strategy = ["WRITE_BACK"],
    need_fsync = False,
    can_append_write = True,
    is_read_out_of_bounds = False,
    is_tmp = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:N/A, fourth_id:segment_id
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) &&
         (0 == file_id.third_id()) && (file_id.fourth_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tenant_id_epoch_id/tmp_data/scatter_id/tmp_file_id/seg%ld.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%03ld/%ld/%s%ld%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR,
              ((file_id.second_id() / ObDirManager::TMP_FILE_SCATTER_DIR_NUM) % ObDirManager::TMP_FILE_SCATTER_DIR_NUM),
              file_id.second_id(), SEG_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tmp_file_id = 0;
    int64_t segment_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%s%%ld.T%hhu", SEG_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tmp_file_id, &segment_id))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(segment_id);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ld.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld%s%hhu",
                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                    SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                    TMP_DATA_DIR_STR, file_id.second_id(), SEG_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tmp_data/tmp_file_id/
  if (OB_FAIL(OB_DIR_MGR.get_local_tmp_file_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id()))) {
    LOG_WARN("fail to get local tmp file dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;

  // ERRSIM for testing: simulate disk space exhaustion during parent dir creation
  // This allows testing the scenario where file allocation succeeds but parent dir creation fails
  ret = OB_E(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_INFO("ERRSIM: simulating disk space exhaustion during parent dir creation", KR(ret), K(file_id));
  } else if (OB_FAIL(OB_DIR_MGR.create_tmp_file_dir(tenant_id, tenant_epoch_id, file_id.second_id()))) {
    LOG_WARN("fail to create tmp file dir", KR(ret));
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t file_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_tmp_file_seq(file_id))) {
    LOG_WARN("fail to get private tmp file seq", K(ret), K(opt));
  } else {
    object_id.set_second_id(file_id);
  }
  return ret;
}
''',
)

# SERVER_META
def_storage_object_type_cfg(
    obj_type = 'SERVER_META',  #ObServerMetaType
    id = 9,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = False,
    write_strategy = ["WRITE_BACK"],
    is_pin_local = True,
    is_overwrite = True,
    server_tenant_can_have = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // super_block.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s%s%hhu", OB_DIR_MGR.get_local_cache_root_dir(),
              get_type_str(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // local_cache_root_dir
  if (OB_FAIL(databuff_printf(path, length, pos, "%s", OB_DIR_MGR.get_local_cache_root_dir()))) {
    LOG_WARN("fail to get local cache root dir", KR(ret));
  }
  return ret;
}''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(0);
  object_id.set_third_id(0);
  object_id.set_fourth_id(0);
  object_id.set_fifth_id(0);
  return OB_SUCCESS;
}''',
)

def_storage_object_type_cfg(
    obj_type = 'PRIVATE_TABLET_META',  #ObPrivateTabletMetaType
    id = 10,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tablet_meta',
    read_odirect = False,
    write_strategy = ["WRITE_BACK"],
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:meta_version_id, fifth_id:N/A
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.meta_version_id() >= 0 && 0 == file_id.fifth_id();
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_UNLIKELY(ls_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id for private tablet meta", KR(ret), K(ls_id), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%ld_%ld/%s/%02ld/%ld/%ld/%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR,
      ls_id, ls_epoch_id, TABLET_META_DIR_STR,
      file_id.second_id() % ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM,
      file_id.second_id(), file_id.third_id(), VER_KEY_STR,
      file_id.meta_version_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format private tablet meta local path", KR(ret), K(file_id),
             K(ls_id), K(ls_epoch_id));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 6))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t ls_id = 0;
    int64_t ls_epoch_id = 0;
    int64_t scatter_id = 0;
    int64_t tablet_id = 0;
    int64_t third_id = 0;
    int64_t meta_version_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld_%%ld/%s/%%ld/%%ld/%%ld/%s%%ld.T%hhu",
                TABLET_META_DIR_STR, VER_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, &ls_epoch_id, &scatter_id,
                                    &tablet_id, &third_id, &meta_version_id))) {
    } else if (OB_UNLIKELY(6 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      set_ss_object_first_id_(0, macro_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_ss_third_id(ObTabletID(tablet_id).is_ls_inner_tablet(), third_id, third_id);
      macro_id.set_meta_version_id(meta_version_id);
      macro_id.set_seq_id(0);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_UNLIKELY(ls_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id for private tablet meta", KR(ret), K(ls_id), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld_%ld/%s/%ld/%ld/%s%ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
      tenant_id, tenant_epoch_id, LS_DIR_STR, ls_id, ls_epoch_id, TABLET_META_DIR_STR,
      file_id.second_id(), file_id.third_id(), VER_KEY_STR,
      file_id.meta_version_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format private tablet meta remote path", KR(ret), K(file_id),
             K(ls_id), K(ls_epoch_id), K(server_id));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 5))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t ls_id = 0;
    int64_t ls_epoch_id = 0;
    int64_t tablet_id = 0;
    int64_t third_id = 0;
    int64_t meta_version_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld_%%ld/%s/%%ld/%%ld/%s%%ld.T%hhu",
                TABLET_META_DIR_STR, VER_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, &ls_epoch_id, &tablet_id,
                                    &third_id, &meta_version_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      set_ss_object_first_id_(0, macro_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_ss_third_id(ObTabletID(tablet_id).is_ls_inner_tablet(), third_id, third_id);
      macro_id.set_meta_version_id(meta_version_id);
      macro_id.set_seq_id(0);
    }
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.get_tablet_meta_tablet_id_private_macro_third_id_dir(
      path, length, tenant_id, tenant_epoch_id, ls_id, ls_epoch_id,
      file_id.second_id(), file_id.third_id());
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.create_tablet_meta_tablet_id_private_macro_third_id_dir(
      tenant_id, tenant_epoch_id, ls_id, ls_epoch_id,
      file_id.second_id(), file_id.third_id());
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,meta_version_id=%ld,reorganization_scn=%ld)", get_type_str(),
      opt.ss_private_tablet_opt_.tablet_id_, opt.ss_private_tablet_opt_.meta_version_id_,
      opt.ss_private_tablet_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_private_tablet_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_private_tablet_opt_.is_inner_tablet_,
      opt.ss_private_tablet_opt_.ls_id_, opt.ss_private_tablet_opt_.reorganization_scn_);
  object_id.set_meta_version_id(opt.ss_private_tablet_opt_.meta_version_id_);
  object_id.set_seq_id(0);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'PRIVATE_SLOG_FILE',  #ObPrivateSlogFileType
    id = 11,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tenant_meta',
    read_odirect = False,
    write_strategy = ["WRITE_BACK"],
    is_overwrite = True,
    use_reserved_disk_space = True,
    can_append_write = True,
    server_tenant_can_have = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:N/A, fourth_id:tenant_epoch_id, fifth_id:file_id
  return (0 == file_id.third_id()) && (file_id.fourth_id() >= 0) &&
         (file_id.fifth_id() >= 0) && (file_id.fifth_id() < INT64_MAX);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // server_slog/seq%ld.T%hhu or tenant_id_epoch_id/slog/seq%ld.T%hhu
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%s/%s%ld%s%hhu",
                OB_DIR_MGR.get_local_cache_root_dir(), SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s%ld%s%hhu", OB_DIR_MGR.get_local_cache_root_dir(),
                tenant_id, file_id.fourth_id(), SLOG_STR, SEQ_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tenant_id = 0;
    int64_t epoch_id = 0;
    int64_t object_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%s%%ld.T%hhu",
                SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tenant_id, &epoch_id, &object_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tenant_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(epoch_id);
      macro_id.set_fifth_id(object_id);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/slog/seq%ld.T%hhu or cluster_id/server_id/server_slog/seq%ld.T%hhu
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%s%ld%s%hhu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR,
                server_id, SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s%ld%s%hhu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR,
                server_id, tenant_id, file_id.fourth_id(), SLOG_STR, SEQ_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tenant_id_epoch_id/slog/seq%ld or /server_id/server_slog/seq%ld
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  char tablet_name[512] = {0};
  int64_t macro_seq_id = 0;
  int64_t tenant_id = 0;
  int64_t tenant_epoch_id = 0;
  const char *judge_path = nullptr;
  bool is_500_tenant = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, SERVER_DIR_STR)) { // 500 tenant
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%s/%s%%ld.T%hhu",
                       SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &macro_seq_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_500_tenant = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%s%%ld.T%hhu",
                SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tenant_id, &tenant_epoch_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_500_tenant = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_500_tenant) {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(OB_SERVER_TENANT_ID);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(0);
      macro_id.set_fifth_id(macro_seq_id);
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tenant_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(tenant_epoch_id);
      macro_id.set_fifth_id(macro_seq_id);
    }
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/slog/
  if (OB_FAIL(OB_DIR_MGR.get_tenant_slog_dir(path, length, file_id.second_id(), file_id.fourth_id()))) {
    LOG_WARN("fail to get tenant slog dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_slog_dir(file_id.second_id(), file_id.fourth_id()))) {
    LOG_WARN("fail to create slog dir", KR(ret));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_slog_ckpt_obj_opt_.tenant_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_slog_ckpt_obj_opt_.tenant_epoch_id_);
  object_id.set_fifth_id(opt.ss_slog_ckpt_obj_opt_.file_id_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'PRIVATE_CKPT_FILE',  #ObPrivateCkptFileType
    id = 12,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tenant_meta',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    server_tenant_can_have = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:N/A, fourth_id:tenant_epoch_id, fifth_id:file_id
  return (0 == file_id.third_id()) && (file_id.fourth_id() >= 0) &&
         (file_id.fifth_id() >= 0) && (file_id.fifth_id() < INT64_MAX);
}''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/ckpt/object_id.T%hhu or cluster_id/server_id/server_ckpt/object_id.T%hhu
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%ld%s%hhu", object_storage_root_dir,
                CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, SERVER_DIR_STR, CKPT_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld%s%hhu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id,
                file_id.fourth_id(), CKPT_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tenant_id_epoch_id/ckpt/object_id or /server_id/server_ckpt/object_id
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  char tablet_name[512] = {0};
  int64_t ckpt_id = 0;
  int64_t tenant_id = 0;
  int64_t tenant_epoch_id = 0;
  const char *judge_path = nullptr;
  bool is_500_tenant = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, SERVER_DIR_STR)) { // 500 tenant
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%s/%%ld.T%hhu",
                       SERVER_DIR_STR, CKPT_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ckpt_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_500_tenant = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%%ld.T%hhu",
                CKPT_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tenant_id, &tenant_epoch_id, &ckpt_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_500_tenant = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_500_tenant) {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(OB_SERVER_TENANT_ID);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(0);
      macro_id.set_fifth_id(ckpt_id);
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tenant_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(tenant_epoch_id);
      macro_id.set_fifth_id(ckpt_id);
    }
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_slog_ckpt_obj_opt_.tenant_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_slog_ckpt_obj_opt_.tenant_epoch_id_);
  object_id.set_fifth_id(opt.ss_slog_ckpt_obj_opt_.file_id_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'MAJOR_PREWARM_DATA',  #ObMajorPrewarmDataType
    id = 13,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:compaction_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu/%lu/%s/%s/%s%ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'MAJOR_PREWARM_DATA_INDEX',  #ObMajorPrewarmDataIndexType
    id = 14,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:compaction_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu/%lu/%s/%s/%s%ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'MAJOR_PREWARM_META',  #ObMajorPrewarmMetaType
    id = 15,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:compaction_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu/%lu/%s/%s/%s%ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'MAJOR_PREWARM_META_INDEX',  #ObMajorPrewarmMetaIndexType
    id = 16,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:compaction_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu/%lu/%s/%s/%s%ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'TENANT_DISK_SPACE_META',  #ObTenantDiskSpaceMetaType
    id = 17,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = False,
    write_strategy = ["WRITE_BACK"],
    is_pin_local = True,
    is_overwrite = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:N/A, fourth_id:tenant_epoch_id
  return is_valid_tenant_id(file_id.second_id()) && 0 == file_id.third_id() && file_id.fourth_id() >= 0;
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tenant_id_epoch_id/tenant_disk_space_meta
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld_%ld/%s%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), file_id.second_id(), file_id.fourth_id(), get_type_str(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id
  if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(path, length, file_id.second_id(), file_id.fourth_id()))) {
    LOG_WARN("fail to get local tenant dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tenant_dir(file_id.second_id(), file_id.fourth_id()))) {
    LOG_WARN("fail to create tenant dir", KR(ret));
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu,tenant_epoch_id=%lu)",
               get_type_str(), opt.ss_tenant_level_opt_.tenant_id_, opt.ss_tenant_level_opt_.tenant_epoch_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
                K(opt.ss_tenant_level_opt_.tenant_id_), K(opt.ss_tenant_level_opt_.tenant_epoch_id_));
    }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tenant_level_opt_.tenant_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_tenant_level_opt_.tenant_epoch_id_);
  return ret;
}
''',
)
def_storage_object_type_cfg(
    obj_type = 'IS_SHARED_TENANT_DELETED',  #ObIsSharedTenantDeletedType
    id = 18,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    server_tenant_can_have = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id
  return is_valid_tenant_id(file_id.second_id());
}''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/tenant_id/is_shared_tenant_deleted.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s%s%hhu",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR,
              file_id.second_id(), get_type_str(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t tenant_id = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%%ld/%s.T%hhu",
              TENANT_DIR_STR, get_type_str(), (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tenant_id))) {
  } else if (OB_UNLIKELY(1 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tenant_id);
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_shared_tenant_id_opt_.tenant_id_);
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu)",
             get_type_str(), opt.ss_shared_tenant_id_opt_.tenant_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_shared_tenant_id_opt_.tenant_id_));
  }
  return ret;
}
''',
)

# Legacy object type definitions are kept for reference and intentionally disabled.
r"""
def_storage_object_type_cfg(
    obj_type = 'SHARED_MICRO_DATA_MACRO',  #ObSharedMicroDataMacroType
    id = 19,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MICRO_META_MACRO',  #ObSharedMicroMetaMacroType
    id = 20,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
)
"""

def_storage_object_type_cfg(
    obj_type = 'UNSEALED_REMOTE_SEG_FILE',  #ObUnsealedRemoteSegFileType
    id = 19,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:N/A, fourth_id:segment_id, fifth_id:valid_length
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) &&
         (0 == file_id.third_id()) && (file_id.fourth_id() >= 0) && (file_id.fifth_id() > 0);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ldlen%ld.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld%s%ld%s%hhu",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
              TMP_DATA_DIR_STR, file_id.second_id(), SEG_KEY_STR, file_id.fourth_id(), LEN_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tmp_file_id/seg%ldlen%ld
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t second_id = 0;
  int64_t segment_id = 0;
  int64_t len_id = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%s%%ld%s%%ld.T%hhu",
                    SEG_KEY_STR, LEN_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &second_id, &segment_id, &len_id))) {
  } else if (OB_UNLIKELY(3 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(second_id);
    macro_id.set_third_id(0);
    macro_id.set_fourth_id(segment_id);
    macro_id.set_fifth_id(len_id);
  }
  return ret;
}
''',
)


def_storage_object_type_cfg(
    obj_type = 'SHARED_TABLET_META',  #ObSharedTabletMetaType, stored in object_storage
    id = 20,
    owner = 'jianyue',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_path_include_inner_tablet = True,
    support_store_in_table = True,
    ss_meta_ctrl_family = 'TABLET',
    ss_meta_ctrl_lock_type = 'SHARED_TABLET_LOCK',
    ss_meta_ctrl_dir = 'META_DIR_STR',
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.meta_scn() >= 0 && file_id.seq_id() >= 0;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.is_in_table() || file_id.is_in_control()) {
    if (OB_FAIL(to_relative_remote_path_format(path, length, pos, file_id))) {
      LOG_WARN("failed to format table key", K(ret), K(file_id));
    }
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s%020ld%s%hhu",
        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s%020ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
      SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s/%s%020ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s%020ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
      SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  bool is_in_table = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  int64_t meta_scn = 0;
  char tablet_name[512] = {0};
  const char *object_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else {
    is_in_table = 0 == STRNCMP(path, OB_TABLE_PREFIX, STRLEN(OB_TABLE_PREFIX))
        || 0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))
        || 0 == STRNCMP(path, TABLET_DIR_STR, STRLEN(TABLET_DIR_STR))
        || 0 == STRNCMP(path, "/ls/", STRLEN("/ls/"))
        || 0 == STRNCMP(path, "/tablet/", STRLEN("/tablet/"));
    if (0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))) {
      object_path = path;
    } else if (OB_NOT_NULL(object_path = STRSTR(path, "/ls/"))) {
      ++object_path;
    }
    if (OB_NOT_NULL(object_path)) {
      is_inner_tablet = true;
      if (OB_FAIL(databuff_printf(format, sizeof(format),
          "%s/%%ld/%%[^/]/%s/%s%%020ld%s%hhu", LS_DIR_STR, META_DIR_STR,
          SCN_KEY_STR, OBJECT_TYPE_STR, (uint8_t)type_))) {
        LOG_WARN("fail to databuff printf", KR(ret));
      } else if (FALSE_IT(num = sscanf(object_path, format, &ls_id, tablet_name, &meta_scn))) {
      } else if (OB_UNLIKELY(3 != num)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path), K(format), K(num));
      } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
        LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
      }
    } else {
      if (0 == STRNCMP(path, TABLET_DIR_STR, STRLEN(TABLET_DIR_STR))) {
        object_path = path;
      } else if (OB_NOT_NULL(object_path = STRSTR(path, "/tablet/"))) {
        ++object_path;
      }
      if (OB_ISNULL(object_path)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path));
      } else if (OB_FAIL(databuff_printf(format, sizeof(format),
          "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu", TABLET_DIR_STR, META_DIR_STR,
          SCN_KEY_STR, OBJECT_TYPE_STR, (uint8_t)type_))) {
        LOG_WARN("fail to databuff printf", KR(ret));
      } else if (FALSE_IT(num = sscanf(object_path, format, &tablet_id,
                                        &reorganization_scn, &meta_scn))) {
      } else if (OB_UNLIKELY(3 != num)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path), K(format), K(num));
      }
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_location(is_in_table
        ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(meta_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
''',
    relative_remote_path_to_macro_id = '''
int relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return remote_path_to_macro_id(path, macro_id);
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(0, object_id);
  object_id.set_second_id(opt.ss_tablet_meta_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_tablet_meta_opt_.is_inner_tablet_,
      opt.ss_tablet_meta_opt_.ls_id_, opt.ss_tablet_meta_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_tablet_meta_opt_.op_id_);
  object_id.set_seq_id(0);
  object_id.set_location(opt.ss_tablet_meta_opt_.is_in_table_
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  return ret;
}
'''
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_TABLET_SUB_META',  #ObSharedTabletSubMetaType
    id = 21,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'tablet_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_path_include_inner_tablet = True,
    support_store_in_table = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}
''',
   to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s/%s%ld%s%0*ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        META_DIR_STR, OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR,
        static_cast<int>(OBJ_TIERED_METADATA_SEQ_WIDTH), file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s%ld%s%0*ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, static_cast<int>(OBJ_TIERED_METADATA_SEQ_WIDTH),
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.is_in_table()) {
    if (OB_FAIL(to_relative_remote_path_format(path, length, pos, file_id))) {
      LOG_WARN("failed to format table key", K(ret), K(file_id));
    }
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s%ld%s%0*ld%s%hhu",
        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        META_DIR_STR, OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR,
        static_cast<int>(OBJ_TIERED_METADATA_SEQ_WIDTH), file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s%ld%s%0*ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, static_cast<int>(OBJ_TIERED_METADATA_SEQ_WIDTH),
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  bool is_in_table = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  char tablet_name[512] = {0};
  const char *object_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else {
    is_in_table = 0 == STRNCMP(path, OB_TABLE_PREFIX, STRLEN(OB_TABLE_PREFIX))
        || 0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))
        || 0 == STRNCMP(path, TABLET_DIR_STR, STRLEN(TABLET_DIR_STR))
        || 0 == STRNCMP(path, "/ls/", STRLEN("/ls/"))
        || 0 == STRNCMP(path, "/tablet/", STRLEN("/tablet/"));
    if (0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))) {
      object_path = path;
    } else if (OB_NOT_NULL(object_path = STRSTR(path, "/ls/"))) {
      ++object_path;
    }
    if (OB_NOT_NULL(object_path)) {
      is_inner_tablet = true;
      if (OB_FAIL(databuff_printf(format, sizeof(format),
          "%s/%%ld/%%[^/]/%s/%s%%ld%s%%ld.T%hhu", LS_DIR_STR, META_DIR_STR,
          OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
        LOG_WARN("fail to databuff printf", KR(ret));
      } else if (FALSE_IT(num = sscanf(object_path, format, &ls_id, tablet_name, &op_id, &seq_id))) {
      } else if (OB_UNLIKELY(4 != num)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path), K(format), K(num));
      } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
        LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
      }
    } else {
      if (0 == STRNCMP(path, TABLET_DIR_STR, STRLEN(TABLET_DIR_STR))) {
        object_path = path;
      } else if (OB_NOT_NULL(object_path = STRSTR(path, "/tablet/"))) {
        ++object_path;
      }
      if (OB_ISNULL(object_path)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path));
      } else if (OB_FAIL(databuff_printf(format, sizeof(format),
          "%s/%%ld/%%ld/%s/%s%%ld%s%%ld.T%hhu", TABLET_DIR_STR, META_DIR_STR,
          OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
        LOG_WARN("fail to databuff printf", KR(ret));
      } else if (FALSE_IT(num = sscanf(object_path, format, &tablet_id,
                                        &reorganization_scn, &op_id, &seq_id))) {
      } else if (OB_UNLIKELY(4 != num)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path), K(format), K(num));
      }
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_location(is_in_table
        ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    relative_remote_path_to_macro_id = '''
int relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return remote_path_to_macro_id(path, macro_id);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
        SHARED_TABLET_SUB_META_CACHE_DIR_STR, LS_DIR_STR, file_id.ls_id(),
        get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, file_id.op_id(),
        SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
      SHARED_TABLET_SUB_META_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t ls_id = 0;
  int64_t reorganization_scn = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  if (nullptr != strstr(path, "/ls/")) {
    const char *sub_path = ObString(path).reverse_find('/', 2);
    if (OB_ISNULL(sub_path)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu", OP_KEY_STR, SEQ_KEY_STR,
        (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1,
                                      tablet_name_part2, &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s",
                                       tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = ObString(path).reverse_find('/', 1);
    if (OB_ISNULL(sub_path)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
        OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id,
                                      &reorganization_scn, &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_sub_meta_ls_id_dir(
        path, length, tenant_id, tenant_epoch_id, file_id.ls_id()))) {
      LOG_WARN("fail to get shared tablet sub meta ls id dir", KR(ret));
    }
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_sub_meta_scatter_dir(
      path, length, tenant_id, tenant_epoch_id,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {
    LOG_WARN("fail to get shared tablet sub meta scatter dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(OB_DIR_MGR.create_shared_tablet_sub_meta_ls_id_dir(
        tenant_id, tenant_epoch_id, file_id.ls_id()))) {
      LOG_WARN("fail to create shared tablet sub meta ls id dir", KR(ret));
    }
  } else if (OB_FAIL(OB_DIR_MGR.create_shared_tablet_sub_meta_scatter_dir(
      tenant_id, tenant_epoch_id,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {
    LOG_WARN("fail to create shared tablet sub meta scatter dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,ls_id=%lu,op_id=%lu,seq_id=%lu,in_table=%d)",
      get_type_str(), opt.ss_tablet_sub_meta_opt_.tablet_id_,
      opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.op_id_,
      opt.ss_tablet_sub_meta_opt_.data_seq_, opt.ss_tablet_sub_meta_opt_.is_in_table_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(0, object_id);
  object_id.set_second_id(opt.ss_tablet_sub_meta_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_tablet_sub_meta_opt_.is_inner_tablet_,
      opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_tablet_sub_meta_opt_.op_id_);
  object_id.set_seq_id(opt.ss_tablet_sub_meta_opt_.data_seq_);
  object_id.set_location(opt.ss_tablet_sub_meta_opt_.is_in_table_
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  return ret;
}
''',
)
def_storage_object_type_cfg(
    obj_type = 'TENANT_ROOT_KEY',
    id = 22,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_overwrite = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // TENANT_ROOT_KEY.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s%s%hhu", get_type_str(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/TENANT_ROOT_KEY.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(0);
  object_id.set_third_id(0);
  object_id.set_fourth_id(0);
  object_id.set_fifth_id(0);
  return OB_SUCCESS;
}''',
)

def_storage_object_type_cfg(
    obj_type = 'EXTERNAL_TABLE_FILE',
    id = 23,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tenant_data',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:server-level seq id, third_id:N/A, fourth_id:offset / 2MB
  return (file_id.second_id() < UINT64_MAX) && (0 == file_id.third_id()) &&
         (file_id.fourth_id() >= 0) && (file_id.fourth_id() < INT64_MAX);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tenant_id_epoch_id/external_table_file/scatter_id/seq%ldidx%ld.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%lu%s%ld%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              EXTERNAL_TABLE_FILE_DIR_STR, (file_id.hash() % ObDirManager::EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM),
              SEQ_KEY_STR, file_id.second_id(), IDX_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    uint64_t server_seq_id = 0;
    int64_t offset_idx = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld.T%hhu",
                SEQ_KEY_STR, IDX_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &server_seq_id, &offset_idx))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(server_seq_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(offset_idx);
    }
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (server_seq_id=%lu, offset_idx=%ld)",
              get_type_str(), opt.ss_external_table_file_opt_.server_seq_id_,
              opt.ss_external_table_file_opt_.offset_idx_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos),
              K(get_type_str()),
              K(opt.ss_external_table_file_opt_.server_seq_id_),
              K(opt.ss_external_table_file_opt_.offset_idx_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_external_table_file_opt_.server_seq_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_external_table_file_opt_.offset_idx_);
  return OB_SUCCESS;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, EXTERNAL_TABLE_FILE_DIR_STR))) {
    LOG_WARN("failed to get external table file dir", KR(ret),
        K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}
''',
)
def_storage_object_type_cfg(
    obj_type = 'MACRO_CACHE_CKPT_DATA',
    id = 24,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:version_id, third_id:N/A, fourth_id:tenant-level seq_id
  return (file_id.second_id() < UINT64_MAX) && (0 == file_id.third_id()) &&
         (file_id.fourth_id() < UINT64_MAX);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/data/version_id/seq_id.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%lu/%s%lu%s%hhu", object_storage_root_dir,
              CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
              MACRO_CACHE_CKPT_DIR_STR, DATA_MACRO_DIR_STR, file_id.second_id(), SEQ_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  // /version_id/seq_id
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t second_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu/%s%%lu.T%hhu", SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &second_id, &seq_id))) {
  } else if (OB_UNLIKELY(2 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(second_id);
    macro_id.set_third_id(0);
    macro_id.set_fourth_id(seq_id);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (version_id_=%lu, seq_id=%lu)", get_type_str(),
              opt.ss_macro_cache_ckpt_opt_.version_id_, opt.ss_macro_cache_ckpt_opt_.seq_id_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_macro_cache_ckpt_opt_.version_id_), K(opt.ss_macro_cache_ckpt_opt_.seq_id_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_macro_cache_ckpt_opt_.version_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_macro_cache_ckpt_opt_.seq_id_);
  return OB_SUCCESS;
}
''',
)
def_storage_object_type_cfg(
    obj_type = 'MACRO_CACHE_CKPT_META',
    id = 25,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id: version id
  return (file_id.second_id() < UINT64_MAX);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/meta/version_id.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%s%lu%s%hhu",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
              tenant_id, tenant_epoch_id, MACRO_CACHE_CKPT_DIR_STR, META_DIR_STR,
              VER_KEY_STR, file_id.second_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  // /version_id
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t version_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%lu.T%hhu", VER_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &version_id))) {
  } else if (OB_UNLIKELY(1 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(version_id);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (version_id_=%lu)", get_type_str(),
              opt.ss_macro_cache_ckpt_opt_.version_id_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_macro_cache_ckpt_opt_.version_id_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_macro_cache_ckpt_opt_.version_id_);
  return OB_SUCCESS;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_INC_MAJOR_DATA_MACRO',  #ObSharedIncMajorDataMacroType
    id = 26,
    owner = 'cyh438514',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_fd_cache = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, INC_MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.op_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, DATA_MACRO_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}
''',
    is_path_include_inner_tablet = True,
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_INC_MAJOR_META_MACRO',  #ObSharedIncMajorMetaMacroType
    id = 27,
    owner = 'cyh438514',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_fd_cache = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, INC_MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.op_id(), META_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, META_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}
''',
    is_path_include_inner_tablet = True,
)


# Import optimized SHARED_MINI_DATA_MACRO_V2, SHARED_MINI_META_MACRO_V2, SHARED_MINOR_DATA_MACRO_V2, SHARED_MINOR_META_MACRO_V2 configurations
exec(open("shared_mini_minor_data_macro_config.py").read())

# Legacy object type definitions are kept for reference and intentionally disabled.
r"""
def_storage_object_type_cfg(
    obj_type = 'SHARED_TABLET_MACRO_DIFF',  #ObSharedTabletMacroDiffType
    id = 88,
    owner = 'cxf262476',
    access_mode = 'shared',
    data_type = 'tablet_meta',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_path_include_inner_tablet = True,
    is_shared_tablet_macro_diff = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
}
''',
)
"""

def_storage_object_type_cfg(
    obj_type = 'SHARED_INC_MAJOR_COMPACTION_DATA_MACRO',  #ObSharedIncMajorCompactionDataMacroType
    id = 28,
    owner = 'cw403059',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_fd_cache = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, INC_MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.op_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, DATA_MACRO_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}
''',
    is_path_include_inner_tablet = True,
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_INC_MAJOR_COMPACTION_META_MACRO',  #ObSharedIncMajorCompactionMetaMacroType
    id = 29,
    owner = 'cw403059',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_fd_cache = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, INC_MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.op_id(), META_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, META_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
    const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}
''',
    is_path_include_inner_tablet = True,
)
def_storage_object_type_cfg(
    obj_type = 'SHARED_LS_CKPT',  #ObSharedLsCkptType
    id = 30,
    owner = 'donglou.zl',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    support_store_in_table = True,
    ss_meta_ctrl_family = 'LS',
    ss_meta_ctrl_lock_type = 'SHARED_LS_CKPT_LOCK',
    ss_meta_ctrl_dir = 'META_DIR_STR',
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && 0 == file_id.third_id() && file_id.meta_scn() >= 0 && 0 == file_id.seq_id();
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.is_in_table() || file_id.is_in_control()) {
    if (OB_FAIL(to_relative_remote_path_format(path, length, pos, file_id))) {
      LOG_WARN("failed to format table key", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s%020ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
      LS_DIR_STR, file_id.second_id(), META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s%020ld%s%hhu",
      LS_DIR_STR, file_id.second_id(), META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t ls_id = 0;
  int64_t meta_scn = 0;
  const char *object_path = nullptr;
  bool is_in_table = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_in_table = 0 == STRNCMP(path, OB_TABLE_PREFIX, STRLEN(OB_TABLE_PREFIX))
        || 0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))
        || 0 == STRNCMP(path, "/ls/", STRLEN("/ls/"));
    if (0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))) {
      object_path = path;
    } else if (OB_NOT_NULL(object_path = STRSTR(path, "/ls/"))) {
      ++object_path;
    }
    if (OB_ISNULL(object_path)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(format, sizeof(format),
        "%s/%%ld/%s/%s%%020ld%s%hhu", LS_DIR_STR, META_DIR_STR, SCN_KEY_STR,
        OBJECT_TYPE_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(object_path, format, &ls_id, &meta_scn))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_location(is_in_table
        ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(ls_id);
    macro_id.set_third_id(0);
    macro_id.set_meta_scn(meta_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
''',
    relative_remote_path_to_macro_id = '''
int relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return remote_path_to_macro_id(path, macro_id);
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(0, object_id);
  object_id.set_location(opt.ss_ls_meta_opt_.is_in_table_
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  object_id.set_second_id(opt.ss_ls_meta_opt_.ls_id_);
  object_id.set_third_id(0);
  object_id.set_meta_scn(opt.ss_ls_meta_opt_.meta_scn_);
  object_id.set_seq_id(0);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_LS_META',  #ObSharedLsMetaType
    id = 31,
    owner = 'donglou.zl',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    support_store_in_table = True,
    ss_meta_ctrl_family = 'LS',
    ss_meta_ctrl_lock_type = 'SHARED_LS_LOCK',
    ss_meta_ctrl_dir = 'META_DIR_STR',
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && 0 == file_id.third_id() && file_id.meta_scn() >= 0 && 0 == file_id.seq_id();
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.is_in_table() || file_id.is_in_control()) {
    if (OB_FAIL(to_relative_remote_path_format(path, length, pos, file_id))) {
      LOG_WARN("failed to format table key", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s%020ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
      LS_DIR_STR, file_id.second_id(), META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos,
    const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s%020ld%s%hhu",
      LS_DIR_STR, file_id.second_id(), META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    remote_path_to_macro_id = '''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t ls_id = 0;
  int64_t meta_scn = 0;
  const char *object_path = nullptr;
  bool is_in_table = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_in_table = 0 == STRNCMP(path, OB_TABLE_PREFIX, STRLEN(OB_TABLE_PREFIX))
        || 0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))
        || 0 == STRNCMP(path, "/ls/", STRLEN("/ls/"));
    if (0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))) {
      object_path = path;
    } else if (OB_NOT_NULL(object_path = STRSTR(path, "/ls/"))) {
      ++object_path;
    }
    if (OB_ISNULL(object_path)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(format, sizeof(format),
        "%s/%%ld/%s/%s%%020ld%s%hhu", LS_DIR_STR, META_DIR_STR, SCN_KEY_STR,
        OBJECT_TYPE_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(object_path, format, &ls_id, &meta_scn))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_location(is_in_table
        ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(ls_id);
    macro_id.set_third_id(0);
    macro_id.set_meta_scn(meta_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
''',
    relative_remote_path_to_macro_id = '''
int relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return remote_path_to_macro_id(path, macro_id);
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(0, object_id);
  object_id.set_location(opt.ss_ls_meta_opt_.is_in_table_
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  object_id.set_second_id(opt.ss_ls_meta_opt_.ls_id_);
  object_id.set_third_id(0);
  object_id.set_meta_scn(opt.ss_ls_meta_opt_.meta_scn_);
  object_id.set_seq_id(0);
  return ret;
}
''',
)


def_storage_object_type_cfg(
    obj_type = 'SHARED_MARK_SWEEP_GC_DIAG',  #ObSharedMarkSweepGCDiagType
    id = 32,
    owner = 'binifei.bnf',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id: round_id
  // fourth_id: bit[0]=group(1bit), bits[1:8]=type(8bits), bits[9:28]=bucket(20bits), bits[29:63]=reserved
  const uint64_t encoded = static_cast<uint64_t>(file_id.fourth_id());
  const uint64_t group = encoded & 0x1;
  const uint64_t type = (encoded >> 1) & 0xFF;
  const uint64_t reserved = encoded >> 29;
  return (file_id.second_id() >= 0)
      && (0 == file_id.third_id())
      && (file_id.fourth_id() >= 0)
      && (type < 3)
      && (2 != type || 1 == group)
      && (0 == reserved)
      && (0 == file_id.fifth_id());
}''',
    to_relative_remote_path_format = '''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  static const char *GROUP_STRS[] = {"del", "kept"};
  static const char *TYPE_STRS[]  = {"macros", "tablet_meta_scns", "study_results"};
  const uint64_t encoded = static_cast<uint64_t>(file_id.fourth_id());
  const int64_t group = static_cast<int64_t>(encoded & 0x1);
  const int64_t type = static_cast<int64_t>((encoded >> 1) & 0xFF);
  const int64_t bucket = static_cast<int64_t>((encoded >> 9) & 0xFFFFF);
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // mark_sweep_gc_diag/round{id}/{del|kept}/{macros|tablet_meta_scns}/bkt{bucket}.T32
  // Study uses kept/study_results/bkt0.T32; old DEL/KEPT directories and encodings stay unchanged.
  if (OB_UNLIKELY(!is_valid(file_id) || type >= ARRAYSIZEOF(TYPE_STRS))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep gc diag object id", KR(ret), K(file_id), K(group), K(type), K(bucket));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s%ld/%s/%s/%s%ld%s%hhu",
                              MARK_SWEEP_GC_DIAG_DIR_STR, ROUND_DIR_STR, file_id.second_id(),
                              GROUP_STRS[group], TYPE_STRS[type],
                              BKT_KEY_STR, bucket, OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id,
    const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  static const int64_t GROUP_CNT = 2;
  static const int64_t TYPE_CNT = 3;
  static const int64_t MAX_BUCKET_CNT = 1L << 20;
  if (OB_UNLIKELY(opt.ss_mark_sweep_gc_diag_opt_.round_id_ < 0
      || opt.ss_mark_sweep_gc_diag_opt_.group_ < 0
      || opt.ss_mark_sweep_gc_diag_opt_.group_ >= GROUP_CNT
      || opt.ss_mark_sweep_gc_diag_opt_.type_ < 0
      || opt.ss_mark_sweep_gc_diag_opt_.type_ >= TYPE_CNT
      || (2 == opt.ss_mark_sweep_gc_diag_opt_.type_ && 1 != opt.ss_mark_sweep_gc_diag_opt_.group_)
      || opt.ss_mark_sweep_gc_diag_opt_.bucket_ < 0
      || opt.ss_mark_sweep_gc_diag_opt_.bucket_ >= MAX_BUCKET_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep gc diag object option", KR(ret), K(opt));
  } else {
    const uint64_t encoded = static_cast<uint64_t>(opt.ss_mark_sweep_gc_diag_opt_.group_)
        | (static_cast<uint64_t>(opt.ss_mark_sweep_gc_diag_opt_.type_) << 1)
        | (static_cast<uint64_t>(opt.ss_mark_sweep_gc_diag_opt_.bucket_) << 9);
    set_ss_object_first_id_(default_cg_id, object_id);
    object_id.set_second_id(opt.ss_mark_sweep_gc_diag_opt_.round_id_);
    object_id.set_third_id(0);
    object_id.set_fourth_id(static_cast<int64_t>(encoded));
    object_id.set_fifth_id(0);
  }
  return ret;
}
''',
)


def create_ss_meta_ctrl_path_cfg(family, dir_str, is_lock_row,
                                 include_inner_tablet=False):
    control_scn_validity = ('file_id.meta_scn() == 0'
                            if is_lock_row else 'file_id.meta_scn() >= 0')
    if family == 'LS':
        is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id: ls_id, third_id: N/A, fourth_id: control_scn, fifth_id: N/A
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.third_id() == 0
      && %s
      && file_id.fifth_id() == 0;
}
''' % control_scn_validity
        to_relative_remote_path_format = f'''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  }} else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s%020ld%s%hhu",
              LS_DIR_STR, file_id.second_id(), {dir_str},
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {{
    LOG_WARN("fail to format ss meta ctrl path", KR(ret), K(file_id));
  }}
  return ret;
}}
'''
        relative_remote_path_to_macro_id = f'''
int relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{{
  int ret = OB_SUCCESS;
  char format[512] = {{0}};
  int num = 0;
  int64_t ls_id = 0;
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  }} else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s/%%ld/%s/%s%%020ld%s%hhu",
              LS_DIR_STR, {dir_str}, SCN_KEY_STR, OBJECT_TYPE_STR, (uint8_t)type_))) {{
    LOG_WARN("fail to build ss meta ctrl path format", KR(ret), K(type_));
  }} else if (FALSE_IT(num = sscanf(path, format, &ls_id, &control_scn))) {{
  }} else if (OB_UNLIKELY(2 != num)) {{
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_WARN("unexpected ss meta ctrl path", KR(ret), K(path), K(format), K(num));
  }} else {{
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(ls_id);
    macro_id.set_third_id(0);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }}
  return ret;
}}
'''
    elif include_inner_tablet:
        if family not in ('TABLET', 'SSTABLE'):
            raise ValueError(
                "only TABLET/SSTABLE ctrl paths can include inner tablets")
        is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // [inner tablet] second_id: tablet_id, third_id: ls_id, fourth_id: control_scn
  // [user tablet]  second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && %s
      && ((file_id.meta_is_inner_tablet()
              && file_id.ls_id() > 0
              && file_id.ls_id() < INT64_MAX)
          || (!file_id.meta_is_inner_tablet()
              && file_id.reorganization_scn() >= 0))
      && file_id.fifth_id() == 0;
}
''' % control_scn_validity
        to_relative_remote_path_format = f'''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  }} else if (file_id.meta_is_inner_tablet()) {{
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {{
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl path argument", KR(ret), K(file_id));
    }} else if (OB_FAIL(databuff_printf(path, length, pos,
                "%s/%ld/%s/%s/%s%020ld%s%hhu",
                LS_DIR_STR, file_id.ls_id(), tablet_name, {dir_str},
                SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {{
      LOG_WARN("fail to format inner tablet ctrl path", KR(ret), K(file_id));
    }}
  }} else if (OB_FAIL(databuff_printf(path, length, pos,
              "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), {dir_str},
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {{
    LOG_WARN("fail to format user tablet ctrl path", KR(ret), K(file_id));
  }}
  return ret;
}}
'''
        relative_remote_path_to_macro_id = f'''
int relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{{
  int ret = OB_SUCCESS;
  char format[512] = {{0}};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {{0}};
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  }} else if (NULL != STRSTR(path, LS_DIR_STR)) {{
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%511[^/]/%s/%s%%020ld%s%hhu",
                LS_DIR_STR, {dir_str}, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {{
      LOG_WARN("fail to build inner tablet ctrl path format", KR(ret), K(type_));
    }} else if (FALSE_IT(num = sscanf(path, format, &ls_id, tablet_name, &control_scn))) {{
    }} else if (OB_UNLIKELY(3 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected inner tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }} else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {{
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    }} else {{
      is_inner_tablet = true;
    }}
  }} else {{
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
                TABLET_DIR_STR, {dir_str}, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {{
      LOG_WARN("fail to build user tablet ctrl path format", KR(ret), K(type_));
    }} else if (FALSE_IT(num = sscanf(
        path, format, &tablet_id, &reorganization_scn, &control_scn))) {{
    }} else if (OB_UNLIKELY(3 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected user tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }}
  }}
  if (OB_SUCC(ret)) {{
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }}
  return ret;
}}
'''
    else:
        is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && %s
      && file_id.reorganization_scn() >= 0
      && file_id.fifth_id() == 0;
}
''' % control_scn_validity
        to_relative_remote_path_format = f'''
int to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  }} else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), {dir_str},
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {{
    LOG_WARN("fail to format ss meta ctrl path", KR(ret), K(file_id));
  }}
  return ret;
}}
'''
        relative_remote_path_to_macro_id = f'''
int relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{{
  int ret = OB_SUCCESS;
  char format[512] = {{0}};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  }} else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
              TABLET_DIR_STR, {dir_str}, SCN_KEY_STR, OBJECT_TYPE_STR, (uint8_t)type_))) {{
    LOG_WARN("fail to build ss meta ctrl path format", KR(ret), K(type_));
  }} else if (FALSE_IT(num = sscanf(path, format, &tablet_id, &reorganization_scn, &control_scn))) {{
  }} else if (OB_UNLIKELY(3 != num)) {{
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_WARN("unexpected ss meta ctrl path", KR(ret), K(path), K(format), K(num));
  }} else {{
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(false, 0, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }}
  return ret;
}}
'''

    return {
        'is_valid': is_valid,
        'to_relative_remote_path_format': to_relative_remote_path_format,
        'relative_remote_path_to_macro_id': relative_remote_path_to_macro_id,
    }


def create_ss_meta_ctrl_type_cfg(obj_type, type_id, family, dir_str,
                                 is_lock_row=False, lock_type=None,
                                 include_inner_tablet=False,
                                 enum_comment=None,
                                 path_cfg_builder=create_ss_meta_ctrl_path_cfg):
    cfg = {
        'obj_type': obj_type,
        'id': type_id,
        'owner': 'muwei.ym',
        'access_mode': 'shared',
        'data_type': 'others',
        'read_odirect': False,
        'write_strategy': ["WRITE_THROUGH"],
        'always_store_in_table': True,
        'ss_meta_ctrl_family': family,
        'ss_meta_ctrl_should_backup': is_lock_row,
        'ss_meta_ctrl_is_lock_row': is_lock_row,
        'ss_meta_ctrl_lock_type': lock_type,
        'ss_meta_ctrl_dir': dir_str,
        'is_path_include_inner_tablet': include_inner_tablet,
    }
    if enum_comment is not None:
        cfg['enum_comment'] = enum_comment
    cfg.update(path_cfg_builder(
        family, dir_str, is_lock_row, include_inner_tablet))
    return cfg


def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_MINOR_SSTABLE_CTRL', 33, 'SSTABLE', 'META_DIR_STR',
    lock_type='SHARED_MINOR_LOCK', include_inner_tablet=True))

def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_MAJOR_SSTABLE_CTRL', 34, 'SSTABLE', 'META_DIR_STR',
    lock_type='SHARED_MAJOR_LOCK', include_inner_tablet=True))

def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_INC_MAJOR_SSTABLE_CTRL', 35, 'SSTABLE', 'META_DIR_STR',
    lock_type='SHARED_INC_MAJOR_LOCK', include_inner_tablet=True))

def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_LS_LOCK', 36, 'LS', 'LOCK_DIR_STR', is_lock_row=True))

def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_TABLET_LOCK', 37, 'TABLET', 'LOCK_DIR_STR', is_lock_row=True,
    include_inner_tablet=True))

def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_LS_CKPT_LOCK', 38, 'LS', 'LOCK_DIR_STR', is_lock_row=True))

def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_MINOR_LOCK', 39, 'SSTABLE', 'LOCK_DIR_STR', is_lock_row=True,
    include_inner_tablet=True,
    enum_comment='SPLIT 功能复用 MINOR lock row，不再单独定义 SPLIT lock row 类型。'))

def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_MAJOR_LOCK', 40, 'SSTABLE', 'LOCK_DIR_STR', is_lock_row=True,
    include_inner_tablet=True))

def_storage_object_type_cfg(**create_ss_meta_ctrl_type_cfg(
    'SHARED_INC_MAJOR_LOCK', 41, 'SSTABLE', 'LOCK_DIR_STR', is_lock_row=True,
    include_inner_tablet=True))
