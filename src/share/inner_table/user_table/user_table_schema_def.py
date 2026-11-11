# -*- coding: utf-8 -*-
# User table schema definitions.
#   inner table:  src/share/inner_table/ob_inner_table_schema_def.py
#   user table:   src/share/inner_table/user_table/user_table_schema_def.py
#
# Generate C++ (manual; commit ob_user_table_schema.{h,cpp} to git):
#   cd src/share/inner_table/user_table && python generate_user_table_schema.py
#
# Column tuple: (name, type, nullable, default)
#   nullable: 'true' / 'false', default: '' or SQL literal e.g. '0', 'NULL'
#
# Partition (optional, same as inner table def):
#   partition_columns = ['ls_id']
#   partition_expr = ['hash', 'ls_id', 128]
# Tablegroup (optional):
#   tablegroup_name = 'shared_meta'    # partitioned tables (hash by ls_id, 64 partitions)
#   tablegroup_name = 'oceanbase'  # non-partitioned tables
# 局部索引（可选）：仅支持普通非唯一局部索引。
#   index = {'索引名': {
#       'index_columns': ['索引列名'],
#       'store_columns': ['存储列名'],
#   }}
#
# Shared-storage SS Meta V2 tables (moved from inner table 596/597/598).

def_table_schema(
  owner = 'shared_storage',
  database_name = 'oceanbase',
  table_name = '__all_shared_meta',
  table_type = 'user_table',
  tablegroup_name = 'shared_meta',
  partition_columns = ['ls_id'],
  partition_expr = ['hash', 'ls_id', 128],
  # Cross-LS scans always filter by meta_type. Keep it before ls_id so the
  # tenant_id + meta_type predicate forms a contiguous rowkey prefix.
  rowkey_columns = [
    ('tenant_id', 'int', 'false', ''),
    ('meta_type', 'int', 'false', ''),
    ('ls_id', 'int', 'false', ''),
    ('meta_key', 'varchar:1024', 'false', ''),
  ],
  normal_columns = [
    ('meta_value', 'varbinary:OB_SS_MAX_META_VALUE_LENGTH', 'false', ''),
    ('gmt_create', 'timestamp', 'true', 'null'),
    ('gmt_modified', 'timestamp', 'true', 'null'),
  ],
)

def_table_schema(
  owner = 'shared_storage',
  database_name = 'oceanbase',
  table_name = '__all_shared_meta_control',
  table_type = 'user_table',
  tablegroup_name = 'shared_meta',
  partition_columns = ['ls_id'],
  partition_expr = ['hash', 'ls_id', 128],
  rowkey_columns = [
    ('tenant_id', 'int', 'false', ''),
    ('meta_type', 'int', 'false', ''),
    ('ls_id', 'int', 'false', ''),
    ('meta_key', 'varchar:1024', 'false', ''),
  ],
  normal_columns = [
    ('meta_value', 'varbinary:OB_MAX_VARCHAR_LENGTH', 'false', ''),
    ('gmt_create', 'timestamp', 'true', 'null'),
    ('gmt_modified', 'timestamp', 'true', 'null'),
  ],
)

def_table_schema(
  owner = 'shared_storage',
  database_name = 'oceanbase',
  table_name = '__all_shared_tiered_meta',
  table_type = 'user_table',
  tablegroup_name = 'shared_meta',
  partition_columns = ['ls_id'],
  partition_expr = ['hash', 'ls_id', 128],
  rowkey_columns = [
    ('tenant_id', 'int', 'false', ''),
    ('ls_id', 'int', 'false', ''),
    ('meta_key', 'varchar:1024', 'false', ''),
    ('part', 'int', 'false', ''),
  ],
  normal_columns = [
    ('meta_type', 'int', 'false', ''),
    ('meta_value', 'varbinary:OB_SS_MAX_META_VALUE_LENGTH', 'false', ''),
    ('gmt_create', 'timestamp', 'true', 'null'),
    ('gmt_modified', 'timestamp', 'true', 'null'),
  ],
)

# Tenant-level OUTTABLE LOB control table.
def_table_schema(
  owner = 'lob',
  database_name = 'oceanbase',
  table_name = '__all_outtable_lob_control',
  table_type = 'aux_outtable_lob_control',
  partition_columns = ['hash'],
  partition_expr = ['hash', 'hash', 128],
  rowkey_columns = [
    ('macro_id', 'varbinary:128', 'false', ''),
    ('hash', 'bigint unsigned', 'false', ''),
  ],
  normal_columns = [
    ('status', 'bigint', 'false', ''),
    ('last_modify_time', 'timestamp:6', 'false', ''),
    ('info', 'varbinary:OB_MAX_VARBINARY_LENGTH', 'true', 'NULL'),
  ],
  index = {
    'idx_lob_ctrl_hash_status': {
      'index_columns': ['hash', 'status', 'macro_id'],
    },
    'idx_lob_ctrl_status_time': {
      'index_columns': ['status', 'last_modify_time', 'macro_id'],
      'store_columns': ['info'],
    },
  },
)
