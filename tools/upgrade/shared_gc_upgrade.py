#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
import fcntl
import json
import logging
import os
import sys
import tempfile
import time
import run_modules

from my_error import MyError


GC_PARAMETER = '_enable_shared_gc_service'
# Keep pre, post, and retry state beside the entry scripts, not in their extraction directories.
STATE_FILE = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),
                          'shared_gc_upgrade_state.json')
GC_DAG_TYPES = ('MARK_SWEEP', 'SS_GC_EXEC', 'SS_FAIL_GC',
                'SS_LOCK_ROW_GC', 'SS_LS_META_GC')
USER_COUNTER_NAMES = (
    'ob_max_used_shared_user_mark_sweep_gc_round_id',
    'ob_max_used_shared_user_mark_sweep_gc_task_id',
    'ob_max_used_shared_user_fail_gc_task_id',
    'ob_max_used_shared_user_fail_gc_macro_id_task_id',
    'ob_max_used_shared_user_ls_meta_gc_task_id')
LEGACY_COUNTER_NAMES = tuple(name.replace('ob_max_used_shared_user_', 'ob_max_used_shared_', 1)
                             for name in USER_COUNTER_NAMES)
CONTROL_COLUMNS = ('executor_server_id', 'gc_round_id', 'allow_gc_round_id',
                   'gc_meta_scn', 'last_succ_gc_meta_scn')


class SharedGCNotReady(MyError):
  pass


def create_upgrade(connection, cursor, host, port, user, password, timeout, module_set, is_pre):
  modules = ((run_modules.MODULE_BEGIN_UPGRADE, run_modules.MODULE_BEGIN_ROLLING_UPGRADE,
              run_modules.MODULE_SPECIAL_ACTION) if is_pre else
             (run_modules.MODULE_END_ROLLING_UPGRADE, run_modules.MODULE_TENANT_UPRADE,
              run_modules.MODULE_END_UPRADE, run_modules.MODULE_POST_CHECK))
  if not module_set.intersection(modules):
    return None
  return SharedGCUpgrade(connection, cursor,
                         dict(host=host, port=port, user=user, password=password,
                              database='oceanbase', raise_on_warnings=True), timeout)


def prepare_upgrade(gc_upgrade):
  if gc_upgrade is not None:
    autocommit = gc_upgrade.connection.autocommit
    try:
      gc_upgrade.connection.autocommit = True
      gc_upgrade.prepare()
    finally:
      gc_upgrade.connection.autocommit = autocommit


def resume_upgrade(gc_upgrade, module_set):
  modules = (run_modules.MODULE_END_ROLLING_UPGRADE, run_modules.MODULE_TENANT_UPRADE,
             run_modules.MODULE_END_UPRADE)
  remaining_modules = set(module_set)
  if gc_upgrade is not None and remaining_modules.intersection(modules):
    autocommit = gc_upgrade.connection.autocommit
    try:
      gc_upgrade.connection.autocommit = True
      state = gc_upgrade.state.data
      if state is not None and state.get('phase') in ('migrated', 'restoring', 'restored'):
        if not state.get('migration_complete', False):
          raise MyError('shared GC migration has not completed')
        is_finished = gc_upgrade.wait(gc_upgrade.check_if_upgrade_finished,
                                      'wait for consistent upgrade state before resuming')
        if is_finished:
          # Resume post checks and GC restoration without re-entering completed upgrade steps.
          remaining_modules.difference_update(modules)
          logging.info('END UPGRADE already completed; remaining modules: %s',
                       sorted(remaining_modules))
        elif state['phase'] != 'migrated':
          raise MyError('upgrade mode is active after GC restoration started; '
                        'check the saved upgrade job before retrying')
    finally:
      gc_upgrade.connection.autocommit = autocommit
  return remaining_modules


def close_upgrade(gc_upgrade):
  if gc_upgrade is not None:
    gc_upgrade.close()


def is_user_tenant(tenant_id):
  return tenant_id >= 1000 and tenant_id % 2 == 0


def config_bool(value):
  normalized = str(value).lower()
  if normalized in ('true', '1'):
    return True
  if normalized in ('false', '0'):
    return False
  raise MyError('invalid boolean configuration value: {0}'.format(value))


def unique_rows(rows, key_columns, description):
  result = {}
  for row in rows:
    key = tuple(row[name] for name in key_columns)
    if key in result:
      raise MyError('duplicate {0}: {1}'.format(description, key))
    result[key] = row
  return result


def check_dag_rows(instances, rows):
  expected = set((ip, port, tenant_id, kind)
                 for ip, port, tenant_id in instances for kind in GC_DAG_TYPES)
  actual = unique_rows(rows, ('svr_ip', 'svr_port', 'tenant_id', 'key'), 'GC DAG row')
  if set(actual) != expected:
    raise SharedGCNotReady('GC DAG rows do not cover tenant instances: missing={0}, extra={1}'
                          .format(sorted(expected - set(actual)), sorted(set(actual) - expected)))
  for key, row in actual.items():
    value = row['value']
    if value is None or int(value) != value or int(value) < 0:
      raise MyError('invalid GC DAG count: {0}, {1}'.format(key, value))
    if int(value) != 0:
      raise SharedGCNotReady('GC DAG is not drained: {0}, count={1}'.format(key, value))


class SharedGCState:
  def __init__(self, path):
    if not path:
      raise MyError('shared GC state file is required')
    self.path = os.path.abspath(path)
    self.lock_file = None
    self.data = None

  def open(self):
    # Keep the lock inode stable and persist state through atomic replacement.
    self.lock_file = open(self.path + '.lock', 'a+')
    try:
      fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
      self.close()
      raise MyError('another upgrade process uses state file: {0}'.format(self.path))
    if os.path.exists(self.path):
      with open(self.path, 'r') as source:
        self.data = json.load(source)
      if self.data.get('format') != 2:
        raise MyError('unsupported shared GC state format')

  def save(self):
    if self.lock_file is None or self.data is None:
      raise MyError('shared GC state is not locked or initialized')
    directory = os.path.dirname(self.path)
    fd, temporary = tempfile.mkstemp(prefix='.shared_gc_state.', dir=directory)
    try:
      with os.fdopen(fd, 'w') as target:
        json.dump(self.data, target, ensure_ascii=True, sort_keys=True, indent=2)
        target.write('\n')
        target.flush()
        os.fsync(target.fileno())
      os.rename(temporary, self.path)
      directory_fd = os.open(directory, os.O_RDONLY)
      try:
        os.fsync(directory_fd)
      finally:
        os.close(directory_fd)
    finally:
      if os.path.exists(temporary):
        os.unlink(temporary)

  def close(self):
    if self.lock_file is not None:
      self.lock_file.close()
      self.lock_file = None


class SharedGCUpgrade:
  def __init__(self, connection, cursor, connection_args, timeout):
    self.connection = connection
    self.cursor = cursor
    self.connection_args = dict(connection_args)
    self.state = SharedGCState(STATE_FILE)
    self.timeout = int(timeout) if int(timeout) > 0 else 600
    self.state.open()

  def close(self):
    self.state.close()

  def query(self, sql, args):
    self.cursor.execute(sql, args)
    columns = [item[0].lower() for item in self.cursor.description]
    return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

  def execute(self, sql, args):
    self.cursor.execute(sql, args)
    return self.cursor.rowcount

  def wait(self, check, description):
    deadline = time.time() + self.timeout
    while True:
      try:
        return check()
      except SharedGCNotReady as error:
        if time.time() >= deadline:
          raise MyError('{0} timed out: {1}'.format(description, error))
        logging.info('%s: %s', description, error)
        time.sleep(min(5, max(0, deadline - time.time())))

  def servers(self):
    rows = self.query('select svr_ip, svr_port, inner_port, zone, status, '
                      'start_service_time, build_version from oceanbase.__all_server', ())
    result = unique_rows(rows, ('svr_ip', 'svr_port'), 'observer')
    if not result:
      raise MyError('observer inventory is empty')
    for row in rows:
      if str(row['status']).lower() != 'active' or int(row['start_service_time']) <= 0:
        raise SharedGCNotReady('observer is not serving: {0}'.format(row['svr_ip']))
    return result

  def tenants(self):
    rows = self.query('select tenant_id, tenant_name, status from oceanbase.__all_tenant '
                      'order by tenant_id', ())
    result = unique_rows(rows, ('tenant_id',), 'tenant')
    if (1,) not in result:
      raise MyError('SYS tenant is missing')
    for row in rows:
      if str(row['status']).upper() != 'NORMAL':
        raise SharedGCNotReady('tenant is not NORMAL: {0}'.format(row['tenant_id']))
    return rows

  def check_tenant_jobs(self):
    for table in ('DBA_OB_TENANTS where status != \'NORMAL\'',
                  'CDB_OB_RESTORE_PROGRESS', '__all_virtual_clone_job'):
      rows = self.query('select count(*) as cnt from oceanbase.' + table, ())
      if len(rows) != 1 or rows[0]['cnt'] != 0:
        raise SharedGCNotReady('tenant creation/restore/clone is not idle: {0}'.format(table))

  def instances(self, tenants, servers):
    rows = self.query('select svr_ip, svr_port, tenant_id, stopped '
                      'from oceanbase.__all_virtual_dump_tenant_info', ())
    tenant_ids = set(row['tenant_id'] for row in tenants)
    rows = [row for row in rows if row['tenant_id'] in tenant_ids]
    result = unique_rows(rows, ('svr_ip', 'svr_port', 'tenant_id'), 'tenant instance')
    for server in servers:
      if server + (1,) not in result:
        raise SharedGCNotReady('observer SYS instance is missing: {0}'.format(server))
    if set(key[2] for key in result) != tenant_ids:
      raise SharedGCNotReady('tenant instance inventory is incomplete')
    for key, row in result.items():
      if key[:2] not in servers or int(row['stopped']) != 0:
        raise SharedGCNotReady('tenant instance is stopped or outside inventory: {0}'.format(key))
    return set(result)

  def check_upgrade_mode(self, servers):
    rows = self.query('select svr_ip, svr_port, value from oceanbase.GV$OB_PARAMETERS '
                      'where name = %s', ('enable_upgrade_mode',))
    actual = unique_rows(rows, ('svr_ip', 'svr_port'), 'upgrade mode')
    if set(actual) != set(servers) or any(not config_bool(row['value'])
                                        for row in actual.values()):
      raise SharedGCNotReady('upgrade mode must be True on every expected observer')

  def check_if_upgrade_finished(self):
    servers = self.servers()
    rows = self.query('select svr_ip, svr_port, name, value from oceanbase.GV$OB_PARAMETERS '
                      'where name in (%s, %s)', ('enable_upgrade_mode', '_upgrade_stage'))
    actual = unique_rows(rows, ('svr_ip', 'svr_port', 'name'), 'upgrade completion')
    expected = set(server + (name,) for server in servers
                   for name in ('enable_upgrade_mode', '_upgrade_stage'))
    if set(actual) != expected:
      raise SharedGCNotReady('upgrade completion rows do not cover every observer')
    is_finished = True
    is_upgrading = True
    for server in servers:
      enabled = config_bool(actual[server + ('enable_upgrade_mode',)]['value'])
      stage = str(actual[server + ('_upgrade_stage',)]['value']).upper()
      is_finished = is_finished and not enabled and stage == 'NONE'
      is_upgrading = is_upgrading and enabled and stage in ('PREUPGRADE', 'DBUPGRADE', 'POSTUPGRADE')
    if not is_finished and not is_upgrading:
      raise SharedGCNotReady('upgrade state has not synchronized on every observer')
    return is_finished

  def check_upgrade_finished(self):
    if not self.check_if_upgrade_finished():
      raise SharedGCNotReady('END UPGRADE has not completed on every observer')

  def gc_values(self, instances):
    rows = self.query('select svr_ip, svr_port, tenant_id, value '
                      'from oceanbase.__all_virtual_tenant_parameter_info where name = %s',
                      (GC_PARAMETER,))
    actual = unique_rows(rows, ('svr_ip', 'svr_port', 'tenant_id'), 'GC configuration')
    if not instances.issubset(set(actual)):
      raise SharedGCNotReady('GC parameter rows do not cover tenant instances')
    return dict((key, config_bool(actual[key]['value'])) for key in instances)

  def check_gc_disabled(self, instances):
    if any(self.gc_values(instances).values()):
      raise SharedGCNotReady('shared GC is not disabled on every tenant instance')

  def raw_gc_configs(self, tenant_ids):
    rows = self.query('select tenant_id, zone, svr_type, svr_ip, svr_port, value, '
                      'config_version from oceanbase.__all_virtual_tenant_parameter '
                      'where name = %s', (GC_PARAMETER,))
    rows = [row for row in rows if row['tenant_id'] in tenant_ids]
    unique_rows(rows, ('tenant_id', 'zone', 'svr_type', 'svr_ip', 'svr_port'), 'GC scope')
    for row in rows:
      config_bool(row['value'])
    return rows

  def set_gc(self, cursor, tenant_name, value, scope):
    sql = 'alter system set ' + GC_PARAMETER + ' = %s'
    args = ['True' if value else 'False']
    if scope is not None and str(scope['svr_ip']).upper() != 'ANY':
      sql += ' server = %s'
      args.append('{0}:{1}'.format(scope['svr_ip'], scope['svr_port']))
    elif scope is not None and scope['zone']:
      sql += ' zone = %s'
      args.append(scope['zone'])
    if tenant_name is not None:
      sql += ' tenant = %s'
      args.append(tenant_name)
    cursor.execute(sql, tuple(args))

  def prepare_gc_upgrade_state(self, tenants, servers, values, configs):
    saved_state = self.state.data
    # A completed job starts a new snapshot; unfinished retries retain their original values.
    if saved_state is None or saved_state.get('phase') == 'restored':
      self.state.data = {
          'format': 2, 'tenants': tenants, 'migration_complete': False,
          'migration_tenants': [row['tenant_id'] for row in tenants
                                if is_user_tenant(row['tenant_id'])],
          'original_configs': configs,
          'original_values': [list(key) + [value] for key, value in sorted(values.items())]}
    else:
      saved_servers = unique_rows(saved_state['attempt_servers'], ('svr_ip', 'svr_port'),
                                  'saved observer')
      if set(saved_servers) != set(servers):
        raise MyError('observer inventory differs from the saved upgrade state')
      old_tenants = dict((row['tenant_id'], row['tenant_name']) for row in saved_state['tenants'])
      current_tenants = dict((row['tenant_id'], row['tenant_name']) for row in tenants)
      if any(current_tenants.get(key) != value for key, value in old_tenants.items()):
        raise MyError('a saved tenant was dropped or renamed')
      added_tenant_ids = set(current_tenants) - set(old_tenants)
      # Freeze the legacy scope after its first drain; later USER tenants use new fetchers.
      if not saved_state['migration_complete'] and 'original_gc_fetchers' not in saved_state:
        saved_state['migration_tenants'].extend(sorted(tenant_id for tenant_id in added_tenant_ids
                                                    if is_user_tenant(tenant_id)))
      # Preserve the first configuration snapshot on retry and append new tenant originals.
      saved_state['tenants'].extend(row for row in tenants if row['tenant_id'] in added_tenant_ids)
      saved_state['original_configs'].extend(
          row for row in configs if row['tenant_id'] in added_tenant_ids)
      saved_state['original_values'].extend(list(key) + [value] for key, value in values.items()
                                         if key[2] in added_tenant_ids)
    self.state.data['attempt_servers'] = list(servers.values())
    self.state.data['phase'] = 'preparing'
    self.state.data['original_gc_controls'] = {}
    self.state.save()

  def prepare(self):
    # Confirm upgrade mode before saving original values and disabling GC.
    servers = self.servers()
    self.check_upgrade_mode(servers)
    self.check_tenant_jobs()
    tenants = self.tenants()
    instances = self.instances(tenants, servers)
    values = self.gc_values(instances)
    configs = self.raw_gc_configs(set(row['tenant_id'] for row in tenants))
    self.prepare_gc_upgrade_state(tenants, servers, values, configs)
    names = dict((row['tenant_id'], row['tenant_name']) for row in tenants)
    for tenant in tenants:
      self.set_gc(self.cursor, tenant['tenant_name'], False, None)
    for scope in configs:
      self.set_gc(self.cursor, names[scope['tenant_id']], False, scope)
    self.connection.commit()
    self.wait(lambda: self.check_gc_disabled(instances), 'disable shared GC')
    disabled_servers = self.servers()
    if set(disabled_servers) != set(servers):
      raise MyError('observer inventory changed while disabling shared GC; restart pre upgrade')
    disabled_tenants = self.tenants()
    if disabled_tenants != tenants:
      raise MyError('tenant inventory changed while disabling shared GC; restart pre upgrade')
    disabled_instances = self.instances(disabled_tenants, disabled_servers)
    self.wait(lambda: self.check_gc_disabled(disabled_instances), 'confirm shared GC disabled')
    # Record the restart baseline only after GC is disabled and synchronized.
    restart_baseline = self.servers()
    if set(restart_baseline) != set(servers):
      raise MyError('observer inventory changed after disabling shared GC; restart pre upgrade')
    self.state.data['attempt_servers'] = list(restart_baseline.values())
    self.state.data['phase'] = 'prepared'
    self.state.save()

  def require_state(self, phases):
    phase = self.state.data.get('phase') if self.state.data is not None else None
    if self.state.data is None or phase not in phases:
      raise MyError('shared GC prerequisite is missing: state_file={0}, phase={1}, expected={2}; '
                    'run the required upgrade steps using the same entry script directory'
                    .format(self.state.path, phase, phases))

  def check_saved_inventory(self):
    servers = self.servers()
    baseline = unique_rows(self.state.data['attempt_servers'], ('svr_ip', 'svr_port'), 'saved observer')
    if set(servers) != set(baseline):
      raise MyError('observer inventory changed during shared GC upgrade')
    builds = set(str(row['build_version']).strip() for row in servers.values())
    if len(builds) != 1 or not next(iter(builds)):
      raise SharedGCNotReady('observers do not run one complete build_version')
    # The release workflow must verify that this build contains the GC fix.
    for key, row in servers.items():
      if int(row['start_service_time']) <= int(baseline[key]['start_service_time']):
        raise SharedGCNotReady('observer has not restarted since GC was disabled: {0}'.format(key))
    tenants = self.tenants()
    current = dict((row['tenant_id'], row['tenant_name']) for row in tenants)
    saved = dict((row['tenant_id'], row['tenant_name']) for row in self.state.data['tenants'])
    if current != saved:
      raise MyError('tenant inventory changed; review the scope and restart pre upgrade')
    return servers, tenants

  def check_gc_controls_idle(self, servers, instances):
    for tenant_id in self.state.data['migration_tenants']:
      with self.tenant_cursor(tenant_id - 1, servers, instances) as (_, cursor):
        control = self.read_gc_control(cursor, tenant_id, for_update=False)
      if control[0] != 'IDLE':
        raise SharedGCNotReady(
            'GC control is not idle: tenant_id={0}, status={1}, executor={2}'
            .format(tenant_id, control[0], control[1]))

  def check_drain(self):
    servers, tenants = self.check_saved_inventory()
    self.check_upgrade_mode(servers)
    self.check_tenant_jobs()
    instances = self.instances(tenants, servers)
    self.check_gc_disabled(instances)
    rows = self.query('select svr_ip, svr_port, tenant_id, `key`, `value` '
                      'from oceanbase.__all_virtual_dag_scheduler where value_type = %s '
                      'and `key` in (%s, %s, %s, %s, %s)', ('DAG_COUNT',) + GC_DAG_TYPES)
    tenant_ids = set(row['tenant_id'] for row in tenants)
    rows = [row for row in rows if row['tenant_id'] in tenant_ids]
    check_dag_rows(instances, rows)
    if self.instances(tenants, self.servers()) != instances:
      raise SharedGCNotReady('tenant placement changed while checking GC drain')
    # Old timers exit with the old process; new processes cannot start normal GC when disabled.
    self.check_gc_disabled(instances)
    return servers, tenants, instances

  @contextmanager
  def tenant_cursor(self, tenant_id, servers, instances):
    import mysql.connector
    candidates = sorted(key for key in instances if key[2] == tenant_id)
    if not candidates:
      raise MyError('tenant has no serving instance: {0}'.format(tenant_id))
    server = servers[candidates[0][:2]]
    args = dict(self.connection_args)
    args['host'] = server['svr_ip']
    args['port'] = int(server['inner_port'])
    connection = mysql.connector.connect(**args)
    cursor = connection.cursor(buffered=True)
    try:
      # Pin the saved tenant ID so a replacement with the same name cannot be modified.
      cursor.execute('alter system change tenant tenant_id = %s', (tenant_id,))
      yield connection, cursor
    finally:
      try:
        connection.rollback()
      finally:
        cursor.close()
        connection.close()

  def read_gc_control(self, cursor, tenant_id, for_update):
    sql = ('select status, ' + ', '.join(CONTROL_COLUMNS) +
           ' from oceanbase.__all_shared_gc_control where tenant_id = %s')
    if for_update:
      sql += ' for update'
    cursor.execute(sql, (tenant_id,))
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] not in ('IDLE', 'DOING'):
      raise MyError('USER GC control is missing or invalid: {0}'.format(tenant_id))
    return list(rows[0])

  def save_gc_upgrade_snapshot(self, servers, instances):
    controls = {}
    need_collect_legacy_fetchers = 'original_gc_fetchers' not in self.state.data
    fetchers = {} if need_collect_legacy_fetchers else self.state.data['original_gc_fetchers']
    for tenant_id in self.state.data['migration_tenants']:
      if need_collect_legacy_fetchers:
        with self.tenant_cursor(tenant_id, servers, instances) as (connection, cursor):
          values = self.read_gc_fetchers(cursor, tenant_id, LEGACY_COUNTER_NAMES, True)
          fetchers[str(tenant_id)] = dict((name, values.get(name, 1))
                                         for name in LEGACY_COUNTER_NAMES)
      with self.tenant_cursor(tenant_id - 1, servers, instances) as (connection, cursor):
        controls[str(tenant_id)] = self.read_gc_control(
            cursor, tenant_id, for_update=False)
    # Persist the complete first-drain baseline; standby replay may advance legacy IDs later.
    self.state.data['original_gc_fetchers'] = fetchers
    self.state.data['original_gc_controls'] = controls
    self.state.data['migration_complete'] = False
    self.state.data['phase'] = 'drained'
    self.state.save()

  def release_gc_executors_after_drain(self, servers, instances):
    # Restarted observers, disabled GC and zero DAGs make every remaining DOING row stale.
    for tenant_id in self.state.data['migration_tenants']:
      with self.tenant_cursor(tenant_id - 1, servers, instances) as (connection, cursor):
        cursor.execute('begin')
        control = self.read_gc_control(cursor, tenant_id, for_update=True)
        if control[0] == 'DOING':
          cursor.execute("update oceanbase.__all_shared_gc_control set status = 'IDLE' "
                         "where tenant_id = %s and status = 'DOING'", (tenant_id,))
          if cursor.rowcount != 1:
            raise MyError('unexpected affected rows when releasing drained GC executor: '
                          'tenant_id={0}, affected_rows={1}'.format(tenant_id, cursor.rowcount))
          updated = self.read_gc_control(cursor, tenant_id, for_update=False)
          if updated != ['IDLE'] + control[1:]:
            raise MyError('GC control fields changed while releasing drained executor: {0}'
                          .format(tenant_id))
        connection.commit()
        if control[0] == 'DOING':
          logging.info('released drained shared GC executor: tenant_id=%s, executor=%s',
                       tenant_id, control[1])

  def check_gc_drain_before_upgrade(self):
    self.require_state(('prepared', 'drained', 'migrated'))
    servers, tenants, instances = self.wait(self.check_drain, 'drain shared GC before data upgrade')
    self.release_gc_executors_after_drain(servers, instances)
    self.check_gc_controls_idle(servers, instances)
    if self.state.data['phase'] == 'prepared':
      self.save_gc_upgrade_snapshot(servers, instances)
    expected = set(str(value) for value in self.state.data['migration_tenants'])
    if set(self.state.data['original_gc_controls']) != expected:
      raise MyError('GC control snapshots do not cover all original USER tenants')
    fetchers = self.state.data.get('original_gc_fetchers', {})
    if set(fetchers) != expected or any(set(values) != set(LEGACY_COUNTER_NAMES)
                                       for values in fetchers.values()):
      raise MyError('GC fetcher snapshots do not cover all original USER tenants')
    return servers, tenants, instances

  def check_gc_upgrade_prerequisites(self):
    self.require_state(('drained', 'migrated'))
    servers, tenants, instances = self.check_gc_drain_before_upgrade()
    rows = self.query('select svr_ip, svr_port, value from oceanbase.GV$OB_PARAMETERS '
                      'where name = %s', ('_upgrade_stage',))
    actual = unique_rows(rows, ('svr_ip', 'svr_port'), 'post upgrade stage')
    if set(actual) != set(servers) or any(str(row['value']).upper() != 'POSTUPGRADE'
                                        for row in actual.values()):
      raise MyError('tenant data upgrade requires POSTUPGRADE on every observer')
    return servers, tenants, instances

  def read_gc_fetchers(self, cursor, tenant_id, names, allow_missing):
    cursor.execute('select name, value from oceanbase.__all_sys_stat '
                   'where tenant_id = 0 and zone = %s and name in (%s, %s, %s, %s, %s)',
                   ('',) + names)
    values = {}
    for name, value in cursor.fetchall():
      if name not in names or name in values or not str(value).isdigit():
        raise MyError('invalid GC fetcher row: tenant_id={0}, name={1}'.format(tenant_id, name))
      values[name] = int(value)
      if (not 0 <= values[name] < (1 << 63) or
          not allow_missing and not 0 < values[name] < (1 << 63) - 1):
        raise MyError('GC fetcher is outside its ID range: tenant_id={0}, name={1}'
                      .format(tenant_id, name))
    if not allow_missing and set(values) != set(names):
      raise MyError('USER GC fetchers are incomplete: {0}; the 4.6.3.1 compatibility processor '
                    'must run during a data-version upgrade from below 4.6.3.1'
                    .format(tenant_id))
    return values

  def check_gc_id_upgrade_complete(self):
    servers, tenants, instances = self.check_gc_upgrade_prerequisites()
    for tenant_id in self.state.data['migration_tenants']:
      old_values = self.state.data['original_gc_fetchers'][str(tenant_id)]
      with self.tenant_cursor(tenant_id - 1, servers, instances) as (connection, cursor):
        new_values = self.read_gc_fetchers(cursor, tenant_id, USER_COUNTER_NAMES, False)
        control = self.read_gc_control(cursor, tenant_id, for_update=False)
      original_control = self.state.data['original_gc_controls'][str(tenant_id)]
      # A disabled mark-sweep timer may release its existing executor without advancing GC.
      status_unchanged = control[0] == original_control[0]
      executor_released = original_control[0] == 'DOING' and control[0] == 'IDLE'
      if control[1:] != original_control[1:] or not (status_unchanged or executor_released):
        raise MyError('GC control changed during the data-version upgrade: {0}'.format(tenant_id))
      for old_name, new_name in zip(LEGACY_COUNTER_NAMES, USER_COUNTER_NAMES):
        old_value = old_values[old_name]
        if old_value >= (1 << 63) - 1 - 1000 or new_values[new_name] < old_value + 1000:
          raise MyError('USER GC fetcher was not calibrated: tenant_id={0}, name={1}, '
                        'legacy_baseline={2}, meta={3}; a same-version 4.6.3.1 binary replacement '
                        'does not run the compatibility processor'
                        .format(tenant_id, new_name, old_value, new_values[new_name]))
    self.state.data['migration_complete'] = True
    self.state.data['phase'] = 'migrated'
    self.state.save()

  def check_gc_restore_prerequisites(self):
    self.require_state(('migrated', 'restoring', 'restored'))
    if not self.state.data['migration_complete']:
      raise MyError('shared GC migration has not completed')
    self.wait(self.check_upgrade_finished, 'wait for END UPGRADE before post check or GC restore')

  def expected_gc_values(self, instances, servers):
    saved = dict((tuple(row[:3]), row[3]) for row in self.state.data['original_values'])
    result = {}
    for key in instances:
      if key in saved:
        result[key] = saved[key]
      else:
        matches = []
        for scope in self.state.data['original_configs']:
          if scope['tenant_id'] != key[2]:
            continue
          same_server = scope['svr_ip'] == key[0] and int(scope['svr_port']) == int(key[1])
          wildcard = str(scope['svr_ip']).upper() == 'ANY' and int(scope['svr_port']) == 0
          server_matches = same_server or wildcard
          if server_matches:
            zone_matches = not scope['zone'] or scope['zone'] == servers[key[:2]]['zone']
            if zone_matches:
              matches.append(scope)
        if matches:
          latest_scope = max(matches, key=lambda row: row['config_version'])
          result[key] = config_bool(latest_scope['value'])
        else:
          result[key] = True
    return result

  def check_restored(self):
    servers = self.servers()
    current = self.tenants()
    names = dict((row['tenant_id'], row['tenant_name']) for row in current)
    saved = self.state.data['tenants']
    if any(names.get(row['tenant_id']) != row['tenant_name'] for row in saved):
      raise MyError('a managed tenant was dropped or renamed before GC restoration')
    instances = self.instances(saved, servers)
    if self.gc_values(instances) != self.expected_gc_values(instances, servers):
      raise SharedGCNotReady('GC original values have not propagated to every managed instance')

  def restore_gc_service_config(self):
    self.check_gc_restore_prerequisites()
    tenants = self.state.data['tenants']
    scopes = self.state.data['original_configs']
    servers = self.servers()
    instances = self.instances(tenants, servers)
    if self.state.data['phase'] == 'migrated':
      # Resumed restoration may already have enabled GC, so its controls can be DOING.
      self.wait(lambda: self.check_gc_controls_idle(servers, instances),
                'wait for idle GC controls before restoring shared GC')
    self.state.data['phase'] = 'restoring'
    self.state.save()
    for tenant in tenants:
      tenant_scopes = [row for row in scopes if row['tenant_id'] == tenant['tenant_id']]
      # SET/RESET must run in SYS without switching to a different tenant.
      with self.tenant_cursor(1, servers, instances) as (connection, cursor):
        query_timeout = self.timeout * 1000 * 1000
        cursor.execute('set @@session.ob_query_timeout = %s, @@session.ob_trx_timeout = %s',
                       (query_timeout, query_timeout * (len(tenant_scopes) + 3)))
        cursor.execute('begin')
        # Keep the saved identity stable until all configuration RPCs have completed.
        cursor.execute('select tenant_name, status from oceanbase.__all_tenant '
                       'where tenant_id = %s for update', (tenant['tenant_id'],))
        rows = cursor.fetchall()
        if (len(rows) != 1 or rows[0][0] != tenant['tenant_name'] or
            str(rows[0][1]).upper() != 'NORMAL'):
          raise MyError('managed tenant was dropped, renamed or is not NORMAL: {0}'
                        .format(tenant['tenant_id']))
        has_global = any(not row['zone'] and str(row['svr_ip']).upper() == 'ANY' and
                         int(row['svr_port']) == 0 for row in tenant_scopes)
        if not has_global:
          # RESET restores the default; the original absence is saved in state.
          cursor.execute('alter system reset ' + GC_PARAMETER + ' tenant = %s',
                         (tenant['tenant_name'],))
        # Replay each tenant's version order to preserve server and zone overrides.
        for scope in sorted(tenant_scopes, key=lambda row: row['config_version']):
          self.set_gc(cursor, tenant['tenant_name'], config_bool(scope['value']), scope)
        # Configuration RPCs commit independently; this only releases the tenant row lock.
        connection.commit()
    self.wait(self.check_restored, 'restore original shared GC configuration')
    self.state.data['phase'] = 'restored'
    self.state.save()
