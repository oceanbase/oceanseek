function(rs_unittest case)
  ob_unittest(${ARGV})
  target_sources(${case} PRIVATE 
  ob_rs_test_utils.h 
  ob_rs_test_utils.cpp 
  fake_zone_manager.cpp
  fake_freeze_info_manager.cpp
  fake_zone_merge_manager.cpp
  )
  target_link_libraries(${case} PRIVATE mockcontainer)
endfunction()


#rs_unittest(test_bootstrap)
rs_unittest(test_rs_reentrant_thread)
rs_unittest(test_tenant_all_zone_merge_strategy)
rs_unittest(test_primary_ls_service)
rs_unittest(test_ls_balance_helper)
#rs_unittest(test_multi_cluster_manager)
#rs_unittest(test_ob_backup_task_scheduler)
#rs_unittest(test_zone_merge_manager)
#rs_unittest(test_freeze_info_manager)
rs_unittest(test_ss_window_compaction_info)
rs_unittest(test_archive_checkpoint)
rs_unittest(test_import_table)
rs_unittest(test_heartbeat_service)
rs_unittest(test_ddl_task_struct)
ob_unittest(test_ddl_tablet_scheduler)
rs_unittest(test_fts_rowkey_doc_snapshot_hold)
rs_unittest(test_table_redefinition_task)
rs_unittest(test_fork_table_tablet_task_operator)
rs_unittest(test_fork_table_task)
rs_unittest(test_parallel_ddl_control)
rs_unittest(test_partition_balance)
rs_unittest(test_partition_exchange)
rs_unittest(test_ls_balance_group_info)
rs_unittest(test_dump_inner_table_schema)
rs_unittest(test_user_table_schema_applier)
rs_unittest(test_major_merge_progress_checker)
ob_unittest(test_daily_major_freeze_launcher)
rs_unittest(test_tenant_parallel_create_executor)
rs_unittest(test_standalone_launch)
rs_unittest(test_max_id_cache_unittest)
