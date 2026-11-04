if(OB_BUILD_OPENSOURCE)
  ob_unittest(test_vector_index_serialize)
else()
  set(DATA_ARCHIVE "${CMAKE_CURRENT_SOURCE_DIR}/data/dataset.tar.gz")
  set(BUILD_DATA_DIR "${CMAKE_CURRENT_BINARY_DIR}/data")

  file(MAKE_DIRECTORY ${BUILD_DATA_DIR})

  execute_process(
    COMMAND sh -c "tar xzf ${DATA_ARCHIVE} --directory ${BUILD_DATA_DIR}"
    RESULT_VARIABLE RET)
  if(RET AND NOT RET EQUAL 0)
    message(FATAL_ERROR "failed to extract dataset in ${DATA_ARCHIVE}")
  endif()

  ob_unittest(test_vector_index_serialize)
  ob_unittest(test_kmeans)
  ob_unittest(test_simd_vector_op)
  ob_unittest(test_hybrid_search)
  ob_unittest(test_ltable_bg_drop_ltable)
endif()

ob_unittest(test_vector_index_util)
ob_unittest(test_vsag_adaptor)
ob_unittest(test_vec_index_priority_queue_manager)
ob_unittest(test_vec_async_task_thread_pool)
ob_unittest(test_vec_idx_async_task_scheduler)
ob_unittest(test_ivf_map_cent_cache_ref_cnt)
ob_unittest(test_ivf_centroid_gc)
ob_unittest(test_spfresh_task_handler_retry)
ob_unittest(test_spfresh_null_vector_maintenance)
ob_unittest(test_ivf_cache_rewarm_id_reader)
ob_unittest(test_vector_index_snap_meta)
