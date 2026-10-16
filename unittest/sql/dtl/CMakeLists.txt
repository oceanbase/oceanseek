sql_unittest(test_dtl_rpc_channel)
sql_unittest(test_dtl_linked_buffer_serialize_compat)
sql_unittest(test_dtl_compression)
# Sender regression tests need real diagnostic counters, not the unittest perf-mode override.
get_target_property(dtl_compression_test_links test_dtl_compression LINK_LIBRARIES)
list(REMOVE_ITEM dtl_compression_test_links mock_di)
set_property(TARGET test_dtl_compression PROPERTY LINK_LIBRARIES "${dtl_compression_test_links}")
sql_unittest(benchmark_dtl_compression)
# Farm moves the unittest artifacts to a different workspace before running CTest.
file(COPY compression_data DESTINATION "${CMAKE_CURRENT_BINARY_DIR}")
target_compile_definitions(benchmark_dtl_compression PRIVATE
  DTL_COMPRESSION_DEFAULT_DATA_DIR="compression_data")
# Keep the executable relative, as ob_unittest does; NAME/COMMAND resolves targets
# to absolute build paths that no longer exist on the Farm execution node.
add_test(test_dtl_compression_tpc_samples benchmark_dtl_compression)
set_tests_properties(test_dtl_compression_tpc_samples PROPERTIES TIMEOUT 300)

sql_unittest(test_dtl_batch_unblock_race)
