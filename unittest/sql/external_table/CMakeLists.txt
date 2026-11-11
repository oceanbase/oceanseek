sql_unittest(test_parquet_bloom_filter)
sql_unittest(test_parquet_lazy_ranges)
target_sources(test_parquet_lazy_ranges PRIVATE
  ../engine/basic/utils/expr_maker.cpp)
sql_unittest(test_orc_bloom_filter)
sql_unittest(test_iceberg_delete_application)
sql_unittest(test_iceberg_partition_value)
target_sources(test_iceberg_partition_value PRIVATE
  ../engine/basic/utils/expr_maker.cpp)
sql_unittest(test_ext_plugin_task)
sql_unittest(test_ext_predicate_json)
file(COPY . DESTINATION . FILES_MATCHING PATTERN "*.parquet")
file(COPY . DESTINATION . FILES_MATCHING PATTERN "*.orc")

sql_unittest(test_ext_plugin_dispatcher)

sql_unittest(test_ext_plugin_mixed_reader)
target_sources(test_ext_plugin_mixed_reader PRIVATE
  ../engine/basic/utils/expr_maker.cpp)
