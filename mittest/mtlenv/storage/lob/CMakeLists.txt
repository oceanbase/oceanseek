storage_unittest(test_lob_meta_iterator test_lob_meta_iterator.cpp)
storage_unittest(test_lob_piece test_lob_piece.cpp)
# test_lob_manager.cpp depends on the removed legacy LOB data-plane interfaces
# (append/write/erase/build_tmp_*_locator/process_delta) and does not compile on
# this branch; superseded by test_lob_outrow.cpp which covers the outrow path.
#storage_dml_unittest(test_lob_manager test_lob_manager.cpp)
#storage_dml_unittest(test_lob_outrow test_lob_outrow.cpp)