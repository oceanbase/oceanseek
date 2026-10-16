#package_name:dbms_test
#author: lakebase

CREATE OR REPLACE PACKAGE BODY dbms_test

  FUNCTION register_mds(
    ls_id       BIGINT,
    tablet_id   BIGINT,
    mds_type    VARCHAR(65535),
    k           BIGINT,
    v           BIGINT,
    check_dup   BIGINT DEFAULT NULL
  )
  RETURN INT;
  PRAGMA INTERFACE(C, DBMS_TEST_REGISTER_MDS);

END dbms_test;
//
