#package_name:dbms_test
#author: lakebase

CREATE OR REPLACE PACKAGE dbms_test AUTHID CURRENT_USER

  FUNCTION register_mds(
    ls_id       BIGINT,
    tablet_id   BIGINT,
    mds_type    VARCHAR(65535),
    k           BIGINT,
    v           BIGINT,
    check_dup   BIGINT DEFAULT NULL
  )
  RETURN INT;

END dbms_test;
//
