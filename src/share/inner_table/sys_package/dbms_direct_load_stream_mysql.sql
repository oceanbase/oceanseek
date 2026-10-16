#package_name:dbms_direct_load_stream
#author:cw403059

CREATE OR REPLACE PACKAGE dbms_direct_load_stream AUTHID CURRENT_USER
  FUNCTION create_table(
    IN target_table          VARCHAR(65535),
    IN ttl_seconds           BIGINT) RETURN BIGINT;

  PROCEDURE close(
    IN stream_id             BIGINT);
END dbms_direct_load_stream;
