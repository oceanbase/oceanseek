CREATE OR REPLACE PACKAGE BODY dbms_direct_load_stream
  FUNCTION create_table(
    IN target_table          VARCHAR(65535),
    IN ttl_seconds           BIGINT) RETURN BIGINT;
    PRAGMA INTERFACE(C, DBMS_DIRECT_LOAD_STREAM_CREATE_TABLE);

  PROCEDURE close(
    IN stream_id             BIGINT);
    PRAGMA INTERFACE(C, DBMS_DIRECT_LOAD_STREAM_CLOSE);
END dbms_direct_load_stream;
