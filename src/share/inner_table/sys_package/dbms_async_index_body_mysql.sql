CREATE OR REPLACE PACKAGE BODY dbms_async_index

  -- refresh index
  PROCEDURE do_refresh(
    IN     table_name    VARCHAR(65535)
  );
  PRAGMA INTERFACE(C, DBMS_ASYNC_INDEX_MYSQL_REFRESH);

  PROCEDURE refresh(
    IN     table_name    VARCHAR(65535))
  BEGIN
    COMMIT;
    CALL do_refresh(table_name);
  END;

  -- forcemerge index
  PROCEDURE do_forcemerge(
    IN     table_name    VARCHAR(65535)
  );
  PRAGMA INTERFACE(C, DBMS_ASYNC_INDEX_MYSQL_FORCEMERGE);

  PROCEDURE forcemerge(
    IN     table_name    VARCHAR(65535))
  BEGIN
    COMMIT;
    CALL do_forcemerge(table_name);
  END;

END dbms_async_index;
