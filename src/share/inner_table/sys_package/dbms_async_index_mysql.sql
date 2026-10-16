CREATE OR REPLACE PACKAGE dbms_async_index AUTHID CURRENT_USER

  PROCEDURE refresh(
    IN     table_name    VARCHAR(65535));

  PROCEDURE forcemerge(
    IN     table_name    VARCHAR(65535));

END dbms_async_index;
