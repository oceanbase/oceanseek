CREATE OR REPLACE PACKAGE BODY dbms_logic_table

  PROCEDURE do_prewarm(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED
  );
  PRAGMA INTERFACE(C, DBMS_LOGIC_TABLE_MYSQL_PREWARM);

  PROCEDURE prewarm(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED)
  BEGIN
    COMMIT;
    CALL do_prewarm(collection_id, namespace_id);
  END;

  PROCEDURE do_drop_namespace(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED
  );
  PRAGMA INTERFACE(C, DBMS_LOGIC_TABLE_MYSQL_DROP_NAMESPACE);

  PROCEDURE drop_namespace(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED)
  BEGIN
    COMMIT;
    CALL do_drop_namespace(collection_id, namespace_id);
  END;

  PROCEDURE do_drop_ltable(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED,
    IN ltable_id     BIGINT UNSIGNED
  );
  PRAGMA INTERFACE(C, DBMS_LOGIC_TABLE_MYSQL_DROP_LTABLE);

  PROCEDURE drop_ltable(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED,
    IN ltable_id     BIGINT UNSIGNED)
  BEGIN
    COMMIT;
    CALL do_drop_ltable(collection_id, namespace_id, ltable_id);
  END;

  PROCEDURE do_set_namespace_resource_limit(
    IN collection_name VARCHAR(256),
    IN namespace_name  VARCHAR(256),
    IN config_json     VARCHAR(4096)
  );
  PRAGMA INTERFACE(C, DBMS_LOGIC_TABLE_MYSQL_SET_NAMESPACE_RESOURCE_LIMIT);

  PROCEDURE set_namespace_resource_limit(
    IN collection_name VARCHAR(256),
    IN namespace_name  VARCHAR(256),
    IN config_json     VARCHAR(4096))
  BEGIN
    COMMIT;
    CALL do_set_namespace_resource_limit(collection_name, namespace_name, config_json);
  END;

  PROCEDURE do_gather_namespace_stats_inner();
  PRAGMA INTERFACE(C, DBMS_LOGIC_TABLE_MYSQL_GATHER_NAMESPACE_STATS_INNER);

  PROCEDURE gather_namespace_stats_inner()
  BEGIN
    COMMIT;
    CALL do_gather_namespace_stats_inner();
  END;

END dbms_logic_table;
