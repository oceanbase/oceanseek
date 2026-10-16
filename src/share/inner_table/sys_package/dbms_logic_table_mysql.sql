CREATE OR REPLACE PACKAGE dbms_logic_table AUTHID CURRENT_USER

  PROCEDURE prewarm(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED);

  PROCEDURE drop_namespace(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED);

  PROCEDURE drop_ltable(
    IN collection_id VARCHAR(256),
    IN namespace_id  BIGINT UNSIGNED,
    IN ltable_id     BIGINT UNSIGNED);

  PROCEDURE set_namespace_resource_limit(
    IN collection_name VARCHAR(256),
    IN namespace_name  VARCHAR(256),
    IN config_json     VARCHAR(4096));

  PROCEDURE gather_namespace_stats_inner();

END dbms_logic_table;
