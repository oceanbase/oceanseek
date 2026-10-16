# User table schema (def + generated C++)

改 `user_table_schema_def.py` 后：

```bash
cd src/share/inner_table/user_table && python generate_user_table_schema.py
```

提交生成的 `ob_user_table_schema.h` / `ob_user_table_schema.cpp`（与 def 同步，随 observer 编译进二进制）。

STEP 7（`create_user_table_for_meta_tenant_`）通过生成函数填充 `share::schema::ObTableSchema`，再生成 DDL：表不存在则 CREATE；已存在则与 catalog 对比后 ALTER。
