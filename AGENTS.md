# ⛔ 强制约束（每次工具调用前必须检查）

## 进入子目录时必须读 AGENTS.md
进入子目录的时候，若该目录存在 AGENTS.md，**必须先 Read 该文件**，提取其中的禁止/强制规则，并在后续操作中严格遵守。
上下文压缩后约束不能丢失——不要依赖记忆，要重新读文件确认。

# 目录结构
- src：核心代码，4.6.0之前该目录是开源的，之后转为闭源。
- close_modules： 核心代码，4.6.0 之前该目录一直是闭源的。
- unittest： 单测目录，没有observer环境
- mittest:  有部分observer环境的模拟测试
- tools/obtest: 完整observer环境的测试
- tools/deploy/mysql_test/: 主要面向sql的测试

# 描述
- lakebase的observer只支持Shared Storage，不支持Shared Nothing。
- OceanBase AI是产品名， lakebase是代码库的名字。

# 禁止事项
- 禁止使用git push，必须使用 ob flow 工具链来进行代码提交管理。
- 禁止直接使用cmake, 必须使用build.sh init初始化项目。
- 禁止直接使用make, 必须使用ob-make 编译。
- 禁止访问oceanbase/oceanbase.git

