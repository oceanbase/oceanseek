# 目录结构
- src：核心代码
- close_modules： 不开源的核心代码
- unittest： 单测目录，没有observer环境
- mittest:  有部分observer环境的模拟测试
- tools/obtest: 完整observer环境的测试, tools/obtest/AGENTS.md描述了obtest的用法，按需加载
- tools/deploy/mysql_test: 主要面向sql的测试

# 描述
- 本代码库是lakebase的代码。
- OceanBase AI是产品名， lakebase是代码库的名字。
- lakebase 只支持Shared Storage + logservice，不支持Shared Nothing。代码库内Shared Nothing相关的代码均为历史遗留代码，考虑方案和编码的时候不需要考虑Shared Nothing功能。
- lakebase 的租户只支持MySQL模式，不支持Oracle模式。

# 禁止事项
- 禁止使用git push，必须使用 ob flow 工具链来进行代码提交管理。
- 禁止直接使用cmake, 必须使用build.sh init初始化项目。
- 禁止直接使用make, 必须使用ob-make 编译。
- 禁止访问oceanbase/oceanbase.git。
- 禁止关闭gitlab中别人的意见。
