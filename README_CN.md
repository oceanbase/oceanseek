<p align="center">
    <a href="https://github.com/oceanbase/oceanseek">
        <img alt="OceanBase Logo" src="images/logo.svg" width="50%" />
    </a>
</p>
<p align="center">
    <a href="https://www.oceanbase.com/docs/oceanseek">
        <img alt="Chinese doc" src="https://img.shields.io/badge/文档-简体中文-blue" />
    </a>
    <a href="https://en.oceanbase.com/docs/oceanseek">
        <img alt="English doc" src="https://img.shields.io/badge/docs-English-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanseek/commits/master">
        <img alt="last commit" src="https://img.shields.io/github/last-commit/oceanbase/oceanseek/master" />
    </a>
    <a href="https://github.com/oceanbase/oceanseek">
        <img alt="stars" src="https://img.shields.io/badge/dynamic/json?color=blue&label=stars&query=stargazers_count&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanseek" />
    </a>
    <a href="https://github.com/oceanbase/oceanseek/blob/master/LICENSE">
        <img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue" />
    </a>
</p>

<p align="center">
    <a href="https://ask.oceanbase.com">
        <img alt="中文论坛" src="https://img.shields.io/badge/Support-Join%20OceanBase-brightgreen?logo=support" />
    </a>
    <a href="images/dingtalk.png">
        <img alt="DingTalk" src="https://img.shields.io/badge/Dingtalk-Dingtalk-brightgreen?logo=Dingtalk" />
    </a>
</p>

[English](README.md) | 中文版 | [日本語版](README_JA.md)

**OceanSeek** 是 OceanBase AI 数据库的开源发行版。

OceanSeek是新一代**多模融合、湖库一体的存算分离分布式数据库**。它构建在 OceanBase 经过大规模生产验证的分布式底座之上，面向 AI 应用、Agent 数据管理、多模态数据处理、RAG/语义搜索与湖仓分析等场景，将关系、宽表、向量、JSON、文本、空间等多种数据模型统一到同一套事务、权限、版本与生命周期机制之下。

架构上，OceanSeek 将传统数据库的计算、日志、存储三大能力解耦为三个可独立演进的层次。各层职责清晰、可分别扩展，共同提供高性能、高可用、低成本与弹性伸缩的数据库服务。此架构带来的关键收益是：计算层可以单副本部署，数据持久性由日志服务的 Paxos 三副本独立保障，从而实现 RPO=0 的高可用，同时保持计算层的线性扩展能力——不必为了可靠性在计算层付出多副本成本。

# 关键特性
- **多模融合**：原生支持关系表、宽表、向量、JSON 文档、文本、空间、数组/映射等数据类型，同一张表可同时存在结构化列、JSON 列、向量列与文本列，查询时可跨类型。每种类型均有匹配的索引加速路径：二级索引、向量索引（HNSW/IVF）、JSON 索引、多值索引、全文索引（BM25）、GIS 索引、搜索索引。
- **混合搜索**：向量 + 全文 + 标量，非结构化 + 半结构化 + 结构化，1 条 SQL 完成多路召回，内置 RRF、MINMAX_NORMALIZER、WEIGHT_SUM 等融合排序算法。
- **Agent 原生**：提供租户级、Database 级、Namespace 级、表级、行/列级五层隔离，支撑百万/千万级 Agent 数据隔离；支持一条 SQL 完成记忆召回；Fork Table/Database 基于 Copy-on-Write 实现秒级数据沙箱，共享基线数据、隔离增量变更，试错失败可丢弃、不污染生产数据。
- **RAG Pipeline in DB**：文档解析、文档分割、文本向量化、索引构建、混合搜索、融合排序、模型推理全链路在库内完成。提供AI 函数，支持通过 Python UDF 扩展 AI 算子。文档入库、答案出库，无需在多个系统间搬运数据。
- **实时数仓**：流批一体，高频实时写入与大数据量批量导入均可；支持增量/全量刷新的物化视图；行存、列存、行列混存灵活定义；向量化执行引擎与原生 MPP 并行执行。
- **湖-仓-库一体**：基于同一份数据、同一套元数据、同一套 SQL 语义，同时满足湖的低成本、仓的分析能力、库的事务性要求。支持 Iceberg / Hive /Paimon 融合、Parquet/ORC/CSV 适配，并通过 CatalogSync 将库内表信息同步至 HMS，实现元数据统一管理。
- **存算分离**：计算、日志、存储独立部署、独立扩展；计算节点无状态，弹性扩缩容无需迁移数据；热数据缓存在本地 SSD，冷数据保留在对象存储，系统自动识别访问热度并迁移。
- **低成本**：保持与OceanBase同等的数据压缩能力的情况下，计算节点支持单副本部署以降低计算成本；数据共享存储在对象存储，降低存储成本。
- **MySQL / KV 兼容**：支持 MySQL 模式与 KV 模式（OBKV-Table、OBKV-HBase），沿用 ODP、MySQL 生态驱动与 OCP、ODC、OMS 等工具链。
- **开放生态**：支持 S3 协议对象存储、Iceberg/Paimon 开放表格式与统一 Catalog，减少数据复制、格式转换与系统锁定。

更多信息请参考 [OceanBase AI 产品](https://www.oceanbase.com/product/ai-database)

# 快速开始

更多信息参考[快速体验 OceanSeek](https://open.oceanbase.com/oceanseek-quickstart)。

**前置条件**：OceanSeek以对象存储作为全量数据的持久化底座，部署前需准备 **S3 协议兼容**的对象存储，并创建**数据桶**与**日志桶**（可异构介质、可独立配置）。对象存储需满足 S3 兼容性、高可用、访问凭据轮转与带宽可调四项要求。

## 🐳 使用 docker

使用[docker image](https://github.com/oceanbase/docker-images/tree/main/oceanseek)可在Docker Compose环境中一键拉起 OceanSeek 实例，可参考文档 [docker 快速上手](https://open.oceanbase.com/oceanseek-quickstart)了解具体的使用方法。

## ☸️ 使用 Kubernetes

使用 [ob-operator](https://github.com/oceanbase/ob-operator) 可在 Kubernetes 环境中快速部署和管理 OceanSeek实例，可参考文档 [ob-operator 快速上手](https://open.oceanbase.com/oceanseek-quickstart)了解具体的使用方法。

# 案例

OceanBase 已服务超过 2000 家来自不同行业的客户，包括金融服务、电信、零售、互联网等。OceanSeek 与 OceanBase 共享同一套分布式底座。

更详细的信息请参考[客户案例](https://www.oceanbase.com/customer)。

# 系统架构
OceanSeek 将计算、日志、存储三层解耦：计算节点无状态，事务日志由独立的日志服务多副本保障，全量数据持久化在对象存储。详见 [系统架构介绍](https://www.oceanbase.com/docs/common-oceanseek-1000000007251078)。

# 社区贡献

非常欢迎社区贡献。开发、编译与本地运行方式请参考[开发与构建指南](docs/docs/zh/build-and-run.md)。提交 Issue 或 PR 前，建议先阅读其中的代码规范与提交约定。

# License

OceanSeek 根据 Apache License 2.0 获得许可。有关详细信息，请参阅 [LICENSE](LICENSE) 文件。

# 社区

有以下加入社区的方法：

* [中文论坛](https://ask.oceanbase.com/)(推荐): 社区交流与分享, 寻求帮助, 获取最新信息.
* [钉钉群:  33254054](images/dingtalk.png): 寻求帮助, 获取最新信息.
* 微信群 (添加微信小助手: OBCE666): 社区交流与分享, 获取最新信息.
* [GitHub Issues](https://github.com/oceanbase/oceanseek/issues): 反馈使用 OceanSeek 时遇到的问题或建议。

