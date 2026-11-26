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
    <a href="https://discord.gg/74cF8vbNEs">
        <img alt="Support" src="https://img.shields.io/badge/Discord-Join%20Oceanbase-brightgreen?logo=discord" />
    </a>
    <a href="https://stackoverflow.com/questions/tagged/oceanbase">
        <img alt="Stack Overflow" src="https://img.shields.io/badge/Stack-Stack%20Overflow-brightgreen?logo=stackoverflow" />
    </a>
</p>

English | [中文版](README_CN.md) | [日本語版](README_JA.md)

**OceanSeek** is the open-source distribution of the OceanBase AI database.

OceanSeek is a next-generation distributed database featuring an integrated lakehouse architecture with disaggregated storage and compute. Built on OceanBase's large-scale, production-proven distributed platform, it is designed for AI applications, agent data management, multimodal data processing, RAG and semantic search, and lakehouse analytics. It unifies multiple data models, including relational, wide-column, vector, JSON, text, and spatial data, under a single set of transaction, access control, versioning, and lifecycle management mechanisms.

Architecturally, OceanSeek decouples the traditional database's compute, logging, and storage capabilities into three independently evolving layers. Each layer has clear responsibilities and can be extended independently, together providing high-performance, highly available, cost-effective, and elastically scalable database services. A key benefit of this architecture is that the compute layer can be deployed with a single replica, while data durability is independently guaranteed by three Paxos-backed replicas of the LogService. This delivers high availability with RPO = 0, preserves the linear scalability of the compute layer, and avoids the cost of deploying multiple compute replicas solely for reliability.

# Key Features

- **Multi-Model Integration**: Natively supports relational tables, wide-column tables, vectors, JSON documents, text, spatial data, arrays, maps, and more. A single table can contain structured columns, JSON columns, vector columns, and text columns, and queries can span multiple data types. Each data type has a corresponding indexing path, including secondary indexes, vector indexes (HNSW/IVF), JSON indexes, multi-value indexes, full-text indexes (BM25), GIS indexes, and search indexes.

- **Hybrid Search**: Combines vector, full-text, and scalar search across unstructured, semi-structured, and structured data. A single SQL statement can perform multi-path retrieval, with built-in rank-fusion algorithms such as RRF, MINMAX_NORMALIZER, and WEIGHT_SUM.

- **Agent-Native Architecture**: Provides five layers of isolation—tenant, database, namespace, table, and row/column level—to support data isolation for millions to tens of millions of agents. Memory retrieval can be completed with a single SQL statement. Forked tables and databases use copy-on-write to enable sandbox provisioning in seconds, allowing baseline data to be shared, incremental changes to remain isolated, and failed experiments to be discarded without affecting production data.

- **RAG Pipeline in DB**: Document parsing, document chunking, text vectorization, index building, hybrid search, rank fusion, and model inference are all completed inside the database. OceanSeek provides AI functions and supports extending AI operators through Python UDFs. Documents are ingested and answers are retrieved without moving data across multiple systems.

- **Real-time Data Warehouse**: Unifies stream and batch processing, supporting both high-frequency real-time writes and large-scale batch imports. It supports materialized views with incremental or full refresh, flexible row-based, column-based, and hybrid row-column storage layouts, and parallel execution through a vectorized engine and native MPP.

- **Lakehouse Integration**: Based on unified data, metadata, and SQL semantics, OceanSeek combines the low cost of a data lake, the analytical power of a data warehouse, and the transactional capabilities of a database. It supports integration with Iceberg, Hive, and Paimon, adapts to Parquet, ORC, and CSV formats, and synchronizes table metadata to HMS through CatalogSync for unified metadata management.

- **Storage-Compute Separation**: Compute, logs, and storage can be deployed and scaled independently. Compute nodes are stateless, enabling elastic scaling without data migration. Hot data is cached on local SSDs, while cold data is stored in object storage, and the system automatically migrates data based on access patterns.

- **Low Cost**: OceanSeek delivers data compression efficiency comparable to OceanBase. Compute nodes support single-replica deployment to reduce compute costs, while shared object storage reduces storage costs.

- **MySQL/KV Compatibility**: Supports both MySQL and KV modes (OBKV-Table and OBKV-HBase), and works with ODP, MySQL ecosystem drivers, and toolchains such as OCP, ODC, and OMS.

- **Open Ecosystem**: Supports S3-compatible object storage, open table formats such as Iceberg and Paimon, and a unified catalog, reducing data duplication, format conversion, and vendor lock-in.

For more information, please refer to [OceanBase AI Product](https://www.oceanbase.com/product/ai-database).

# Quick Start

For more information, please refer to [Quick Start Guide to OceanSeek](https://open.oceanbase.com/oceanseek-quickstart).

**Prerequisites**: OceanSeek uses object storage as the persistent storage layer for all data. Before deployment, you need to prepare an **S3-compatible** object storage system and create separate **data buckets** and **log buckets**. Different storage media are supported and can be configured independently. The object storage system must meet four requirements: S3 compatibility, high availability, access credential rotation, and adjustable bandwidth.

## 🐳 Start with Docker

You can start an OceanSeek instance in a Docker Compose environment by using the [Docker image](https://github.com/oceanbase/docker-images/tree/main/oceanseek). For more information, please refer to the [Docker Quick Start](https://open.oceanbase.com/oceanseek-quickstart).

## ☸️ Start with Kubernetes

You can use [ob-operator](https://github.com/oceanbase/ob-operator) to quickly deploy and manage OceanSeek in a Kubernetes environment. For more information, please refer to the [ob-operator Quick Start](https://open.oceanbase.com/oceanseek-quickstart).

# Case Studies

OceanBase serves over 2,000 clients across a wide range of industries, including financial services, telecommunications, retail, and the internet sector. OceanSeek shares the same distributed foundation as OceanBase.

For more information, please refer to [Customer Cases](https://www.oceanbase.com/customer).

# System Architecture

OceanSeek decouples compute, log, and storage into three layers: stateless compute nodes, a replicated LogService for transaction durability, and object storage for persistent data. For more information, please refer to the [System Architecture Introduction](https://docs.oceanbase.com/oceanbase-ai-database/system-architecture).

# Contributing

Community contributions are highly welcome. For development, compilation, and local execution instructions, please refer to the [Development and Build Guide](docs/docs/en/build-and-run.md). Before submitting an issue or pull request, it is recommended to read the code style guide and contribution conventions.

# License

OceanSeek is licensed under the Apache License, Version 2.0. For more information, please refer to the [LICENSE](LICENSE) file.

# Community

Join the OceanBase community via:

* [Discord](https://discord.gg/74cF8vbNEs): Best for asking questions, sharing feedback, getting the latest news, and connecting with other OceanBase users.
* [GitHub Issues](https://github.com/oceanbase/oceanseek/issues): Best for reporting bugs, requesting features, and tracking project issues.
* [Chinese User Forum](https://ask.oceanbase.com/): For Chinese users who want to ask questions, share feedback, get the latest news, and connect with other OceanBase users.
