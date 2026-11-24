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

[English](README.md) | [中文版](README_CN.md) | 日本語版

**OceanSeek** は、OceanBase AI Database のオープンソース版です。**複数のデータモデルを扱える分散データベースで、データレイクとデータベースの機能を一つの基盤に統合しています。**

OceanSeek は、大規模な本番環境で検証された OceanBase の分散基盤上に構築されています。リレーショナルテーブル、ワイドカラムテーブル、ベクトル、JSON、テキスト、空間データを扱い、トランザクション、アクセス権限、バージョン、データのライフサイクルを、どのモデルでも同じ仕組みで管理できます。AI アプリケーションや AI エージェントのデータ管理、マルチモーダルデータの処理、RAG、セマンティック検索、レイクハウスでの分析などに利用できます。

OceanSeek は、コンピュート、ログ、ストレージを分離した 3 層構成を採用しています。各層を独立して改良・拡張できるため、性能と可用性を確保しながら、負荷に応じてリソースやコストを調整できます。ログサービスが Paxos による 3 レプリカ構成でデータの永続性を保証するため、コンピュート層は単一レプリカ構成でも RPO（目標復旧時点）ゼロの高可用性を実現できます。コンピュート層を冗長化するコストを抑えながら、ノードの追加に応じて処理能力を線形に拡張できます。

# 主な特長

- **マルチモデル対応**：リレーショナルテーブル、ワイドカラムテーブルのほか、ベクトル、JSON ドキュメント、テキスト、空間データ、配列、マップをネイティブにサポートします。構造化データの列、JSON 列、ベクトル列、テキスト列を同じテーブルに定義し、型をまたいでクエリを実行できます。クエリの高速化には、データに応じてセカンダリインデックス、ベクトルインデックス（HNSW、IVF）、JSON インデックス、多値インデックス、全文インデックス（BM25）、GIS インデックス、検索インデックスを利用できます。
- **ハイブリッド検索**：ベクトル検索、全文検索、スカラー値による検索を組み合わせ、非構造化・半構造化・構造化データから 1 つの SQL 文で候補を取得できます。検索結果を統合して順位を付けるためのアルゴリズムとして、`RRF`、`MINMAX_NORMALIZER`、`WEIGHT_SUM` を備えています。
- **AI エージェントのデータ管理**：テナント、データベース、名前空間、テーブル、行・列の 5 つのレベルでデータを分離でき、数百万から数千万規模のエージェントに対応します。エージェントのメモリから関連する情報を取り出す処理も、1 つの SQL 文で実行できます。Fork Table と Fork Database はコピーオンライト方式を採用しており、テーブルやデータベースから数秒で検証用のサンドボックスを作成できます。元のデータは共有され、サンドボックスでの変更は分離されるため、検証に失敗しても本番データに影響を与えずにサンドボックスを破棄できます。
- **データベース内で完結する RAG パイプライン**：ドキュメントの解析・分割から、テキストのベクトル化、インデックスの構築、ハイブリッド検索、検索結果の統合とランキング、モデルによる推論までをデータベース内で実行できます。組み込みの AI 関数に加え、Python のユーザー定義関数（UDF）で独自の AI 処理を追加できます。ドキュメントを取り込んでから回答を得るまで、システム間でデータを移す必要はありません。
- **リアルタイムデータウェアハウス**：ストリーム処理とバッチ処理の両方に対応し、高頻度のリアルタイム書き込みにも、大量データの一括取り込みにも利用できます。マテリアライズドビューは差分更新と全件更新に対応します。データの格納形式は行形式、列形式、行・列の混在形式から選択できます。クエリの処理には、ベクトル化実行エンジンとネイティブの大規模並列処理（MPP）を利用できます。
- **データレイク、データウェアハウス、データベースの統合**：同じデータとメタデータを共有し、SQL の動作も共通化することで、データレイクの低コスト、データウェアハウスの分析機能、データベースのトランザクション処理を一つの基盤で実現します。Iceberg、Hive、Paimon と連携でき、Parquet、ORC、CSV 形式に対応します。CatalogSync を使ってデータベース内のテーブル情報を Hive Metastore（HMS）に同期し、メタデータを一元管理できます。
- **コンピュートとストレージの分離**：コンピュート、ログ、ストレージの各層を個別にデプロイし、拡張できます。コンピュートノードはステートレスなので、ノードを増減してもデータの移行は不要です。アクセス頻度の高いデータはローカル SSD にキャッシュし、アクセス頻度の低いデータはオブジェクトストレージに保持します。データの配置はアクセス頻度に応じて自動的に調整されます。
- **運用コストの削減**：OceanBase と同等のデータ圧縮性能を維持しつつ、コンピュートノードを単一レプリカ構成で運用することで、計算リソースのコストを抑えられます。各ノードがオブジェクトストレージ上のデータを共有するため、ストレージコストも削減できます。
- **MySQL・KV 互換**：MySQL モードと KV モード（OBKV-Table、OBKV-HBase）に対応します。ODP や MySQL 対応ドライバーのほか、OCP、ODC、OMS などの既存ツールを引き続き利用できます。
- **オープンなエコシステム**：S3 互換のオブジェクトストレージ、Iceberg や Paimon などのオープンなテーブルフォーマット、共通のカタログに対応します。データの複製やフォーマット変換を減らし、特定のシステムへの依存を抑えられます。

詳しくは、[OceanBase AI Database の製品ページ](https://www.oceanbase.com/product/ai-database)をご覧ください。

# クイックスタート

セットアップの手順は、[OceanSeek クイックスタート](https://open.oceanbase.com/oceanseek-quickstart)をご覧ください。

**事前準備**：OceanSeek はすべてのデータをオブジェクトストレージに永続化します。デプロイする前に、**S3 互換のオブジェクトストレージ**を用意し、**データ用バケット**と**ログ用バケット**を作成してください。バケットは個別に設定でき、データ用とログ用で異なる種類のストレージを使うこともできます。オブジェクトストレージには、S3 との互換性、高可用性、アクセス認証情報のローテーション、帯域幅の調整という 4 つの要件があります。

## 🐳 Dockerで始める

[OceanSeek の Docker イメージ](https://github.com/oceanbase/docker-images/tree/main/oceanseek)を使うと、Docker Compose で コマンドを 1 つ実行するだけで OceanSeek インスタンスを起動できます。手順は [Docker クイックスタート](https://open.oceanbase.com/oceanseek-quickstart)をご覧ください。

## ☸️ Kubernetes にデプロイする

[ob-operator](https://github.com/oceanbase/ob-operator) を使うと、Kubernetes 上で OceanSeek インスタンスをデプロイ・管理できます。手順は [ob-operator クイックスタート](https://open.oceanbase.com/oceanseek-quickstart)をご覧ください。

# 導入事例

OceanSeek と OceanBase は、同じ分散基盤を採用しています。OceanBase は金融、通信、小売、インターネットサービスなど、幅広い業種の 2,000 社以上のお客様に利用されています。

詳しくは、[OceanBase の導入事例](https://jp.oceanbase.com/customer)をご覧ください。

# システムアーキテクチャ

OceanSeek では、コンピュート、ログ、ストレージが独立しています。ステートレスなコンピュートノードが処理を担い、独立したログサービスが複数のレプリカでトランザクションログを保護します。すべてのデータはオブジェクトストレージに永続化されます。詳しくは、[システムアーキテクチャの概要](https://www.oceanbase.com/docs/common-oceanseek-1000000007251078)をご覧ください。

# コントリビューション

OceanSeek では、コミュニティの皆さんからのコントリビューションを歓迎しています。開発環境の準備、ビルド、ローカルでの実行手順は、[開発・ビルドガイド（中国語）](docs/docs/en/build-and-run.md)をご覧ください。Issue やプルリクエストを作成する前に、ガイドのコーディング規約と投稿時のルールをご確認ください。

# ライセンス

OceanSeek は Apache License 2.0 の下で提供されています。詳細は [LICENSE](LICENSE) をご覧ください。

## コミュニティ

- [GitHub Issues](https://github.com/oceanbase/oceanseek/issues)：OceanSeek の不具合報告や改善提案を受け付けています。
