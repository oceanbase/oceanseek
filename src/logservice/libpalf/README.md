# logservice 接入点在线更新

通过完整 URL 更新 observer/libpalf 的对象存储接入点：

```sql
ALTER SYSTEM SET logservice_access_point =
  's3://bucket/log?host=http://storage.example.com:8082&access_id=<AK>&access_key=<SK>&s3_region=<region>&logservice_cluster_id=<当前日志服务集群ID>&addressing_model=path_style';
```

协议、bucket、目录和日志服务集群 ID 必须保持一致。endpoint、AK/SK、region 和寻址方式可在同一次 SQL 中更新。`addressing_model` 支持 `path_style`、`virtual_hosted_style`，未指定时沿用默认 virtual-hosted-style。

SQL 解析阶段对凭据加密，RootService 使用候选 URL 读取 ACCESS_POINT、验证当前集群身份，通过后沿用参数持久化和分发流程。每台 observer 在配置刷新时解密完整 URL，并同步调用 libpalf 热更新。校验只读取 ACCESS_POINT，不需要写入权限，也不依赖旧凭据继续有效。

libpalf 校验成功后在同一临界区替换 URL 和 AccessPointReader，LM 信息仍由原有后台任务刷新；已有日志流保留。更新请求串行执行，不新增应用版本控制。失败时保留旧运行态并记录错误，等待下一次配置刷新再次尝试，不新增自动重试任务。

SQL 成功表示配置已接受，不保证每台 observer 都已经应用。通过各节点日志确认生效：`update logservice access point succeeded` 表示本机已完成切换；`update logservice access point failed, will retry on next reload` 表示本次应用失败。沿用原有日志观测方式，不新增虚拟表状态列。新增的更新结果日志不展示完整 URL 或凭据。

配置刷新同步等待对象存储校验。若个别节点的 DNS/权限校验失败，配置已经持久化，但该节点保留旧 URL；修复可达性后，需要再次触发配置刷新才能重试。重启读取持久化配置，应确认所有 observer 已应用后再退役旧 endpoint/凭据。

本功能需要配套包含 `libpalf_update_object_store_url` 和 `libpalf_validate_object_store_url` 的 libpalf 库及头文件。LM/LN 的配置仍通过 `ls_ctrl update-object-store` 更新；两侧配置分别生效。
