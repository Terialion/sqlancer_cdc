# Pull Images 决策清单（一页版）

适用场景：使用 pull_images.py 选择并生成 Flink CDC 运行环境。

## 1) Flink runtime 镜像 要不要拉？
- 要拉：本机没有现成 jobmanager/taskmanager 运行环境，需要用 Docker 直接启动 Flink 集群。
- 可不拉：你已经有可用的 Flink 集群（容器或外部集群）并且版本已确认兼容。

结论：
- 本地从零启动，建议拉。
- 只做配置生成或使用已有集群，可不拉。

## 2) Flink CDC 镜像 要不要拉？
- 要拉：你希望在容器内使用 Flink CDC CLI（flink-cdc.sh）做提交与管理。
- 可不拉：你已经在现有环境中具备可用 CLI，或只使用已有提交链路。

结论：
- 标准演示环境建议拉。
- 生产已有统一提交工具链时可不拉。

## 3) Source/Sink 镜像 要不要拉？
- 要拉：本地缺少对应组件服务（如 mysql、doris、kafka、elasticsearch）。
- 可不拉：你将连接到外部已有服务，且网络可达、账号可用。

结论：
- 本地联调建议拉。
- 对接共享测试环境可不拉。

## 4) Connector/JDBC 依赖 要不要下载？
- 要下载：cdc/lib 中缺少本次 source/sink 需要的 connector jar。
- 要下载：mysql source 时缺少 mysql-connector-java.jar。
- 视场景下载：paimon sink 常需要 hadoop-uber.jar。

结论：
- 首次跑某个 source/sink 组合，建议下载。
- 已确认 cdc/lib 依赖齐全时可跳过下载。

## 5) mysql -> doris 最小推荐选择
- 运行模式：快速模式。
- source/sink：mysql -> doris。
- 下载 CDC 依赖：是（首次）/ 否（依赖齐全后）。
- 生成配置文件：是。
- Compose 项目名：cdcup（默认）。

## 6) 常见失败与快速判断
- 报 Cannot find any table：
  - 原因：MySQL 中没有匹配 tables 规则的表。
  - 处理：先建种子表再提交。

- 报 MySQL 连接或类缺失：
  - 原因：mysql-connector-java.jar 缺失。
  - 处理：补充 JDBC 驱动到 cdc/lib。

- 报 connector not found：
  - 原因：source/sink 对应 connector jar 缺失。
  - 处理：下载对应 connector 到 cdc/lib。

## 7) 一句话决策规则
- 你要在本机从零启动并跑通链路：全拉、全下、全生成。
- 你已有稳定集群和依赖：尽量少拉，重点只生成配置并提交。
