# sqlancer_cdc — CDC E2E Playbook & Runner

[![Stars](https://img.shields.io/github/stars/Terialion/sqlancer_cdc?style=social)](https://github.com/Terialion/sqlancer_cdc/stargazers) [![License](https://img.shields.io/github/license/Terialion/sqlancer_cdc)](https://github.com/Terialion/sqlancer_cdc/blob/master/LICENSE)

轻量说明：本模块包含针对 CDC（Change Data Capture）场景的端到端（E2E）试验与验证脚本、pipeline 定义以及一个用于自动准备运行镜像/配置的 `tools/pull_images.py` 工具。旨在帮助开发者在本地或 CI 中快速复现 CDC 行为与兼容性问题。

目录已按“高星仓库常见模式”重整：文档、探针、生成器、校验器分目录存放，脚本统一使用真实子目录路径。

目录（快速导航）
- 简介 / Quick Start
- 先决条件
- 一步部署（pull_images -> docker compose -> 运行 E2E）
- `pull_images.py` 用法
- `run_sqlancer_cdc_e2e.sh` 用法示例
- 故障排查
- 贡献与许可

## 简介 / TL;DR

推荐目录结构（当前）：

```text
sqlancer_cdc/
├── docs/                   # 设计说明、决策文档
├── probes/                 # Bug/行为探针脚本
├── generators/             # SQL 生成器（DDL/DML/SELECT）
├── validators/             # 一致性与 schema 校验器
├── tools/                  # 工具脚本（如 pull_images）
├── run_sqlancer_cdc_e2e.sh # 主入口
├── cdcup.sh                # 环境入口
├── pipeline-definition*.yaml
└── docker-compose.yaml
```

快速运行（最小示例）：

```bash
cd /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc

# 准备镜像与运行时依赖（交互式）
python3 tools/pull_images.py

# 启动服务
./cdcup up
或
docker compose -f docker-compose.yaml up -d

# 运行 E2E（Doris pipeline 示例）
./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml --sink-type doris
```

## 先决条件
- Docker & Docker Compose（v2 推荐）
- Python 3.8+
- Git
- 网络可以访问所需镜像仓库（或提前准备好 `cdc/lib` 的依赖 jar）

## 一步部署说明（推荐顺序）

1. 准备镜像与依赖：使用 `tools/pull_images.py`（推荐）

交互式（按提示选择）：

```bash
cd /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc
python3 tools/pull_images.py
```

批量模式（非交互）：

```bash
python3 tools/pull_images.py \
  --batch \
  --mode quick \
  --source-type mysql \
  --sink-type doris \
  --cdc-version 3.2.1 \
  --project-name cdcup \
  --output-dir /tmp/pull_images_batch_run \
  --target-dir /tmp/pull_images_batch_cdc
```

常用开关：
- `--skip-image-pull`：仅生成配置，不拉镜像。
- `--skip-download-cdc`：跳过 CDC 二进制/jar 下载（当 `cdc/lib` 已准备好时使用）。

2. 启动容器

```bash
docker compose -f docker-compose.yaml up -d
docker compose -f docker-compose.yaml ps
```

3. 运行 E2E 流程

```bash
cd /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc
./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml --sink-type doris
```

## `tools/pull_images.py` 快速参考

功能：自动生成/拉取用于 CDC E2E 的 docker-compose、下载所需 connector/jar、并可导出到指定目录。

主要选项：
- `--batch`：非交互模式
- `--mode <quick|full>`：预设镜像/组件组合
- `--source-type <mysql|...>`：Source 类型
- `--sink-type <doris|starrocks|paimon|...>`：Sink 类型
- `--cdc-version <x.y.z>`：指定 CDC 版本
- `--project-name <name>`：compose 项目名（默认 `cdcup`）
- `--output-dir`、`--target-dir`：输出目录与目标路径

示例（批量）：

```bash
python3 tools/pull_images.py --batch --mode quick --source-type mysql --sink-type doris \
  --cdc-version 3.2.1 --project-name cdcup --output-dir /tmp/pull_images_run --target-dir /tmp/pull_images_cdc
```

提示：若仅需生成配置请加 `--skip-image-pull`，若你已手动把所需 jars 放在 `cdc/lib` 下可加 `--skip-download-cdc`。

## `run_sqlancer_cdc_e2e.sh` 用法与示例

最常见参数（摘要，更多请运行 `-h` 查看）：
- `--pipeline-yaml <file>`：pipeline 定义文件
- `--sink-type <doris|starrocks|paimon|...>`：指定 sink
- `--base-seed <int>`：随机种子（可重现）
- `--dml-count / --ddl-count / --mixed-count`：控制语句数量
- `--enable-pqs-presence-probe`：在结束时运行 PQS 探针
- `--random-transform`：启用随机 transform 路径

示例 — 最小运行：

```bash
./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml --sink-type doris
```

示例 — CI/可重现运行：

```bash
./run_sqlancer_cdc_e2e.sh \
  --pipeline-yaml pipeline-definition-doris.yaml \
  --sink-type doris \
  --base-seed 111 \
  --dml-count 120 \
  --ddl-count 20 \
  --mixed-count 80
```

高级示例：启用 PQS 探针与随机 transform

```bash
./run_sqlancer_cdc_e2e.sh \
  --pipeline-yaml pipeline-definition-doris.yaml \
  --sink-type doris \
  --enable-pqs-presence-probe \
  --random-transform
```

输出与报告：脚本会在 `REPORT_DIR`（可配置）下产出实验报告与 `experiment_archive.txt` 元数据，用于复现与诊断。

## 故障排查（快速）

- 若容器未启动：检查 `docker compose -f docker-compose.yaml ps`。
- 查看 JobManager / TaskManager / sink 日志（容器日志）来定位错误。
- 在 `REPORT_DIR` 中检查 `experiment_archive.txt` 与报告文件，包含运行参数与变更历史。
- 依赖缺失：确认 `cdc/lib` 是否包含所需 connector/JDBC/Hadoop jars，或使用 `pull_images.py` 下载。

## 贡献 & 社区

欢迎提交 issue 或 PR：

- Fork 本仓库并新建分支
- 提交变更并打开 Pull Request，描述复现步骤与测试方法

若需我协助：我可以帮你生成 PR 描述、把 `cdc/lib` 加入 `.gitignore` 或添加英文版 README。

## 许可证

请参见仓库根目录的 `LICENSE` 文件。

---