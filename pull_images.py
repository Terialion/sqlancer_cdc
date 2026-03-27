#!/usr/bin/env python3
"""Interactive image and CDC artifact downloader for cdcup.

Features:
- Choose Flink and Flink CDC independently.
- Choose source/sink independently (based on src/* presets).
- Pull broader image ranges for source and sink services.
- Optionally download Flink CDC package + connector jars + extra libs.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set


@dataclass
class ImageChoice:
    name: str
    repository: str
    tag: str

    @property
    def ref(self) -> str:
        return f"{self.repository}:{self.tag}"


@dataclass
class ImagePreset:
    key: str
    label: str
    default_repository: str
    tags: List[str]
    default_tag: str


IMAGE_PRESETS: Dict[str, ImagePreset] = {
    "flink": ImagePreset(
        key="flink",
        label="Flink Runtime",
        default_repository="flink",
        tags=[
            "1.17.2-scala_2.12",
            "1.18.1-scala_2.12",
            "1.19.0-scala_2.12",
            "1.19.1-scala_2.12",
            "1.19.2-scala_2.12",
            "1.19.3-scala_2.12",
            "1.19.4-scala_2.12",
            "1.20.0-scala_2.12",
            "1.20.1-scala_2.12",
            "1.20.2-scala_2.12",
            "1.20.3-scala_2.12",
            "latest",
        ],
        default_tag="1.20.3-scala_2.12",
    ),
    "flinkcdc": ImagePreset(
        key="flinkcdc",
        label="Flink CDC Image",
        default_repository="apache/flink-cdc",
        tags=["3.0.0", "3.0.1", "3.1.0", "3.1.1", "3.2.0", "3.2.1", "3.3.0", "3.4.0", "3.5.0", "3.6.0"],
        default_tag="3.2.1",
    ),
    "mysql": ImagePreset(
        key="mysql",
        label="MySQL Source",
        default_repository="mysql",
        tags=["5.7", "8.0", "8.1", "8.2", "8.3", "8.4", "latest"],
        default_tag="8.0",
    ),
    "postgres": ImagePreset(
        key="postgres",
        label="Postgres Source",
        default_repository="postgres",
        tags=["13", "14", "15", "16", "latest"],
        default_tag="16",
    ),
    "doris": ImagePreset(
        key="doris",
        label="Doris Sink",
        default_repository="apache/doris",
        tags=[
            "doris-all-in-one-2.0.5",
            "doris-all-in-one-2.1.0",
            "doris-all-in-one-2.1.3",
            "doris-all-in-one-2.1.5",
            "doris-all-in-one-2.1.6",
            "doris-all-in-one-3.0.0",
            "doris-all-in-one-3.1.0",
        ],
        default_tag="doris-all-in-one-2.1.0",
    ),
    "starrocks": ImagePreset(
        key="starrocks",
        label="StarRocks Sink",
        default_repository="starrocks/allin1-ubuntu",
        tags=["3.2.5", "3.3.10", "3.4.2", "3.5.10", "latest"],
        default_tag="3.5.10",
    ),
    "kafka": ImagePreset(
        key="kafka",
        label="Kafka Broker",
        default_repository="confluentinc/cp-kafka",
        tags=["7.2.0", "7.3.0", "7.4.4", "7.5.3", "latest"],
        default_tag="7.4.4",
    ),
    "zookeeper": ImagePreset(
        key="zookeeper",
        label="Zookeeper",
        default_repository="confluentinc/cp-zookeeper",
        tags=["7.2.0", "7.3.0", "7.4.4", "7.5.3", "latest"],
        default_tag="7.4.4",
    ),
    "elasticsearch": ImagePreset(
        key="elasticsearch",
        label="Elasticsearch Sink",
        default_repository="docker.elastic.co/elasticsearch/elasticsearch",
        tags=["7.17.22", "8.12.2", "8.14.3", "8.15.0", "latest"],
        default_tag="8.14.3",
    ),
}

CDC_VERSIONS = ["3.0.0", "3.0.1", "3.1.0", "3.1.1", "3.2.0", "3.2.1", "3.3.0", "3.4.0", "3.5.0", "3.6.0"]

SOURCE_CONNECTOR = {
    "mysql": "flink-cdc-pipeline-connector-mysql",
    "postgres": "flink-cdc-pipeline-connector-postgres",
    "values": "flink-cdc-pipeline-connector-values",
}

SINK_CONNECTOR = {
    "doris": "flink-cdc-pipeline-connector-doris",
    "kafka": "flink-cdc-pipeline-connector-kafka",
    "paimon": "flink-cdc-pipeline-connector-paimon",
    "starrocks": "flink-cdc-pipeline-connector-starrocks",
    "elasticsearch": "flink-cdc-pipeline-connector-elasticsearch",
    "iceberg": "flink-cdc-pipeline-connector-iceberg",
    "hudi": "flink-cdc-pipeline-connector-hudi",
    "values": "flink-cdc-pipeline-connector-values",
}


def yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def to_yaml(value: Any, indent: int = 0) -> List[str]:
    pad = "  " * indent
    lines: List[str] = []
    if isinstance(value, dict):
        for k, v in value.items():
            if isinstance(v, (dict, list)):
                lines.append(f"{pad}{k}:")
                lines.extend(to_yaml(v, indent + 1))
            elif isinstance(v, str) and "\\n" in v:
                lines.append(f"{pad}{k}: |-")
                for row in v.split("\\n"):
                    lines.append(f"{pad}  {row}")
            else:
                lines.append(f"{pad}{k}: {yaml_scalar(v)}")
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                lines.append(f"{pad}-")
                lines.extend(to_yaml(item, indent + 1))
            else:
                lines.append(f"{pad}- {yaml_scalar(item)}")
    else:
        lines.append(f"{pad}{yaml_scalar(value)}")
    return lines


def build_compose(source_type: str, sink_type: str, image_ref: Dict[str, str], project_name: str) -> Dict[str, Any]:
    services: Dict[str, Any] = {
        "jobmanager": {
            "image": image_ref.get("flink", "flink:1.20.3-scala_2.12"),
            "hostname": "jobmanager",
            "ports": ["8081"],
            "command": "jobmanager",
            "environment": {
                "FLINK_PROPERTIES": "jobmanager.rpc.address: jobmanager\\nexecution.checkpointing.interval: 3000"
            },
        },
        "taskmanager": {
            "image": image_ref.get("flink", "flink:1.20.3-scala_2.12"),
            "hostname": "taskmanager",
            "command": "taskmanager",
            "environment": {
                "FLINK_PROPERTIES": "jobmanager.rpc.address: jobmanager\\ntaskmanager.numberOfTaskSlots: 4\\nexecution.checkpointing.interval: 3000"
            },
        },
    }

    if source_type == "mysql":
        services["mysql"] = {
            "image": image_ref.get("mysql", "mysql:8.0"),
            "hostname": "mysql",
            "environment": {"MYSQL_ALLOW_EMPTY_PASSWORD": "true", "MYSQL_DATABASE": "cdcup"},
            "ports": ["3306"],
            "volumes": ["cdc-data:/data"],
        }
    elif source_type == "postgres":
        services["postgres"] = {
            "image": image_ref.get("postgres", "postgres:16"),
            "hostname": "postgres",
            "environment": {"POSTGRES_HOST_AUTH_METHOD": "trust", "POSTGRES_DB": "cdcup"},
            "ports": ["5432"],
            "volumes": ["cdc-data:/data"],
        }

    if sink_type == "doris":
        services["doris"] = {
            "image": image_ref.get("doris", "apache/doris:doris-all-in-one-2.1.0"),
            "hostname": "doris",
            "ports": ["8030", "8040", "9030"],
            "volumes": ["cdc-data:/data"],
        }
    elif sink_type == "starrocks":
        services["starrocks"] = {
            "image": image_ref.get("starrocks", "starrocks/allin1-ubuntu:3.5.10"),
            "hostname": "starrocks",
            "ports": ["8080", "9030"],
            "volumes": ["cdc-data:/data"],
        }
    elif sink_type == "kafka":
        services["zookeeper"] = {
            "image": image_ref.get("zookeeper", "confluentinc/cp-zookeeper:7.4.4"),
            "hostname": "zookeeper",
            "ports": ["2181"],
            "environment": {"ZOOKEEPER_CLIENT_PORT": "2181", "ZOOKEEPER_TICK_TIME": "2000"},
        }
        services["kafka"] = {
            "image": image_ref.get("kafka", "confluentinc/cp-kafka:7.4.4"),
            "depends_on": ["zookeeper"],
            "hostname": "kafka",
            "ports": ["9092"],
            "environment": {
                "KAFKA_BROKER_ID": "1",
                "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
                "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT",
                "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            },
        }
    elif sink_type == "elasticsearch":
        services["elasticsearch"] = {
            "image": image_ref.get("elasticsearch", "docker.elastic.co/elasticsearch/elasticsearch:8.14.3"),
            "hostname": "elasticsearch",
            "ports": ["9200"],
            "environment": {"discovery.type": "single-node", "xpack.security.enabled": "false"},
            "volumes": ["cdc-data:/data"],
        }

    return {"name": project_name, "services": services, "volumes": {"cdc-data": {}}}


def build_pipeline(source_type: str, sink_type: str) -> Dict[str, Any]:
    pipeline: Dict[str, Any] = {"pipeline": {"parallelism": 1}}

    if source_type == "mysql":
        pipeline["source"] = {
            "type": "mysql",
            "hostname": "mysql",
            "port": 3306,
            "username": "root",
            "password": "",
            "tables": "\\.*.\\.*",
            "server-id": "5400-6400",
            "server-time-zone": "UTC",
        }
    elif source_type == "postgres":
        pipeline["source"] = {
            "type": "postgres",
            "hostname": "postgres",
            "port": 5432,
            "username": "postgres",
            "password": "",
            "tables": "public.\\.*",
            "decoding.plugin.name": "pgoutput",
        }
    else:
        pipeline["source"] = {"type": "values", "event-set.id": "SINGLE_SPLIT_MULTI_TABLES"}

    if sink_type == "doris":
        pipeline["sink"] = {
            "type": "doris",
            "fenodes": "doris:8030",
            "benodes": "doris:8040",
            "jdbc-url": "jdbc:mysql://doris:9030",
            "username": "root",
            "password": "",
            "table.create.properties.light_schema_change": "true",
            "table.create.properties.replication_num": 1,
        }
    elif sink_type == "starrocks":
        pipeline["sink"] = {
            "type": "starrocks",
            "jdbc-url": "jdbc:mysql://starrocks:9030",
            "load-url": "starrocks:8080",
            "username": "root",
            "password": "",
            "table.create.properties.replication_num": 1,
        }
    elif sink_type == "kafka":
        pipeline["sink"] = {"type": "kafka", "properties.bootstrap.servers": "PLAINTEXT://kafka:9092"}
    elif sink_type == "elasticsearch":
        pipeline["sink"] = {"type": "elasticsearch", "hosts": "http://elasticsearch:9200"}
    elif sink_type == "paimon":
        pipeline["sink"] = {
            "type": "paimon",
            "catalog.properties.metastore": "filesystem",
            "catalog.properties.warehouse": "/data/paimon-warehouse",
        }
    elif sink_type == "iceberg":
        pipeline["sink"] = {
            "type": "iceberg",
            "catalog-type": "hadoop",
            "warehouse": "/data/iceberg-warehouse",
        }
    elif sink_type == "hudi":
        pipeline["sink"] = {"type": "hudi", "path": "/data/hudi-warehouse"}
    else:
        pipeline["sink"] = {"type": "values"}

    return pipeline


def write_configs(
    source_type: str,
    sink_type: str,
    image_ref: Dict[str, str],
    output_dir: Path,
    dry_run: bool,
    project_name: str,
) -> None:
    compose = build_compose(source_type, sink_type, image_ref, project_name)
    pipeline = build_pipeline(source_type, sink_type)

    compose_path = output_dir / "docker-compose.yaml"
    pipeline_path = output_dir / "pipeline-definition.yaml"

    compose_yaml = "\n".join(to_yaml(compose)) + "\n"
    pipeline_yaml = "\n".join(to_yaml(pipeline)) + "\n"

    if dry_run:
        print(f"\n(dry-run) 将写入: {compose_path}")
        print(f"(dry-run) 将写入: {pipeline_path}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        compose_path.write_text(compose_yaml, encoding="utf-8")
        pipeline_path.write_text(pipeline_yaml, encoding="utf-8")
    except PermissionError:
        print(f"\nERROR: 无法写入输出目录 {output_dir}", file=sys.stderr)
        print("请使用 -o/--output-dir 指定可写目录，例如: -o /tmp/pull_images_out", file=sys.stderr)
        raise
    print(f"\n已生成: {compose_path}")
    print(f"已生成: {pipeline_path}")


def prompt_yes_no(message: str, default_yes: bool = True) -> bool:
    hint = "Y/n" if default_yes else "y/N"
    while True:
        raw = input(f"{message} [{hint}]: ").strip().lower()
        if not raw:
            return default_yes
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("请输入 y 或 n")


def print_section(title: str) -> None:
    width = max(40, shutil.get_terminal_size(fallback=(80, 24)).columns)
    line = "─" * min(width - 2, 60)
    print(f"\n📦 {title}")
    print(line)


def print_progress(step: int, total: int, title: str) -> None:
    done = "🟩" * step
    todo = "⬜" * (total - step)
    print(f"\n{done}{todo}  ({step}/{total}) {title}")


def print_banner() -> None:
    width = max(40, shutil.get_terminal_size(fallback=(80, 24)).columns)
    line = "═" * min(width - 2, 60)
    print(f"\n{line}")
    print("🚀 cdcup 交互式资源下载工具")
    print("🎯 目标: 快速选择镜像与 connector，并生成可运行配置")
    print("⌨️  提示: 回车采用默认值，输入编号可快速选择")
    print(line)


def print_runtime_glossary() -> None:
    print("\n🧠 术语说明")
    print("- Flink runtime 镜像: 运行 JobManager/TaskManager 的基础运行时容器。")
    print("- Flink CDC 镜像: 包含 Flink CDC CLI，主要用于提交与管理 CDC pipeline。")
    print("- Connector/JDBC 依赖: 通常放在 cdc/lib 下，按 source/sink 组合准备。")


def prompt_from_list(label: str, values: List[str], default_value: str, allow_custom: bool = True) -> str:
    print(f"\n{label}")
    default_idx = 1
    for idx, value in enumerate(values, start=1):
        if value == default_value:
            default_idx = idx
        default_hint = " ⭐默认" if value == default_value else ""
        print(f"  {idx:>2}. {value}{default_hint}")

    custom_idx = len(values) + 1
    if allow_custom:
        print(f"  {custom_idx}. 自定义")
    while True:
        raw = input(f"选择编号（默认 {default_idx}）: ").strip()
        if not raw:
            return values[default_idx - 1]
        if raw.isdigit():
            num = int(raw)
            if 1 <= num <= len(values):
                return values[num - 1]
            if allow_custom and num == custom_idx:
                custom = input("输入自定义值: ").strip()
                if custom:
                    return custom
        print("请输入有效编号")


def choose_image(preset_key: str) -> ImageChoice:
    preset = IMAGE_PRESETS[preset_key]
    print(f"\n配置镜像: {preset.label}")
    repository = input(f"  仓库（默认 {preset.default_repository}）: ").strip() or preset.default_repository
    tag = prompt_from_list("  版本选择:", preset.tags, preset.default_tag)
    return ImageChoice(name=preset.label, repository=repository, tag=tag)


def default_image_choice(preset_key: str) -> ImageChoice:
    preset = IMAGE_PRESETS[preset_key]
    return ImageChoice(name=preset.label, repository=preset.default_repository, tag=preset.default_tag)


def prompt_mode() -> str:
    print("\n运行模式:")
    print("  1. 快速模式（推荐，最少交互，使用官方推荐默认镜像）")
    print("  2. 自定义模式（逐项选择仓库与版本）")
    while True:
        raw = input("选择模式（默认 1）: ").strip()
        if not raw or raw == "1":
            return "quick"
        if raw == "2":
            return "advanced"
        print("请输入有效编号")


def prompt_project_name(default_name: str = "cdcup") -> str:
    pattern = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
    while True:
        entered = input(f"Compose 项目名（默认 {default_name}）: ").strip()
        if not entered:
            return default_name
        if pattern.fullmatch(entered):
            return entered
        print("项目名仅允许小写字母、数字、下划线、中划线，且必须以字母或数字开头")


def prompt_source_sink() -> tuple[str, str]:
    profiles = [
        ("mysql -> doris (推荐入门)", "mysql", "doris"),
        ("mysql -> kafka", "mysql", "kafka"),
        ("postgres -> doris", "postgres", "doris"),
        ("values -> values (连通性快速验证)", "values", "values"),
    ]
    print("\n🧭 可选快速场景:")
    for idx, (label, _, _) in enumerate(profiles, start=1):
        default_hint = " ⭐默认" if idx == 1 else ""
        print(f"  {idx:>2}. {label}{default_hint}")
    print(f"  {len(profiles) + 1:>2}. 自定义 source/sink")

    while True:
        raw = input(f"选择场景（默认 1）: ").strip()
        if not raw:
            return profiles[0][1], profiles[0][2]
        if raw.isdigit():
            num = int(raw)
            if 1 <= num <= len(profiles):
                return profiles[num - 1][1], profiles[num - 1][2]
            if num == len(profiles) + 1:
                source_type = prompt_from_list(
                    "\n选择 source 类型:", ["mysql", "postgres", "values"], "mysql", allow_custom=False
                )
                sink_type = prompt_from_list(
                    "\n选择 sink 类型:",
                    ["doris", "kafka", "paimon", "starrocks", "elasticsearch", "iceberg", "hudi", "values"],
                    "doris",
                    allow_custom=False,
                )
                return source_type, sink_type
        print("请输入有效编号")


def run_cmd(cmd: List[str], dry_run: bool) -> bool:
    print("$ " + " ".join(cmd))
    if dry_run:
        return True
    proc = subprocess.run(cmd, text=True)
    return proc.returncode == 0


def pull_images(images: List[ImageChoice], dry_run: bool) -> int:
    failed = 0
    for item in images:
        print(f"\n>>> pulling {item.ref}")
        if not run_cmd(["docker", "pull", item.ref], dry_run):
            failed += 1
    return failed


def cdc_tar_url(version: str) -> str:
    if version.startswith("3.0."):
        return f"https://github.com/ververica/flink-cdc-connectors/releases/download/release-{version}/flink-cdc-{version}-bin.tar.gz"
    return f"https://dlcdn.apache.org/flink/flink-cdc-{version}/flink-cdc-{version}-bin.tar.gz"


def cdc_group_path(version: str) -> str:
    if version.startswith("3.0."):
        return "com/ververica"
    return "org/apache/flink"


def connector_jar_url(version: str, artifact: str) -> str:
    group_path = cdc_group_path(version)
    return f"https://repo1.maven.org/maven2/{group_path}/{artifact}/{version}/{artifact}-{version}.jar"


def has_connector_jar(lib_dir: Path, artifact: str) -> bool:
    if (lib_dir / f"{artifact}.jar").exists():
        return True
    return any(lib_dir.glob(f"{artifact}-*.jar"))


def runtime_notes(source_type: str, sink_type: str) -> List[str]:
    notes: List[str] = []
    if source_type in {"mysql", "postgres"}:
        notes.append("source 使用表匹配规则，提交前请先确保匹配到至少一张真实表。")
    if source_type == "values" or sink_type == "values":
        notes.append("使用 values source/sink 时，运行环境必须存在 values connector jar。")
    return notes


def validate_runtime_preconditions(
    source_type: str,
    sink_type: str,
    target_dir: Path,
    do_cdc_download: bool,
) -> List[str]:
    if do_cdc_download:
        return []

    warnings: List[str] = []
    lib_dir = target_dir / "lib"

    required_connectors = [SOURCE_CONNECTOR[source_type], SINK_CONNECTOR[sink_type]]
    for artifact in sorted(set(required_connectors)):
        if not has_connector_jar(lib_dir, artifact):
            warnings.append(f"缺少 connector jar: {artifact} (检查目录: {lib_dir})")

    if source_type == "mysql" and not (lib_dir / "mysql-connector-java.jar").exists():
        warnings.append(f"缺少 MySQL JDBC 驱动: mysql-connector-java.jar (检查目录: {lib_dir})")

    if sink_type == "paimon" and not (lib_dir / "hadoop-uber.jar").exists():
        warnings.append(f"缺少 Hadoop 依赖: hadoop-uber.jar (检查目录: {lib_dir})")

    return warnings


def download_cdc_artifacts(
    cdc_version: str,
    connectors: Set[str],
    need_mysql_driver: bool,
    need_hadoop_uber: bool,
    target_dir: Path,
    dry_run: bool,
) -> int:
    failed = 0
    lib_dir = target_dir / "lib"
    target_dir.mkdir(parents=True, exist_ok=True)
    lib_dir.mkdir(parents=True, exist_ok=True)

    tar_tmp = Path("/tmp") / f"flink-cdc-{cdc_version}-bin.tar.gz"
    tar_url = cdc_tar_url(cdc_version)
    print(f"\n>>> 下载 Flink CDC 包: {tar_url}")
    if not run_cmd(["curl", "-fL", tar_url, "-o", str(tar_tmp)], dry_run):
        failed += 1
    else:
        if not run_cmd(["tar", "--strip-components=1", "-xzf", str(tar_tmp), "-C", str(target_dir)], dry_run):
            failed += 1

    for connector in sorted(connectors):
        url = connector_jar_url(cdc_version, connector)
        out = lib_dir / f"{connector}.jar"
        print(f"\n>>> 下载 connector: {connector}")
        if not run_cmd(["curl", "-fL", url, "-o", str(out)], dry_run):
            failed += 1

    if need_mysql_driver:
        print("\n>>> 下载 mysql-connector-java")
        if not run_cmd(
            [
                "curl",
                "-fL",
                "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar",
                "-o",
                str(lib_dir / "mysql-connector-java.jar"),
            ],
            dry_run,
        ):
            failed += 1

    if need_hadoop_uber:
        print("\n>>> 下载 hadoop-uber")
        if not run_cmd(
            [
                "curl",
                "-fL",
                "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar",
                "-o",
                str(lib_dir / "hadoop-uber.jar"),
            ],
            dry_run,
        ):
            failed += 1

    return failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="交互式拉取 cdcup 镜像并下载 CDC/JAR 依赖",
        epilog=(
            "示例:\n"
            "  python3 pull_images.py -h\n"
            "  python3 pull_images.py -o /tmp/my-run -t ./cdc\n"
            "说明:\n"
            "  默认会在脚本目录生成 docker-compose.yaml 和 pipeline-definition.yaml。"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("-n", "--dry-run", action="store_true", help="仅打印动作，不执行")
    parser.add_argument("-t", "--target-dir", default="./cdc", help="CDC 包下载/解压目录（默认 ./cdc）")
    parser.add_argument(
        "-o",
        "--output-dir",
        default=str(Path(__file__).resolve().parent),
        help="docker-compose.yaml 与 pipeline-definition.yaml 输出目录（默认脚本目录）",
    )
    parser.add_argument(
        "-p",
        "--project-name",
        default="",
        help="Compose 项目名称（影响容器命名；留空则交互输入）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = args.dry_run

    if not dry_run and shutil.which("docker") is None:
        print("ERROR: 未检测到 docker 命令", file=sys.stderr)
        return 2
    if not dry_run and shutil.which("curl") is None:
        print("ERROR: 未检测到 curl 命令", file=sys.stderr)
        return 2
    if not dry_run and shutil.which("tar") is None:
        print("ERROR: 未检测到 tar 命令", file=sys.stderr)
        return 2

    total_steps = 6

    print_banner()
    print("支持独立选择 Flink/FlinkCDC/source/sink，可下载 cdc 包、connector 和 lib jars。")

    chosen_images: List[ImageChoice] = []
    image_ref_map: Dict[str, str] = {}
    print_progress(1, total_steps, "选择运行模式")
    mode = prompt_mode()

    print("\n已选择模式:", "快速模式" if mode == "quick" else "自定义模式")

    print_progress(2, total_steps, "基础镜像")
    print_section("基础镜像")
    print_runtime_glossary()
    if prompt_yes_no("是否拉取 Flink runtime 镜像？", default_yes=True):
        chosen = default_image_choice("flink") if mode == "quick" else choose_image("flink")
        chosen_images.append(chosen)
        image_ref_map["flink"] = chosen.ref

    print_progress(3, total_steps, "CDC 版本")
    print_section("CDC 版本")
    cdc_version = prompt_from_list("\n选择 Flink CDC 版本（用于 cdc 包与 connector 版本）:", CDC_VERSIONS, "3.2.1")

    if prompt_yes_no("是否拉取 Flink CDC 镜像？", default_yes=False):
        cdc_img = default_image_choice("flinkcdc") if mode == "quick" else choose_image("flinkcdc")
        cdc_img.tag = cdc_version
        chosen_images.append(cdc_img)
        image_ref_map["flinkcdc"] = cdc_img.ref

    print_progress(4, total_steps, "Source / Sink")
    print_section("Source / Sink")
    source_type, sink_type = prompt_source_sink()

    if source_type == "mysql" and prompt_yes_no("是否拉取 MySQL source 镜像？", default_yes=True):
        chosen = default_image_choice("mysql") if mode == "quick" else choose_image("mysql")
        chosen_images.append(chosen)
        image_ref_map["mysql"] = chosen.ref
    elif source_type == "postgres" and prompt_yes_no("是否拉取 Postgres source 镜像？", default_yes=True):
        chosen = default_image_choice("postgres") if mode == "quick" else choose_image("postgres")
        chosen_images.append(chosen)
        image_ref_map["postgres"] = chosen.ref

    if sink_type == "doris" and prompt_yes_no("是否拉取 Doris sink 镜像？", default_yes=True):
        chosen = default_image_choice("doris") if mode == "quick" else choose_image("doris")
        chosen_images.append(chosen)
        image_ref_map["doris"] = chosen.ref
    elif sink_type == "starrocks" and prompt_yes_no("是否拉取 StarRocks sink 镜像？", default_yes=True):
        chosen = default_image_choice("starrocks") if mode == "quick" else choose_image("starrocks")
        chosen_images.append(chosen)
        image_ref_map["starrocks"] = chosen.ref
    elif sink_type == "kafka":
        if prompt_yes_no("是否拉取 Kafka sink 相关镜像（kafka + zookeeper）？", default_yes=True):
            zk = default_image_choice("zookeeper") if mode == "quick" else choose_image("zookeeper")
            kk = default_image_choice("kafka") if mode == "quick" else choose_image("kafka")
            chosen_images.append(zk)
            chosen_images.append(kk)
            image_ref_map["zookeeper"] = zk.ref
            image_ref_map["kafka"] = kk.ref
    elif sink_type == "elasticsearch" and prompt_yes_no("是否拉取 Elasticsearch sink 镜像？", default_yes=True):
        chosen = default_image_choice("elasticsearch") if mode == "quick" else choose_image("elasticsearch")
        chosen_images.append(chosen)
        image_ref_map["elasticsearch"] = chosen.ref

    if chosen_images:
        print("\n将拉取以下镜像:")
        for item in chosen_images:
            print(f"- {item.name}: {item.ref}")

    print_progress(5, total_steps, "产物与配置")
    print_section("产物与配置")
    do_cdc_download = prompt_yes_no("是否下载 Flink CDC 包与 connector/lib jars？", default_yes=True)
    do_generate_files = prompt_yes_no("是否生成 docker-compose.yaml 与 pipeline-definition.yaml？", default_yes=True)
    project_name = args.project_name.strip()
    if not project_name:
        project_name = prompt_project_name("cdcup")

    print("\n🧾 执行摘要:")
    print(f"- source/sink: {source_type} -> {sink_type}")
    print(f"- Flink CDC version: {cdc_version}")
    print(f"- Compose project name: {project_name}")
    print(f"- 下载 CDC 依赖: {'是' if do_cdc_download else '否'}")
    print(f"- 生成配置文件: {'是' if do_generate_files else '否'}")

    notes = runtime_notes(source_type, sink_type)
    if notes:
        print("\n💡 运行提示:")
        for note in notes:
            print(f"- {note}")

    precheck_warnings = validate_runtime_preconditions(
        source_type=source_type,
        sink_type=sink_type,
        target_dir=Path(args.target_dir).resolve(),
        do_cdc_download=do_cdc_download,
    )
    if precheck_warnings:
        print("\n⚠️ 检测到运行时前置条件风险:")
        for warning in precheck_warnings:
            print(f"- {warning}")
        if not prompt_yes_no("是否仍继续执行？", default_yes=False):
            print("已取消。")
            return 0

    if not chosen_images and not do_cdc_download and not do_generate_files:
        print("未选择任何动作，退出。")
        return 0

    print_progress(6, total_steps, "确认并执行")
    if not prompt_yes_no("\n确认执行上述动作？", default_yes=True):
        print("已取消。")
        return 0

    failed = 0
    if chosen_images:
        failed += pull_images(chosen_images, dry_run)

    if do_cdc_download:
        connectors: Set[str] = {SOURCE_CONNECTOR[source_type], SINK_CONNECTOR[sink_type]}
        need_mysql_driver = source_type == "mysql"
        need_hadoop_uber = sink_type == "paimon"

        print("\n将下载以下 connector jars:")
        for c in sorted(connectors):
            print(f"- {c}")
        if need_mysql_driver:
            print("- mysql-connector-java")
        if need_hadoop_uber:
            print("- hadoop-uber")

        failed += download_cdc_artifacts(
            cdc_version=cdc_version,
            connectors=connectors,
            need_mysql_driver=need_mysql_driver,
            need_hadoop_uber=need_hadoop_uber,
            target_dir=Path(args.target_dir).resolve(),
            dry_run=dry_run,
        )

    if do_generate_files:
        write_configs(
            source_type=source_type,
            sink_type=sink_type,
            image_ref=image_ref_map,
            output_dir=Path(args.output_dir).resolve(),
            dry_run=dry_run,
            project_name=project_name,
        )

    print("\n✅ === 完成 ===")
    print(f"失败项数量: {failed}")
    if failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
