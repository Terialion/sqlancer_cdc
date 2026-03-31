#!/usr/bin/env python3
"""
Validate data consistency between source MySQL and sink Doris/StarRocks after Flink CDC sync.

Checks per table:
1) Row count
2) Numeric-column SUM fingerprints
3) Row-level hash XOR fingerprint (order-independent)
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Tuple

import mysql.connector
from mysql.connector import Error

NUMERIC_TYPES = {
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
    "decimal",
    "numeric",
    "float",
    "double",
    "real",
    "bit",
    "year",
}


@dataclass
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class TableResult:
    table: str
    mysql_count: Optional[int]
    doris_count: Optional[int]
    status: str
    checksum_result: str
    detail: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CDC consistency validator for MySQL -> Doris")

    parser.add_argument("--source-host", default="localhost")
    parser.add_argument("--source-port", type=int, default=None)
    parser.add_argument("--source-user", default="root")
    parser.add_argument("--source-password", default="")

    parser.add_argument("--sink-host", default="localhost")
    parser.add_argument("--sink-port", type=int, default=None)
    parser.add_argument("--sink-user", default="root")
    parser.add_argument("--sink-password", default="")

    parser.add_argument("--database", default="cdc_playground", help="Source schema/database to validate")
    parser.add_argument("--wait-seconds", type=int, default=10, help="Quiescence wait before validation")
    parser.add_argument("--missing-table-retries", type=int, default=3, help="Retries for sink table appearance")
    parser.add_argument("--retry-interval-seconds", type=int, default=5)
    parser.add_argument("--auto-detect-ports", action="store_true", default=True)
    parser.add_argument(
        "--no-auto-detect-ports",
        action="store_false",
        dest="auto_detect_ports",
        help="Disable docker port auto-detection",
    )
    parser.add_argument(
        "--apply-sql-file",
        default="",
        help="Execute SQL statements in this file on source before validation",
    )

    return parser.parse_args()


def detect_docker_port(container: str, container_port: int) -> Optional[int]:
    cmd = ["docker", "port", container, f"{container_port}/tcp"]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return None

    if not output:
        return None
    # Typical output: 0.0.0.0:32777 or :::32777
    try:
        return int(output.split(":")[-1])
    except Exception:
        return None


def split_sql_statements(sql_text: str) -> List[str]:
    statements: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    escape = False

    for ch in sql_text:
        if escape:
            buf.append(ch)
            escape = False
            continue

        if ch == "\\":
            buf.append(ch)
            escape = True
            continue

        if ch == "'" and not in_double:
            in_single = not in_single
            buf.append(ch)
            continue

        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch)
            continue

        if ch == ";" and not in_single and not in_double:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            continue

        buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)

    return statements


def apply_sql_file_to_source(conn, sql_file: str) -> Tuple[int, int]:
    if not sql_file:
        return 0, 0
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    with open(sql_file, "r", encoding="utf-8") as f:
        raw = f.read()

    statements = split_sql_statements(raw)
    ok = 0
    fail = 0

    with conn.cursor() as cur:
        for stmt in statements:
            try:
                cur.execute(stmt)
                ok += 1
            except Exception:
                # SQLancer contains dialect-heavy statements; continue on failures.
                fail += 1
                continue
    return ok, fail


def connect_db(cfg: DbConfig):
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        autocommit=True,
    )


def query_one(conn, sql: str, params: Optional[Sequence] = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


def query_all(conn, sql: str, params: Optional[Sequence] = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def list_tables(conn, schema: str) -> List[str]:
    rows = query_all(
        conn,
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    return [r[0] for r in rows]


def table_exists(conn, schema: str, table: str) -> bool:
    row = query_one(
        conn,
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return bool(row and row[0] > 0)


def get_columns(conn, schema: str, table: str) -> List[Tuple[str, str]]:
    rows = query_all(
        conn,
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [(r[0], str(r[1]).lower()) for r in rows]


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def row_count(conn, schema: str, table: str) -> int:
    sql = f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)}"
    row = query_one(conn, sql)
    return int(row[0]) if row else 0


def numeric_sums(conn, schema: str, table: str, columns: List[Tuple[str, str]]) -> Dict[str, str]:
    num_cols = [c for c in columns if c[1] in NUMERIC_TYPES]
    if not num_cols:
        return {}

    exprs = []
    aliases = []
    for col, _ in num_cols:
        alias = f"s_{col}"
        aliases.append(alias)
        exprs.append(
            f"IFNULL(SUM(CAST({quote_ident(col)} AS DECIMAL(38,10))), 0) AS {quote_ident(alias)}"
        )

    sql = f"SELECT {', '.join(exprs)} FROM {quote_ident(schema)}.{quote_ident(table)}"
    row = query_one(conn, sql)
    if not row:
        return {col: "0" for col, _ in num_cols}

    out: Dict[str, str] = {}
    for (col, _), val in zip(num_cols, row):
        if val is None:
            out[col] = "0"
        elif isinstance(val, Decimal):
            out[col] = format(val, "f")
        else:
            out[col] = str(val)
    return out


def row_xor_hash(conn, schema: str, table: str, columns: List[Tuple[str, str]]) -> int:
    if not columns:
        return 0

    cast_exprs = [f"COALESCE(CAST({quote_ident(c)} AS CHAR), 'NULL')" for c, _ in columns]
    row_expr = f"CONCAT_WS('|', {', '.join(cast_exprs)})"

    sql = f"SELECT {row_expr} FROM {quote_ident(schema)}.{quote_ident(table)}"
    xor_val = 0
    with conn.cursor() as cur:
        cur.execute(sql)
        for (row_str,) in cur:
            digest = hashlib.blake2b(
                row_str.encode("utf-8"), digest_size=8
            ).digest()
            row_hash = int.from_bytes(digest, byteorder="big", signed=False)
            xor_val ^= row_hash
    return xor_val


def format_table(results: List[TableResult]) -> str:
    headers = [
        "Table",
        "MySQL Count",
        "Doris Count",
        "Status",
        "Checksum",
        "Detail",
    ]
    rows = []
    for r in results:
        rows.append(
            [
                r.table,
                "-" if r.mysql_count is None else str(r.mysql_count),
                "-" if r.doris_count is None else str(r.doris_count),
                r.status,
                r.checksum_result,
                r.detail,
            ]
        )

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt(row: Sequence[str]) -> str:
        return " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    sep = "-+-".join("-" * w for w in widths)
    output = [fmt(headers), sep]
    output.extend(fmt(row) for row in rows)
    return "\n".join(output)


def validate_tables(
    mysql_conn,
    doris_conn,
    schema: str,
    retries: int,
    retry_interval: int,
) -> List[TableResult]:
    results: List[TableResult] = []
    tables = list_tables(mysql_conn, schema)

    if not tables:
        print(f"[WARN] No tables found in source database: {schema}")
        return results

    for table in tables:
        try:
            mysql_columns = get_columns(mysql_conn, schema, table)
            mysql_cnt = row_count(mysql_conn, schema, table)

            exists = table_exists(doris_conn, schema, table)
            attempt = 0
            while not exists and attempt < retries:
                attempt += 1
                time.sleep(retry_interval)
                exists = table_exists(doris_conn, schema, table)

            if not exists:
                results.append(
                    TableResult(
                        table=table,
                        mysql_count=mysql_cnt,
                        doris_count=None,
                        status="PENDING",
                        checksum_result="N/A",
                        detail="table not found in sink (possible sync delay)",
                    )
                )
                continue

            doris_columns = get_columns(doris_conn, schema, table)
            doris_col_names = [c[0] for c in doris_columns]

            common_columns = [c for c in mysql_columns if c[0] in doris_col_names]
            if not common_columns:
                results.append(
                    TableResult(
                        table=table,
                        mysql_count=mysql_cnt,
                        doris_count=row_count(doris_conn, schema, table),
                        status="FAIL",
                        checksum_result="N/A",
                        detail="no common columns between source and sink",
                    )
                )
                continue

            doris_cnt = row_count(doris_conn, schema, table)

            mysql_num = numeric_sums(mysql_conn, schema, table, common_columns)
            doris_num = numeric_sums(doris_conn, schema, table, common_columns)
            num_ok = mysql_num == doris_num

            mysql_xor = row_xor_hash(mysql_conn, schema, table, common_columns)
            doris_xor = row_xor_hash(doris_conn, schema, table, common_columns)
            xor_ok = mysql_xor == doris_xor

            count_ok = mysql_cnt == doris_cnt
            status = "PASS" if (count_ok and num_ok and xor_ok) else "FAIL"
            checksum_result = (
                f"num={'OK' if num_ok else 'DIFF'}, xor={'OK' if xor_ok else 'DIFF'}"
            )
            detail = (
                f"mx_xor={mysql_xor}, dr_xor={doris_xor}, common_cols={len(common_columns)}"
            )

            results.append(
                TableResult(
                    table=table,
                    mysql_count=mysql_cnt,
                    doris_count=doris_cnt,
                    status=status,
                    checksum_result=checksum_result,
                    detail=detail,
                )
            )

        except Error as e:
            results.append(
                TableResult(
                    table=table,
                    mysql_count=None,
                    doris_count=None,
                    status="ERROR",
                    checksum_result="N/A",
                    detail=str(e).replace("\n", " ")[:220],
                )
            )

    return results


def main() -> int:
    args = parse_args()

    if args.auto_detect_ports:
        detected_src = detect_docker_port("cdcup-mysql-1", 3306)
        detected_sink = detect_docker_port("cdcup-doris-1", 9030)
        if args.source_port is None and detected_src is not None:
            args.source_port = detected_src
        if args.sink_port is None and detected_sink is not None:
            args.sink_port = detected_sink

    if args.source_port is None:
        args.source_port = 32777
    if args.sink_port is None:
        args.sink_port = 9030

    source_cfg = DbConfig(
        host=args.source_host,
        port=args.source_port,
        user=args.source_user,
        password=args.source_password,
        database=args.database,
    )
    sink_cfg = DbConfig(
        host=args.sink_host,
        port=args.sink_port,
        user=args.sink_user,
        password=args.sink_password,
        database=args.database,
    )

    print(f"[INFO] Quiescence wait: {args.wait_seconds}s")
    time.sleep(args.wait_seconds)

    mysql_conn = None
    doris_conn = None
    try:
        mysql_conn = connect_db(source_cfg)
        doris_conn = connect_db(sink_cfg)

        if args.apply_sql_file:
            print(f"[INFO] Applying SQL file to source: {args.apply_sql_file}")
            ok_count, fail_count = apply_sql_file_to_source(mysql_conn, args.apply_sql_file)
            print(f"[INFO] SQL apply done: success={ok_count}, failed={fail_count}")
            # Give CDC a short settle window after applying generated statements.
            time.sleep(3)

        results = validate_tables(
            mysql_conn,
            doris_conn,
            args.database,
            retries=args.missing_table_retries,
            retry_interval=args.retry_interval_seconds,
        )

        if not results:
            print("[INFO] No validation results.")
            return 0

        print()
        print(format_table(results))
        print()

        total = len(results)
        passed = sum(1 for r in results if r.status == "PASS")
        failed = sum(1 for r in results if r.status == "FAIL")
        pending = sum(1 for r in results if r.status == "PENDING")
        errors = sum(1 for r in results if r.status == "ERROR")

        print(
            f"Summary: total={total}, pass={passed}, fail={failed}, pending={pending}, error={errors}"
        )
        return 1 if (failed > 0 or errors > 0) else 0

    except Error as e:
        print(f"[FATAL] DB connection/query error: {e}")
        return 2
    finally:
        if mysql_conn is not None and mysql_conn.is_connected():
            mysql_conn.close()
        if doris_conn is not None and doris_conn.is_connected():
            doris_conn.close()


if __name__ == "__main__":
    sys.exit(main())
