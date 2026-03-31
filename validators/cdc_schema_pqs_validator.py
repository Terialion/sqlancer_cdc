#!/usr/bin/env python3

import argparse
import random
import re
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple


def run_mysql_query(
	host: str,
	port: int,
	user: str,
	password: str,
	database: str,
	sql: str,
) -> Tuple[int, str, str]:
	cmd = [
		"mysql",
		"-h",
		host,
		"-P",
		str(port),
		"-u",
		user,
		"-N",
		"-B",
	]
	if password != "":
		cmd.extend(["-p" + password])
	if database != "":
		cmd.append(database)
	cmd.extend(["-e", sql])

	proc = subprocess.run(cmd, capture_output=True, text=True)
	return proc.returncode, proc.stdout, proc.stderr


def parse_tsv(stdout: str) -> List[List[str]]:
	rows: List[List[str]] = []
	for line in stdout.splitlines():
		line = line.rstrip("\n")
		if line == "":
			continue
		rows.append(line.split("\t"))
	return rows


def ascii_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
	text_rows = [["" if c is None else str(c) for c in row] for row in rows]
	width = [len(h) for h in headers]
	for row in text_rows:
		for i, col in enumerate(row):
			if i < len(width):
				width[i] = max(width[i], len(col))

	sep = "+" + "+".join("-" * (w + 2) for w in width) + "+"
	out = [sep]
	out.append("| " + " | ".join(headers[i].ljust(width[i]) for i in range(len(headers))) + " |")
	out.append(sep)
	for row in text_rows:
		out.append("| " + " | ".join(row[i].ljust(width[i]) for i in range(len(headers))) + " |")
	out.append(sep)
	return "\n".join(out)


def quote_ident(name: str) -> str:
	return "`" + name.replace("`", "``") + "`"


def quote_sql_value(v: str, data_type: str) -> str:
	t = data_type.lower()
	if v == "NULL":
		return "NULL"
	if t in {
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
		"boolean",
	}:
		return v
	return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


def normalize_type_for_compare(data_type: str, column_type: str) -> str:
	dt = data_type.lower()
	ct = column_type.lower()

	if dt in {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}:
		return dt
	if dt in {"float", "double", "real"}:
		return "float_double"
	if dt in {"decimal", "numeric"}:
		m = re.search(r"\((\d+)\s*,\s*(\d+)\)", ct)
		if m:
			return f"decimal({m.group(1)},{m.group(2)})"
		return "decimal"
	if dt in {"char", "varchar", "text", "tinytext", "mediumtext", "longtext", "string"}:
		return "string"
	if dt in {"date", "datev2"}:
		return "date"
	if dt in {"timestamp", "timestamp_ltz", "datetime", "datetimev2"}:
		return "timestamp"
	if dt in {"json", "variant", "map", "row", "struct"}:
		return "json_like"
	if dt in {"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"}:
		return "binary_like"
	if dt in {"boolean", "bool"}:
		return "tinyint"
	return dt


def parse_decimal_ps(norm: str) -> Tuple[Optional[int], Optional[int]]:
	m = re.fullmatch(r"decimal\((\d+),(\d+)\)", norm)
	if not m:
		return None, None
	return int(m.group(1)), int(m.group(2))


def is_compatible(mysql_norm: str, sink_norm: str) -> Tuple[bool, str]:
	direct = {
		"tinyint": {"tinyint", "smallint", "int", "integer", "bigint"},
		"smallint": {"smallint", "int", "integer", "bigint"},
		"mediumint": {"int", "integer", "bigint"},
		"int": {"int", "integer", "bigint"},
		"integer": {"int", "integer", "bigint"},
		"bigint": {"bigint"},
		"float_double": {"float_double"},
		"string": {"string"},
		"date": {"date"},
		"timestamp": {"timestamp"},
		"json_like": {"json_like", "string"},
		"binary_like": {"binary_like", "string"},
	}

	if mysql_norm.startswith("decimal"):
		if not sink_norm.startswith("decimal"):
			return False, "decimal mapped to non-decimal"
		mp, ms = parse_decimal_ps(mysql_norm)
		sp, ss = parse_decimal_ps(sink_norm)
		if mp is not None and sp is not None and sp < mp:
			return False, f"decimal precision loss source={mp},{ms} sink={sp},{ss}"
		if ms is not None and ss is not None and ss < ms:
			return False, f"decimal scale loss source={mp},{ms} sink={sp},{ss}"
		return True, "ok"

	allow = direct.get(mysql_norm)
	if allow is None:
		return (mysql_norm == sink_norm), "type mismatch"
	return (sink_norm in allow), "type mismatch"


def collect_tables(args: argparse.Namespace) -> List[str]:
	if args.tables.strip() != "":
		return [t.strip() for t in args.tables.split(",") if t.strip()]

	sql = (
		"SELECT table_name "
		"FROM information_schema.tables "
		f"WHERE table_schema='{args.database}' AND table_type='BASE TABLE' "
		"ORDER BY table_name"
	)
	rc, out, err = run_mysql_query(
		args.mysql_host,
		args.mysql_port,
		args.mysql_user,
		args.mysql_password,
		"",
		sql,
	)
	if rc != 0:
		raise RuntimeError(f"failed to list source tables: {err.strip()}")
	return [r[0] for r in parse_tsv(out)]


def get_columns(
	host: str,
	port: int,
	user: str,
	password: str,
	database: str,
	table: str,
) -> List[Dict[str, str]]:
	sql = (
		"SELECT column_name, data_type, column_type "
		"FROM information_schema.columns "
		f"WHERE table_schema='{database}' AND table_name='{table}' "
		"ORDER BY ordinal_position"
	)
	rc, out, err = run_mysql_query(host, port, user, password, "", sql)
	if rc != 0:
		raise RuntimeError(f"failed to fetch columns for {database}.{table}: {err.strip()}")
	rows = parse_tsv(out)
	return [
		{
			"column_name": r[0],
			"data_type": (r[1] if len(r) > 1 else "").lower(),
			"column_type": (r[2] if len(r) > 2 else "").lower(),
		}
		for r in rows
	]


def get_primary_key_columns(
	host: str,
	port: int,
	user: str,
	password: str,
	database: str,
	table: str,
) -> List[str]:
	sql = (
		"SELECT kcu.column_name "
		"FROM information_schema.table_constraints tc "
		"JOIN information_schema.key_column_usage kcu "
		"  ON tc.constraint_name = kcu.constraint_name "
		" AND tc.table_schema = kcu.table_schema "
		" AND tc.table_name = kcu.table_name "
		"WHERE tc.table_schema = '{db}' "
		"  AND tc.table_name = '{tb}' "
		"  AND tc.constraint_type = 'PRIMARY KEY' "
		"ORDER BY kcu.ordinal_position"
	).format(db=database, tb=table)

	rc, out, err = run_mysql_query(host, port, user, password, "", sql)
	if rc != 0:
		raise RuntimeError(f"failed to fetch primary key for {database}.{table}: {err.strip()}")
	return [r[0] for r in parse_tsv(out)]


def fetch_random_pivot(
	host: str,
	port: int,
	user: str,
	password: str,
	database: str,
	table: str,
	cols: List[Dict[str, str]],
) -> Optional[List[str]]:
	projection = ", ".join(quote_ident(c["column_name"]) for c in cols)
	sql = f"SELECT {projection} FROM {quote_ident(table)} ORDER BY RAND() LIMIT 1"
	rc, out, _ = run_mysql_query(host, port, user, password, database, sql)
	if rc != 0:
		return None
	rows = parse_tsv(out)
	if not rows:
		return None
	return rows[0]


def fetch_where_rows(
	host: str,
	port: int,
	user: str,
	password: str,
	database: str,
	table: str,
	cols: List[Dict[str, str]],
	predicate: str,
) -> List[Tuple[str, ...]]:
	projection = ", ".join(quote_ident(c["column_name"]) for c in cols)
	order_by = ", ".join(quote_ident(c["column_name"]) for c in cols)
	sql = f"SELECT {projection} FROM {quote_ident(table)} WHERE {predicate} ORDER BY {order_by}"
	rc, out, err = run_mysql_query(host, port, user, password, database, sql)
	if rc != 0:
		raise RuntimeError(f"query failed: {err.strip()} | sql={sql}")
	return [tuple(r) for r in parse_tsv(out)]


def canonicalize_scalar(value: str, data_type: str, column_type: str) -> str:
	if value is None:
		return ""
	v = value
	dt = (data_type or "").lower()
	ct = (column_type or "").lower()

	# MySQL client prints NULL as literal string in batch mode.
	if v == "NULL":
		return "<NULL>"

	# Normalize fixed char padding differences across engines.
	if dt in {"char"} or ct.startswith("char("):
		v = v.rstrip()

	# Normalize numeric textual forms, e.g. 1, 1.0, 1.000.
	if dt in {
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
	}:
		try:
			from decimal import Decimal

			d = Decimal(v)
			v = format(d.normalize(), "f")
		except Exception:
			pass

	# Normalize datetime textual precision differences.
	if dt in {"datetime", "timestamp", "timestamp_ltz"}:
		v = v.replace("T", " ")
		if "." in v:
			main, frac = v.split(".", 1)
			frac = frac.rstrip("0")
			v = main if frac == "" else main + "." + frac

	return v


def canonicalize_row(row: Tuple[str, ...], cols: List[Dict[str, str]]) -> Tuple[str, ...]:
	vals: List[str] = []
	for i, c in enumerate(cols):
		if i < len(row):
			vals.append(canonicalize_scalar(row[i], c.get("data_type", ""), c.get("column_type", "")))
		else:
			vals.append("")
	return tuple(vals)


def pk_tuple_from_row(row: Tuple[str, ...], pk_indexes: List[int]) -> Tuple[str, ...]:
	return tuple(row[i] if i < len(row) else "" for i in pk_indexes)


def build_pk_row_map(rows: List[Tuple[str, ...]], pk_indexes: List[int]) -> Dict[Tuple[str, ...], Tuple[str, ...]]:
	out: Dict[Tuple[str, ...], Tuple[str, ...]] = {}
	for r in rows:
		out[pk_tuple_from_row(r, pk_indexes)] = r
	return out


def values_different(
	src_raw: str,
	sink_raw: str,
	src_norm: str,
	sink_norm: str,
	data_type: str,
	column_type: str,
	float_epsilon: float,
) -> bool:
	if src_norm == sink_norm:
		return False

	dt = (data_type or "").lower()
	ct = (column_type or "").lower()

	# Allow tiny numeric drift for floating-point columns across engines.
	if dt in {"float", "double", "real"} or ct.startswith("float") or ct.startswith("double"):
		try:
			from decimal import Decimal

			s = Decimal(src_norm)
			t = Decimal(sink_norm)
			eps = Decimal(str(float_epsilon))
			return abs(s - t) > eps
		except Exception:
			return src_raw != sink_raw

	return True


def main() -> int:
	parser = argparse.ArgumentParser(
		description="Validate schema type compatibility and run PQS on CDC source/sink"
	)
	parser.add_argument("--mysql-host", default="127.0.0.1")
	parser.add_argument("--mysql-port", type=int, required=True)
	parser.add_argument("--mysql-user", default="root")
	parser.add_argument("--mysql-password", default="")

	parser.add_argument("--sink-host", default="127.0.0.1")
	parser.add_argument("--sink-port", type=int, required=True)
	parser.add_argument("--sink-user", default="root")
	parser.add_argument("--sink-password", default="")

	parser.add_argument("--database", required=True)
	parser.add_argument("--tables", default="")
	parser.add_argument("--sleep-seconds", type=int, default=15)
	parser.add_argument("--pqs-trials-per-table", type=int, default=3)
	parser.add_argument("--sink-consistency-retries", type=int, default=5)
	parser.add_argument("--sink-consistency-delay", type=int, default=2)
	parser.add_argument("--float-epsilon", type=float, default=0.05)
	parser.add_argument("--seed", type=int, default=42)

	args = parser.parse_args()

	print(f"[INFO] quiet period after DDL/DML: sleep {args.sleep_seconds}s")
	time.sleep(max(0, args.sleep_seconds))

	try:
		tables = collect_tables(args)
	except Exception as exc:
		print(f"[ERROR] {exc}")
		return 2

	if not tables:
		print("[WARN] no table found, skip validator")
		return 0

	random_gen = random.Random(args.seed)

	critical_rows: List[List[str]] = []
	exact_diff_rows: List[List[str]] = []

	for table in tables:
		try:
			source_cols = get_columns(
				args.mysql_host,
				args.mysql_port,
				args.mysql_user,
				args.mysql_password,
				args.database,
				table,
			)
			sink_cols = get_columns(
				args.sink_host,
				args.sink_port,
				args.sink_user,
				args.sink_password,
				args.database,
				table,
			)
		except Exception as exc:
			critical_rows.append([table, "*", "schema metadata fetch failed", str(exc), "", ""])
			continue

		source_map = {c["column_name"]: c for c in source_cols}
		sink_map = {c["column_name"]: c for c in sink_cols}

		source_only = sorted(set(source_map.keys()) - set(sink_map.keys()))
		sink_only = sorted(set(sink_map.keys()) - set(source_map.keys()))

		for col in source_only:
			critical_rows.append(
				[table, col, "missing in sink", "source column not found", source_map[col]["column_type"], ""]
			)
		for col in sink_only:
			critical_rows.append(
				[table, col, "extra in sink", "sink has unexpected column", "", sink_map[col]["column_type"]]
			)

		common_cols = sorted(set(source_map.keys()) & set(sink_map.keys()))
		for col in common_cols:
			src = source_map[col]
			snk = sink_map[col]
			src_norm = normalize_type_for_compare(src["data_type"], src["column_type"])
			snk_norm = normalize_type_for_compare(snk["data_type"], snk["column_type"])

			ok, reason = is_compatible(src_norm, snk_norm)
			if not ok:
				critical_rows.append([table, col, reason, "type incompatible", src["column_type"], snk["column_type"]])

			if src["column_type"] != snk["column_type"]:
				exact_diff_rows.append([table, col, src["column_type"], snk["column_type"]])

	print("\n[SCHEMA] full-column type compatibility check")
	if exact_diff_rows:
		print(ascii_table(["table", "column", "source_type", "sink_type"], exact_diff_rows))
	else:
		print("[SCHEMA] no exact type text difference")

	if critical_rows:
		print("\n[SCHEMA][CRITICAL]")
		print(
			ascii_table(
				["table", "column", "reason", "detail", "source_type", "sink_type"],
				critical_rows,
			)
		)
		print("\n[EXIT] critical schema issues found, stop before PQS")
		return 1

	print("[SCHEMA] compatibility passed")

	pqs_bugs: List[List[str]] = []
	pqs_pass: List[List[str]] = []
	pqs_value_mismatches: List[List[str]] = []

	for table in tables:
		try:
			source_cols = get_columns(
				args.mysql_host,
				args.mysql_port,
				args.mysql_user,
				args.mysql_password,
				args.database,
				table,
			)
		except Exception as exc:
			pqs_bugs.append([table, "0", "metadata", str(exc), ""])
			continue

		try:
			pk_cols = get_primary_key_columns(
				args.mysql_host,
				args.mysql_port,
				args.mysql_user,
				args.mysql_password,
				args.database,
				table,
			)
		except Exception as exc:
			pqs_bugs.append([table, "0", "pk_metadata", str(exc), ""])
			continue

		if not pk_cols:
			pqs_pass.append([table, "0", "SKIP", "no primary key for deterministic PQS", ""])
			continue

		if not source_cols:
			pqs_pass.append([table, "0", "SKIP", "no columns", ""])
			continue

		for trial in range(1, args.pqs_trials_per_table + 1):
			pivot = fetch_random_pivot(
				args.mysql_host,
				args.mysql_port,
				args.mysql_user,
				args.mysql_password,
				args.database,
				table,
				source_cols,
			)
			if pivot is None:
				pqs_pass.append([table, str(trial), "SKIP", "empty table", ""])
				continue

			col_index = {c["column_name"]: i for i, c in enumerate(source_cols)}
			pk_indexes = [col_index[k] for k in pk_cols if k in col_index]
			parts: List[str] = []

			# Guaranteed match path: constrain by PK with random equivalent predicates.
			for pk in pk_cols:
				if pk not in col_index:
					continue
				i = col_index[pk]
				if i >= len(pivot):
					continue
				col_name = quote_ident(pk)
				val = quote_sql_value(pivot[i], source_cols[i]["data_type"])
				template = random_gen.choice([
					f"{col_name}={val}",
					f"{col_name} IN ({val})",
					f"{col_name}>={val} AND {col_name}<={val}",
					f"NOT ({col_name}<>{val})",
				])
				parts.append(template)

			if not parts:
				# No PK and no usable column value: skip this trial instead of false alarm.
				pqs_pass.append([table, str(trial), "SKIP", "no usable predicate columns", ""])
				continue

			predicate = " AND ".join(parts)

			final_src_set: Optional[set] = None
			final_sink_set: Optional[set] = None
			query_error: Optional[str] = None

			for attempt in range(0, args.sink_consistency_retries + 1):
				try:
					src_rows = fetch_where_rows(
						args.mysql_host,
						args.mysql_port,
						args.mysql_user,
						args.mysql_password,
						args.database,
						table,
						source_cols,
						predicate,
					)
					sink_rows = fetch_where_rows(
						args.sink_host,
						args.sink_port,
						args.sink_user,
						args.sink_password,
						args.database,
						table,
						source_cols,
						predicate,
					)
				except Exception as exc:
					query_error = str(exc)
					break

				pivot_tuple = tuple(pivot)
				src_set = set(src_rows)
				sink_set = set(sink_rows)
				final_src_set = src_set
				final_sink_set = sink_set

				src_pk_set = {pk_tuple_from_row(r, pk_indexes) for r in src_rows}
				sink_pk_set = {pk_tuple_from_row(r, pk_indexes) for r in sink_rows}
				pivot_pk = tuple(pivot[i] if i < len(pivot) else "" for i in pk_indexes)

				if pivot_pk in src_pk_set and pivot_pk in sink_pk_set and src_pk_set == sink_pk_set:
					break

				if attempt < args.sink_consistency_retries:
					time.sleep(max(0, args.sink_consistency_delay))

			if query_error is not None:
				pqs_bugs.append([table, str(trial), "query_failed", query_error, predicate])
				continue

			pivot_tuple = tuple(pivot)
			src_set = final_src_set if final_src_set is not None else set()
			sink_set = final_sink_set if final_sink_set is not None else set()

			src_rows_final = list(src_set)
			sink_rows_final = list(sink_set)
			src_pk_set = {pk_tuple_from_row(r, pk_indexes) for r in src_rows_final}
			sink_pk_set = {pk_tuple_from_row(r, pk_indexes) for r in sink_rows_final}
			pivot_pk = tuple(pivot[i] if i < len(pivot) else "" for i in pk_indexes)

			if pivot_pk not in src_pk_set:
				pqs_pass.append([table, str(trial), "SKIP", "predicate missed pivot in source", predicate])
				continue
			if pivot_pk not in sink_pk_set:
				pqs_bugs.append([table, str(trial), "LOGIC BUG", "pivot missing in sink", predicate])
				continue
			if src_pk_set != sink_pk_set:
				pqs_bugs.append(
					[
						table,
						str(trial),
						"LOGIC BUG",
						f"PK result mismatch source={len(src_pk_set)} sink={len(sink_pk_set)}",
						predicate,
					]
				)
				continue

			# Value-level compare after PK parity, with normalization to avoid format-only false alarms.
			src_pk_map = build_pk_row_map(src_rows_final, pk_indexes)
			sink_pk_map = build_pk_row_map(sink_rows_final, pk_indexes)
			src_row = src_pk_map.get(pivot_pk)
			sink_row = sink_pk_map.get(pivot_pk)
			if src_row is not None and sink_row is not None:
				src_norm = canonicalize_row(src_row, source_cols)
				sink_norm = canonicalize_row(sink_row, source_cols)
				if src_norm != sink_norm:
					pk_kv = []
					for i, pk in enumerate(pk_cols):
						if i < len(pivot_pk):
							pk_kv.append(f"{pk}={pivot_pk[i]}")
					pk_repr = ",".join(pk_kv)
					has_hard_mismatch = False

					for idx, c in enumerate(source_cols):
						src_raw = src_row[idx] if idx < len(src_row) else ""
						sink_raw = sink_row[idx] if idx < len(sink_row) else ""
						src_n = src_norm[idx] if idx < len(src_norm) else ""
						sink_n = sink_norm[idx] if idx < len(sink_norm) else ""
						if values_different(
							src_raw,
							sink_raw,
							src_n,
							sink_n,
							c.get("data_type", ""),
							c.get("column_type", ""),
							args.float_epsilon,
						):
							has_hard_mismatch = True
							pqs_value_mismatches.append(
								[
									table,
									str(trial),
									pk_repr,
									predicate,
									c.get("column_name", ""),
									src_raw,
									sink_raw,
									src_n,
									sink_n,
								]
							)
					if has_hard_mismatch:
						pqs_bugs.append([table, str(trial), "LOGIC BUG", "PK equal but value mismatch after normalization", predicate])
						continue

			pqs_pass.append([table, str(trial), "PASS", "source/sink result equal", predicate])

	print("\n[PQS] pivoted query synthesis checks")
	if pqs_pass:
		print(ascii_table(["table", "trial", "status", "detail", "predicate"], pqs_pass))

	if pqs_bugs:
		print("\n[PQS][LOGIC BUG]")
		print(ascii_table(["table", "trial", "status", "detail", "predicate"], pqs_bugs))
		if pqs_value_mismatches:
			print("\n[PQS][VALUE MISMATCH DETAILS]")
			print(
				ascii_table(
					["table", "trial", "pk", "predicate", "column", "source_raw", "sink_raw", "source_norm", "sink_norm"],
					pqs_value_mismatches,
				)
			)
		return 1

	print("[PQS] all checks passed")
	return 0


if __name__ == "__main__":
	sys.exit(main())
