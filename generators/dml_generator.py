#!/usr/bin/env python3
"""
可控随机 DML 语句生成器 (Controlled Random DML Statement Generator)

生成 INSERT, UPDATE, DELETE, REPLACE 语句，基于表 schema。
支持可重现的随机化和可控的复杂度。
"""

import argparse
import random
import sys
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class DataType(Enum):
    """MySQL 数据类型"""
    INT = "INT"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    VARCHAR = "VARCHAR(256)"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL(10,2)"
    DATETIME = "DATETIME"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"


class ColumnConstraint(Enum):
    """列约束"""
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT NULL"
    DEFAULT = "DEFAULT"
    NONE = ""


@dataclass
class Column:
    """表列定义"""
    name: str
    data_type: DataType
    constraint: ColumnConstraint = ColumnConstraint.NONE
    default_value: Any = None
    
    def is_pk(self) -> bool:
        return self.constraint == ColumnConstraint.PRIMARY_KEY
    
    def is_nullable(self) -> bool:
        return self.constraint != ColumnConstraint.NOT_NULL and self.constraint != ColumnConstraint.PRIMARY_KEY


@dataclass
class Table:
    """表定义"""
    name: str
    columns: List[Column]
    
    def get_pk_column(self) -> Column:
        """获取主键列，如果没有则返回None"""
        for col in self.columns:
            if col.is_pk():
                return col
        return None
    
    def get_non_pk_columns(self) -> List[Column]:
        """获取非主键列"""
        return [col for col in self.columns if not col.is_pk()]


class ValueGenerator:
    """值生成器"""
    
    def __init__(self, seed: int = None):
        if seed is not None:
            random.seed(seed)
    
    def generate_value(self, col: Column) -> str:
        """为列生成一个值"""
        if col.data_type == DataType.TINYINT:
            return str(random.randint(-128, 127))
        elif col.data_type == DataType.SMALLINT:
            return str(random.randint(-32768, 32767))
        elif col.data_type == DataType.INT:
            return str(random.randint(-100000, 100000))
        elif col.data_type == DataType.BIGINT:
            return str(random.randint(-10**12, 10**12))
        elif col.data_type == DataType.VARCHAR:
            tokens = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "User", "Alpha", "Beta", "Gamma"]
            suffix = random.choice(["", "-x", "_v2", "#tag", "-long-text"])
            return f"'{random.choice(tokens)}{random.randint(0, 999)}{suffix}'"
        elif col.data_type == DataType.TEXT:
            texts = [
                "hello world",
                "payload_123",
                "edge-case-value",
                "long_text_block_for_sync_test",
                "stateful_random_data",
            ]
            return f"'{random.choice(texts)}'"
        elif col.data_type in [DataType.FLOAT, DataType.DOUBLE]:
            return str(round(random.uniform(-100000.0, 100000.0), random.choice([3, 4, 6])))
        elif col.data_type == DataType.DECIMAL:
            return str(round(random.uniform(-100000.0, 100000.0), random.choice([2, 4, 6])))
        elif col.data_type == DataType.BOOLEAN:
            return random.choice(["0", "1"])
        elif col.data_type in [DataType.DATETIME, DataType.TIMESTAMP]:
            return f"'{random.randint(2020, 2025)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d} 10:00:00'"
        elif col.data_type == DataType.DATE:
            return f"'{random.randint(2020, 2025)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}'"
        else:
            return "NULL"
    
    def generate_null_or_value(self, col: Column) -> str:
        """50% 概率生成 NULL 或实际值"""
        if col.is_nullable() and random.random() < 0.3:  # 30% 概率NULL
            return "NULL"
        return self.generate_value(col)


class DMLGenerator:
    """DML 语句生成器"""
    
    def __init__(self, table: Table, seed: int = None, allow_complex_where: bool = False):
        self.table = table
        self.value_gen = ValueGenerator(seed)
        self.allow_complex_where = allow_complex_where
        self.seed = seed
    
    def generate_insert(self) -> str:
        """生成 INSERT 语句"""
        # 随机选择列子集（必须包含NOT NULL列）
        pk_col = self.table.get_pk_column()
        non_pk_cols = self.table.get_non_pk_columns()
        
        cols = []
        # 必须包含主键列
        if pk_col:
            cols.append(pk_col)
        
        # 必须包含所有 NOT NULL 列
        not_null_cols = [col for col in non_pk_cols if col.constraint == ColumnConstraint.NOT_NULL]
        cols.extend(not_null_cols)
        
        # 可选地添加其他可空列 (50-100%)
        nullable_cols = [col for col in non_pk_cols if col.is_nullable()]
        if nullable_cols:
            num_cols = random.randint(len(nullable_cols) // 2, len(nullable_cols))
            cols.extend(random.sample(nullable_cols, min(num_cols, len(nullable_cols))))
        
        col_names = ", ".join([f"`{col.name}`" for col in cols])
        values = ", ".join([self.value_gen.generate_value(col) for col in cols])
        
        prefix = random.choice(["", "LOW_PRIORITY ", "DELAYED ", "HIGH_PRIORITY "])
        ignore = random.choice(["", "IGNORE"]) if random.random() < 0.2 else ""
        
        stmt = f"INSERT {prefix}{ignore} INTO `{self.table.name}` ({col_names}) VALUES ({values})"
        return stmt.strip()
    
    def generate_replace(self) -> str:
        """生成 REPLACE 语句 (类似 INSERT，但使用 REPLACE 关键字)"""
        pk_col = self.table.get_pk_column()
        non_pk_cols = self.table.get_non_pk_columns()
        
        cols = []
        if pk_col:
            cols.append(pk_col)
        
        # 必须包含所有 NOT NULL 列
        not_null_cols = [col for col in non_pk_cols if col.constraint == ColumnConstraint.NOT_NULL]
        cols.extend(not_null_cols)
        
        # 可选地添加其他可空列
        nullable_cols = [col for col in non_pk_cols if col.is_nullable()]
        if nullable_cols:
            num_cols = random.randint(len(nullable_cols) // 2, len(nullable_cols))
            cols.extend(random.sample(nullable_cols, min(num_cols, len(nullable_cols))))
        
        col_names = ", ".join([f"`{col.name}`" for col in cols])
        values = ", ".join([self.value_gen.generate_value(col) for col in cols])
        
        prefix = random.choice(["", "LOW_PRIORITY ", "DELAYED "])
        
        stmt = f"REPLACE {prefix}INTO `{self.table.name}` ({col_names}) VALUES ({values})"
        return stmt.strip()
    
    def generate_update(self) -> str:
        """生成 UPDATE 语句"""
        non_pk_cols = self.table.get_non_pk_columns()
        
        if not non_pk_cols:
            # 如果没有非PK列，跳过UPDATE
            return None
        
        # 随机选择要更新的列 (50-100%)
        num_cols = random.randint(max(1, len(non_pk_cols) // 2), len(non_pk_cols))
        update_cols = random.sample(non_pk_cols, num_cols)
        
        # 构建 SET 子句
        set_clause = ", ".join([
            f"`{col.name}` = {self.value_gen.generate_value(col)}"
            for col in update_cols
        ])
        
        # 构建 WHERE 子句（70% 概率有WHERE）
        where_clause = ""
        if random.random() < 0.7:
            where_clause = self._generate_where_clause()
        
        stmt = f"UPDATE `{self.table.name}` SET {set_clause}"
        if where_clause:
            stmt += f" WHERE {where_clause}"
        
        return stmt
    
    def generate_delete(self) -> str:
        """生成 DELETE 语句"""
        # 可选：LOW_PRIORITY, QUICK, IGNORE
        prefix_parts = []
        if random.random() < 0.2:
            prefix_parts.append(random.choice(["LOW_PRIORITY", "QUICK"]))
        if random.random() < 0.2:
            prefix_parts.append("IGNORE")
        
        prefix = " ".join(prefix_parts)
        prefix = f"{prefix} " if prefix else ""
        
        # 构建 WHERE 子句 (80% 概率有WHERE)
        where_clause = ""
        if random.random() < 0.8:
            where_clause = self._generate_where_clause()
        
        stmt = f"DELETE {prefix}FROM `{self.table.name}`"
        if where_clause:
            stmt += f" WHERE {where_clause}"
        
        return stmt
    
    def _generate_where_clause(self) -> str:
        """生成简单的 WHERE 子句"""
        # 随机选择一列作为条件
        col = random.choice(self.table.columns)

        if self.allow_complex_where and len(self.table.columns) >= 2 and random.random() < 0.5:
            c1, c2 = random.sample(self.table.columns, 2)
            p1 = self._generate_single_predicate(c1)
            p2 = self._generate_single_predicate(c2)
            conj = random.choice(["AND", "OR"])
            return f"({p1}) {conj} ({p2})"

        return self._generate_single_predicate(col)

    def _generate_single_predicate(self, col: Column) -> str:
        """生成单列谓词"""
        
        if col.data_type in [DataType.TINYINT, DataType.SMALLINT, DataType.INT, DataType.BIGINT]:
            form = random.choice(["cmp", "between", "in"])
            if form == "cmp":
                op = random.choice(["=", ">", "<", ">=", "<=", "!="])
                value = random.randint(-1000, 1000)
                return f"`{col.name}` {op} {value}"
            if form == "between":
                a = random.randint(-1000, 0)
                b = random.randint(0, 1000)
                return f"`{col.name}` BETWEEN {a} AND {b}"
            vals = sorted({random.randint(-200, 200) for _ in range(3)})
            return f"`{col.name}` IN ({', '.join(map(str, vals))})"
        elif col.data_type == DataType.VARCHAR:
            op = random.choice(["=", "LIKE", "!="])
            if op == "LIKE":
                return f"`{col.name}` LIKE '{random.choice(['A', 'B', 'C', 'User', 'Alpha'])}%'"
            return f"`{col.name}` {op} '{random.choice(['Alice', 'Bob', 'User1', 'Alpha7'])}'"
        elif col.data_type == DataType.TEXT:
            form = random.choice(["like", "isnotnull"])
            if form == "like":
                return f"`{col.name}` LIKE '%{random.choice(['sync', 'edge', 'payload', 'test'])}%'"
            return f"`{col.name}` IS NOT NULL"
        elif col.data_type == DataType.BOOLEAN:
            value = random.choice(["0", "1"])
            return f"`{col.name}` = {value}"
        elif col.data_type in [DataType.FLOAT, DataType.DOUBLE, DataType.DECIMAL]:
            op = random.choice([">", "<", ">=", "<="])
            value = round(random.uniform(-1000, 1000), 3)
            return f"`{col.name}` {op} {value}"
        elif col.data_type in [DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP]:
            if random.random() < 0.5:
                return f"`{col.name}` >= '{random.randint(2020, 2024)}-01-01'"
            return f"`{col.name}` IS NOT NULL"
        else:
            return f"`{col.name}` IS NOT NULL"
    
    def generate(self, count: int = 10, dml_type: str = "all") -> List[str]:
        """
        生成 DML 语句集合
        
        Args:
            count: 生成的语句总数
            dml_type: "all" (混合) / "insert" / "replace" / "update" / "delete"
        
        Returns:
            DML 语句列表
        """
        statements = []
        
        if dml_type == "all":
            weights = [0.4, 0.2, 0.2, 0.2]
            population = ["INSERT", "REPLACE", "UPDATE", "DELETE"]
            types = random.choices(population=population, weights=weights, k=count)
        else:
            types = [dml_type.upper()] * count

        for stmt_type in types:
            try:
                if stmt_type == "INSERT":
                    stmt = self.generate_insert()
                elif stmt_type == "REPLACE":
                    stmt = self.generate_replace()
                elif stmt_type == "UPDATE":
                    stmt = self.generate_update()
                elif stmt_type == "DELETE":
                    stmt = self.generate_delete()
                
                if stmt:
                    statements.append(stmt)
            except Exception as e:
                print(f"[WARN] Failed to generate {stmt_type}: {e}", file=sys.stderr)
                continue
        
        return statements


def create_example_table() -> Table:
    """创建示例表 (t0)"""
    return Table(
        name="t0",
        columns=[
            Column(name="c0", data_type=DataType.INT, constraint=ColumnConstraint.PRIMARY_KEY),
            Column(name="c1", data_type=DataType.VARCHAR, constraint=ColumnConstraint.NOT_NULL),
            Column(name="c2", data_type=DataType.INT),
            Column(name="c3", data_type=DataType.VARCHAR),
            Column(name="c4", data_type=DataType.FLOAT),
        ]
    )


def parse_data_type(type_name: str) -> DataType:
    t = (type_name or "").strip().upper()
    if t.startswith("INT"):
        return DataType.INT
    if t.startswith("BIGINT"):
        return DataType.BIGINT
    if t.startswith("SMALLINT"):
        return DataType.SMALLINT
    if t.startswith("TINYINT"):
        if t.startswith("TINYINT(1)"):
            return DataType.BOOLEAN
        return DataType.TINYINT
    if t.startswith("VARCHAR"):
        return DataType.VARCHAR
    if t.startswith("CHAR"):
        return DataType.VARCHAR
    if t.startswith("VARBINARY") or t.startswith("BINARY"):
        return DataType.VARCHAR
    if t.startswith("TEXT"):
        return DataType.TEXT
    if t.startswith("JSON"):
        return DataType.TEXT
    if t.startswith("FLOAT"):
        return DataType.FLOAT
    if t.startswith("DOUBLE"):
        return DataType.DOUBLE
    if t.startswith("DECIMAL"):
        return DataType.DECIMAL
    if t.startswith("DATETIME"):
        return DataType.DATETIME
    if t.startswith("TIMESTAMP"):
        return DataType.TIMESTAMP
    if t.startswith("DATE"):
        return DataType.DATE
    if t.startswith("BOOL") or t.startswith("TINYINT(1)"):
        return DataType.BOOLEAN
    return DataType.VARCHAR


def parse_constraint(constraint_name: str) -> ColumnConstraint:
    c = (constraint_name or "").strip().upper()
    if c in {"PK", "PRIMARY_KEY", "PRIMARY KEY"}:
        return ColumnConstraint.PRIMARY_KEY
    if c in {"NOT_NULL", "NOT NULL"}:
        return ColumnConstraint.NOT_NULL
    if c in {"UNIQUE"}:
        return ColumnConstraint.UNIQUE
    return ColumnConstraint.NONE


def parse_columns_spec(raw: str, table_name: str) -> Table:
    # Format: name:type:constraint,name:type:constraint
    cols: List[Column] = []
    for token in (raw or "").split(","):
      token = token.strip()
      if not token:
          continue
      parts = [p.strip() for p in token.split(":")]
      if len(parts) < 2:
          continue
      name = parts[0]
      dtype = parse_data_type(parts[1])
      constraint = parse_constraint(parts[2] if len(parts) >= 3 else "")
      cols.append(Column(name=name, data_type=dtype, constraint=constraint))

    if not cols:
        return create_example_table()

    if not any(c.constraint == ColumnConstraint.PRIMARY_KEY for c in cols):
        cols[0].constraint = ColumnConstraint.PRIMARY_KEY

    return Table(name=table_name, columns=cols)


def main():
    parser = argparse.ArgumentParser(
        description="可控随机 DML 语句生成器"
    )
    parser.add_argument("--table-name", default="t0", help="表名 (default: t0)")
    parser.add_argument("--columns", help="列定义: name:type:constraint (e.g., id:INT:PK,name:VARCHAR:NOT_NULL)")
    parser.add_argument("--count", type=int, default=10, help="生成语句数 (default: 10)")
    parser.add_argument("--seed", type=int, help="随机种子 (可重现性)")
    parser.add_argument("--type", choices=["all", "insert", "replace", "update", "delete"],
                       default="all", help="DML 类型 (default: all)")
    parser.add_argument("--output-sql", help="输出 SQL 文件")
    parser.add_argument("--output-format", choices=["sql", "csv", "json"], default="sql",
                       help="输出格式")
    parser.add_argument("--allow-complex-where", action="store_true", help="启用更复杂的 WHERE 谓词组合")
    
    args = parser.parse_args()
    
    # 创建表定义：优先使用传入的列定义，支持运行时 schema。
    if args.columns:
        table = parse_columns_spec(args.columns, args.table_name)
    else:
        table = create_example_table()
    
    # 生成语句
    gen = DMLGenerator(table, seed=args.seed, allow_complex_where=args.allow_complex_where)
    statements = gen.generate(count=args.count, dml_type=args.type)
    
    print(f"[INFO] Generated {len(statements)} {args.type} statements (seed: {args.seed})", file=sys.stderr)
    
    # 输出
    if args.output_format == "sql":
        output = "\n".join(statements)
    elif args.output_format == "csv":
        import csv
        import io
        output_io = io.StringIO()
        writer = csv.writer(output_io)
        writer.writerow(["statement_index", "statement"])
        for i, stmt in enumerate(statements, 1):
            writer.writerow([i, stmt])
        output = output_io.getvalue()
    else:  # json
        import json
        output = json.dumps([
            {"index": i, "statement": stmt}
            for i, stmt in enumerate(statements, 1)
        ], indent=2)
    
    if args.output_sql:
        with open(args.output_sql, "w") as f:
            f.write(output)
        print(f"[INFO] Output written to {args.output_sql}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
