#!/usr/bin/env python3
"""
DDL 语句生成器 (DDL Statement Generator)

生成有效的 CREATE TABLE, ALTER TABLE, DROP INDEX 等 DDL 语句。
支持可重现的随机化和多样化的表结构。
"""

import argparse
import random
import sys
from typing import List, Dict
from dataclasses import dataclass
from enum import Enum


class DataType(Enum):
    """MySQL 数据类型"""
    TINYINT = "TINYINT"
    INT = "INT"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    VARCHAR = "VARCHAR(256)"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL(10,2)"
    DECIMAL_WIDE = "DECIMAL(18,6)"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    CHAR = "CHAR(32)"
    VARCHAR_WIDE = "VARCHAR(1024)"


@dataclass
class Column:
    """列定义"""
    name: str
    data_type: DataType
    is_pk: bool = False
    is_not_null: bool = False
    has_default: bool = False
    default_value: str = None


class DDLGenerator:
    """DDL 语句生成器"""
    
    def __init__(self, seed: int = None):
        if seed is not None:
            random.seed(seed)
        self.table_counter = 0
    
    def _generate_column_name(self, index: int) -> str:
        """生成列名"""
        prefix = random.choice(['c', 'col', 'field', 'data'])
        return f"{prefix}{index}"
    
    def _generate_data_type(self) -> DataType:
        """随机选择数据类型"""
        return random.choice([
            DataType.TINYINT,
            DataType.SMALLINT,
            DataType.INT,
            DataType.BIGINT,
            DataType.FLOAT,
            DataType.DOUBLE,
            DataType.DECIMAL,
            DataType.DECIMAL_WIDE,
            DataType.BOOLEAN,
            DataType.DATE,
            DataType.DATETIME,
            DataType.TIMESTAMP,
            DataType.CHAR,
            DataType.VARCHAR,
            DataType.VARCHAR_WIDE,
            DataType.TEXT,
        ])
    
    def _generate_columns(self, num_cols: int) -> List[Column]:
        """生成列集合"""
        columns = []
        
        # 第一列通常是主键
        pk_col = Column(
            name=self._generate_column_name(0),
            data_type=DataType.INT,
            is_pk=True,
            is_not_null=True
        )
        columns.append(pk_col)
        
        # 其他列
        for i in range(1, num_cols):
            col = Column(
                name=self._generate_column_name(i),
                data_type=self._generate_data_type(),
                is_pk=False,
                is_not_null=random.random() < 0.3,  # 30% 概率 NOT NULL
                has_default=random.random() < 0.2   # 20% 概率有默认值
            )
            columns.append(col)
        
        return columns
    
    def _format_column_def(self, col: Column) -> str:
        """格式化列定义"""
        parts = [f"`{col.name}`", col.data_type.value]
        
        if col.is_pk:
            parts.append("PRIMARY KEY")
        
        if col.is_not_null:
            parts.append("NOT NULL")
        
        if col.has_default and col.data_type in [DataType.INT, DataType.BIGINT, DataType.BOOLEAN]:
            parts.append(f"DEFAULT {random.randint(1, 10)}")
        
        return " ".join(parts)
    
    def generate_create_table(self) -> str:
        """生成 CREATE TABLE 语句"""
        self.table_counter += 1
        table_name = f"t{self.table_counter}"
        
        # 随机列数 (2-6列)
        num_cols = random.randint(2, 6)
        columns = self._generate_columns(num_cols)
        
        # 构建列定义
        col_defs = ",\n    ".join([self._format_column_def(col) for col in columns])
        
        # 可选：添加唯一约束（30% 概率）
        constraints = ""
        if random.random() < 0.3:
            # 选择一个非PK列作为UNIQUE
            non_pk_cols = [col for col in columns if not col.is_pk]
            if non_pk_cols:
                unique_col = random.choice(non_pk_cols)
                constraints = f",\n    UNIQUE KEY `uk_{unique_col.name}` (`{unique_col.name}`)"
        
        stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n    {col_defs}{constraints}\n)"
        
        return stmt
    
    def generate_alter_table_add_column(self, table_name: str, existing_columns: List[str] = None) -> str:
        """生成 ALTER TABLE ADD COLUMN 语句"""
        existing = set(existing_columns or [])
        col_name = ""
        # Avoid duplicate-column failures by retrying random names.
        for _ in range(20):
            candidate = f"new_col_{random.randint(1, 1000)}"
            if candidate not in existing:
                col_name = candidate
                break

        if not col_name:
            # Fallback for high-column scenarios: deterministic unique suffix.
            while True:
                candidate = f"new_col_{random.randint(1001, 999999)}"
                if candidate not in existing:
                    col_name = candidate
                    break

        data_type = self._generate_data_type().value
        
        stmt = f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {data_type}"
        
        return stmt
    
    def generate_alter_table_modify_column(self, table_name: str, col_name: str) -> str:
        """生成 ALTER TABLE MODIFY COLUMN 语句"""
        data_type = self._generate_data_type().value
        
        stmt = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{col_name}` {data_type}"
        
        return stmt
    
    def generate_alter_table_drop_column(self, table_name: str, col_name: str) -> str:
        """生成 ALTER TABLE DROP COLUMN 语句"""
        stmt = f"ALTER TABLE `{table_name}` DROP COLUMN `{col_name}`"
        
        return stmt
    
    def generate_create_index(self, table_name: str, col_name: str) -> str:
        """生成 CREATE INDEX 语句"""
        index_type = random.choice(["", "UNIQUE"])
        index_type_str = f"{index_type} " if index_type else ""
        index_name = f"idx_{col_name}_{random.randint(1, 1000)}"
        
        stmt = f"CREATE {index_type_str}INDEX `{index_name}` ON `{table_name}` (`{col_name}`)"
        
        return stmt
    
    def generate_drop_index(self, table_name: str, index_name: str) -> str:
        """生成 DROP INDEX 语句"""
        stmt = f"DROP INDEX `{index_name}` ON `{table_name}`"
        
        return stmt
    
    def generate_drop_table(self, table_name: str) -> str:
        """生成 DROP TABLE 语句"""
        stmt = f"DROP TABLE IF EXISTS `{table_name}`"
        
        return stmt
    
    def generate(
        self,
        count: int = 10,
        ddl_type: str = "all",
        table_name: str = "t0",
        existing_columns: List[str] = None,
        protected_columns: List[str] = None,
        drop_ratio: int = 35,
        enable_modify: bool = False,
    ) -> List[str]:
        """
        生成 DDL 语句集合
        
        Args:
            count: 生成的语句总数
            ddl_type: "all" (混合) / "create" / "alter_add" / "alter_modify" / "create_index"
            table_name: 用于 ALTER 操作的表名
        
        Returns:
            DDL 语句列表
        """
        statements = []
        
        if ddl_type == "all":
            # 混合生成 (60% CREATE, 20% ALTER_ADD, 10% CREATE_INDEX, 10% DROP)
            types = []
            types.extend(["create"] * int(count * 0.6))
            types.extend(["alter_add"] * int(count * 0.2))
            types.extend(["create_index"] * int(count * 0.1))
            types.extend(["drop"] * int(count * 0.1))
            random.shuffle(types)
        else:
            types = [ddl_type] * count
        
        # 跟踪创建的表（用于 ALTER/INDEX 操作）
        created_tables = []
        created_indices = []
        schema_cols = list(existing_columns or [])
        protected = set(protected_columns or ["c0"])
        
        for stmt_type in types[:count]:
            try:
                if stmt_type == "create":
                    stmt = self.generate_create_table()
                    # 记录表名
                    if "CREATE TABLE IF NOT EXISTS" in stmt:
                        # 提取表名
                        import re
                        match = re.search(r'`(\w+)`', stmt)
                        if match:
                            created_tables.append(match.group(1))
                elif stmt_type == "alter_add":
                    # 如果没有表，跳过
                    target_table = created_tables[-1] if created_tables else table_name
                    stmt = self.generate_alter_table_add_column(target_table, schema_cols)
                    added = self._extract_added_column(stmt)
                    if added and added not in schema_cols:
                        schema_cols.append(added)
                elif stmt_type == "alter_modify":
                    # 如果没有表，跳过
                    target_table = created_tables[-1] if created_tables else table_name
                    stmt = self.generate_alter_table_modify_column(target_table, "col0")
                elif stmt_type == "create_index":
                    # 如果没有表，跳过
                    target_table = created_tables[-1] if created_tables else table_name
                    index_name = f"idx_{target_table}_{random.randint(1, 1000)}"
                    stmt = self.generate_create_index(target_table, "col0")
                    created_indices.append(index_name)
                elif stmt_type == "drop":
                    # 删除最后创建的表
                    if created_tables:
                        target_table = created_tables.pop()
                        stmt = self.generate_drop_table(target_table)
                    else:
                        stmt = self.generate_drop_table("t_temp")
                else:
                    if stmt_type == "alter_mixed":
                        target_table = created_tables[-1] if created_tables else table_name
                        can_drop = [c for c in schema_cols if c not in protected]
                        can_modify = [c for c in schema_cols if c not in protected]
                        drop_prob = max(0, min(100, drop_ratio)) / 100.0
                        r = random.random()

                        # 混合策略：DROP（可控比例）/MODIFY（约25%）/ADD（其余）
                        if can_drop and r < drop_prob:
                            drop_col = random.choice(can_drop)
                            stmt = self.generate_alter_table_drop_column(target_table, drop_col)
                            schema_cols = [c for c in schema_cols if c != drop_col]
                        elif enable_modify and can_modify and r < min(0.95, drop_prob + 0.25):
                            modify_col = random.choice(can_modify)
                            stmt = self.generate_alter_table_modify_column(target_table, modify_col)
                        else:
                            stmt = self.generate_alter_table_add_column(target_table, schema_cols)
                            added = self._extract_added_column(stmt)
                            if added and added not in schema_cols:
                                schema_cols.append(added)
                    else:
                        continue
                
                if stmt:
                    statements.append(stmt)
            except Exception as e:
                print(f"[WARN] Failed to generate {stmt_type}: {e}", file=sys.stderr)
                continue
        
        return statements

    @staticmethod
    def _extract_added_column(stmt: str) -> str:
        import re
        m = re.search(r"ADD\s+COLUMN\s+`([^`]+)`", stmt, flags=re.IGNORECASE)
        return m.group(1) if m else ""


def parse_csv_list(raw: str) -> List[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def main():
    parser = argparse.ArgumentParser(
        description="DDL 语句生成器"
    )
    parser.add_argument("--count", type=int, default=10, help="生成语句数 (default: 10)")
    parser.add_argument("--seed", type=int, help="随机种子 (可重现性)")
    parser.add_argument("--type", choices=["all", "create", "alter_add", "alter_modify", "alter_mixed", "create_index", "drop"],
                       default="all", help="DDL 类型 (default: all)")
    parser.add_argument("--table-name", default="t0", help="用于 ALTER 操作的表名")
    parser.add_argument("--existing-cols", default="", help="现有列名列表，逗号分隔 (用于 alter_mixed)")
    parser.add_argument("--protected-cols", default="c0", help="禁止删除列名列表，逗号分隔")
    parser.add_argument("--drop-ratio", type=int, default=35, help="alter_mixed 中 DROP COLUMN 概率，0-100")
    parser.add_argument("--enable-modify", action="store_true", help="允许 alter_mixed 生成 MODIFY COLUMN")
    parser.add_argument("--output-sql", help="输出 SQL 文件")
    parser.add_argument("--output-format", choices=["sql", "csv"], default="sql", help="输出格式")
    
    args = parser.parse_args()
    
    # 生成语句
    gen = DDLGenerator(seed=args.seed)
    statements = gen.generate(
        count=args.count,
        ddl_type=args.type,
        table_name=args.table_name,
        existing_columns=parse_csv_list(args.existing_cols),
        protected_columns=parse_csv_list(args.protected_cols),
        drop_ratio=args.drop_ratio,
        enable_modify=args.enable_modify,
    )
    
    print(f"[INFO] Generated {len(statements)} {args.type} statements (seed: {args.seed})", file=sys.stderr)
    
    # 输出
    if args.output_format == "sql":
        output = "\n".join(statements)
    else:  # csv
        import csv
        import io
        output_io = io.StringIO()
        writer = csv.writer(output_io)
        writer.writerow(["statement_index", "statement"])
        for i, stmt in enumerate(statements, 1):
            writer.writerow([i, stmt])
        output = output_io.getvalue()
    
    if args.output_sql:
        with open(args.output_sql, "w") as f:
            f.write(output)
        print(f"[INFO] Output written to {args.output_sql}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
