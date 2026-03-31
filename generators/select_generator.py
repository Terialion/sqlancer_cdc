#!/usr/bin/env python3
"""
SELECT 查询生成器 (SELECT Query Generator)

生成有效的 SELECT 查询来验证 CDC 同步的数据一致性。
支持：简单列选择、WHERE条件、聚合函数、ORDER BY、LIMIT
"""

import argparse
import random
import sys
from typing import List, Dict
from dataclasses import dataclass
from enum import Enum


class AggregateFunc(Enum):
    """聚合函数"""
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"
    NONE = None


@dataclass
class Column:
    """列定义"""
    name: str
    data_type: str  # INT, VARCHAR, FLOAT, etc.
    is_numeric: bool


@dataclass
class Table:
    """表定义"""
    name: str
    columns: List[Column]


class SelectGenerator:
    """SELECT 查询生成器"""
    
    def __init__(self, table: Table, seed: int = None):
        self.table = table
        if seed is not None:
            random.seed(seed)
    
    def generate_simple_select(self) -> str:
        """生成简单的 SELECT 查询"""
        # 随机选择列（50-100%）
        num_cols = random.randint(len(self.table.columns) // 2, len(self.table.columns))
        selected_cols = random.sample(self.table.columns, num_cols)
        col_names = ", ".join([f"`{col.name}`" for col in selected_cols])
        
        stmt = f"SELECT {col_names} FROM `{self.table.name}`"
        
        # 可选：WHERE 条件（70% 概率）
        if random.random() < 0.7:
            where = self._generate_where()
            stmt += f" WHERE {where}"
        
        # 可选：ORDER BY（50% 概率）
        if random.random() < 0.5:
            order_col = random.choice(self.table.columns)
            order = random.choice(["ASC", "DESC"])
            stmt += f" ORDER BY `{order_col.name}` {order}"
        
        # 可选：LIMIT（50% 概率）
        if random.random() < 0.5:
            limit = random.randint(1, 100)
            stmt += f" LIMIT {limit}"
        
        return stmt
    
    def generate_aggregate_select(self) -> str:
        """生成聚合查询"""
        # 选择一个聚合函数
        agg_func = random.choice([
            AggregateFunc.COUNT,
            AggregateFunc.SUM,
            AggregateFunc.AVG,
            AggregateFunc.MAX,
            AggregateFunc.MIN
        ])
        
        # 选择目标列
        if agg_func == AggregateFunc.COUNT:
            agg_col = "*"
        else:
            # 对于SUM/AVG/MAX/MIN，选择数值列
            numeric_cols = [col for col in self.table.columns if col.is_numeric]
            if not numeric_cols:
                numeric_cols = self.table.columns
            agg_col = f"`{random.choice(numeric_cols).name}`"
        
        stmt = f"SELECT {agg_func.value}({agg_col}) as result FROM `{self.table.name}`"
        
        # 可选：WHERE 条件（50% 概率）
        if random.random() < 0.5:
            where = self._generate_where()
            stmt += f" WHERE {where}"
        
        return stmt
    
    def generate_count_query(self) -> str:
        """生成用于验证行数的 COUNT 查询"""
        stmt = f"SELECT COUNT(*) as row_count FROM `{self.table.name}`"
        
        # 可选：按某列分组
        if random.random() < 0.3:
            group_col = random.choice(self.table.columns)
            stmt += f" GROUP BY `{group_col.name}`"
        
        return stmt
    
    def generate_checksum_query(self) -> str:
        """生成用于数据校验的查询（MD5/XOR哈希）"""
        # 选择所有列进行校验
        col_list = ", ".join([f"`{col.name}`" for col in self.table.columns])
        
        stmt = f"SELECT MD5(GROUP_CONCAT(CONCAT_WS(',', {col_list}) ORDER BY {self._get_order_col()} ASC)) as checksum FROM `{self.table.name}`"
        
        return stmt
    
    def _generate_where(self) -> str:
        """生成 WHERE 条件"""
        col = random.choice(self.table.columns)
        
        if col.is_numeric:
            op = random.choice(["=", ">", "<", ">=", "<=", "!="])
            value = random.randint(1, 100)
            return f"`{col.name}` {op} {value}"
        else:
            op = random.choice(["=", "LIKE"])
            if op == "LIKE":
                return f"`{col.name}` LIKE '%{random.choice(['Alice', 'Bob', 'Charlie'])}%'"
            else:
                return f"`{col.name}` = '{random.choice(['Alice', 'Bob', 'Charlie'])}'"
    
    def _get_order_col(self) -> str:
        """获取用于排序的列"""
        # 优先使用主键或第一列
        if self.table.columns:
            return f"`{self.table.columns[0].name}`"
        return "`id`"
    
    def generate(self, count: int = 10, query_type: str = "all") -> List[str]:
        """
        生成 SELECT 查询集合
        
        Args:
            count: 生成的查询总数
            query_type: "all" (混合) / "simple" / "aggregate" / "count" / "checksum"
        
        Returns:
            SELECT 查询列表
        """
        queries = []
        
        if query_type == "all":
            # 混合生成（50% simple, 30% aggregate, 20% count）
            types = []
            types.extend(["simple"] * int(count * 0.5))
            types.extend(["aggregate"] * int(count * 0.3))
            types.extend(["count"] * int(count * 0.2))
            random.shuffle(types)
        else:
            types = [query_type] * count
        
        for q_type in types[:count]:
            try:
                if q_type == "simple":
                    query = self.generate_simple_select()
                elif q_type == "aggregate":
                    query = self.generate_aggregate_select()
                elif q_type == "count":
                    query = self.generate_count_query()
                elif q_type == "checksum":
                    query = self.generate_checksum_query()
                else:
                    continue
                
                if query:
                    queries.append(query)
            except Exception as e:
                print(f"[WARN] Failed to generate {q_type}: {e}", file=sys.stderr)
                continue
        
        return queries


def create_example_table() -> Table:
    """创建示例表 (t0)"""
    return Table(
        name="t0",
        columns=[
            Column(name="c0", data_type="INT", is_numeric=True),
            Column(name="c1", data_type="VARCHAR", is_numeric=False),
            Column(name="c2", data_type="INT", is_numeric=True),
            Column(name="c3", data_type="VARCHAR", is_numeric=False),
            Column(name="c4", data_type="FLOAT", is_numeric=True),
        ]
    )


def main():
    parser = argparse.ArgumentParser(
        description="SELECT 查询生成器"
    )
    parser.add_argument("--table-name", default="t0", help="表名 (default: t0)")
    parser.add_argument("--count", type=int, default=10, help="生成查询数 (default: 10)")
    parser.add_argument("--seed", type=int, help="随机种子 (可重现性)")
    parser.add_argument("--type", choices=["all", "simple", "aggregate", "count", "checksum"],
                       default="all", help="查询类型 (default: all)")
    parser.add_argument("--output-sql", help="输出 SQL 文件")
    
    args = parser.parse_args()
    
    # 创建表
    table = create_example_table()
    
    # 生成查询
    gen = SelectGenerator(table, seed=args.seed)
    queries = gen.generate(count=args.count, query_type=args.type)
    
    print(f"[INFO] Generated {len(queries)} {args.type} queries (seed: {args.seed})", file=sys.stderr)
    
    # 输出
    output = "\n".join(queries)
    
    if args.output_sql:
        with open(args.output_sql, "w") as f:
            f.write(output)
        print(f"[INFO] Output written to {args.output_sql}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
