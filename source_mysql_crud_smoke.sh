#!/usr/bin/env bash
set -euo pipefail

SRC_DB=${SRC_DB:-database0}
TABLE_NAME=${TABLE_NAME:-source_smoke}

MYSQL_PORT=$(docker port cdcup-mysql-1 3306/tcp | awk -F: '{print $2}')

run_mysql() {
  mysql -h127.0.0.1 -P"${MYSQL_PORT}" -uroot -e "$1"
}

echo "[1/8] ensure source database"
run_mysql "CREATE DATABASE IF NOT EXISTS ${SRC_DB};"

echo "[2/8] drop old table"
run_mysql "DROP TABLE IF EXISTS ${SRC_DB}.${TABLE_NAME};"

echo "[3/8] create table"
run_mysql "CREATE TABLE ${SRC_DB}.${TABLE_NAME}(id INT PRIMARY KEY, name VARCHAR(64), v INT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"

echo "[4/8] insert rows"
run_mysql "INSERT INTO ${SRC_DB}.${TABLE_NAME}(id,name,v) VALUES (1,'A',10),(2,'B',20),(3,'C',30);"

echo "[5/8] select after insert"
run_mysql "SELECT id,name,v FROM ${SRC_DB}.${TABLE_NAME} ORDER BY id;"

echo "[6/8] update row"
run_mysql "UPDATE ${SRC_DB}.${TABLE_NAME} SET v=99,name='A1' WHERE id=1;"

echo "[7/8] delete row"
run_mysql "DELETE FROM ${SRC_DB}.${TABLE_NAME} WHERE id=2;"

echo "[8/8] select final"
run_mysql "SELECT id,name,v FROM ${SRC_DB}.${TABLE_NAME} ORDER BY id;"

echo "PASS: source CREATE/DROP/CRUD works"
