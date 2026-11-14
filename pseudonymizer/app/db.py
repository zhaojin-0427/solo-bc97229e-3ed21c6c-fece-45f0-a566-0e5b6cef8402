"""SQLite 连接管理与建表。

三张核心表:

id_mapping(正式/已发布映射)
  - 只有批次成功发布后才写入
  - original_id 全局唯一 -> 同一原号永远只有一个对外编号
  - pseudonym  全局唯一 -> 不同原号绝不可能同号

pseudonym_registry(UUID 全局占用登记)
  - 预留阶段即登记，保证并发批次不会给不同原号分配到同一个 UUID
  - 失败批次回滚时只删除自己登记的编号

mapping_reservation(批次级预留)
  - 转换中、尚未发布的映射仅存在于此，外部查询不可见
  - 失败即按 batch_id 整批清除，不触碰其他批次
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

from .config import Config

# 每个线程独立连接(线程池中的工作线程各自持有)，按数据库路径区分，
# 避免同一线程在不同库(测试/多实例)之间串用连接
_local = threading.local()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # WAL 允许读写并发; NORMAL 兼顾安全与吞吐
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def get_conn(cfg: Config) -> sqlite3.Connection:
    conns = getattr(_local, "conns", None)
    if conns is None:
        conns = {}
        _local.conns = conns
    conn = conns.get(cfg.db_path)
    if conn is None:
        conn = _connect(cfg.db_path)
        conns[cfg.db_path] = conn
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """显式短事务。文件读写绝不能放在这个上下文里。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


SCHEMA = """
CREATE TABLE IF NOT EXISTS id_mapping (
    original_id TEXT PRIMARY KEY,
    pseudonym  TEXT NOT NULL UNIQUE,
    batch_id   TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pseudonym_registry (
    pseudonym TEXT PRIMARY KEY,
    original_id TEXT NOT NULL,
    batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mapping_reservation (
    batch_id    TEXT NOT NULL,
    original_id TEXT NOT NULL,
    pseudonym  TEXT NOT NULL,
    PRIMARY KEY (batch_id, original_id)
);
-- 注意: 不允许对 (original_id, pseudonym) 加 UNIQUE，
-- 重叠批次会为同一原号各预留一行相同的 (原号, 假名)。
-- 全局唯一性由 pseudonym_registry / id_mapping 的 UNIQUE 保证。

CREATE TABLE IF NOT EXISTS batch (
    batch_id     TEXT PRIMARY KEY,
    original_name TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('processing','success','failed')),
    error        TEXT,
    row_count    INTEGER NOT NULL DEFAULT 0,
    new_mappings INTEGER NOT NULL DEFAULT 0,
    reuse_mappings INTEGER NOT NULL DEFAULT 0,
    output_name  TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_batch_status ON batch(status);
CREATE INDEX IF NOT EXISTS idx_reservation_batch ON mapping_reservation(batch_id);
CREATE INDEX IF NOT EXISTS idx_registry_batch ON pseudonym_registry(batch_id);
"""


def init_db(cfg: Config) -> None:
    cfg.ensure_dirs()
    conn = _connect(cfg.db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
