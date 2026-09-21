"""SQLite 持久化层。

- WAL 模式 + busy_timeout：多线程/多连接并发写时串行化，避免 "database is locked"。
- 每次操作使用独立短连接，事务用 BEGIN IMMEDIATE 提前取写锁。
- 所有表 CREATE IF NOT EXISTS，重启后数据（含未完成调拨、审计记录）原样保留。
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager

from .errors import ConflictError

SCHEMA = """
-- 存放点
CREATE TABLE IF NOT EXISTS sites (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

-- 物资批次：同一物资按到货批次分开存放，带有效期与乐观锁版本号
CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    item        TEXT NOT NULL,              -- 物资种类，如 饮用水/应急照明
    site_id     INTEGER NOT NULL REFERENCES sites(id),
    quantity    INTEGER NOT NULL CHECK (quantity >= 0),
    expiry_date TEXT,                       -- ISO 日期，NULL 表示无有效期
    status      TEXT NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE / DEPLETED
    version     INTEGER NOT NULL DEFAULT 0,      -- 乐观锁：并发扣减冲突检测
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_fefo
    ON batches (item, site_id, status, expiry_date);

-- 行动批次（一次安置行动）
CREATE TABLE IF NOT EXISTS campaigns (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    code       TEXT NOT NULL UNIQUE,        -- 行动批次号
    name       TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE / CLOSED
    created_at TEXT NOT NULL
);

-- 受助家庭
CREATE TABLE IF NOT EXISTS households (
    id             TEXT PRIMARY KEY,        -- 家庭标识
    head_name      TEXT NOT NULL DEFAULT '',
    members        INTEGER NOT NULL DEFAULT 1,
    status         TEXT NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE / SUSPENDED / EXPIRED
    eligible_until TEXT                     -- 资格截止日，NULL 表示长期有效
);

-- 发放额度：某行动批次下某家庭对某物资的剩余可领数量
CREATE TABLE IF NOT EXISTS quotas (
    campaign_id  INTEGER NOT NULL REFERENCES campaigns(id),
    household_id TEXT NOT NULL REFERENCES households(id),
    item         TEXT NOT NULL,
    amount       INTEGER NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (campaign_id, household_id, item)
);

-- 发放单（一次领用）
CREATE TABLE IF NOT EXISTS distributions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    serial_no    TEXT NOT NULL,             -- 来源流水号（幂等键）
    campaign_id  INTEGER NOT NULL REFERENCES campaigns(id),
    household_id TEXT NOT NULL REFERENCES households(id),
    item         TEXT NOT NULL,
    site_id      INTEGER NOT NULL REFERENCES sites(id),
    quantity     INTEGER NOT NULL,
    returned_qty INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'ISSUED',  -- ISSUED / PARTIALLY_RETURNED / RETURNED
    actor        TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_distributions_household
    ON distributions (household_id, campaign_id);

-- 发放单明细：实际从哪些批次出库（FEFO 分配结果），退回时按此回补
CREATE TABLE IF NOT EXISTS distribution_lines (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    distribution_id INTEGER NOT NULL REFERENCES distributions(id),
    batch_id        INTEGER NOT NULL REFERENCES batches(id),
    quantity        INTEGER NOT NULL,
    returned        INTEGER NOT NULL DEFAULT 0
);

-- 跨点调拨单：PENDING 状态重启后仍然存在
CREATE TABLE IF NOT EXISTS transfers (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    serial_no    TEXT NOT NULL,
    item         TEXT NOT NULL,
    from_site_id INTEGER NOT NULL REFERENCES sites(id),
    to_site_id   INTEGER NOT NULL REFERENCES sites(id),
    quantity     INTEGER NOT NULL,
    status       TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING / COMPLETED / CANCELLED
    approver     TEXT NOT NULL,
    reason       TEXT NOT NULL,
    created_by   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS transfer_lines (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    transfer_id INTEGER NOT NULL REFERENCES transfers(id),
    batch_id    INTEGER NOT NULL REFERENCES batches(id),  -- 源存放点批次
    expiry_date TEXT,
    quantity    INTEGER NOT NULL
);

-- 盘点记录：账实差异留痕
CREATE TABLE IF NOT EXISTS stocktakes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   INTEGER NOT NULL REFERENCES batches(id),
    counted    INTEGER NOT NULL,
    system_qty INTEGER NOT NULL,
    variance   INTEGER NOT NULL,
    applied    INTEGER NOT NULL DEFAULT 0,  -- 是否已按盘点数调整账面
    actor      TEXT NOT NULL,
    approver   TEXT NOT NULL,
    reason     TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- 审计日志：追加写，所有关键操作留审批人与原因
CREATE TABLE IF NOT EXISTS audit_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL,
    actor      TEXT NOT NULL,
    action     TEXT NOT NULL,
    entity     TEXT NOT NULL DEFAULT '',
    detail     TEXT NOT NULL DEFAULT '{}',  -- JSON
    approver   TEXT,
    reason     TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log (action);

-- 幂等键：现场终端断网补传按流水号去重
CREATE TABLE IF NOT EXISTS idempotency_keys (
    serial_no     TEXT PRIMARY KEY,
    operation     TEXT NOT NULL,
    request_hash  TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""


class Database:
    """SQLite 文件库。path 相同即共享数据，服务重启不影响。"""

    def __init__(self, path: str):
        self.path = str(path)
        with self.read() as conn:
            conn.executescript(SCHEMA)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def read(self):
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def write(self):
        """写事务：BEGIN IMMEDIATE 提前取写锁，提交前任何异常都回滚。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            self._rollback_quietly(conn)
            if "locked" in str(exc):
                raise ConflictError("数据库写锁竞争，操作冲突，请重试") from exc
            raise
        except BaseException:
            self._rollback_quietly(conn)
            raise
        finally:
            conn.close()

    @staticmethod
    def _rollback_quietly(conn):
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
