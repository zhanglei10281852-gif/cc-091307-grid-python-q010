"""社区应急物资领用服务.

背景: 临时安置行动中, 饮用水和应急照明被多个小组同时登记领用, 事后库存
对不上、受助家庭是否重复领取无法确认. 本模块提供覆盖全流程的领域服务:

- 物资批次 / 有效期 / 存放点管理, 全部出入库记入库存台账(可盘点对账)
- 按 行动批次 + 家庭标识 校验领取资格与发放额度
- 先到期先发(FEFO)出库建议, 过期批次禁止出库
- 额度调整 / 退回 / 报损 / 跨点调拨 均强制记录审批人与原因(审计日志)
- 断网终端按流水号幂等补传: 同一流水号重复提交返回首次执行结果
- 并发领取最后一批物资时基于版本号乐观锁返回冲突, 绝不超发
- 按角色的查询视图: 仓管员仅见本存放点信息, 管理者可查批次库存 /
  家庭领取历史 / 异常盘点 / 审计日志
- SQLite 持久化: 重启后未完成调拨、审计与幂等记录继续存在
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional

__all__ = [
    "ReliefService",
    "Service",
    "Role",
    "Viewer",
    "ServiceError",
    "ValidationError",
    "NotFoundError",
    "InsufficientStockError",
    "EligibilityError",
    "ConflictError",
    "PermissionDeniedError",
]


# --------------------------------------------------------------------- 错误
class ServiceError(Exception):
    """业务错误基类."""


class ValidationError(ServiceError):
    """请求参数非法."""


class NotFoundError(ServiceError):
    """引用的实体不存在."""


class InsufficientStockError(ServiceError):
    """库存不足, 携带请求量与当前可用量."""

    def __init__(self, message: str, *, requested: int, available: int):
        super().__init__(message)
        self.requested = requested
        self.available = available


class EligibilityError(ServiceError):
    """领取资格失效或额度不足."""


class ConflictError(ServiceError):
    """并发冲突: 数据已被其他操作修改, 需重新规划后重试."""


class PermissionDeniedError(ServiceError):
    """当前角色无权访问该信息."""


# --------------------------------------------------------------------- 角色
class Role(str, Enum):
    MANAGER = "manager"   # 管理者: 全量查询与盘点
    KEEPER = "keeper"     # 仓管员: 仅本存放点的库存与资格核查


@dataclass(frozen=True)
class Viewer:
    """查询者身份; 仓管员必须携带所属存放点."""

    role: Role
    location_code: Optional[str] = None


SCHEMA = """
CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS materials (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    unit TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY,
    material_id INTEGER NOT NULL REFERENCES materials(id),
    batch_no TEXT NOT NULL,
    expiry_date TEXT NOT NULL,               -- ISO 日期, 有效期至(含当日)
    UNIQUE (material_id, batch_no)
);
CREATE TABLE IF NOT EXISTS stock (
    id INTEGER PRIMARY KEY,
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    location_id INTEGER NOT NULL REFERENCES locations(id),
    quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    version INTEGER NOT NULL DEFAULT 0,      -- 乐观锁版本号
    UNIQUE (batch_id, location_id)
);
CREATE TABLE IF NOT EXISTS operations (      -- 行动批次
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed'))
);
CREATE TABLE IF NOT EXISTS families (
    id INTEGER PRIMARY KEY,
    family_code TEXT NOT NULL UNIQUE,        -- 家庭标识
    head_name TEXT NOT NULL DEFAULT '',
    members INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS entitlements (    -- 领取资格与发放额度
    id INTEGER PRIMARY KEY,
    operation_id INTEGER NOT NULL REFERENCES operations(id),
    family_id INTEGER NOT NULL REFERENCES families(id),
    material_id INTEGER NOT NULL REFERENCES materials(id),
    quota INTEGER NOT NULL CHECK (quota >= 0),
    used INTEGER NOT NULL DEFAULT 0 CHECK (used >= 0),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
    valid_from TEXT,
    valid_until TEXT,
    created_by TEXT,
    created_reason TEXT,
    UNIQUE (operation_id, family_id, material_id)
);
CREATE TABLE IF NOT EXISTS claims (          -- 领用单
    id INTEGER PRIMARY KEY,
    serial_no TEXT NOT NULL UNIQUE,          -- 终端流水号(幂等键)
    entitlement_id INTEGER NOT NULL REFERENCES entitlements(id),
    location_id INTEGER NOT NULL REFERENCES locations(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    actor TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'completed'
        CHECK (status IN ('completed', 'partially_returned', 'returned')),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS claim_items (     -- 领用明细(按批次)
    id INTEGER PRIMARY KEY,
    claim_id INTEGER NOT NULL REFERENCES claims(id),
    stock_id INTEGER NOT NULL REFERENCES stock(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    returned_qty INTEGER NOT NULL DEFAULT 0 CHECK (returned_qty >= 0)
);
CREATE TABLE IF NOT EXISTS transfers (       -- 跨点调拨
    id INTEGER PRIMARY KEY,
    serial_no TEXT NOT NULL UNIQUE,
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    from_location_id INTEGER NOT NULL REFERENCES locations(id),
    to_location_id INTEGER NOT NULL REFERENCES locations(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'completed', 'cancelled')),
    approver TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_by TEXT,
    completed_at TEXT
);
CREATE TABLE IF NOT EXISTS stock_ledger (    -- 库存台账: 每笔变动留痕
    id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    stock_id INTEGER NOT NULL REFERENCES stock(id),
    change INTEGER NOT NULL,
    balance_after INTEGER NOT NULL,
    ref_type TEXT NOT NULL,                  -- receipt/claim/return/damage/transfer_*
    ref_id INTEGER,
    serial_no TEXT,
    actor TEXT,
    approver TEXT,
    reason TEXT
);
CREATE TABLE IF NOT EXISTS audits (          -- 审计日志
    id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id INTEGER,
    actor TEXT,
    approver TEXT,
    reason TEXT,
    details TEXT                             -- JSON
);
CREATE TABLE IF NOT EXISTS idempotency_keys (-- 幂等记录(断网补传)
    serial_no TEXT PRIMARY KEY,
    op_type TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response TEXT NOT NULL,                  -- JSON, 首次执行结果
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stock_batch ON stock(batch_id);
CREATE INDEX IF NOT EXISTS idx_ledger_stock ON stock_ledger(stock_id);
CREATE INDEX IF NOT EXISTS idx_claims_entitlement ON claims(entitlement_id);
CREATE INDEX IF NOT EXISTS idx_claim_items_claim ON claim_items(claim_id);
CREATE INDEX IF NOT EXISTS idx_audits_action ON audits(action);
CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers(status);
"""


class ReliefService:
    """物资领用领域服务.

    db_path: SQLite 文件路径, ":memory:" 表示纯内存(测试用).
    clock:   可注入时钟(返回 datetime), 便于测试有效期/过期逻辑.
    """

    def __init__(self, db_path: str = ":memory:", *,
                 clock: Optional[Callable[[], datetime]] = None):
        self._db_path = db_path
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False,
                                     isolation_level=None)  # 手动控制事务
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        if db_path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "ReliefService":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------------------------------------------------------- 基础
    def _now(self) -> datetime:
        return self._clock()

    def _now_iso(self) -> str:
        return self._now().isoformat()

    def _today(self) -> str:
        return self._now().date().isoformat()

    @contextmanager
    def _txn(self):
        """写事务: 进程内串行化 + BEGIN IMMEDIATE, 保证读改写原子."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def _query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def _one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        rows = self._query(sql, params)
        return rows[0] if rows else None

    @staticmethod
    def _require_text(value: Optional[str], label: str) -> None:
        if not value or not str(value).strip():
            raise ValidationError(f"{label}不能为空")

    @classmethod
    def _require_approval(cls, approver: Optional[str], reason: Optional[str]) -> None:
        cls._require_text(approver, "审批人")
        cls._require_text(reason, "原因")

    @staticmethod
    def _require_positive(quantity: int, label: str = "数量") -> None:
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValidationError(f"{label}必须为正整数")

    # ------------------------------------------------------------ 幂等框架
    def _run_idempotent(self, serial_no: str, op_type: str,
                        payload: dict, work: Callable[[], dict]) -> dict:
        """按流水号幂等执行: 同一流水号重复提交直接返回首次结果.

        流水号相同但请求内容不同视为客户端错误; 执行失败不落幂等记录,
        终端修正后可用同一流水号重试.
        """
        self._require_text(serial_no, "流水号")
        req_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
            .encode("utf-8")
        ).hexdigest()
        with self._txn():
            row = self._one(
                "SELECT op_type, request_hash, response FROM idempotency_keys "
                "WHERE serial_no = ?", (serial_no,))
            if row is not None:
                if row["op_type"] != op_type or row["request_hash"] != req_hash:
                    raise ValidationError(
                        f"流水号 {serial_no} 已用于其他请求, 禁止复用")
                resp = json.loads(row["response"])
                resp["idempotent_replay"] = True
                return resp
            result = work()
            self._conn.execute(
                "INSERT INTO idempotency_keys"
                " (serial_no, op_type, request_hash, response, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (serial_no, op_type, req_hash,
                 json.dumps(result, ensure_ascii=False, default=str),
                 self._now_iso()))
            result["idempotent_replay"] = False
            return result

    # ------------------------------------------------------------ 留痕工具
    def _audit(self, action: str, entity_type: str, entity_id: Optional[int], *,
               actor: Optional[str] = None, approver: Optional[str] = None,
               reason: Optional[str] = None, details: Optional[dict] = None) -> None:
        self._conn.execute(
            "INSERT INTO audits (ts, action, entity_type, entity_id, actor,"
            " approver, reason, details) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (self._now_iso(), action, entity_type, entity_id, actor, approver,
             reason,
             json.dumps(details, ensure_ascii=False, default=str)
             if details is not None else None))

    def _ledger(self, stock_id: int, change: int, ref_type: str,
                ref_id: Optional[int], *, serial_no: Optional[str] = None,
                actor: Optional[str] = None, approver: Optional[str] = None,
                reason: Optional[str] = None) -> None:
        balance = self._one("SELECT quantity FROM stock WHERE id = ?",
                            (stock_id,))["quantity"]
        self._conn.execute(
            "INSERT INTO stock_ledger (ts, stock_id, change, balance_after,"
            " ref_type, ref_id, serial_no, actor, approver, reason)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (self._now_iso(), stock_id, change, balance, ref_type, ref_id,
             serial_no, actor, approver, reason))

    # ------------------------------------------------------------ 实体解析
    def _material_id(self, code: str) -> int:
        row = self._one("SELECT id FROM materials WHERE code = ?", (code,))
        if row is None:
            raise NotFoundError(f"物资不存在: {code}")
        return row["id"]

    def _location_id(self, code: str) -> int:
        row = self._one("SELECT id FROM locations WHERE code = ?", (code,))
        if row is None:
            raise NotFoundError(f"存放点不存在: {code}")
        return row["id"]

    def _operation(self, code: str) -> sqlite3.Row:
        row = self._one("SELECT * FROM operations WHERE code = ?", (code,))
        if row is None:
            raise NotFoundError(f"行动批次不存在: {code}")
        return row

    def _family(self, code: str) -> sqlite3.Row:
        row = self._one("SELECT * FROM families WHERE family_code = ?", (code,))
        if row is None:
            raise NotFoundError(f"家庭不存在: {code}")
        return row

    def _ensure_stock_id(self, batch_id: int, location_id: int) -> int:
        row = self._one("SELECT id FROM stock WHERE batch_id = ? AND location_id = ?",
                        (batch_id, location_id))
        if row is not None:
            return row["id"]
        cur = self._conn.execute(
            "INSERT INTO stock (batch_id, location_id, quantity, version)"
            " VALUES (?, ?, 0, 0)", (batch_id, location_id))
        return cur.lastrowid

    def _find_stock(self, material_code: str, batch_no: str,
                    location_code: str) -> sqlite3.Row:
        row = self._one(
            "SELECT s.id, s.quantity, s.version, s.batch_id, s.location_id,"
            " b.batch_no, b.expiry_date"
            " FROM stock s"
            " JOIN batches b ON b.id = s.batch_id"
            " JOIN materials m ON m.id = b.material_id"
            " JOIN locations l ON l.id = s.location_id"
            " WHERE m.code = ? AND b.batch_no = ? AND l.code = ?",
            (material_code, batch_no, location_code))
        if row is None:
            raise NotFoundError(
                f"库存不存在: {material_code}/{batch_no}@{location_code}")
        return row

    # ------------------------------------------------------------ 基础建档
    def add_location(self, code: str, name: str) -> None:
        self._require_text(code, "存放点编码")
        with self._txn():
            try:
                self._conn.execute(
                    "INSERT INTO locations (code, name) VALUES (?, ?)", (code, name))
            except sqlite3.IntegrityError:
                raise ValidationError(f"存放点已存在: {code}") from None

    def add_material(self, code: str, name: str, unit: str) -> None:
        self._require_text(code, "物资编码")
        with self._txn():
            try:
                self._conn.execute(
                    "INSERT INTO materials (code, name, unit) VALUES (?, ?, ?)",
                    (code, name, unit))
            except sqlite3.IntegrityError:
                raise ValidationError(f"物资已存在: {code}") from None

    def add_operation(self, code: str, name: str) -> None:
        self._require_text(code, "行动批次编码")
        with self._txn():
            try:
                self._conn.execute(
                    "INSERT INTO operations (code, name) VALUES (?, ?)", (code, name))
            except sqlite3.IntegrityError:
                raise ValidationError(f"行动批次已存在: {code}") from None

    def add_family(self, family_code: str, head_name: str = "",
                   members: int = 1) -> None:
        self._require_text(family_code, "家庭标识")
        with self._txn():
            try:
                self._conn.execute(
                    "INSERT INTO families (family_code, head_name, members)"
                    " VALUES (?, ?, ?)", (family_code, head_name, members))
            except sqlite3.IntegrityError:
                raise ValidationError(f"家庭已存在: {family_code}") from None

    def close_operation(self, code: str, *, actor: str) -> dict:
        """关闭行动批次: 关闭后该批次下的领取一律拒绝."""
        self._require_text(actor, "操作员")
        with self._txn():
            op = self._operation(code)
            if op["status"] == "closed":
                return {"code": code, "status": "closed", "idempotent_replay": True}
            self._conn.execute(
                "UPDATE operations SET status = 'closed' WHERE id = ?", (op["id"],))
            self._audit("close_operation", "operation", op["id"], actor=actor)
            return {"code": code, "status": "closed", "idempotent_replay": False}

    # ------------------------------------------------------------ 入库
    def receive_stock(self, *, serial_no: str, material_code: str, batch_no: str,
                      expiry_date: str, location_code: str, quantity: int,
                      actor: str, reason: Optional[str] = None) -> dict:
        """物资入库(同批次号有效期必须一致); 按流水号幂等."""
        self._require_positive(quantity)
        self._require_text(actor, "操作员")
        payload = dict(material_code=material_code, batch_no=batch_no,
                       expiry_date=expiry_date, location_code=location_code,
                       quantity=quantity, actor=actor, reason=reason)

        def work() -> dict:
            mid = self._material_id(material_code)
            lid = self._location_id(location_code)
            batch = self._one(
                "SELECT id, expiry_date FROM batches"
                " WHERE material_id = ? AND batch_no = ?", (mid, batch_no))
            if batch is not None:
                if batch["expiry_date"] != expiry_date:
                    raise ValidationError(
                        f"批次 {batch_no} 已存在且有效期为 {batch['expiry_date']},"
                        f" 与本次 {expiry_date} 不一致")
                batch_id = batch["id"]
            else:
                batch_id = self._conn.execute(
                    "INSERT INTO batches (material_id, batch_no, expiry_date)"
                    " VALUES (?, ?, ?)", (mid, batch_no, expiry_date)).lastrowid
            stock_id = self._ensure_stock_id(batch_id, lid)
            self._conn.execute(
                "UPDATE stock SET quantity = quantity + ?, version = version + 1"
                " WHERE id = ?", (quantity, stock_id))
            self._ledger(stock_id, quantity, "receipt", None,
                         serial_no=serial_no, actor=actor, reason=reason)
            new_qty = self._one("SELECT quantity FROM stock WHERE id = ?",
                                (stock_id,))["quantity"]
            self._audit("receive_stock", "stock", stock_id, actor=actor,
                        reason=reason, details=payload)
            return {"stock_id": stock_id, "batch_id": batch_id,
                    "material_code": material_code, "batch_no": batch_no,
                    "location_code": location_code, "quantity_added": quantity,
                    "new_quantity": new_qty}

        return self._run_idempotent(serial_no, "receive_stock", payload, work)

    # ------------------------------------------------------------ 资格与额度
    def grant_entitlement(self, *, serial_no: str, operation_code: str,
                          family_code: str, material_code: str, quota: int,
                          actor: str, valid_from: Optional[str] = None,
                          valid_until: Optional[str] = None,
                          reason: Optional[str] = None) -> dict:
        """授予家庭在某行动批次下的领取资格与额度."""
        if not isinstance(quota, int) or quota < 0:
            raise ValidationError("额度必须为非负整数")
        self._require_text(actor, "操作员")
        payload = dict(operation_code=operation_code, family_code=family_code,
                       material_code=material_code, quota=quota, actor=actor,
                       valid_from=valid_from, valid_until=valid_until, reason=reason)

        def work() -> dict:
            op = self._operation(operation_code)
            fam = self._family(family_code)
            mid = self._material_id(material_code)
            existing = self._one(
                "SELECT id FROM entitlements WHERE operation_id = ?"
                " AND family_id = ? AND material_id = ?",
                (op["id"], fam["id"], mid))
            if existing is not None:
                raise ValidationError("该家庭在此行动批次下已有此物资资格,"
                                      " 请使用 adjust_quota 调整额度")
            ent_id = self._conn.execute(
                "INSERT INTO entitlements (operation_id, family_id, material_id,"
                " quota, valid_from, valid_until, created_by, created_reason)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (op["id"], fam["id"], mid, quota, valid_from, valid_until,
                 actor, reason)).lastrowid
            self._audit("grant_entitlement", "entitlement", ent_id, actor=actor,
                        reason=reason, details=payload)
            return {"entitlement_id": ent_id, "operation_code": operation_code,
                    "family_code": family_code, "material_code": material_code,
                    "quota": quota, "used": 0}

        return self._run_idempotent(serial_no, "grant_entitlement", payload, work)

    def _entitlement_row(self, operation_code: str, family_code: str,
                         material_code: str) -> Optional[sqlite3.Row]:
        return self._one(
            "SELECT e.*, o.status AS op_status, o.code AS op_code,"
            " f.family_code AS fam_code, m.code AS mat_code"
            " FROM entitlements e"
            " JOIN operations o ON o.id = e.operation_id"
            " JOIN families f ON f.id = e.family_id"
            " JOIN materials m ON m.id = e.material_id"
            " WHERE o.code = ? AND f.family_code = ? AND m.code = ?",
            (operation_code, family_code, material_code))

    def adjust_quota(self, *, serial_no: str, operation_code: str,
                     family_code: str, material_code: str, new_quota: int,
                     approver: str, reason: str, actor: str) -> dict:
        """调整发放额度(留审批人与原因); 新额度不得低于已领取数量."""
        self._require_approval(approver, reason)
        self._require_text(actor, "操作员")
        if not isinstance(new_quota, int) or new_quota < 0:
            raise ValidationError("额度必须为非负整数")
        payload = dict(operation_code=operation_code, family_code=family_code,
                       material_code=material_code, new_quota=new_quota,
                       approver=approver, reason=reason, actor=actor)

        def work() -> dict:
            ent = self._entitlement_row(operation_code, family_code, material_code)
            if ent is None:
                raise NotFoundError("领取资格不存在")
            if new_quota < ent["used"]:
                raise ValidationError(
                    f"新额度 {new_quota} 低于已领取数量 {ent['used']}")
            self._conn.execute(
                "UPDATE entitlements SET quota = ? WHERE id = ?",
                (new_quota, ent["id"]))
            self._audit("adjust_quota", "entitlement", ent["id"], actor=actor,
                        approver=approver, reason=reason,
                        details={"old_quota": ent["quota"], "new_quota": new_quota})
            return {"entitlement_id": ent["id"], "old_quota": ent["quota"],
                    "new_quota": new_quota, "used": ent["used"]}

        return self._run_idempotent(serial_no, "adjust_quota", payload, work)

    def revoke_entitlement(self, *, serial_no: str, operation_code: str,
                           family_code: str, material_code: str,
                           approver: str, reason: str, actor: str) -> dict:
        """注销领取资格(留审批人与原因); 注销后领取一律拒绝."""
        self._require_approval(approver, reason)
        self._require_text(actor, "操作员")
        payload = dict(operation_code=operation_code, family_code=family_code,
                       material_code=material_code, approver=approver,
                       reason=reason, actor=actor)

        def work() -> dict:
            ent = self._entitlement_row(operation_code, family_code, material_code)
            if ent is None:
                raise NotFoundError("领取资格不存在")
            if ent["status"] == "revoked":
                raise ValidationError("资格已注销, 请勿重复操作")
            self._conn.execute(
                "UPDATE entitlements SET status = 'revoked' WHERE id = ?",
                (ent["id"],))
            self._audit("revoke_entitlement", "entitlement", ent["id"],
                        actor=actor, approver=approver, reason=reason)
            return {"entitlement_id": ent["id"], "status": "revoked"}

        return self._run_idempotent(serial_no, "revoke_entitlement", payload, work)

    # ------------------------------------------------------------ FEFO 规划
    def _plan_inner(self, material_id: int, location_id: int, quantity: int,
                    allow_expired: bool) -> list[dict]:
        """先到期先发: 按有效期升序扣减, 返回带版本号的出库计划."""
        sql = (
            "SELECT s.id AS stock_id, s.quantity, s.version,"
            " b.batch_no, b.expiry_date"
            " FROM stock s JOIN batches b ON b.id = s.batch_id"
            " WHERE b.material_id = ? AND s.location_id = ? AND s.quantity > 0")
        params: list[Any] = [material_id, location_id]
        if not allow_expired:
            sql += " AND b.expiry_date >= ?"
            params.append(self._today())
        sql += " ORDER BY b.expiry_date ASC, s.id ASC"
        rows = self._query(sql, tuple(params))

        plan: list[dict] = []
        remaining = quantity
        for row in rows:
            take = min(row["quantity"], remaining)
            plan.append({"stock_id": row["stock_id"],
                         "batch_no": row["batch_no"],
                         "expiry_date": row["expiry_date"],
                         "quantity": take,
                         "expected_version": row["version"]})
            remaining -= take
            if remaining == 0:
                break
        if remaining > 0:
            available = quantity - remaining
            raise InsufficientStockError(
                f"库存不足: 需 {quantity}, 当前可发 {available}",
                requested=quantity, available=available)
        return plan

    def plan_allocation(self, *, material_code: str, location_code: str,
                        quantity: int, allow_expired: bool = False) -> list[dict]:
        """出库建议(只读): 返回 FEFO 出库计划, 可随领取请求一并提交."""
        self._require_positive(quantity)
        mid = self._material_id(material_code)
        lid = self._location_id(location_code)
        plan = self._plan_inner(mid, lid, quantity, allow_expired)
        for item in plan:
            item["location_code"] = location_code
        return plan

    # ------------------------------------------------------------ 领取
    def claim(self, *, serial_no: str, operation_code: str, family_code: str,
              material_code: str, location_code: str, quantity: int,
              actor: str, plan: Optional[list[dict]] = None) -> dict:
        """登记领用: 校验资格与额度, 按 FEFO 出库, 全程幂等.

        plan: 可选的预规划出库计划(来自 plan_allocation). 两名仓管员基于
        同一库存快照各自规划后同时提交时, 后到者因版本号校验失败收到
        ConflictError, 不会超发.
        """
        self._require_positive(quantity)
        self._require_text(actor, "操作员")
        payload = dict(operation_code=operation_code, family_code=family_code,
                       material_code=material_code, location_code=location_code,
                       quantity=quantity, actor=actor)

        def work() -> dict:
            ent = self._entitlement_row(operation_code, family_code, material_code)
            if ent is None:
                raise EligibilityError("该家庭在此行动批次下无此物资的领取资格")
            if ent["op_status"] != "active":
                raise EligibilityError(f"行动批次 {operation_code} 已关闭")
            if ent["status"] != "active":
                raise EligibilityError("该家庭的领取资格已注销")
            today = self._today()
            if ent["valid_from"] and ent["valid_from"] > today:
                raise EligibilityError(f"资格 {ent['valid_from']} 起才生效")
            if ent["valid_until"] and ent["valid_until"] < today:
                raise EligibilityError(f"资格已于 {ent['valid_until']} 过期")
            remaining_quota = ent["quota"] - ent["used"]
            if quantity > remaining_quota:
                raise EligibilityError(
                    f"超出可领额度: 申请 {quantity}, 剩余 {remaining_quota}")

            lid = self._location_id(location_code)
            mid = ent["material_id"]
            use_plan = plan if plan is not None else self._plan_inner(
                mid, lid, quantity, allow_expired=False)
            if sum(int(p["quantity"]) for p in use_plan) != quantity:
                raise ValidationError("出库计划总量与申请数量不一致")

            items: list[dict] = []
            for p in use_plan:
                take = int(p["quantity"])
                self._require_positive(take, "计划数量")
                stock_id = p.get("stock_id")
                expected_version = p.get("expected_version")
                if stock_id is None or expected_version is None:
                    raise ValidationError("出库计划缺少 stock_id/expected_version")
                row = self._one(
                    "SELECT s.id, s.location_id, b.material_id, b.id AS batch_id,"
                    " b.batch_no, b.expiry_date"
                    " FROM stock s JOIN batches b ON b.id = s.batch_id"
                    " WHERE s.id = ?", (stock_id,))
                if row is None or row["location_id"] != lid \
                        or row["material_id"] != mid:
                    raise ValidationError("出库计划与领取请求不符")
                if row["expiry_date"] < today:
                    raise ValidationError(f"批次 {row['batch_no']} 已过期, 禁止出库")
                # 乐观锁: 版本号与库存量双重校验, 并发下后到者必失败
                cur = self._conn.execute(
                    "UPDATE stock SET quantity = quantity - ?, version = version + 1"
                    " WHERE id = ? AND version = ? AND quantity >= ?",
                    (take, stock_id, expected_version, take))
                if cur.rowcount != 1:
                    raise ConflictError(
                        f"批次 {row['batch_no']} 库存已被其他操作变更,"
                        " 请重新规划出库")
                items.append({"stock_id": stock_id, "batch_id": row["batch_id"],
                              "batch_no": row["batch_no"],
                              "expiry_date": row["expiry_date"],
                              "quantity": take})

            cur = self._conn.execute(
                "UPDATE entitlements SET used = used + ?"
                " WHERE id = ? AND used + ? <= quota AND status = 'active'",
                (quantity, ent["id"], quantity))
            if cur.rowcount != 1:
                raise ConflictError("领取额度已被并发修改, 请重试")

            claim_id = self._conn.execute(
                "INSERT INTO claims (serial_no, entitlement_id, location_id,"
                " quantity, actor, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (serial_no, ent["id"], lid, quantity, actor,
                 self._now_iso())).lastrowid
            for item in items:
                item_id = self._conn.execute(
                    "INSERT INTO claim_items (claim_id, stock_id, quantity)"
                    " VALUES (?, ?, ?)",
                    (claim_id, item["stock_id"], item["quantity"])).lastrowid
                item["claim_item_id"] = item_id
                self._ledger(item["stock_id"], -item["quantity"], "claim",
                             claim_id, serial_no=serial_no, actor=actor)
            self._audit("claim", "claim", claim_id, actor=actor,
                        details={**payload,
                                 "items": [{k: v for k, v in it.items()
                                            if k != "stock_id"} for it in items]})
            return {"claim_id": claim_id, "serial_no": serial_no,
                    "operation_code": operation_code, "family_code": family_code,
                    "material_code": material_code, "location_code": location_code,
                    "quantity": quantity,
                    "items": [{k: it[k] for k in
                               ("claim_item_id", "batch_no", "expiry_date",
                                "quantity")} for it in items],
                    "remaining_quota": remaining_quota - quantity}

        return self._run_idempotent(serial_no, "claim", payload, work)

    # ------------------------------------------------------------ 退回
    def return_goods(self, *, serial_no: str, claim_id: int, approver: str,
                     reason: str, actor: str,
                     items: Optional[list[dict]] = None) -> dict:
        """退回(留审批人与原因): 库存回补原批次, 已用额度相应释放.

        items: [{"claim_item_id": int, "quantity": int}], 缺省为全部可退数量.
        """
        self._require_approval(approver, reason)
        self._require_text(actor, "操作员")
        payload = dict(claim_id=claim_id, approver=approver, reason=reason,
                       actor=actor, items=items)

        def work() -> dict:
            claim = self._one("SELECT * FROM claims WHERE id = ?", (claim_id,))
            if claim is None:
                raise NotFoundError(f"领用单不存在: {claim_id}")
            claim_items = self._query(
                "SELECT ci.*, b.batch_no FROM claim_items ci"
                " JOIN stock s ON s.id = ci.stock_id"
                " JOIN batches b ON b.id = s.batch_id"
                " WHERE ci.claim_id = ?", (claim_id,))
            by_id = {ci["id"]: ci for ci in claim_items}
            if items is None:
                todo = [{"claim_item_id": ci["id"],
                         "quantity": ci["quantity"] - ci["returned_qty"]}
                        for ci in claim_items
                        if ci["quantity"] - ci["returned_qty"] > 0]
            else:
                todo = [{"claim_item_id": int(it["claim_item_id"]),
                         "quantity": int(it["quantity"])} for it in items]
            if not todo:
                raise ValidationError("无可退回数量")

            returned_items: list[dict] = []
            total = 0
            for it in todo:
                ci = by_id.get(it["claim_item_id"])
                if ci is None:
                    raise ValidationError(
                        f"明细 {it['claim_item_id']} 不属于领用单 {claim_id}")
                self._require_positive(it["quantity"], "退回数量")
                remain = ci["quantity"] - ci["returned_qty"]
                if it["quantity"] > remain:
                    raise ValidationError(
                        f"退回数量 {it['quantity']} 超过可退数量 {remain}")
                self._conn.execute(
                    "UPDATE claim_items SET returned_qty = returned_qty + ?"
                    " WHERE id = ?", (it["quantity"], ci["id"]))
                self._conn.execute(
                    "UPDATE stock SET quantity = quantity + ?,"
                    " version = version + 1 WHERE id = ?",
                    (it["quantity"], ci["stock_id"]))
                self._ledger(ci["stock_id"], it["quantity"], "return", claim_id,
                             serial_no=serial_no, actor=actor,
                             approver=approver, reason=reason)
                returned_items.append({"claim_item_id": ci["id"],
                                       "batch_no": ci["batch_no"],
                                       "quantity": it["quantity"]})
                total += it["quantity"]

            self._conn.execute(
                "UPDATE entitlements SET used = MAX(0, used - ?) WHERE id = ?",
                (total, claim["entitlement_id"]))
            sums = self._one(
                "SELECT COALESCE(SUM(quantity), 0) AS q,"
                " COALESCE(SUM(returned_qty), 0) AS r"
                " FROM claim_items WHERE claim_id = ?", (claim_id,))
            status = ("returned" if sums["r"] >= sums["q"]
                      else "partially_returned" if sums["r"] > 0 else "completed")
            self._conn.execute("UPDATE claims SET status = ? WHERE id = ?",
                               (status, claim_id))
            self._audit("return", "claim", claim_id, actor=actor,
                        approver=approver, reason=reason,
                        details={"items": returned_items})
            return {"claim_id": claim_id, "returned_quantity": total,
                    "claim_status": status, "items": returned_items}

        return self._run_idempotent(serial_no, "return_goods", payload, work)

    # ------------------------------------------------------------ 报损
    def report_damage(self, *, serial_no: str, material_code: str, batch_no: str,
                      location_code: str, quantity: int, approver: str,
                      reason: str, actor: str) -> dict:
        """报损(留审批人与原因): 库存不足时拒绝."""
        self._require_approval(approver, reason)
        self._require_text(actor, "操作员")
        self._require_positive(quantity)
        payload = dict(material_code=material_code, batch_no=batch_no,
                       location_code=location_code, quantity=quantity,
                       approver=approver, reason=reason, actor=actor)

        def work() -> dict:
            stock = self._find_stock(material_code, batch_no, location_code)
            if stock["quantity"] < quantity:
                raise InsufficientStockError(
                    f"报损数量超过现存库存: 需 {quantity},"
                    f" 现存 {stock['quantity']}",
                    requested=quantity, available=stock["quantity"])
            self._conn.execute(
                "UPDATE stock SET quantity = quantity - ?, version = version + 1"
                " WHERE id = ?", (quantity, stock["id"]))
            self._ledger(stock["id"], -quantity, "damage", None,
                         serial_no=serial_no, actor=actor,
                         approver=approver, reason=reason)
            self._audit("damage", "stock", stock["id"], actor=actor,
                        approver=approver, reason=reason, details=payload)
            new_qty = self._one("SELECT quantity FROM stock WHERE id = ?",
                                (stock["id"],))["quantity"]
            return {"stock_id": stock["id"], "batch_no": batch_no,
                    "location_code": location_code, "damaged": quantity,
                    "new_quantity": new_qty}

        return self._run_idempotent(serial_no, "report_damage", payload, work)

    # ------------------------------------------------------------ 跨点调拨
    def create_transfer(self, *, serial_no: str, material_code: str,
                        batch_no: str, from_location: str, to_location: str,
                        quantity: int, approver: str, reason: str,
                        actor: str) -> dict:
        """发起调拨(留审批人与原因): 源点立即扣减, 在途至完成为止."""
        self._require_approval(approver, reason)
        self._require_text(actor, "操作员")
        self._require_positive(quantity)
        if from_location == to_location:
            raise ValidationError("调出与调入存放点不能相同")
        payload = dict(material_code=material_code, batch_no=batch_no,
                       from_location=from_location, to_location=to_location,
                       quantity=quantity, approver=approver, reason=reason,
                       actor=actor)

        def work() -> dict:
            stock = self._find_stock(material_code, batch_no, from_location)
            to_lid = self._location_id(to_location)
            if stock["quantity"] < quantity:
                raise InsufficientStockError(
                    f"调出数量超过现存库存: 需 {quantity},"
                    f" 现存 {stock['quantity']}",
                    requested=quantity, available=stock["quantity"])
            self._conn.execute(
                "UPDATE stock SET quantity = quantity - ?, version = version + 1"
                " WHERE id = ?", (quantity, stock["id"]))
            transfer_id = self._conn.execute(
                "INSERT INTO transfers (serial_no, batch_id, from_location_id,"
                " to_location_id, quantity, approver, reason, created_by,"
                " created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (serial_no, stock["batch_id"], stock["location_id"], to_lid,
                 quantity, approver, reason, actor, self._now_iso())).lastrowid
            self._ledger(stock["id"], -quantity, "transfer_out", transfer_id,
                         serial_no=serial_no, actor=actor,
                         approver=approver, reason=reason)
            self._audit("transfer_create", "transfer", transfer_id, actor=actor,
                        approver=approver, reason=reason, details=payload)
            return {"transfer_id": transfer_id, "status": "pending",
                    "batch_no": batch_no, "from_location": from_location,
                    "to_location": to_location, "quantity": quantity}

        return self._run_idempotent(serial_no, "create_transfer", payload, work)

    def _transfer_row(self, transfer_id: int) -> sqlite3.Row:
        row = self._one("SELECT * FROM transfers WHERE id = ?", (transfer_id,))
        if row is None:
            raise NotFoundError(f"调拨单不存在: {transfer_id}")
        return row

    def complete_transfer(self, transfer_id: int, *, actor: str) -> dict:
        """完成调拨: 调入点上账; 重复完成返回首次结果(天然幂等)."""
        self._require_text(actor, "操作员")
        with self._txn():
            t = self._transfer_row(transfer_id)
            if t["status"] == "completed":
                return {"transfer_id": transfer_id, "status": "completed",
                        "idempotent_replay": True}
            if t["status"] != "pending":
                raise ConflictError("调拨单已取消, 无法完成")
            stock_id = self._ensure_stock_id(t["batch_id"], t["to_location_id"])
            self._conn.execute(
                "UPDATE stock SET quantity = quantity + ?, version = version + 1"
                " WHERE id = ?", (t["quantity"], stock_id))
            self._ledger(stock_id, t["quantity"], "transfer_in", transfer_id,
                         serial_no=t["serial_no"], actor=actor)
            self._conn.execute(
                "UPDATE transfers SET status = 'completed', completed_by = ?,"
                " completed_at = ? WHERE id = ?",
                (actor, self._now_iso(), transfer_id))
            self._audit("transfer_complete", "transfer", transfer_id, actor=actor)
            return {"transfer_id": transfer_id, "status": "completed",
                    "idempotent_replay": False}

    def cancel_transfer(self, transfer_id: int, *, approver: str, reason: str,
                        actor: str) -> dict:
        """取消调拨(留审批人与原因): 在途数量退回源点."""
        self._require_approval(approver, reason)
        self._require_text(actor, "操作员")
        with self._txn():
            t = self._transfer_row(transfer_id)
            if t["status"] == "cancelled":
                return {"transfer_id": transfer_id, "status": "cancelled",
                        "idempotent_replay": True}
            if t["status"] != "pending":
                raise ConflictError("调拨单已完成, 无法取消")
            stock_id = self._ensure_stock_id(t["batch_id"], t["from_location_id"])
            self._conn.execute(
                "UPDATE stock SET quantity = quantity + ?, version = version + 1"
                " WHERE id = ?", (t["quantity"], stock_id))
            self._ledger(stock_id, t["quantity"], "transfer_cancel", transfer_id,
                         serial_no=t["serial_no"], actor=actor,
                         approver=approver, reason=reason)
            self._conn.execute(
                "UPDATE transfers SET status = 'cancelled', completed_by = ?,"
                " completed_at = ? WHERE id = ?",
                (actor, self._now_iso(), transfer_id))
            self._audit("transfer_cancel", "transfer", transfer_id, actor=actor,
                        approver=approver, reason=reason)
            return {"transfer_id": transfer_id, "status": "cancelled",
                    "idempotent_replay": False}

    # ------------------------------------------------------------ 查询(按角色)
    @staticmethod
    def _require_manager(viewer: Viewer) -> None:
        if viewer.role != Role.MANAGER:
            raise PermissionDeniedError("该信息仅管理者可见")

    def _keeper_location_id(self, viewer: Viewer) -> Optional[int]:
        """仓管员返回其存放点 id; 管理者返回 None(不限制)."""
        if viewer.role == Role.KEEPER:
            if not viewer.location_code:
                raise PermissionDeniedError("仓管员查询须携带所属存放点")
            return self._location_id(viewer.location_code)
        return None

    def batch_inventory(self, viewer: Viewer, *,
                        material_code: Optional[str] = None,
                        location_code: Optional[str] = None) -> list[dict]:
        """批次库存: 仓管员仅见本存放点, 管理者可全量或按点过滤."""
        keeper_lid = self._keeper_location_id(viewer)
        sql = ("SELECT m.code AS material_code, m.name AS material_name,"
               " m.unit, b.batch_no, b.expiry_date, l.code AS location_code,"
               " s.quantity, (b.expiry_date < ?) AS expired"
               " FROM stock s"
               " JOIN batches b ON b.id = s.batch_id"
               " JOIN materials m ON m.id = b.material_id"
               " JOIN locations l ON l.id = s.location_id"
               " WHERE s.quantity > 0")
        params: list[Any] = [self._today()]
        if keeper_lid is not None:
            sql += " AND s.location_id = ?"
            params.append(keeper_lid)
        elif location_code is not None:
            sql += " AND s.location_id = ?"
            params.append(self._location_id(location_code))
        if material_code is not None:
            sql += " AND m.code = ?"
            params.append(material_code)
        sql += " ORDER BY m.code, l.code, b.expiry_date"
        return [dict(r) for r in self._query(sql, tuple(params))]

    def fefo_suggestion(self, viewer: Viewer, *, material_code: str,
                        location_code: str, quantity: int) -> list[dict]:
        """先到期先发建议: 仓管员仅可查询本存放点."""
        if viewer.role == Role.KEEPER and viewer.location_code != location_code:
            raise PermissionDeniedError("仓管员仅可查询本存放点")
        return self.plan_allocation(material_code=material_code,
                                    location_code=location_code,
                                    quantity=quantity)

    def entitlement_status(self, viewer: Viewer, *, operation_code: str,
                           family_code: str) -> list[dict]:
        """资格核查: 发放点核实家庭可领数量(仓管员与管理者均可用)."""
        self._operation(operation_code)
        fam = self._family(family_code)
        today = self._today()
        rows = self._query(
            "SELECT e.*, o.status AS op_status, m.code AS material_code,"
            " m.name AS material_name, m.unit"
            " FROM entitlements e"
            " JOIN operations o ON o.id = e.operation_id"
            " JOIN materials m ON m.id = e.material_id"
            " WHERE e.operation_id = ? AND e.family_id = ?",
            (self._operation(operation_code)["id"], fam["id"]))
        result = []
        for r in rows:
            valid = (r["status"] == "active" and r["op_status"] == "active"
                     and (not r["valid_from"] or r["valid_from"] <= today)
                     and (not r["valid_until"] or r["valid_until"] >= today))
            result.append({"material_code": r["material_code"],
                           "material_name": r["material_name"],
                           "unit": r["unit"], "quota": r["quota"],
                           "used": r["used"],
                           "remaining": r["quota"] - r["used"],
                           "status": r["status"],
                           "valid_from": r["valid_from"],
                           "valid_until": r["valid_until"],
                           "currently_valid": valid})
        return result

    def family_claim_history(self, viewer: Viewer, *,
                             family_code: str) -> list[dict]:
        """家庭领取历史(仅管理者): 用于核对是否重复领取."""
        self._require_manager(viewer)
        fam = self._family(family_code)
        claims = self._query(
            "SELECT c.id, c.serial_no, c.quantity, c.status, c.actor,"
            " c.created_at, o.code AS operation_code, m.code AS material_code,"
            " l.code AS location_code"
            " FROM claims c"
            " JOIN entitlements e ON e.id = c.entitlement_id"
            " JOIN operations o ON o.id = e.operation_id"
            " JOIN materials m ON m.id = e.material_id"
            " JOIN locations l ON l.id = c.location_id"
            " WHERE e.family_id = ? ORDER BY c.id", (fam["id"],))
        result = []
        for c in claims:
            items = self._query(
                "SELECT b.batch_no, b.expiry_date, ci.quantity, ci.returned_qty"
                " FROM claim_items ci"
                " JOIN stock s ON s.id = ci.stock_id"
                " JOIN batches b ON b.id = s.batch_id"
                " WHERE ci.claim_id = ?", (c["id"],))
            result.append({**{k: c[k] for k in c.keys()}, "items": [dict(i) for i in items]})
        return result

    def pending_transfers(self, viewer: Viewer) -> list[dict]:
        """未完成调拨: 管理者看全部, 仓管员仅看与本点相关的."""
        keeper_lid = self._keeper_location_id(viewer)
        sql = ("SELECT t.id AS transfer_id, t.serial_no, t.quantity, t.status,"
               " t.created_at, t.created_by, t.approver, t.reason,"
               " m.code AS material_code, b.batch_no,"
               " lf.code AS from_location, lt.code AS to_location"
               " FROM transfers t"
               " JOIN batches b ON b.id = t.batch_id"
               " JOIN materials m ON m.id = b.material_id"
               " JOIN locations lf ON lf.id = t.from_location_id"
               " JOIN locations lt ON lt.id = t.to_location_id"
               " WHERE t.status = 'pending'")
        params: list[Any] = []
        if keeper_lid is not None:
            sql += " AND (t.from_location_id = ? OR t.to_location_id = ?)"
            params += [keeper_lid, keeper_lid]
        sql += " ORDER BY t.id"
        now = self._now()
        result = []
        for r in self._query(sql, tuple(params)):
            d = dict(r)
            created = datetime.fromisoformat(d["created_at"])
            d["age_hours"] = round((now - created).total_seconds() / 3600, 2)
            result.append(d)
        return result

    def audit_log(self, viewer: Viewer, *, limit: int = 200,
                  action: Optional[str] = None) -> list[dict]:
        """审计日志(仅管理者)."""
        self._require_manager(viewer)
        sql = "SELECT * FROM audits"
        params: list[Any] = []
        if action is not None:
            sql += " WHERE action = ?"
            params.append(action)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        result = []
        for r in self._query(sql, tuple(params)):
            d = dict(r)
            if d.get("details"):
                d["details"] = json.loads(d["details"])
            result.append(d)
        return result

    def anomaly_report(self, viewer: Viewer, *,
                       stale_transfer_hours: float = 24) -> dict:
        """异常盘点(仅管理者): 账实差异 / 过期在库 / 超发 / 重复领取 / 在途滞留."""
        self._require_manager(viewer)
        today = self._today()

        discrepancies = [dict(r) for r in self._query(
            "SELECT s.id AS stock_id, m.code AS material_code, b.batch_no,"
            " l.code AS location_code, s.quantity AS recorded,"
            " COALESCE(SUM(g.change), 0) AS ledger_sum"
            " FROM stock s"
            " JOIN batches b ON b.id = s.batch_id"
            " JOIN materials m ON m.id = b.material_id"
            " JOIN locations l ON l.id = s.location_id"
            " LEFT JOIN stock_ledger g ON g.stock_id = s.id"
            " GROUP BY s.id HAVING recorded != ledger_sum")]

        expired = [dict(r) for r in self._query(
            "SELECT m.code AS material_code, b.batch_no, b.expiry_date,"
            " l.code AS location_code, s.quantity"
            " FROM stock s"
            " JOIN batches b ON b.id = s.batch_id"
            " JOIN materials m ON m.id = b.material_id"
            " JOIN locations l ON l.id = s.location_id"
            " WHERE s.quantity > 0 AND b.expiry_date < ?", (today,))]

        overdrawn = [dict(r) for r in self._query(
            "SELECT o.code AS operation_code, f.family_code,"
            " m.code AS material_code, e.quota, e.used"
            " FROM entitlements e"
            " JOIN operations o ON o.id = e.operation_id"
            " JOIN families f ON f.id = e.family_id"
            " JOIN materials m ON m.id = e.material_id"
            " WHERE e.used > e.quota")]

        repeated = [dict(r) for r in self._query(
            "SELECT o.code AS operation_code, f.family_code,"
            " m.code AS material_code, COUNT(c.id) AS claim_count,"
            " SUM(c.quantity) AS total_quantity"
            " FROM claims c"
            " JOIN entitlements e ON e.id = c.entitlement_id"
            " JOIN operations o ON o.id = e.operation_id"
            " JOIN families f ON f.id = e.family_id"
            " JOIN materials m ON m.id = e.material_id"
            " WHERE c.status != 'returned'"
            " GROUP BY o.code, f.family_code, m.code"
            " HAVING COUNT(c.id) > 1 ORDER BY total_quantity DESC")]

        pending = self.pending_transfers(Viewer(Role.MANAGER))
        for p in pending:
            p["stale"] = p["age_hours"] >= stale_transfer_hours

        return {"generated_at": self._now_iso(),
                "stock_discrepancies": discrepancies,
                "expired_stock": expired,
                "overdrawn_entitlements": overdrawn,
                "repeated_family_claims": repeated,
                "pending_transfers": pending}


# 兼容既有入口命名
Service = ReliefService
