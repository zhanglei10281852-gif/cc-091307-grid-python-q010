"""社区应急物资领用服务。

核心能力：
- 物资批次 / 有效期 / 存放点 / 领取资格 / 发放额度的维护；
- 发放时按行动批次 + 家庭标识校验可领数量，按先到期先发（FEFO）出库；
- 额度调整、退回、报损、跨点调拨强制留审批人与原因（审计日志）；
- 库存不足或资格失效拒绝操作；
- 现场终端断网补传按流水号幂等；
- 并发抢最后一批物资时返回冲突而非超发（批次行乐观锁）；
- 查询按角色（manager / keeper）限定范围；
- SQLite 持久化，重启后未完成调拨与审计记录继续存在。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Callable, Optional

from .db import Database
from .errors import (
    ConflictError,
    EligibilityError,
    IdempotencyConflictError,
    InsufficientQuotaError,
    InsufficientStockError,
    NotFoundError,
    PermissionDeniedError,
    StateError,
    ValidationError,
)
from .models import ROLE_KEEPER, ROLE_MANAGER, Actor

# 需要审批人 + 原因的操作
_ACTIONS_REQUIRING_APPROVAL = {
    "QUOTA_ADJUST",
    "RETURN",
    "DAMAGE",
    "TRANSFER_INIT",
    "TRANSFER_CANCEL",
    "STOCKTAKE",
    "HOUSEHOLD_STATUS",
}


class ReliefService:
    """物资领用领域服务。所有写操作都要求调用方提供流水号 serial_no。"""

    def __init__(self, db_path: str = "relief.db", clock: Optional[Callable[[], datetime]] = None):
        self.db = Database(db_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def _now(self) -> str:
        return self._clock().isoformat(timespec="seconds")

    def _today(self) -> str:
        return self._clock().date().isoformat()

    @staticmethod
    def _require_field_actor(actor: Actor) -> None:
        if actor.role not in (ROLE_MANAGER, ROLE_KEEPER):
            raise PermissionDeniedError(f"角色 {actor.role} 无权执行现场操作")

    @staticmethod
    def _require_manager(actor: Actor) -> None:
        if actor.role != ROLE_MANAGER:
            raise PermissionDeniedError("仅管理者可执行该操作/查询")

    @staticmethod
    def _check_site_scope(actor: Actor, site_id: int) -> None:
        """仓管员只能操作/查看本存放点。"""
        if actor.role == ROLE_KEEPER and actor.site_id != site_id:
            raise PermissionDeniedError(
                f"仓管员 {actor.name} 只能操作存放点 {actor.site_id}，不能操作 {site_id}"
            )

    @staticmethod
    def _require_approval(approver: Optional[str], reason: Optional[str]) -> None:
        if not approver or not approver.strip():
            raise ValidationError("该操作必须填写审批人")
        if not reason or not reason.strip():
            raise ValidationError("该操作必须填写原因")

    @staticmethod
    def _require_positive(quantity: int, what: str = "数量") -> None:
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValidationError(f"{what}必须为正整数，收到 {quantity!r}")

    def _audit(self, conn, *, actor: str, action: str, entity: str = "",
               detail: Optional[dict] = None, approver: Optional[str] = None,
               reason: Optional[str] = None) -> None:
        conn.execute(
            "INSERT INTO audit_log (ts, actor, action, entity, detail, approver, reason)"
            " VALUES (?,?,?,?,?,?,?)",
            (self._now(), actor, action, entity,
             json.dumps(detail or {}, ensure_ascii=False), approver, reason),
        )

    # ------------------------------------------------------------------
    # 幂等：断网补传按流水号去重
    # ------------------------------------------------------------------
    @staticmethod
    def _hash_payload(payload: dict) -> str:
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _idem_check(self, conn, serial_no: str, operation: str, request_hash: str) -> Optional[dict]:
        """命中流水号：负载一致返回首次结果（幂等重放），不一致报冲突。"""
        row = conn.execute(
            "SELECT * FROM idempotency_keys WHERE serial_no = ?", (serial_no,)
        ).fetchone()
        if row is None:
            return None
        if row["operation"] != operation or row["request_hash"] != request_hash:
            raise IdempotencyConflictError(
                f"流水号 {serial_no} 已被用于不同的请求（{row['operation']}），拒绝重复提交"
            )
        resp = json.loads(row["response_json"])
        resp["idempotent_replay"] = True
        return resp

    def _idem_lookup(self, serial_no: str, operation: str, payload: dict) -> Optional[dict]:
        with self.db.read() as conn:
            return self._idem_check(conn, serial_no, operation, self._hash_payload(payload))

    def _idem_run(self, serial_no: str, operation: str, payload: dict,
                  fn: Callable) -> dict:
        """在写事务内执行 fn 并登记流水号；fn 抛错则整体回滚，流水号不占用。"""
        request_hash = self._hash_payload(payload)
        with self.db.write() as conn:
            hit = self._idem_check(conn, serial_no, operation, request_hash)
            if hit is not None:  # 并发下另一连接已登记同一流水号
                return hit
            result = fn(conn)
            result["idempotent_replay"] = False
            conn.execute(
                "INSERT INTO idempotency_keys (serial_no, operation, request_hash, response_json, created_at)"
                " VALUES (?,?,?,?,?)",
                (serial_no, operation, request_hash,
                 json.dumps(result, ensure_ascii=False), self._now()),
            )
            return result

    @staticmethod
    def _valid_serial(serial_no) -> str:
        if not serial_no or not str(serial_no).strip():
            raise ValidationError("必须提供流水号 serial_no（断网补传幂等键）")
        return str(serial_no)

    def _run_idempotent(self, serial_no: str, operation: str, payload: dict,
                        fn: Callable) -> dict:
        serial_no = self._valid_serial(serial_no)
        hit = self._idem_lookup(serial_no, operation, payload)
        if hit is not None:
            return hit
        return self._idem_run(serial_no, operation, payload, fn)

    # ------------------------------------------------------------------
    # 基础档案：存放点 / 物资批次 / 行动批次 / 家庭 / 额度
    # ------------------------------------------------------------------
    def register_site(self, *, serial_no, actor: Actor, name: str) -> dict:
        self._require_manager(actor)
        payload = {"name": name}

        def fn(conn):
            cur = conn.execute(
                "INSERT INTO sites (name, created_at) VALUES (?,?)", (name, self._now()))
            self._audit(conn, actor=actor.name, action="SITE_REGISTER",
                        entity=f"site:{cur.lastrowid}", detail=payload)
            return {"site_id": cur.lastrowid, "name": name}

        return self._run_idempotent(serial_no, "SITE_REGISTER", payload, fn)

    def add_batch(self, *, serial_no, actor: Actor, item: str, site_id: int,
                  quantity: int, expiry_date: Optional[str] = None) -> dict:
        self._require_manager(actor)
        self._require_positive(quantity)
        payload = {"item": item, "site_id": site_id, "quantity": quantity,
                   "expiry_date": expiry_date}

        def fn(conn):
            self._get_site(conn, site_id)
            cur = conn.execute(
                "INSERT INTO batches (item, site_id, quantity, expiry_date, received_at)"
                " VALUES (?,?,?,?,?)",
                (item, site_id, quantity, expiry_date, self._now()))
            self._audit(conn, actor=actor.name, action="BATCH_ADD",
                        entity=f"batch:{cur.lastrowid}", detail=payload)
            return {"batch_id": cur.lastrowid, **payload}

        return self._run_idempotent(serial_no, "BATCH_ADD", payload, fn)

    def open_campaign(self, *, serial_no, actor: Actor, code: str, name: str = "") -> dict:
        self._require_manager(actor)
        payload = {"code": code, "name": name}

        def fn(conn):
            cur = conn.execute(
                "INSERT INTO campaigns (code, name, created_at) VALUES (?,?,?)",
                (code, name, self._now()))
            self._audit(conn, actor=actor.name, action="CAMPAIGN_OPEN",
                        entity=f"campaign:{cur.lastrowid}", detail=payload)
            return {"campaign_id": cur.lastrowid, "code": code, "status": "ACTIVE"}

        return self._run_idempotent(serial_no, "CAMPAIGN_OPEN", payload, fn)

    def close_campaign(self, *, serial_no, actor: Actor, campaign_id: int) -> dict:
        self._require_manager(actor)
        payload = {"campaign_id": campaign_id}

        def fn(conn):
            camp = self._get_campaign(conn, campaign_id)
            if camp["status"] != "ACTIVE":
                raise StateError(f"行动批次 {campaign_id} 已是 {camp['status']}")
            conn.execute("UPDATE campaigns SET status='CLOSED' WHERE id=?", (campaign_id,))
            self._audit(conn, actor=actor.name, action="CAMPAIGN_CLOSE",
                        entity=f"campaign:{campaign_id}")
            return {"campaign_id": campaign_id, "status": "CLOSED"}

        return self._run_idempotent(serial_no, "CAMPAIGN_CLOSE", payload, fn)

    def register_household(self, *, serial_no, actor: Actor, household_id: str,
                           head_name: str = "", members: int = 1,
                           eligible_until: Optional[str] = None) -> dict:
        self._require_manager(actor)
        payload = {"household_id": household_id, "head_name": head_name,
                   "members": members, "eligible_until": eligible_until}

        def fn(conn):
            conn.execute(
                "INSERT INTO households (id, head_name, members, eligible_until)"
                " VALUES (?,?,?,?)"
                " ON CONFLICT(id) DO UPDATE SET head_name=excluded.head_name,"
                " members=excluded.members, eligible_until=excluded.eligible_until",
                (household_id, head_name, members, eligible_until))
            self._audit(conn, actor=actor.name, action="HOUSEHOLD_REGISTER",
                        entity=f"household:{household_id}", detail=payload)
            return {"household_id": household_id, "status": "ACTIVE"}

        return self._run_idempotent(serial_no, "HOUSEHOLD_REGISTER", payload, fn)

    def set_household_status(self, *, serial_no, actor: Actor, household_id: str,
                             status: str, approver: str, reason: str) -> dict:
        """暂停/恢复/作废家庭领取资格，必须留审批人与原因。"""
        self._require_manager(actor)
        self._require_approval(approver, reason)
        if status not in ("ACTIVE", "SUSPENDED", "EXPIRED"):
            raise ValidationError(f"非法家庭状态 {status!r}")
        payload = {"household_id": household_id, "status": status}

        def fn(conn):
            self._get_household(conn, household_id)
            conn.execute("UPDATE households SET status=? WHERE id=?", (status, household_id))
            self._audit(conn, actor=actor.name, action="HOUSEHOLD_STATUS",
                        entity=f"household:{household_id}", detail=payload,
                        approver=approver, reason=reason)
            return {"household_id": household_id, "status": status}

        return self._run_idempotent(serial_no, "HOUSEHOLD_STATUS", payload, fn)

    def set_quota(self, *, serial_no, actor: Actor, campaign_id: int, household_id: str,
                  item: str, amount: int, approver: str, reason: str) -> dict:
        """设定/调整发放额度（剩余可领数量），必须留审批人与原因。"""
        self._require_manager(actor)
        self._require_approval(approver, reason)
        if not isinstance(amount, int) or amount < 0:
            raise ValidationError(f"额度必须为非负整数，收到 {amount!r}")
        payload = {"campaign_id": campaign_id, "household_id": household_id,
                   "item": item, "amount": amount}

        def fn(conn):
            self._get_campaign(conn, campaign_id)
            self._get_household(conn, household_id)
            row = conn.execute(
                "SELECT amount FROM quotas WHERE campaign_id=? AND household_id=? AND item=?",
                (campaign_id, household_id, item)).fetchone()
            old = row["amount"] if row else None
            conn.execute(
                "INSERT INTO quotas (campaign_id, household_id, item, amount) VALUES (?,?,?,?)"
                " ON CONFLICT(campaign_id, household_id, item)"
                " DO UPDATE SET amount=excluded.amount",
                (campaign_id, household_id, item, amount))
            self._audit(conn, actor=actor.name, action="QUOTA_ADJUST",
                        entity=f"quota:{campaign_id}:{household_id}:{item}",
                        detail={"old": old, "new": amount},
                        approver=approver, reason=reason)
            return {"campaign_id": campaign_id, "household_id": household_id,
                    "item": item, "old_amount": old, "amount": amount}

        return self._run_idempotent(serial_no, "QUOTA_ADJUST", payload, fn)

    # ------------------------------------------------------------------
    # 发放：资格校验 + FEFO 出库 + 乐观锁防超发
    # ------------------------------------------------------------------
    def suggest_outbound(self, *, actor: Actor, item: str, site_id: int,
                         quantity: int) -> dict:
        """先到期先发（FEFO）出库建议，不落库。库存不足时给出 shortfall。"""
        self._require_field_actor(actor)
        self._check_site_scope(actor, site_id)
        self._require_positive(quantity)
        return self._plan_allocation(item, site_id, quantity, strict=False)

    def issue(self, *, serial_no, actor: Actor, campaign_id: int, household_id: str,
              item: str, site_id: int, quantity: int) -> dict:
        """发放物资：校验行动批次与家庭资格/额度，按 FEFO 从批次出库。"""
        self._require_field_actor(actor)
        self._check_site_scope(actor, site_id)
        self._require_positive(quantity)
        serial_no = self._valid_serial(serial_no)
        payload = {"campaign_id": campaign_id, "household_id": household_id,
                   "item": item, "site_id": site_id, "quantity": quantity,
                   "actor": actor.name}
        # 先查幂等：补传时库存可能已空，不能让 FEFO 计划先抛库存不足
        hit = self._idem_lookup(serial_no, "ISSUE", payload)
        if hit is not None:
            return hit
        plan = self._plan_allocation(item, site_id, quantity, strict=True)

        def fn(conn):
            return self._commit_issue(
                conn, actor=actor, campaign_id=campaign_id,
                household_id=household_id, item=item, site_id=site_id,
                quantity=quantity, plan=plan, serial_no=serial_no)

        return self._idem_run(serial_no, "ISSUE", payload, fn)

    def _plan_rows(self, conn, item: str, site_id: int, quantity: int) -> dict:
        """FEFO：按有效期升序（无有效期排最后）从未过期批次中分配。"""
        rows = conn.execute(
            "SELECT * FROM batches"
            " WHERE item=? AND site_id=? AND status='ACTIVE' AND quantity>0"
            "   AND (expiry_date IS NULL OR expiry_date >= ?)"
            " ORDER BY (expiry_date IS NULL), expiry_date, id",
            (item, site_id, self._today())).fetchall()
        lines, remaining, available = [], quantity, 0
        for r in rows:
            available += r["quantity"]
            if remaining > 0:
                take = min(r["quantity"], remaining)
                lines.append({"batch_id": r["id"], "quantity": take,
                              "version": r["version"], "expiry_date": r["expiry_date"]})
                remaining -= take
        return {"item": item, "site_id": site_id, "requested": quantity,
                "lines": lines, "shortfall": remaining, "available": available}

    def _plan_allocation(self, item: str, site_id: int, quantity: int,
                         strict: bool) -> dict:
        with self.db.read() as conn:
            plan = self._plan_rows(conn, item, site_id, quantity)
        if strict and plan["shortfall"] > 0:
            raise InsufficientStockError(
                f"存放点 {site_id} 的 {item} 可用库存 {plan['available']}，"
                f"不足申请量 {quantity}")
        return plan

    def _apply_allocation(self, conn, item: str, site_id: int, quantity: int,
                          planned_lines: list) -> list:
        """在写事务内按 FEFO 计划扣减批次，返回实际出库明细。

        批次行用 version + quantity>=take 条件更新（乐观锁）：被其他仓管员
        抢先时，在写锁保护下按最新库存重新规划；重新规划仍不足（最后一件
        被抢走）才抛 ConflictError——既不超发，也避免库存充足时的误冲突。
        """
        applied, remaining = [], quantity
        queue = list(planned_lines)
        while remaining > 0:
            if not queue:
                fresh = self._plan_rows(conn, item, site_id, remaining)
                if fresh["shortfall"] > 0:
                    raise ConflictError(
                        f"{item} 库存被并发扣减后不足 {quantity}，"
                        "本次操作未生效，请重新提交")
                queue = fresh["lines"]
                continue
            line = queue.pop(0)
            take = min(line["quantity"], remaining)
            cur = conn.execute(
                "UPDATE batches SET quantity = quantity - ?, version = version + 1"
                " WHERE id=? AND version=? AND quantity >= ?",
                (take, line["batch_id"], line["version"], take))
            if cur.rowcount == 0:
                queue.clear()  # 计划已过期，丢弃并按最新库存重规划剩余量
                continue
            conn.execute(
                "UPDATE batches SET status='DEPLETED' WHERE id=? AND quantity=0",
                (line["batch_id"],))
            applied.append({"batch_id": line["batch_id"], "quantity": take,
                            "expiry_date": line["expiry_date"]})
            remaining -= take
        return applied

    def _commit_issue(self, conn, *, actor: Actor, campaign_id: int, household_id: str,
                      item: str, site_id: int, quantity: int, plan: dict,
                      serial_no: str) -> dict:
        camp = self._get_campaign(conn, campaign_id)
        if camp["status"] != "ACTIVE":
            raise EligibilityError(f"行动批次 {camp['code']} 已结束，停止发放")
        hh = self._get_household(conn, household_id)
        if hh["status"] != "ACTIVE":
            raise EligibilityError(f"家庭 {household_id} 领取资格已失效（{hh['status']}）")
        if hh["eligible_until"] and hh["eligible_until"] < self._today():
            raise EligibilityError(f"家庭 {household_id} 领取资格已于 {hh['eligible_until']} 过期")

        # 额度原子扣减：amount >= quantity 条件不满足则拒绝
        cur = conn.execute(
            "UPDATE quotas SET amount = amount - ?"
            " WHERE campaign_id=? AND household_id=? AND item=? AND amount >= ?",
            (quantity, campaign_id, household_id, item, quantity))
        if cur.rowcount == 0:
            row = conn.execute(
                "SELECT amount FROM quotas WHERE campaign_id=? AND household_id=? AND item=?",
                (campaign_id, household_id, item)).fetchone()
            if row is None:
                raise EligibilityError(
                    f"家庭 {household_id} 在行动批次 {camp['code']} 下无 {item} 领取额度")
            raise InsufficientQuotaError(
                f"家庭 {household_id} 的 {item} 剩余额度 {row['amount']}，不足申请量 {quantity}")

        # 批次出库：乐观锁 + 写锁内重规划，并发抢最后一件时报冲突
        applied = self._apply_allocation(conn, item, site_id, quantity, plan["lines"])

        cur = conn.execute(
            "INSERT INTO distributions"
            " (serial_no, campaign_id, household_id, item, site_id, quantity, actor, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (serial_no, campaign_id, household_id, item, site_id, quantity,
             actor.name, self._now()))
        dist_id = cur.lastrowid
        for line in applied:
            conn.execute(
                "INSERT INTO distribution_lines (distribution_id, batch_id, quantity)"
                " VALUES (?,?,?)", (dist_id, line["batch_id"], line["quantity"]))
        self._audit(conn, actor=actor.name, action="ISSUE",
                    entity=f"distribution:{dist_id}",
                    detail={"campaign_id": campaign_id, "household_id": household_id,
                            "item": item, "site_id": site_id, "quantity": quantity,
                            "serial_no": serial_no})
        return {"distribution_id": dist_id, "serial_no": serial_no, "status": "ISSUED",
                "campaign_id": campaign_id, "household_id": household_id,
                "item": item, "site_id": site_id, "quantity": quantity,
                "lines": applied}

    # ------------------------------------------------------------------
    # 退回 / 报损（均需审批人 + 原因）
    # ------------------------------------------------------------------
    def return_items(self, *, serial_no, actor: Actor, distribution_id: int,
                     quantity: int, approver: str, reason: str) -> dict:
        """受助家庭退回物资：按原出库批次回补库存。"""
        self._require_field_actor(actor)
        self._require_approval(approver, reason)
        self._require_positive(quantity)
        payload = {"distribution_id": distribution_id, "quantity": quantity}

        def fn(conn):
            dist = conn.execute(
                "SELECT * FROM distributions WHERE id=?", (distribution_id,)).fetchone()
            if dist is None:
                raise NotFoundError(f"发放单 {distribution_id} 不存在")
            self._check_site_scope(actor, dist["site_id"])
            outstanding = dist["quantity"] - dist["returned_qty"]
            if quantity > outstanding:
                raise ValidationError(
                    f"退回 {quantity} 超过未退回数量 {outstanding}（发放单 {distribution_id}）")
            remaining = quantity
            lines = conn.execute(
                "SELECT * FROM distribution_lines WHERE distribution_id=? ORDER BY id",
                (distribution_id,)).fetchall()
            for line in lines:
                if remaining == 0:
                    break
                take = min(line["quantity"] - line["returned"], remaining)
                if take <= 0:
                    continue
                conn.execute(
                    "UPDATE distribution_lines SET returned = returned + ? WHERE id=?",
                    (take, line["id"]))
                conn.execute(
                    "UPDATE batches SET quantity = quantity + ?, version = version + 1,"
                    " status='ACTIVE' WHERE id=?", (take, line["batch_id"]))
                remaining -= take
            new_returned = dist["returned_qty"] + quantity
            status = "RETURNED" if new_returned == dist["quantity"] else "PARTIALLY_RETURNED"
            conn.execute(
                "UPDATE distributions SET returned_qty=?, status=? WHERE id=?",
                (new_returned, status, distribution_id))
            self._audit(conn, actor=actor.name, action="RETURN",
                        entity=f"distribution:{distribution_id}",
                        detail={"quantity": quantity, "household_id": dist["household_id"],
                                "item": dist["item"]},
                        approver=approver, reason=reason)
            return {"distribution_id": distribution_id, "returned": quantity,
                    "returned_total": new_returned, "status": status}

        return self._run_idempotent(serial_no, "RETURN", payload, fn)

    def report_damage(self, *, serial_no, actor: Actor, batch_id: int,
                      quantity: int, approver: str, reason: str) -> dict:
        """报损：从批次库存中核销，留审批人与原因。"""
        self._require_field_actor(actor)
        self._require_approval(approver, reason)
        self._require_positive(quantity)
        payload = {"batch_id": batch_id, "quantity": quantity}

        def fn(conn):
            batch = self._get_batch(conn, batch_id)
            self._check_site_scope(actor, batch["site_id"])
            cur = conn.execute(
                "UPDATE batches SET quantity = quantity - ?, version = version + 1"
                " WHERE id=? AND quantity >= ?", (quantity, batch_id, quantity))
            if cur.rowcount == 0:
                raise InsufficientStockError(
                    f"批次 {batch_id} 现存 {batch['quantity']}，不足报损量 {quantity}")
            conn.execute(
                "UPDATE batches SET status='DEPLETED' WHERE id=? AND quantity=0", (batch_id,))
            self._audit(conn, actor=actor.name, action="DAMAGE",
                        entity=f"batch:{batch_id}",
                        detail={"item": batch["item"], "site_id": batch["site_id"],
                                "quantity": quantity},
                        approver=approver, reason=reason)
            return {"batch_id": batch_id, "damaged": quantity,
                    "remaining": batch["quantity"] - quantity}

        return self._run_idempotent(serial_no, "DAMAGE", payload, fn)

    # ------------------------------------------------------------------
    # 跨点调拨：发起（源点出库，PENDING）→ 接收（入目的点）/ 取消（退回源点）
    # ------------------------------------------------------------------
    def initiate_transfer(self, *, serial_no, actor: Actor, from_site_id: int,
                          to_site_id: int, item: str, quantity: int,
                          approver: str, reason: str) -> dict:
        self._require_field_actor(actor)
        self._check_site_scope(actor, from_site_id)
        self._require_approval(approver, reason)
        self._require_positive(quantity)
        if from_site_id == to_site_id:
            raise ValidationError("调拨起点与终点不能相同")
        serial_no = self._valid_serial(serial_no)
        payload = {"from_site_id": from_site_id, "to_site_id": to_site_id,
                   "item": item, "quantity": quantity}
        hit = self._idem_lookup(serial_no, "TRANSFER_INIT", payload)
        if hit is not None:
            return hit
        plan = self._plan_allocation(item, from_site_id, quantity, strict=True)

        def fn(conn):
            self._get_site(conn, to_site_id)
            applied = self._apply_allocation(conn, item, from_site_id,
                                             quantity, plan["lines"])
            cur = conn.execute(
                "INSERT INTO transfers"
                " (serial_no, item, from_site_id, to_site_id, quantity, approver, reason,"
                "  created_by, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (serial_no, item, from_site_id, to_site_id, quantity,
                 approver, reason, actor.name, self._now()))
            transfer_id = cur.lastrowid
            for line in applied:
                conn.execute(
                    "INSERT INTO transfer_lines (transfer_id, batch_id, expiry_date, quantity)"
                    " VALUES (?,?,?,?)",
                    (transfer_id, line["batch_id"], line["expiry_date"], line["quantity"]))
            self._audit(conn, actor=actor.name, action="TRANSFER_INIT",
                        entity=f"transfer:{transfer_id}", detail=payload,
                        approver=approver, reason=reason)
            return {"transfer_id": transfer_id, "status": "PENDING", **payload,
                    "lines": [{"batch_id": l["batch_id"], "quantity": l["quantity"]}
                              for l in applied]}

        return self._idem_run(str(serial_no), "TRANSFER_INIT", payload, fn)

    def complete_transfer(self, *, serial_no, actor: Actor, transfer_id: int) -> dict:
        """目的存放点接收在途调拨，按原有效期生成新批次。"""
        self._require_field_actor(actor)
        payload = {"transfer_id": transfer_id}

        def fn(conn):
            t = self._get_transfer(conn, transfer_id)
            if t["status"] != "PENDING":
                raise StateError(f"调拨单 {transfer_id} 状态为 {t['status']}，不能接收")
            self._check_site_scope(actor, t["to_site_id"])
            new_batches = []
            for line in conn.execute(
                    "SELECT * FROM transfer_lines WHERE transfer_id=?", (transfer_id,)):
                cur = conn.execute(
                    "INSERT INTO batches (item, site_id, quantity, expiry_date, received_at)"
                    " VALUES (?,?,?,?,?)",
                    (t["item"], t["to_site_id"], line["quantity"],
                     line["expiry_date"], self._now()))
                new_batches.append({"batch_id": cur.lastrowid, "quantity": line["quantity"]})
            conn.execute(
                "UPDATE transfers SET status='COMPLETED', completed_at=? WHERE id=?",
                (self._now(), transfer_id))
            self._audit(conn, actor=actor.name, action="TRANSFER_COMPLETE",
                        entity=f"transfer:{transfer_id}",
                        detail={"to_site_id": t["to_site_id"], "new_batches": new_batches},
                        approver=t["approver"], reason=t["reason"])
            return {"transfer_id": transfer_id, "status": "COMPLETED",
                    "new_batches": new_batches}

        return self._run_idempotent(serial_no, "TRANSFER_COMPLETE", payload, fn)

    def cancel_transfer(self, *, serial_no, actor: Actor, transfer_id: int,
                        approver: str, reason: str) -> dict:
        """取消在途调拨，物资退回源存放点原批次。"""
        self._require_field_actor(actor)
        self._require_approval(approver, reason)
        payload = {"transfer_id": transfer_id}

        def fn(conn):
            t = self._get_transfer(conn, transfer_id)
            if t["status"] != "PENDING":
                raise StateError(f"调拨单 {transfer_id} 状态为 {t['status']}，不能取消")
            self._check_site_scope(actor, t["from_site_id"])
            for line in conn.execute(
                    "SELECT * FROM transfer_lines WHERE transfer_id=?", (transfer_id,)):
                conn.execute(
                    "UPDATE batches SET quantity = quantity + ?, version = version + 1,"
                    " status='ACTIVE' WHERE id=?", (line["quantity"], line["batch_id"]))
            conn.execute(
                "UPDATE transfers SET status='CANCELLED', completed_at=? WHERE id=?",
                (self._now(), transfer_id))
            self._audit(conn, actor=actor.name, action="TRANSFER_CANCEL",
                        entity=f"transfer:{transfer_id}",
                        detail={"from_site_id": t["from_site_id"]},
                        approver=approver, reason=reason)
            return {"transfer_id": transfer_id, "status": "CANCELLED"}

        return self._run_idempotent(serial_no, "TRANSFER_CANCEL", payload, fn)

    # ------------------------------------------------------------------
    # 盘点：账实差异留痕，可选按盘点数调整账面
    # ------------------------------------------------------------------
    def record_stocktake(self, *, serial_no, actor: Actor, batch_id: int, counted: int,
                         approver: str, reason: str, apply_adjustment: bool = False) -> dict:
        self._require_field_actor(actor)
        self._require_approval(approver, reason)
        if not isinstance(counted, int) or counted < 0:
            raise ValidationError(f"盘点数必须为非负整数，收到 {counted!r}")
        payload = {"batch_id": batch_id, "counted": counted,
                   "apply_adjustment": apply_adjustment}

        def fn(conn):
            batch = self._get_batch(conn, batch_id)
            self._check_site_scope(actor, batch["site_id"])
            variance = counted - batch["quantity"]
            cur = conn.execute(
                "INSERT INTO stocktakes"
                " (batch_id, counted, system_qty, variance, applied, actor, approver, reason, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (batch_id, counted, batch["quantity"], variance,
                 1 if apply_adjustment else 0, actor.name, approver, reason, self._now()))
            if apply_adjustment and variance != 0:
                conn.execute(
                    "UPDATE batches SET quantity=?, version=version+1 WHERE id=?",
                    (counted, batch_id))
                conn.execute(
                    "UPDATE batches SET status='DEPLETED' WHERE id=? AND quantity=0",
                    (batch_id,))
            self._audit(conn, actor=actor.name, action="STOCKTAKE",
                        entity=f"batch:{batch_id}",
                        detail={"counted": counted, "system_qty": batch["quantity"],
                                "variance": variance, "applied": bool(apply_adjustment)},
                        approver=approver, reason=reason)
            return {"stocktake_id": cur.lastrowid, "batch_id": batch_id,
                    "counted": counted, "system_qty": batch["quantity"],
                    "variance": variance, "applied": bool(apply_adjustment)}

        return self._run_idempotent(serial_no, "STOCKTAKE", payload, fn)

    # ------------------------------------------------------------------
    # 查询接口（按角色限定可见范围）
    # ------------------------------------------------------------------
    def batch_inventory(self, *, actor: Actor, site_id: Optional[int] = None) -> list[dict]:
        """批次库存：管理者看全部/指定点；仓管员仅本存放点。"""
        self._require_field_actor(actor)
        if actor.role == ROLE_KEEPER:
            if site_id is not None and site_id != actor.site_id:
                raise PermissionDeniedError("仓管员只能查看本存放点库存")
            site_id = actor.site_id
        sql = ("SELECT b.*, s.name AS site_name FROM batches b"
               " JOIN sites s ON s.id = b.site_id")
        args: list = []
        if site_id is not None:
            sql += " WHERE b.site_id = ?"
            args.append(site_id)
        sql += " ORDER BY b.site_id, b.item, (b.expiry_date IS NULL), b.expiry_date, b.id"
        with self.db.read() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [self._batch_view(r) for r in rows]

    def quota_view(self, *, actor: Actor, campaign_id: int, household_id: str) -> list[dict]:
        """现场核查：某家庭在某行动批次下各物资剩余额度。"""
        self._require_field_actor(actor)
        with self.db.read() as conn:
            rows = conn.execute(
                "SELECT * FROM quotas WHERE campaign_id=? AND household_id=?",
                (campaign_id, household_id)).fetchall()
        return [{"campaign_id": r["campaign_id"], "household_id": r["household_id"],
                 "item": r["item"], "remaining": r["amount"]} for r in rows]

    def household_history(self, *, actor: Actor, household_id: str) -> dict:
        """家庭领取历史：仅管理者。"""
        self._require_manager(actor)
        with self.db.read() as conn:
            hh = self._get_household(conn, household_id)
            quotas = conn.execute(
                "SELECT q.*, c.code AS campaign_code FROM quotas q"
                " JOIN campaigns c ON c.id = q.campaign_id WHERE q.household_id=?",
                (household_id,)).fetchall()
            dists = conn.execute(
                "SELECT d.*, c.code AS campaign_code, s.name AS site_name"
                " FROM distributions d"
                " JOIN campaigns c ON c.id = d.campaign_id"
                " JOIN sites s ON s.id = d.site_id"
                " WHERE d.household_id=? ORDER BY d.id", (household_id,)).fetchall()
        return {
            "household": {"id": hh["id"], "head_name": hh["head_name"],
                          "members": hh["members"], "status": hh["status"],
                          "eligible_until": hh["eligible_until"]},
            "quotas": [{"campaign_code": q["campaign_code"], "item": q["item"],
                        "remaining": q["amount"]} for q in quotas],
            "distributions": [
                {"distribution_id": d["id"], "campaign_code": d["campaign_code"],
                 "item": d["item"], "site": d["site_name"], "quantity": d["quantity"],
                 "returned_qty": d["returned_qty"], "status": d["status"],
                 "serial_no": d["serial_no"], "actor": d["actor"],
                 "created_at": d["created_at"]} for d in dists],
        }

    def pending_transfers(self, *, actor: Actor) -> list[dict]:
        """未完成调拨：管理者看全部，仓管员看与本点相关的。"""
        self._require_field_actor(actor)
        sql = "SELECT * FROM transfers WHERE status='PENDING'"
        args: list = []
        if actor.role == ROLE_KEEPER:
            sql += " AND (from_site_id=? OR to_site_id=?)"
            args += [actor.site_id, actor.site_id]
        sql += " ORDER BY id"
        with self.db.read() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [self._transfer_view(r) for r in rows]

    def anomaly_report(self, *, actor: Actor) -> dict:
        """异常盘点：报损记录、盘点差异、已过期未发放库存。仅管理者。"""
        self._require_manager(actor)
        today = self._today()
        with self.db.read() as conn:
            damages = conn.execute(
                "SELECT * FROM audit_log WHERE action='DAMAGE' ORDER BY id").fetchall()
            variances = conn.execute(
                "SELECT st.*, b.item, b.site_id FROM stocktakes st"
                " JOIN batches b ON b.id = st.batch_id"
                " WHERE st.variance != 0 ORDER BY st.id").fetchall()
            expired = conn.execute(
                "SELECT * FROM batches WHERE quantity > 0"
                " AND expiry_date IS NOT NULL AND expiry_date < ?"
                " ORDER BY expiry_date", (today,)).fetchall()
        return {
            "damages": [{"ts": d["ts"], "actor": d["actor"], "entity": d["entity"],
                         "detail": json.loads(d["detail"]), "approver": d["approver"],
                         "reason": d["reason"]} for d in damages],
            "stocktake_variances": [
                {"stocktake_id": v["id"], "batch_id": v["batch_id"], "item": v["item"],
                 "site_id": v["site_id"], "counted": v["counted"],
                 "system_qty": v["system_qty"], "variance": v["variance"],
                 "applied": bool(v["applied"]), "actor": v["actor"],
                 "approver": v["approver"], "reason": v["reason"]} for v in variances],
            "expired_stock": [self._batch_view(b) for b in expired],
        }

    def audit_trail(self, *, actor: Actor, limit: int = 200) -> list[dict]:
        """审计日志：仅管理者。"""
        self._require_manager(actor)
        with self.db.read() as conn:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [{"id": r["id"], "ts": r["ts"], "actor": r["actor"],
                 "action": r["action"], "entity": r["entity"],
                 "detail": json.loads(r["detail"]), "approver": r["approver"],
                 "reason": r["reason"]} for r in rows]

    # ------------------------------------------------------------------
    # 内部：实体读取与视图
    # ------------------------------------------------------------------
    @staticmethod
    def _get_site(conn, site_id: int):
        row = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"存放点 {site_id} 不存在")
        return row

    @staticmethod
    def _get_campaign(conn, campaign_id: int):
        row = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"行动批次 {campaign_id} 不存在")
        return row

    @staticmethod
    def _get_household(conn, household_id: str):
        row = conn.execute("SELECT * FROM households WHERE id=?", (household_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"家庭 {household_id} 未登记")
        return row

    @staticmethod
    def _get_batch(conn, batch_id: int):
        row = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"批次 {batch_id} 不存在")
        return row

    @staticmethod
    def _get_transfer(conn, transfer_id: int):
        row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"调拨单 {transfer_id} 不存在")
        return row

    @staticmethod
    def _batch_view(r) -> dict:
        return {"batch_id": r["id"], "item": r["item"], "site_id": r["site_id"],
                "site_name": r["site_name"] if "site_name" in r.keys() else None,
                "quantity": r["quantity"], "expiry_date": r["expiry_date"],
                "status": r["status"], "version": r["version"]}

    @staticmethod
    def _transfer_view(r) -> dict:
        return {"transfer_id": r["id"], "item": r["item"],
                "from_site_id": r["from_site_id"], "to_site_id": r["to_site_id"],
                "quantity": r["quantity"], "status": r["status"],
                "approver": r["approver"], "reason": r["reason"],
                "created_by": r["created_by"], "created_at": r["created_at"],
                "completed_at": r["completed_at"]}
