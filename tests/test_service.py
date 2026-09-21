"""物资领用服务测试: 覆盖资格校验、FEFO、幂等、并发冲突、审批留痕、
角色查询、异常盘点与重启持久化."""
import threading
from datetime import datetime, timedelta, timezone

import pytest

from src.service import (
    ConflictError,
    EligibilityError,
    InsufficientStockError,
    PermissionDeniedError,
    ReliefService,
    Role,
    ValidationError,
    Viewer,
)

FIXED_NOW = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)
MANAGER = Viewer(Role.MANAGER)
KEEPER_A = Viewer(Role.KEEPER, "LOC-A")
KEEPER_B = Viewer(Role.KEEPER, "LOC-B")


def make_service(db_path=":memory:", clock=None, seed=True):
    svc = ReliefService(db_path, clock=clock or (lambda: FIXED_NOW))
    if seed:  # 重启场景下基础档案已存在于库中, 不再重复建档
        svc.add_location("LOC-A", "中心仓库")
        svc.add_location("LOC-B", "东区发放点")
        svc.add_material("WATER", "饮用水", "瓶")
        svc.add_material("LAMP", "应急照明灯", "盏")
        svc.add_operation("OP-1", "9月临时安置行动")
        svc.add_family("FAM-001", "张某", 4)
        svc.add_family("FAM-002", "李某", 3)
    return svc


@pytest.fixture
def svc():
    s = make_service()
    yield s
    s.close()


def receive_water(svc, batch_no, expiry, qty, location="LOC-A", serial=None):
    return svc.receive_stock(
        serial_no=serial or f"R-{batch_no}-{location}",
        material_code="WATER", batch_no=batch_no, expiry_date=expiry,
        location_code=location, quantity=qty, actor="keeper1")


def grant(svc, family, quota, material="WATER", serial=None, **kw):
    return svc.grant_entitlement(
        serial_no=serial or f"G-{family}-{material}", operation_code="OP-1",
        family_code=family, material_code=material, quota=quota,
        actor="admin", **kw)


def water_total(svc, location=None):
    inv = svc.batch_inventory(MANAGER, material_code="WATER",
                              location_code=location)
    return sum(row["quantity"] for row in inv)


# ---------------------------------------------------------------- FEFO 规划
def test_fefo_plan_earliest_expiry_first(svc):
    receive_water(svc, "B-LATE", "2026-12-01", 5)
    receive_water(svc, "B-EARLY", "2026-10-01", 5)
    receive_water(svc, "B-MID", "2026-11-01", 5)
    plan = svc.plan_allocation(material_code="WATER", location_code="LOC-A",
                               quantity=7)
    assert [(p["batch_no"], p["quantity"]) for p in plan] == [
        ("B-EARLY", 5), ("B-MID", 2)]


def test_plan_insufficient_stock_raises(svc):
    receive_water(svc, "B1", "2026-10-01", 3)
    with pytest.raises(InsufficientStockError) as exc:
        svc.plan_allocation(material_code="WATER", location_code="LOC-A",
                            quantity=5)
    assert exc.value.available == 3 and exc.value.requested == 5


def test_expired_batch_not_issued(svc):
    receive_water(svc, "B-EXP", "2026-09-01", 3)   # 已过期
    receive_water(svc, "B-OK", "2026-10-01", 2)
    plan = svc.plan_allocation(material_code="WATER", location_code="LOC-A",
                               quantity=2)
    assert [p["batch_no"] for p in plan] == ["B-OK"]
    with pytest.raises(InsufficientStockError) as exc:
        svc.plan_allocation(material_code="WATER", location_code="LOC-A",
                            quantity=3)
    assert exc.value.available == 2  # 过期批次不计入可发量


# ---------------------------------------------------------------- 领取校验
def test_claim_success_deducts_stock_and_quota(svc):
    receive_water(svc, "B-EARLY", "2026-10-01", 5)
    receive_water(svc, "B-LATE", "2026-12-01", 5)
    grant(svc, "FAM-001", 10, valid_until="2026-09-30")
    r = svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=6,
                  actor="keeper1")
    assert r["idempotent_replay"] is False
    assert r["remaining_quota"] == 4
    assert [(i["batch_no"], i["quantity"]) for i in r["items"]] == [
        ("B-EARLY", 5), ("B-LATE", 1)]
    assert water_total(svc) == 4
    ent = svc.entitlement_status(MANAGER, operation_code="OP-1",
                                 family_code="FAM-001")[0]
    assert ent["used"] == 6 and ent["remaining"] == 4


def test_claim_rejects_when_stock_insufficient(svc):
    receive_water(svc, "B1", "2026-10-01", 3)
    grant(svc, "FAM-001", 10)
    with pytest.raises(InsufficientStockError):
        svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=5,
                  actor="keeper1")


def test_claim_rejects_when_quota_exceeded(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 2)
    with pytest.raises(EligibilityError, match="额度"):
        svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=3,
                  actor="keeper1")
    svc.claim(serial_no="C-2", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=2,
              actor="keeper1")
    with pytest.raises(EligibilityError):  # 额度已用完
        svc.claim(serial_no="C-3", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=1,
                  actor="keeper1")


def test_claim_rejects_without_entitlement(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    with pytest.raises(EligibilityError, match="无此物资的领取资格"):
        svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-002",
                  material_code="WATER", location_code="LOC-A", quantity=1,
                  actor="keeper1")


def test_claim_rejects_revoked_entitlement(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 5)
    svc.revoke_entitlement(serial_no="RV-1", operation_code="OP-1",
                           family_code="FAM-001", material_code="WATER",
                           approver="主任王某", reason="资格复核未通过",
                           actor="admin")
    with pytest.raises(EligibilityError, match="注销"):
        svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=1,
                  actor="keeper1")


def test_claim_rejects_expired_and_not_yet_valid_entitlement(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 5, valid_until="2026-09-20")  # 昨天已过期
    grant(svc, "FAM-002", 5, valid_from="2026-09-25")   # 尚未生效
    for family, serial in (("FAM-001", "C-1"), ("FAM-002", "C-2")):
        with pytest.raises(EligibilityError):
            svc.claim(serial_no=serial, operation_code="OP-1",
                      family_code=family, material_code="WATER",
                      location_code="LOC-A", quantity=1, actor="keeper1")


def test_claim_rejects_when_operation_closed(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 5)
    svc.close_operation("OP-1", actor="admin")
    with pytest.raises(EligibilityError, match="已关闭"):
        svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=1,
                  actor="keeper1")


# ---------------------------------------------------------------- 幂等补传
def test_offline_reupload_is_idempotent(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 5)
    kwargs = dict(operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=3,
                  actor="keeper1")
    r1 = svc.claim(serial_no="SN-100", **kwargs)
    r2 = svc.claim(serial_no="SN-100", **kwargs)  # 断网恢复后补传同一流水
    assert r2["idempotent_replay"] is True
    assert r2["claim_id"] == r1["claim_id"]
    assert water_total(svc) == 7  # 只扣减了一次
    ent = svc.entitlement_status(MANAGER, operation_code="OP-1",
                                 family_code="FAM-001")[0]
    assert ent["used"] == 3


def test_same_serial_with_different_payload_rejected(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 5)
    svc.claim(serial_no="SN-100", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=2,
              actor="keeper1")
    with pytest.raises(ValidationError, match="流水号"):
        svc.claim(serial_no="SN-100", operation_code="OP-1",
                  family_code="FAM-001", material_code="WATER",
                  location_code="LOC-A", quantity=3, actor="keeper1")


def test_receive_reupload_idempotent(svc):
    r1 = receive_water(svc, "B1", "2026-10-01", 10, serial="R-1")
    r2 = svc.receive_stock(serial_no="R-1", material_code="WATER",
                           batch_no="B1", expiry_date="2026-10-01",
                           location_code="LOC-A", quantity=10, actor="keeper1")
    assert r2["idempotent_replay"] is True
    assert water_total(svc) == 10


# ---------------------------------------------------------------- 并发冲突
def test_two_keepers_compete_last_batch_conflict(svc):
    """两名仓管员基于同一快照规划最后 1 瓶水, 后到者必须收到冲突."""
    receive_water(svc, "B1", "2026-10-01", 1)
    grant(svc, "FAM-001", 1)
    grant(svc, "FAM-002", 1)
    plan_a = svc.plan_allocation(material_code="WATER", location_code="LOC-A",
                                 quantity=1)
    plan_b = svc.plan_allocation(material_code="WATER", location_code="LOC-A",
                                 quantity=1)
    svc.claim(serial_no="C-A", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=1,
              actor="keeper1", plan=plan_a)
    with pytest.raises(ConflictError, match="重新规划"):
        svc.claim(serial_no="C-B", operation_code="OP-1", family_code="FAM-002",
                  material_code="WATER", location_code="LOC-A", quantity=1,
                  actor="keeper2", plan=plan_b)
    assert water_total(svc) == 0  # 没有超发


def test_threaded_claims_never_overissue(svc):
    """多线程并发领取: 恰有一人成功, 库存不为负, 台账一致."""
    receive_water(svc, "B1", "2026-10-01", 1)
    grant(svc, "FAM-001", 1)
    grant(svc, "FAM-002", 1)
    plans = [svc.plan_allocation(material_code="WATER",
                                 location_code="LOC-A", quantity=1)
             for _ in range(2)]
    outcomes = []

    def worker(serial, family, plan):
        try:
            svc.claim(serial_no=serial, operation_code="OP-1",
                      family_code=family, material_code="WATER",
                      location_code="LOC-A", quantity=1, actor="keeper",
                      plan=plan)
            outcomes.append("ok")
        except (ConflictError, InsufficientStockError):
            outcomes.append("rejected")

    threads = [threading.Thread(target=w, args=a) for w, a in zip(
        [worker, worker],
        [("C-A", "FAM-001", plans[0]), ("C-B", "FAM-002", plans[1])])]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(outcomes) == ["ok", "rejected"]
    assert water_total(svc) == 0
    report = svc.anomaly_report(MANAGER)
    assert report["stock_discrepancies"] == []


def test_plan_from_other_location_rejected(svc):
    receive_water(svc, "B1", "2026-10-01", 5)
    receive_water(svc, "B1", "2026-10-01", 5, location="LOC-B")
    grant(svc, "FAM-001", 5)
    plan_b = svc.plan_allocation(material_code="WATER", location_code="LOC-B",
                                 quantity=1)
    with pytest.raises(ValidationError, match="不符"):
        svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=1,
                  actor="keeper1", plan=plan_b)


# ---------------------------------------------------------------- 审批留痕
def test_adjust_quota_audited(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 5)
    svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=3,
              actor="keeper1")
    r = svc.adjust_quota(serial_no="AQ-1", operation_code="OP-1",
                         family_code="FAM-001", material_code="WATER",
                         new_quota=10, approver="主任王某",
                         reason="家庭人口增加", actor="admin")
    assert r["old_quota"] == 5 and r["new_quota"] == 10
    with pytest.raises(ValidationError, match="低于已领取"):
        svc.adjust_quota(serial_no="AQ-2", operation_code="OP-1",
                         family_code="FAM-001", material_code="WATER",
                         new_quota=2, approver="主任王某", reason="误操作",
                         actor="admin")
    logs = svc.audit_log(MANAGER, action="adjust_quota")
    assert len(logs) == 1
    assert logs[0]["approver"] == "主任王某"
    assert logs[0]["reason"] == "家庭人口增加"
    assert logs[0]["details"] == {"old_quota": 5, "new_quota": 10}


def test_return_goods_restores_stock_and_quota(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 10)
    r = svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=4,
                  actor="keeper1")
    item_id = r["items"][0]["claim_item_id"]
    back = svc.return_goods(serial_no="RT-1", claim_id=r["claim_id"],
                            approver="主任王某", reason="家庭重复领取退回",
                            actor="keeper1",
                            items=[{"claim_item_id": item_id, "quantity": 2}])
    assert back["claim_status"] == "partially_returned"
    assert water_total(svc) == 8
    ent = svc.entitlement_status(MANAGER, operation_code="OP-1",
                                 family_code="FAM-001")[0]
    assert ent["used"] == 2
    with pytest.raises(ValidationError, match="超过可退"):
        svc.return_goods(serial_no="RT-2", claim_id=r["claim_id"],
                         approver="主任王某", reason="误退", actor="keeper1",
                         items=[{"claim_item_id": item_id, "quantity": 3}])
    back2 = svc.return_goods(serial_no="RT-3", claim_id=r["claim_id"],
                             approver="主任王某", reason="剩余全部退回",
                             actor="keeper1")
    assert back2["claim_status"] == "returned"
    assert water_total(svc) == 10
    logs = svc.audit_log(MANAGER, action="return")
    assert all(l["approver"] == "主任王某" and l["reason"] for l in logs)


def test_report_damage_deducts_and_audits(svc):
    receive_water(svc, "B1", "2026-10-01", 5)
    r = svc.report_damage(serial_no="DM-1", material_code="WATER",
                          batch_no="B1", location_code="LOC-A", quantity=2,
                          approver="主任王某", reason="运输破损", actor="keeper1")
    assert r["new_quantity"] == 3
    with pytest.raises(InsufficientStockError):
        svc.report_damage(serial_no="DM-2", material_code="WATER",
                          batch_no="B1", location_code="LOC-A", quantity=10,
                          approver="主任王某", reason="霉变", actor="keeper1")
    logs = svc.audit_log(MANAGER, action="damage")
    assert logs[0]["approver"] == "主任王某"
    assert logs[0]["reason"] == "运输破损"


@pytest.mark.parametrize("op", ["adjust_quota", "return_goods", "report_damage",
                                "create_transfer"])
def test_approval_required_for_sensitive_ops(svc, op):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 5)
    r = svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
                  material_code="WATER", location_code="LOC-A", quantity=1,
                  actor="keeper1")
    calls = {
        "adjust_quota": dict(serial_no="X-1", operation_code="OP-1",
                             family_code="FAM-001", material_code="WATER",
                             new_quota=9, actor="admin"),
        "return_goods": dict(serial_no="X-2", claim_id=r["claim_id"],
                             actor="keeper1"),
        "report_damage": dict(serial_no="X-3", material_code="WATER",
                              batch_no="B1", location_code="LOC-A",
                              quantity=1, actor="keeper1"),
        "create_transfer": dict(serial_no="X-4", material_code="WATER",
                                batch_no="B1", from_location="LOC-A",
                                to_location="LOC-B", quantity=1,
                                actor="keeper1"),
    }
    for missing in ("approver", "reason"):
        kwargs = {**calls[op], "approver": "主任王某", "reason": "例行"}
        kwargs[missing] = ""
        with pytest.raises(ValidationError):
            getattr(svc, op)(**kwargs)


# ---------------------------------------------------------------- 跨点调拨
def test_transfer_lifecycle(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    t = svc.create_transfer(serial_no="T-1", material_code="WATER",
                            batch_no="B1", from_location="LOC-A",
                            to_location="LOC-B", quantity=4,
                            approver="主任王某", reason="东区发放点补货",
                            actor="keeper1")
    assert t["status"] == "pending"
    assert water_total(svc, "LOC-A") == 6
    assert water_total(svc, "LOC-B") == 0  # 在途未上账
    pending = svc.pending_transfers(MANAGER)
    assert [p["transfer_id"] for p in pending] == [t["transfer_id"]]

    done = svc.complete_transfer(t["transfer_id"], actor="keeper2")
    assert done["status"] == "completed"
    assert water_total(svc, "LOC-B") == 4
    again = svc.complete_transfer(t["transfer_id"], actor="keeper2")
    assert again["idempotent_replay"] is True
    assert water_total(svc, "LOC-B") == 4
    with pytest.raises(ConflictError):
        svc.cancel_transfer(t["transfer_id"], approver="主任王某",
                            reason="已完成无法取消", actor="keeper1")
    logs = svc.audit_log(MANAGER, action="transfer_create")
    assert logs[0]["approver"] == "主任王某" and logs[0]["reason"]


def test_transfer_cancel_returns_stock(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    t = svc.create_transfer(serial_no="T-1", material_code="WATER",
                            batch_no="B1", from_location="LOC-A",
                            to_location="LOC-B", quantity=4,
                            approver="主任王某", reason="补货", actor="keeper1")
    svc.cancel_transfer(t["transfer_id"], approver="主任王某",
                        reason="车辆故障取消", actor="keeper1")
    assert water_total(svc, "LOC-A") == 10
    assert svc.pending_transfers(MANAGER) == []
    with pytest.raises(ConflictError):
        svc.complete_transfer(t["transfer_id"], actor="keeper2")


def test_transfer_insufficient_stock_rejected(svc):
    receive_water(svc, "B1", "2026-10-01", 2)
    with pytest.raises(InsufficientStockError):
        svc.create_transfer(serial_no="T-1", material_code="WATER",
                            batch_no="B1", from_location="LOC-A",
                            to_location="LOC-B", quantity=5,
                            approver="主任王某", reason="补货", actor="keeper1")


# ---------------------------------------------------------------- 角色查询
def test_keeper_sees_only_own_location(svc):
    receive_water(svc, "B1", "2026-10-01", 5)
    receive_water(svc, "B1", "2026-10-01", 7, location="LOC-B")
    inv_a = svc.batch_inventory(KEEPER_A)
    assert {row["location_code"] for row in inv_a} == {"LOC-A"}
    assert sum(row["quantity"] for row in inv_a) == 5
    inv_all = svc.batch_inventory(MANAGER)
    assert {row["location_code"] for row in inv_all} == {"LOC-A", "LOC-B"}


def test_manager_only_queries_denied_for_keeper(svc):
    receive_water(svc, "B1", "2026-10-01", 5)
    grant(svc, "FAM-001", 5)
    svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=1,
              actor="keeper1")
    for call in (
        lambda: svc.family_claim_history(KEEPER_A, family_code="FAM-001"),
        lambda: svc.anomaly_report(KEEPER_A),
        lambda: svc.audit_log(KEEPER_A),
        lambda: svc.fefo_suggestion(KEEPER_A, material_code="WATER",
                                    location_code="LOC-B", quantity=1),
    ):
        with pytest.raises(PermissionDeniedError):
            call()
    # 仓管员可以做发放点资格核查
    ent = svc.entitlement_status(KEEPER_A, operation_code="OP-1",
                                 family_code="FAM-001")
    assert ent[0]["remaining"] == 4


def test_family_claim_history_for_manager(svc):
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 10)
    svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=2,
              actor="keeper1")
    svc.claim(serial_no="C-2", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=3,
              actor="keeper2")
    history = svc.family_claim_history(MANAGER, family_code="FAM-001")
    assert [h["quantity"] for h in history] == [2, 3]
    assert history[0]["items"][0]["batch_no"] == "B1"


# ---------------------------------------------------------------- 异常盘点
def test_anomaly_report(svc):
    receive_water(svc, "B-EXP", "2026-09-01", 2)      # 过期在库
    receive_water(svc, "B1", "2026-10-01", 10)
    grant(svc, "FAM-001", 10)
    svc.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=2,
              actor="keeper1")
    svc.claim(serial_no="C-2", operation_code="OP-1", family_code="FAM-001",
              material_code="WATER", location_code="LOC-A", quantity=3,
              actor="keeper2")                          # 同一家庭两次领取
    svc.create_transfer(serial_no="T-1", material_code="WATER", batch_no="B1",
                        from_location="LOC-A", to_location="LOC-B", quantity=1,
                        approver="主任王某", reason="补货", actor="keeper1")
    # 模拟账实不符(绕过服务直接改库)
    svc._conn.execute(
        "UPDATE stock SET quantity = quantity + 5"
        " WHERE id IN (SELECT s.id FROM stock s JOIN batches b"
        "  ON b.id = s.batch_id WHERE b.batch_no = 'B1')")

    report = svc.anomaly_report(MANAGER)
    assert [d["batch_no"] for d in report["stock_discrepancies"]] == ["B1"]
    assert report["stock_discrepancies"][0]["recorded"] - \
        report["stock_discrepancies"][0]["ledger_sum"] == 5
    assert [e["batch_no"] for e in report["expired_stock"]] == ["B-EXP"]
    assert report["repeated_family_claims"][0]["family_code"] == "FAM-001"
    assert report["repeated_family_claims"][0]["claim_count"] == 2
    assert len(report["pending_transfers"]) == 1
    assert report["overdrawn_entitlements"] == []


def test_stale_transfer_flagged(svc):
    now = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)
    svc2 = make_service(clock=lambda: now)
    try:
        receive_water(svc2, "B1", "2026-10-01", 5)
        svc2.create_transfer(serial_no="T-1", material_code="WATER",
                             batch_no="B1", from_location="LOC-A",
                             to_location="LOC-B", quantity=1,
                             approver="主任王某", reason="补货", actor="keeper1")
        now += timedelta(hours=30)  # 时钟前进 30 小时
        report = svc2.anomaly_report(MANAGER, stale_transfer_hours=24)
        assert report["pending_transfers"][0]["stale"] is True
        assert report["pending_transfers"][0]["age_hours"] == 30
    finally:
        svc2.close()


# ---------------------------------------------------------------- 持久化
def test_state_survives_restart(tmp_path):
    db = str(tmp_path / "relief.db")
    svc1 = make_service(db)
    receive_water(svc1, "B1", "2026-10-01", 10)
    grant(svc1, "FAM-001", 5)
    svc1.claim(serial_no="C-1", operation_code="OP-1", family_code="FAM-001",
               material_code="WATER", location_code="LOC-A", quantity=2,
               actor="keeper1")
    svc1.adjust_quota(serial_no="AQ-1", operation_code="OP-1",
                      family_code="FAM-001", material_code="WATER",
                      new_quota=8, approver="主任王某", reason="人口核实",
                      actor="admin")
    t = svc1.create_transfer(serial_no="T-1", material_code="WATER",
                             batch_no="B1", from_location="LOC-A",
                             to_location="LOC-B", quantity=3,
                             approver="主任王某", reason="补货", actor="keeper1")
    svc1.close()

    # 模拟重启: 新实例打开同一数据库文件
    svc2 = make_service(db, seed=False)
    try:
        pending = svc2.pending_transfers(MANAGER)
        assert [p["transfer_id"] for p in pending] == [t["transfer_id"]]
        logs = svc2.audit_log(MANAGER)
        assert {l["action"] for l in logs} >= {
            "claim", "adjust_quota", "transfer_create"}
        # 幂等记录仍在: 断网终端重启后补传不会重复扣减
        replay = svc2.claim(serial_no="C-1", operation_code="OP-1",
                            family_code="FAM-001", material_code="WATER",
                            location_code="LOC-A", quantity=2, actor="keeper1")
        assert replay["idempotent_replay"] is True
        assert water_total(svc2, "LOC-A") == 5
        # 重启后未完成调拨可继续完成
        svc2.complete_transfer(t["transfer_id"], actor="keeper2")
        assert water_total(svc2, "LOC-B") == 3
    finally:
        svc2.close()


# ---------------------------------------------------------------- 其他校验
def test_duplicate_setup_rejected(svc):
    with pytest.raises(ValidationError):
        svc.add_location("LOC-A", "重复")
    with pytest.raises(ValidationError):
        svc.add_material("WATER", "重复", "瓶")
    grant(svc, "FAM-001", 5)
    with pytest.raises(ValidationError, match="已有此物资资格"):
        grant(svc, "FAM-001", 5, serial="G-dup")


def test_batch_expiry_mismatch_rejected(svc):
    receive_water(svc, "B1", "2026-10-01", 5)
    with pytest.raises(ValidationError, match="有效期"):
        receive_water(svc, "B1", "2026-11-01", 5, serial="R-x")
