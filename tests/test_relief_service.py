"""物资领用服务测试：发放/FEFO/资格/幂等/并发/审批留痕/调拨/盘点/RBAC/重启持久化。"""

import os
import tempfile
import threading
import unittest
from datetime import datetime, timezone

from src import (
    ROLE_KEEPER,
    ROLE_MANAGER,
    Actor,
    ConflictError,
    EligibilityError,
    IdempotencyConflictError,
    InsufficientQuotaError,
    InsufficientStockError,
    PermissionDeniedError,
    ReliefService,
    StateError,
    ValidationError,
)

FIXED_NOW = datetime(2026, 9, 21, 10, 0, 0, tzinfo=timezone.utc)


def fixed_clock():
    return FIXED_NOW


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "relief.db")
        self.svc = ReliefService(self.db_path, clock=fixed_clock)
        self.manager = Actor("张管理", ROLE_MANAGER)
        # 两个存放点 + 两名仓管员
        self.site1 = self.svc.register_site(
            serial_no="SETUP-S1", actor=self.manager, name="一号仓")["site_id"]
        self.site2 = self.svc.register_site(
            serial_no="SETUP-S2", actor=self.manager, name="二号仓")["site_id"]
        self.keeper1 = Actor("仓管甲", ROLE_KEEPER, site_id=self.site1)
        self.keeper2 = Actor("仓管乙", ROLE_KEEPER, site_id=self.site2)
        # 行动批次与家庭
        self.camp = self.svc.open_campaign(
            serial_no="SETUP-C1", actor=self.manager,
            code="ACT-2026-09", name="临时安置行动")["campaign_id"]
        self.svc.register_household(
            serial_no="SETUP-H1", actor=self.manager,
            household_id="HH-1", head_name="王五", members=3)
        self.svc.register_household(
            serial_no="SETUP-H2", actor=self.manager,
            household_id="HH-2", head_name="赵六", members=2)

    def tearDown(self):
        self.tmp.cleanup()

    def add_water(self, serial, site, qty, expiry="2026-10-01"):
        return self.svc.add_batch(
            serial_no=serial, actor=self.manager, item="饮用水",
            site_id=site, quantity=qty, expiry_date=expiry)["batch_id"]

    def grant(self, serial, hh, item, amount):
        return self.svc.set_quota(
            serial_no=serial, actor=self.manager, campaign_id=self.camp,
            household_id=hh, item=item, amount=amount,
            approver="李审批", reason="按户核定")

    def stock_of(self, batch_id):
        inv = self.svc.batch_inventory(actor=self.manager)
        return next(b["quantity"] for b in inv if b["batch_id"] == batch_id)


class TestIssueAndFEFO(Base):
    def test_issue_follows_fefo_order(self):
        b1 = self.add_water("B1", self.site1, 5, expiry="2026-10-01")
        b2 = self.add_water("B2", self.site1, 5, expiry="2026-09-25")
        b3 = self.add_water("B3", self.site1, 5, expiry=None)  # 无有效期排最后
        self.grant("Q1", "HH-1", "饮用水", 20)

        r = self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=7)
        # 先到期先发：b2(09-25) 取 5，b1(10-01) 取 2
        self.assertEqual([(l["batch_id"], l["quantity"]) for l in r["lines"]],
                         [(b2, 5), (b1, 2)])
        self.assertEqual(self.stock_of(b2), 0)
        self.assertEqual(self.stock_of(b1), 3)
        self.assertEqual(self.stock_of(b3), 5)
        self.assertFalse(r["idempotent_replay"])

    def test_suggest_outbound_skips_expired_and_reports_shortfall(self):
        self.add_water("B1", self.site1, 4, expiry="2026-09-01")  # 已过期
        ok = self.add_water("B2", self.site1, 3, expiry="2026-09-30")
        plan = self.svc.suggest_outbound(actor=self.keeper1, item="饮用水",
                                         site_id=self.site1, quantity=10)
        self.assertEqual([l["batch_id"] for l in plan["lines"]], [ok])
        self.assertEqual(plan["available"], 3)
        self.assertEqual(plan["shortfall"], 7)

    def test_expired_batch_cannot_be_issued(self):
        self.add_water("B1", self.site1, 10, expiry="2026-09-01")  # 已过期
        self.grant("Q1", "HH-1", "饮用水", 10)
        with self.assertRaises(InsufficientStockError):
            self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=1)

    def test_insufficient_stock_rejected_atomically(self):
        b1 = self.add_water("B1", self.site1, 2)
        self.grant("Q1", "HH-1", "饮用水", 100)
        with self.assertRaises(InsufficientStockError):
            self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=5)
        self.assertEqual(self.stock_of(b1), 2)  # 无部分扣减

    def test_quota_enforced(self):
        self.add_water("B1", self.site1, 50)
        self.grant("Q1", "HH-1", "饮用水", 2)
        # 无额度记录的家庭
        with self.assertRaises(EligibilityError):
            self.svc.issue(serial_no="ISS-0", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-2",
                           item="饮用水", site_id=self.site1, quantity=1)
        # 超出额度
        with self.assertRaises(InsufficientQuotaError):
            self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=5)
        # 额度内成功，剩余额度随之减少
        self.svc.issue(serial_no="ISS-2", actor=self.keeper1,
                       campaign_id=self.camp, household_id="HH-1",
                       item="饮用水", site_id=self.site1, quantity=2)
        quotas = self.svc.quota_view(actor=self.keeper1, campaign_id=self.camp,
                                     household_id="HH-1")
        self.assertEqual(quotas[0]["remaining"], 0)

    def test_ineligible_household_rejected(self):
        self.add_water("B1", self.site1, 10)
        self.grant("Q1", "HH-1", "饮用水", 5)
        self.svc.set_household_status(
            serial_no="HS-1", actor=self.manager, household_id="HH-1",
            status="SUSPENDED", approver="李审批", reason="重复登记待核查")
        with self.assertRaises(EligibilityError):
            self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=1)

    def test_expired_eligibility_rejected(self):
        self.add_water("B1", self.site1, 10)
        self.svc.register_household(
            serial_no="H9", actor=self.manager, household_id="HH-9",
            eligible_until="2026-09-01")  # 资格已于月初过期
        self.grant("Q9", "HH-9", "饮用水", 5)
        with self.assertRaises(EligibilityError):
            self.svc.issue(serial_no="ISS-9", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-9",
                           item="饮用水", site_id=self.site1, quantity=1)

    def test_closed_campaign_rejected(self):
        self.add_water("B1", self.site1, 10)
        self.grant("Q1", "HH-1", "饮用水", 5)
        self.svc.close_campaign(serial_no="CC-1", actor=self.manager,
                                campaign_id=self.camp)
        with self.assertRaises(EligibilityError):
            self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=1)

    def test_invalid_quantity_and_serial_rejected(self):
        self.add_water("B1", self.site1, 10)
        self.grant("Q1", "HH-1", "饮用水", 5)
        with self.assertRaises(ValidationError):
            self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=0)
        with self.assertRaises(ValidationError):
            self.svc.issue(serial_no="", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=1)


class TestIdempotency(Base):
    def test_replay_same_serial_returns_first_result(self):
        b1 = self.add_water("B1", self.site1, 1)
        self.grant("Q1", "HH-1", "饮用水", 1)
        r1 = self.svc.issue(serial_no="SN-100", actor=self.keeper1,
                            campaign_id=self.camp, household_id="HH-1",
                            item="饮用水", site_id=self.site1, quantity=1)
        # 断网补传：库存已空，但同流水号同负载必须返回首次结果而非报错
        r2 = self.svc.issue(serial_no="SN-100", actor=self.keeper1,
                            campaign_id=self.camp, household_id="HH-1",
                            item="饮用水", site_id=self.site1, quantity=1)
        self.assertTrue(r2["idempotent_replay"])
        self.assertEqual(r2["distribution_id"], r1["distribution_id"])
        self.assertEqual(self.stock_of(b1), 0)  # 只扣了一次

    def test_same_serial_different_payload_conflicts(self):
        self.add_water("B1", self.site1, 10)
        self.grant("Q1", "HH-1", "饮用水", 10)
        self.svc.issue(serial_no="SN-200", actor=self.keeper1,
                       campaign_id=self.camp, household_id="HH-1",
                       item="饮用水", site_id=self.site1, quantity=1)
        with self.assertRaises(IdempotencyConflictError):
            self.svc.issue(serial_no="SN-200", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=2)

    def test_failed_operation_does_not_consume_serial(self):
        self.add_water("B1", self.site1, 1)
        self.grant("Q1", "HH-1", "饮用水", 5)
        with self.assertRaises(InsufficientStockError):
            self.svc.issue(serial_no="SN-300", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=9)
        # 补货后同一流水号可正常重试
        self.add_water("B2", self.site1, 10)
        r = self.svc.issue(serial_no="SN-300", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site1, quantity=2)
        self.assertEqual(r["status"], "ISSUED")


class TestConcurrency(Base):
    def test_two_keepers_race_last_unit_one_conflicts(self):
        """两个仓管员同时抢最后一件：一个成功，另一个收到冲突，绝不超发。"""
        b1 = self.add_water("B1", self.site1, 1)
        self.grant("Q1", "HH-1", "饮用水", 1)
        self.grant("Q2", "HH-2", "饮用水", 1)
        keeper_b = Actor("仓管丙", ROLE_KEEPER, site_id=self.site1)

        # 让两个线程都先完成 FEFO 计划（读到同一份库存），再同时提交
        barrier = threading.Barrier(2)
        original = self.svc._plan_allocation

        def synced_plan(item, site_id, quantity, strict):
            plan = original(item, site_id, quantity, strict)
            barrier.wait(timeout=10)
            return plan

        self.svc._plan_allocation = synced_plan

        results = {}

        def worker(tag, serial, hh, actor):
            try:
                r = self.svc.issue(serial_no=serial, actor=actor,
                                   campaign_id=self.camp, household_id=hh,
                                   item="饮用水", site_id=self.site1, quantity=1)
                results[tag] = ("ok", r)
            except Exception as exc:  # noqa: BLE001 - 记录类型断言
                results[tag] = (type(exc).__name__, str(exc))

        t1 = threading.Thread(target=worker, args=("a", "SN-A", "HH-1", self.keeper1))
        t2 = threading.Thread(target=worker, args=("b", "SN-B", "HH-2", keeper_b))
        t1.start(); t2.start(); t1.join(15); t2.join(15)

        outcomes = sorted(v[0] for v in results.values())
        self.assertEqual(outcomes, ["ConflictError", "ok"], results)
        self.assertEqual(self.stock_of(b1), 0)  # 没有超发成负数
        h1 = self.svc.household_history(actor=self.manager, household_id="HH-1")
        h2 = self.svc.household_history(actor=self.manager, household_id="HH-2")
        self.assertEqual(len(h1["distributions"]) + len(h2["distributions"]), 1)

    def test_concurrent_issue_with_plenty_stock_both_succeed(self):
        """库存充足时并发发放不应误报冲突：写锁内重规划后双双成功。"""
        b1 = self.add_water("B1", self.site1, 10)
        self.grant("Q1", "HH-1", "饮用水", 5)
        self.grant("Q2", "HH-2", "饮用水", 5)
        keeper_b = Actor("仓管丙", ROLE_KEEPER, site_id=self.site1)

        barrier = threading.Barrier(2)
        original = self.svc._plan_allocation

        def synced_plan(item, site_id, quantity, strict):
            plan = original(item, site_id, quantity, strict)
            barrier.wait(timeout=10)
            return plan

        self.svc._plan_allocation = synced_plan
        results = {}

        def worker(tag, serial, hh, actor):
            try:
                r = self.svc.issue(serial_no=serial, actor=actor,
                                   campaign_id=self.camp, household_id=hh,
                                   item="饮用水", site_id=self.site1, quantity=4)
                results[tag] = ("ok", r["distribution_id"])
            except Exception as exc:  # noqa: BLE001
                results[tag] = (type(exc).__name__, str(exc))

        t1 = threading.Thread(target=worker, args=("a", "SN-P1", "HH-1", self.keeper1))
        t2 = threading.Thread(target=worker, args=("b", "SN-P2", "HH-2", keeper_b))
        t1.start(); t2.start(); t1.join(15); t2.join(15)

        self.assertEqual(sorted(v[0] for v in results.values()), ["ok", "ok"], results)
        self.assertEqual(self.stock_of(b1), 2)  # 4 + 4 都发出去了


class TestReturnDamageQuotaAudit(Base):
    def test_return_restores_stock_and_audits(self):
        b1 = self.add_water("B1", self.site1, 10)
        self.grant("Q1", "HH-1", "饮用水", 10)
        dist = self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                              campaign_id=self.camp, household_id="HH-1",
                              item="饮用水", site_id=self.site1, quantity=5)
        r = self.svc.return_items(serial_no="RT-1", actor=self.keeper1,
                                  distribution_id=dist["distribution_id"],
                                  quantity=2, approver="李审批", reason="家庭离点退回")
        self.assertEqual(r["status"], "PARTIALLY_RETURNED")
        self.assertEqual(self.stock_of(b1), 7)
        r = self.svc.return_items(serial_no="RT-2", actor=self.keeper1,
                                  distribution_id=dist["distribution_id"],
                                  quantity=3, approver="李审批", reason="剩余全部退回")
        self.assertEqual(r["status"], "RETURNED")
        self.assertEqual(self.stock_of(b1), 10)
        with self.assertRaises(ValidationError):
            self.svc.return_items(serial_no="RT-3", actor=self.keeper1,
                                  distribution_id=dist["distribution_id"],
                                  quantity=1, approver="李审批", reason="超量退回")
        audit = self.svc.audit_trail(actor=self.manager)
        returns = [a for a in audit if a["action"] == "RETURN"]
        self.assertEqual(len(returns), 2)
        self.assertTrue(all(a["approver"] == "李审批" and a["reason"] for a in returns))

    def test_damage_requires_approval_and_stock(self):
        b1 = self.add_water("B1", self.site1, 5)
        with self.assertRaises(ValidationError):  # 缺审批人
            self.svc.report_damage(serial_no="DM-0", actor=self.keeper1,
                                   batch_id=b1, quantity=1,
                                   approver="", reason="包装破损")
        with self.assertRaises(ValidationError):  # 缺原因
            self.svc.report_damage(serial_no="DM-0b", actor=self.keeper1,
                                   batch_id=b1, quantity=1,
                                   approver="李审批", reason=" ")
        with self.assertRaises(InsufficientStockError):
            self.svc.report_damage(serial_no="DM-0c", actor=self.keeper1,
                                   batch_id=b1, quantity=9,
                                   approver="李审批", reason="水浸报废")
        r = self.svc.report_damage(serial_no="DM-1", actor=self.keeper1,
                                   batch_id=b1, quantity=2,
                                   approver="李审批", reason="运输破损")
        self.assertEqual(r["remaining"], 3)
        self.assertEqual(self.stock_of(b1), 3)

    def test_quota_adjustment_audited(self):
        self.grant("Q1", "HH-1", "饮用水", 4)
        r = self.svc.set_quota(serial_no="Q2", actor=self.manager,
                               campaign_id=self.camp, household_id="HH-1",
                               item="饮用水", amount=6,
                               approver="李审批", reason="新增一名家庭成员")
        self.assertEqual((r["old_amount"], r["amount"]), (4, 6))
        audit = self.svc.audit_trail(actor=self.manager)
        adjusts = [a for a in audit if a["action"] == "QUOTA_ADJUST"]
        self.assertEqual(len(adjusts), 2)
        self.assertEqual(adjusts[0]["detail"], {"old": 4, "new": 6})
        self.assertEqual(adjusts[0]["approver"], "李审批")
        with self.assertRaises(ValidationError):
            self.svc.set_quota(serial_no="Q3", actor=self.manager,
                               campaign_id=self.camp, household_id="HH-1",
                               item="饮用水", amount=1, approver="", reason="x")


class TestTransfer(Base):
    def test_transfer_full_lifecycle(self):
        b1 = self.add_water("B1", self.site1, 10, expiry="2026-10-01")
        t = self.svc.initiate_transfer(
            serial_no="TR-1", actor=self.keeper1, from_site_id=self.site1,
            to_site_id=self.site2, item="饮用水", quantity=4,
            approver="李审批", reason="二号点告急")
        self.assertEqual(t["status"], "PENDING")
        self.assertEqual(self.stock_of(b1), 6)  # 源点已出库（在途）

        # 重复接收前：先完成
        done = self.svc.complete_transfer(serial_no="TR-1-C", actor=self.keeper2,
                                          transfer_id=t["transfer_id"])
        self.assertEqual(done["status"], "COMPLETED")
        inv2 = self.svc.batch_inventory(actor=self.keeper2)
        self.assertEqual(len(inv2), 1)
        self.assertEqual(inv2[0]["quantity"], 4)
        self.assertEqual(inv2[0]["expiry_date"], "2026-10-01")  # 有效期随批次保留
        with self.assertRaises(StateError):
            self.svc.complete_transfer(serial_no="TR-1-C2", actor=self.keeper2,
                                       transfer_id=t["transfer_id"])

    def test_cancel_transfer_returns_stock(self):
        b1 = self.add_water("B1", self.site1, 10)
        t = self.svc.initiate_transfer(
            serial_no="TR-2", actor=self.keeper1, from_site_id=self.site1,
            to_site_id=self.site2, item="饮用水", quantity=3,
            approver="李审批", reason="计划调整")
        self.assertEqual(self.stock_of(b1), 7)
        r = self.svc.cancel_transfer(serial_no="TR-2-X", actor=self.keeper1,
                                     transfer_id=t["transfer_id"],
                                     approver="李审批", reason="车辆不足取消")
        self.assertEqual(r["status"], "CANCELLED")
        self.assertEqual(self.stock_of(b1), 10)

    def test_transfer_requires_approval_and_distinct_sites(self):
        self.add_water("B1", self.site1, 10)
        with self.assertRaises(ValidationError):
            self.svc.initiate_transfer(
                serial_no="TR-3", actor=self.keeper1, from_site_id=self.site1,
                to_site_id=self.site2, item="饮用水", quantity=1,
                approver="", reason="x")
        with self.assertRaises(ValidationError):
            self.svc.initiate_transfer(
                serial_no="TR-4", actor=self.keeper1, from_site_id=self.site1,
                to_site_id=self.site1, item="饮用水", quantity=1,
                approver="李审批", reason="同点调拨")

    def test_transfer_idempotent_replay(self):
        b1 = self.add_water("B1", self.site1, 5)
        t1 = self.svc.initiate_transfer(
            serial_no="TR-5", actor=self.keeper1, from_site_id=self.site1,
            to_site_id=self.site2, item="饮用水", quantity=2,
            approver="李审批", reason="补传场景")
        t2 = self.svc.initiate_transfer(
            serial_no="TR-5", actor=self.keeper1, from_site_id=self.site1,
            to_site_id=self.site2, item="饮用水", quantity=2,
            approver="李审批", reason="补传场景")
        self.assertEqual(t2["transfer_id"], t1["transfer_id"])
        self.assertTrue(t2["idempotent_replay"])
        self.assertEqual(self.stock_of(b1), 3)  # 只出库一次

    def test_pending_transfer_and_audit_survive_restart(self):
        self.add_water("B1", self.site1, 10)
        t = self.svc.initiate_transfer(
            serial_no="TR-6", actor=self.keeper1, from_site_id=self.site1,
            to_site_id=self.site2, item="饮用水", quantity=4,
            approver="李审批", reason="重启持久化验证")
        # 模拟重启：同一数据库文件新建服务实例
        svc2 = ReliefService(self.db_path, clock=fixed_clock)
        pending = svc2.pending_transfers(actor=self.manager)
        self.assertEqual([p["transfer_id"] for p in pending], [t["transfer_id"]])
        audit = svc2.audit_trail(actor=self.manager)
        self.assertTrue(any(a["action"] == "TRANSFER_INIT" for a in audit))
        # 重启后可以继续完成该调拨
        done = svc2.complete_transfer(serial_no="TR-6-C", actor=self.keeper2,
                                      transfer_id=t["transfer_id"])
        self.assertEqual(done["status"], "COMPLETED")
        self.assertEqual(svc2.pending_transfers(actor=self.manager), [])


class TestStocktakeAndAnomaly(Base):
    def test_stocktake_variance_and_adjustment(self):
        b1 = self.add_water("B1", self.site1, 5)
        r = self.svc.record_stocktake(serial_no="ST-1", actor=self.keeper1,
                                      batch_id=b1, counted=3,
                                      approver="李审批", reason="月度盘点")
        self.assertEqual(r["variance"], -2)
        self.assertEqual(self.stock_of(b1), 5)  # 未应用调整
        self.svc.record_stocktake(serial_no="ST-2", actor=self.keeper1,
                                  batch_id=b1, counted=3,
                                  approver="李审批", reason="复盘确认",
                                  apply_adjustment=True)
        self.assertEqual(self.stock_of(b1), 3)

    def test_anomaly_report_manager_only(self):
        b1 = self.add_water("B1", self.site1, 5)
        self.add_water("B2", self.site1, 4, expiry="2026-09-01")  # 过期未发
        self.svc.report_damage(serial_no="DM-1", actor=self.keeper1,
                               batch_id=b1, quantity=1,
                               approver="李审批", reason="破损")
        self.svc.record_stocktake(serial_no="ST-1", actor=self.keeper1,
                                  batch_id=b1, counted=3,
                                  approver="李审批", reason="盘点")
        report = self.svc.anomaly_report(actor=self.manager)
        self.assertEqual(len(report["damages"]), 1)
        self.assertEqual(report["damages"][0]["approver"], "李审批")
        self.assertEqual(len(report["stocktake_variances"]), 1)
        self.assertEqual(report["stocktake_variances"][0]["variance"], -1)
        self.assertEqual(len(report["expired_stock"]), 1)
        with self.assertRaises(PermissionDeniedError):
            self.svc.anomaly_report(actor=self.keeper1)


class TestRBAC(Base):
    def setUp(self):
        super().setUp()
        self.add_water("B1", self.site1, 10)
        self.add_water("B2", self.site2, 20)
        self.grant("Q1", "HH-1", "饮用水", 5)
        self.svc.issue(serial_no="ISS-1", actor=self.keeper1,
                       campaign_id=self.camp, household_id="HH-1",
                       item="饮用水", site_id=self.site1, quantity=2)

    def test_keeper_sees_only_own_site(self):
        inv = self.svc.batch_inventory(actor=self.keeper1)
        self.assertEqual({b["site_id"] for b in inv}, {self.site1})
        with self.assertRaises(PermissionDeniedError):
            self.svc.batch_inventory(actor=self.keeper1, site_id=self.site2)
        # 管理者看全部
        inv_all = self.svc.batch_inventory(actor=self.manager)
        self.assertEqual({b["site_id"] for b in inv_all}, {self.site1, self.site2})

    def test_keeper_cannot_operate_other_site(self):
        with self.assertRaises(PermissionDeniedError):
            self.svc.issue(serial_no="ISS-X", actor=self.keeper1,
                           campaign_id=self.camp, household_id="HH-1",
                           item="饮用水", site_id=self.site2, quantity=1)

    def test_manager_queries_denied_for_keeper(self):
        with self.assertRaises(PermissionDeniedError):
            self.svc.household_history(actor=self.keeper1, household_id="HH-1")
        with self.assertRaises(PermissionDeniedError):
            self.svc.audit_trail(actor=self.keeper1)

    def test_household_history_for_manager(self):
        h = self.svc.household_history(actor=self.manager, household_id="HH-1")
        self.assertEqual(h["household"]["head_name"], "王五")
        self.assertEqual(len(h["distributions"]), 1)
        d = h["distributions"][0]
        self.assertEqual((d["item"], d["quantity"], d["status"]),
                         ("饮用水", 2, "ISSUED"))
        self.assertEqual(d["site"], "一号仓")
        self.assertEqual(h["quotas"][0]["remaining"], 3)


if __name__ == "__main__":
    unittest.main()
