"""端到端演示：一次临时安置行动中的物资领用全流程。

运行：python3 demo.py
"""

import json
import os
import tempfile
from datetime import datetime, timezone

from src import (
    ROLE_KEEPER,
    ROLE_MANAGER,
    Actor,
    ConflictError,
    ReliefService,
)


def show(title, data):
    print(f"\n== {title} ==")
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


def main():
    db_path = os.path.join(tempfile.mkdtemp(), "relief.db")
    clock = lambda: datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)  # noqa: E731
    svc = ReliefService(db_path, clock=clock)

    manager = Actor("张管理", ROLE_MANAGER)

    # 1. 建档：存放点 / 物资批次（带有效期）/ 行动批次 / 家庭 / 额度
    s1 = svc.register_site(serial_no="S1", actor=manager, name="一号仓")["site_id"]
    s2 = svc.register_site(serial_no="S2", actor=manager, name="二号仓")["site_id"]
    keeper1 = Actor("仓管甲", ROLE_KEEPER, site_id=s1)
    keeper2 = Actor("仓管乙", ROLE_KEEPER, site_id=s2)
    svc.add_batch(serial_no="B1", actor=manager, item="饮用水",
                  site_id=s1, quantity=50, expiry_date="2026-09-25")
    svc.add_batch(serial_no="B2", actor=manager, item="饮用水",
                  site_id=s1, quantity=50, expiry_date="2026-12-01")
    svc.add_batch(serial_no="B3", actor=manager, item="应急照明",
                  site_id=s1, quantity=30, expiry_date=None)
    camp = svc.open_campaign(serial_no="C1", actor=manager,
                             code="ACT-2026-09", name="临时安置行动")["campaign_id"]
    svc.register_household(serial_no="H1", actor=manager,
                           household_id="HH-001", head_name="王五", members=3)
    svc.set_quota(serial_no="Q1", actor=manager, campaign_id=camp,
                  household_id="HH-001", item="饮用水", amount=12,
                  approver="李审批", reason="三口之家按人定量")

    # 2. FEFO 建议 + 发放（先到期 09-25 批次先出）
    show("出库建议（先到期先发）", svc.suggest_outbound(
        actor=keeper1, item="饮用水", site_id=s1, quantity=10))
    r = svc.issue(serial_no="TERM-0001", actor=keeper1, campaign_id=camp,
                  household_id="HH-001", item="饮用水", site_id=s1, quantity=10)
    show("发放结果", r)

    # 3. 断网补传：同一流水号重发，幂等返回首次结果
    replay = svc.issue(serial_no="TERM-0001", actor=keeper1, campaign_id=camp,
                       household_id="HH-001", item="饮用水", site_id=s1, quantity=10)
    show("断网补传（幂等重放）", replay)

    # 4. 退回 / 报损 / 跨点调拨（均留审批人与原因）
    svc.return_items(serial_no="RT-1", actor=keeper1,
                     distribution_id=r["distribution_id"], quantity=2,
                     approver="李审批", reason="家庭提前离点")
    svc.report_damage(serial_no="DM-1", actor=keeper1, batch_id=2, quantity=3,
                      approver="李审批", reason="搬运破损")
    t = svc.initiate_transfer(serial_no="TR-1", actor=keeper1, from_site_id=s1,
                              to_site_id=s2, item="应急照明", quantity=10,
                              approver="李审批", reason="二号点照明不足")
    show("调拨发起（在途）", svc.pending_transfers(actor=manager))

    # 5. 模拟重启：未完成调拨与审计记录仍在
    svc = ReliefService(db_path, clock=clock)
    svc.complete_transfer(serial_no="TR-1-C", actor=keeper2,
                          transfer_id=t["transfer_id"])
    show("重启后完成调拨", svc.pending_transfers(actor=manager))

    # 6. 并发演示：两个仓管员抢最后一件（手电筒全点仅剩 1 件）
    svc.add_batch(serial_no="B9", actor=manager, item="手电筒",
                  site_id=s1, quantity=1, expiry_date=None)
    svc.register_household(serial_no="H2", actor=manager, household_id="HH-002")
    svc.set_quota(serial_no="Q2", actor=manager, campaign_id=camp,
                  household_id="HH-001", item="手电筒", amount=1,
                  approver="李审批", reason="一户一支")
    svc.set_quota(serial_no="Q3", actor=manager, campaign_id=camp,
                  household_id="HH-002", item="手电筒", amount=1,
                  approver="李审批", reason="一户一支")
    import threading
    barrier = threading.Barrier(2)
    original = svc._plan_allocation

    def synced(item, site_id, quantity, strict):
        plan = original(item, site_id, quantity, strict)
        barrier.wait(timeout=10)
        return plan
    svc._plan_allocation = synced
    outcomes = {}

    def grab(tag, serial, hh):
        try:
            svc.issue(serial_no=serial, actor=keeper1, campaign_id=camp,
                      household_id=hh, item="手电筒", site_id=s1, quantity=1)
            outcomes[tag] = "成功"
        except ConflictError as exc:
            outcomes[tag] = f"冲突: {exc}"

    ts = [threading.Thread(target=grab, args=("仓管甲/终端A", "A-1", "HH-001")),
          threading.Thread(target=grab, args=("仓管甲/终端B", "B-1", "HH-002"))]
    [t_.start() for t_ in ts]; [t_.join() for t_ in ts]
    show("并发抢最后一件（一成功一冲突）", outcomes)

    # 7. 管理者视图
    show("批次库存（管理者）", svc.batch_inventory(actor=manager))
    show("家庭领取历史", svc.household_history(actor=manager, household_id="HH-001"))
    show("异常盘点", svc.anomaly_report(actor=manager))
    show("审计日志（最近 6 条）", svc.audit_trail(actor=manager, limit=6))


if __name__ == "__main__":
    main()
