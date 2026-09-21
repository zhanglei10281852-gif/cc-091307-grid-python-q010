"""端到端演示: 临时安置行动中的物资发放全流程.

运行: python3 demo.py
"""
import os
import tempfile

from src.service import (
    ConflictError,
    ReliefService,
    Role,
    Viewer,
)

DB = os.path.join(tempfile.gettempdir(), "relief_demo.db")
MANAGER = Viewer(Role.MANAGER)
KEEPER_A = Viewer(Role.KEEPER, "LOC-A")


def show(title):
    print(f"\n=== {title} ===")


def main():
    if os.path.exists(DB):
        os.remove(DB)
    svc = ReliefService(DB)

    show("建档: 存放点 / 物资 / 行动批次 / 家庭")
    svc.add_location("LOC-A", "中心仓库")
    svc.add_location("LOC-B", "东区安置点")
    svc.add_material("WATER", "饮用水", "瓶")
    svc.add_material("LAMP", "应急照明灯", "盏")
    svc.add_operation("OP-0921", "9月临时安置行动")
    svc.add_family("FAM-001", "张某", 4)
    svc.add_family("FAM-002", "李某", 3)
    print("完成")

    show("入库: 两批饮用水(先到期先发将优先早出批次)")
    svc.receive_stock(serial_no="IN-1", material_code="WATER", batch_no="W-0901",
                      expiry_date="2026-10-01", location_code="LOC-A",
                      quantity=50, actor="仓管员甲", reason="采购入库")
    svc.receive_stock(serial_no="IN-2", material_code="WATER", batch_no="W-0915",
                      expiry_date="2026-12-01", location_code="LOC-A",
                      quantity=100, actor="仓管员甲", reason="采购入库")
    svc.receive_stock(serial_no="IN-3", material_code="LAMP", batch_no="L-0901",
                      expiry_date="2028-06-01", location_code="LOC-A",
                      quantity=30, actor="仓管员甲", reason="调拨入库")

    show("发放额度: 按行动批次 + 家庭标识授权")
    svc.grant_entitlement(serial_no="G-1", operation_code="OP-0921",
                          family_code="FAM-001", material_code="WATER",
                          quota=24, valid_until="2026-09-30", actor="社工乙",
                          reason="四口之家")
    svc.grant_entitlement(serial_no="G-2", operation_code="OP-0921",
                          family_code="FAM-002", material_code="WATER",
                          quota=12, valid_until="2026-09-30", actor="社工乙")
    ent = svc.entitlement_status(KEEPER_A, operation_code="OP-0921",
                                 family_code="FAM-001")
    print("FAM-001 饮用水资格:", ent[0]["quota"], "瓶, 剩余", ent[0]["remaining"])

    show("FEFO 出库建议")
    plan = svc.plan_allocation(material_code="WATER", location_code="LOC-A",
                               quantity=60)
    for p in plan:
        print(f"  批次 {p['batch_no']} (有效期至 {p['expiry_date']}): {p['quantity']} 瓶")

    show("登记领用(终端流水号 SN-0001)")
    r = svc.claim(serial_no="SN-0001", operation_code="OP-0921",
                  family_code="FAM-001", material_code="WATER",
                  location_code="LOC-A", quantity=12, actor="仓管员甲")
    print("领用单", r["claim_id"], "出库明细:",
          [(i["batch_no"], i["quantity"]) for i in r["items"]])

    show("断网恢复后补传同一流水号 → 幂等返回, 不重复扣减")
    r2 = svc.claim(serial_no="SN-0001", operation_code="OP-0921",
                   family_code="FAM-001", material_code="WATER",
                   location_code="LOC-A", quantity=12, actor="仓管员甲")
    print("idempotent_replay =", r2["idempotent_replay"], ", claim_id 不变:",
          r2["claim_id"] == r["claim_id"])

    show("两名仓管员竞争最后库存 → 后到者收到冲突")
    svc.receive_stock(serial_no="IN-4", material_code="LAMP", batch_no="L-LAST",
                      expiry_date="2028-01-01", location_code="LOC-A",
                      quantity=1, actor="仓管员甲")
    svc.grant_entitlement(serial_no="G-3", operation_code="OP-0921",
                          family_code="FAM-001", material_code="LAMP",
                          quota=1, actor="社工乙")
    svc.grant_entitlement(serial_no="G-4", operation_code="OP-0921",
                          family_code="FAM-002", material_code="LAMP",
                          quota=1, actor="社工乙")
    plan_a = svc.plan_allocation(material_code="LAMP", location_code="LOC-A",
                                 quantity=1)
    plan_b = svc.plan_allocation(material_code="LAMP", location_code="LOC-A",
                                 quantity=1)
    svc.claim(serial_no="SN-A", operation_code="OP-0921", family_code="FAM-001",
              material_code="LAMP", location_code="LOC-A", quantity=1,
              actor="仓管员甲", plan=plan_a)
    try:
        svc.claim(serial_no="SN-B", operation_code="OP-0921",
                  family_code="FAM-002", material_code="LAMP",
                  location_code="LOC-A", quantity=1, actor="仓管员乙",
                  plan=plan_b)
    except ConflictError as e:
        print("仓管员乙收到冲突:", e)

    show("退回 / 报损 / 额度调整 / 调拨(均留审批人与原因)")
    svc.return_goods(serial_no="RT-1", claim_id=r["claim_id"], approver="主任王某",
                     reason="家庭重复领取退回", actor="仓管员甲",
                     items=[{"claim_item_id": r["items"][0]["claim_item_id"],
                             "quantity": 2}])
    svc.report_damage(serial_no="DM-1", material_code="WATER", batch_no="W-0901",
                      location_code="LOC-A", quantity=3, approver="主任王某",
                      reason="搬运破损", actor="仓管员甲")
    svc.adjust_quota(serial_no="AQ-1", operation_code="OP-0921",
                     family_code="FAM-002", material_code="WATER", new_quota=18,
                     approver="主任王某", reason="临时接收亲属", actor="社工乙")
    t = svc.create_transfer(serial_no="T-1", material_code="WATER",
                            batch_no="W-0915", from_location="LOC-A",
                            to_location="LOC-B", quantity=20,
                            approver="主任王某", reason="安置点补货",
                            actor="仓管员甲")
    print("调拨单", t["transfer_id"], "已创建(在途), 暂不完成")

    show("重启服务 → 未完成调拨与审计记录仍在")
    svc.close()
    svc = ReliefService(DB)
    print("未完成调拨:", svc.pending_transfers(MANAGER))
    print("审计条数:", len(svc.audit_log(MANAGER)))

    show("管理者视角: 批次库存 / 家庭领取历史 / 异常盘点")
    for row in svc.batch_inventory(MANAGER):
        print(" ", row["material_code"], row["batch_no"], row["location_code"],
              row["quantity"], "expired" if row["expired"] else "")
    print("FAM-001 领取历史:",
          [(h["material_code"], h["quantity"], h["status"])
           for h in svc.family_claim_history(MANAGER, family_code="FAM-001")])
    report = svc.anomaly_report(MANAGER)
    print("异常盘点: 账实差异", len(report["stock_discrepancies"]),
          "| 过期在库", len(report["expired_stock"]),
          "| 重复领取", len(report["repeated_family_claims"]),
          "| 在途调拨", len(report["pending_transfers"]))
    svc.close()


if __name__ == "__main__":
    main()
