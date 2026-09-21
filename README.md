# 社区应急物资领用

该项目服务于网格员和社区管理工作，负责社区应急物资领用相关信息的规范化处理与留痕。

运行环境：Python 3.11。代码位于 `src` 目录，配置与数据文件应按部署环境提供。

## 功能与设计

`src/service.py` 实现 `ReliefService`，针对"多小组同时登记领用、事后对不上库存、家庭重复领取无法确认"的问题：

| 需求 | 实现 |
| --- | --- |
| 物资批次 / 有效期 / 存放点 | `batches` + `stock` 表，全部出入库记入 `stock_ledger` 台账 |
| 领取资格与发放额度 | `entitlements` 按 行动批次 + 家庭标识 + 物资 唯一授权，含生效窗口 |
| 先到期先发 | `plan_allocation` / `fefo_suggestion` 按有效期升序出库，过期批次禁发 |
| 审批留痕 | 额度调整 / 退回 / 报损 / 跨点调拨强制填写审批人与原因，写入 `audits` |
| 库存不足 / 资格失效 | 分别抛出 `InsufficientStockError` / `EligibilityError` |
| 断网补传幂等 | 所有变更操作按终端流水号落 `idempotency_keys`，重复提交返回首次结果 |
| 并发不超发 | 库存行版本号乐观锁，竞争最后一批时后到者收到 `ConflictError` |
| 按角色查询 | `Viewer(role, location)`：仓管员仅见本存放点，管理者可查批次库存、家庭领取历史、异常盘点、审计日志 |
| 重启保留 | SQLite 持久化，未完成调拨、审计与幂等记录重启后继续存在 |

异常盘点（`anomaly_report`）覆盖：账实差异（库存行 vs 台账合计）、过期在库、额度超发、同一家庭重复领取、在途滞留调拨。

## 运行

```bash
python3 -m pytest tests/   # 测试
python3 demo.py            # 端到端演示
```

## 用法示例

```python
from src.service import ReliefService, Viewer, Role

svc = ReliefService("relief.db")          # ":memory:" 可用于测试
svc.add_location("LOC-A", "中心仓库")
svc.add_material("WATER", "饮用水", "瓶")
svc.add_operation("OP-0921", "9月临时安置行动")
svc.add_family("FAM-001", "张某", 4)

svc.receive_stock(serial_no="IN-1", material_code="WATER", batch_no="W-0901",
                  expiry_date="2026-10-01", location_code="LOC-A",
                  quantity=50, actor="仓管员甲")
svc.grant_entitlement(serial_no="G-1", operation_code="OP-0921",
                      family_code="FAM-001", material_code="WATER",
                      quota=24, valid_until="2026-09-30", actor="社工乙")

# 先到期先发建议 → 登记领用(流水号幂等)
plan = svc.plan_allocation(material_code="WATER", location_code="LOC-A", quantity=12)
svc.claim(serial_no="SN-0001", operation_code="OP-0921", family_code="FAM-001",
          material_code="WATER", location_code="LOC-A", quantity=12,
          actor="仓管员甲", plan=plan)

manager = Viewer(Role.MANAGER)
svc.batch_inventory(manager)                        # 批次库存
svc.family_claim_history(manager, family_code="FAM-001")  # 家庭领取历史
svc.anomaly_report(manager)                         # 异常盘点
```
