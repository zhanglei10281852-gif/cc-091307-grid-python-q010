# 社区应急物资领用

面向网格员和社区管理工作的应急物资领用服务，解决临时安置行动中"多小组同时登记领用、
事后对不上库存、无法确认受助家庭是否重复领取"的问题：所有发放按行动批次与家庭标识
校验额度、按先到期先发出库，关键操作全部留痕可审计。

运行环境：Python 3.11（仅标准库，无第三方依赖）。代码位于 `src` 目录。

## 快速开始

```bash
python3 -m unittest discover -s tests   # 运行测试（28 个用例）
python3 demo.py                          # 端到端演示：发放/补传/调拨/并发/审计
```

```python
from src import ReliefService, Actor, ROLE_MANAGER, ROLE_KEEPER

svc = ReliefService("relief.db")                 # SQLite 持久化，重启数据不丢
admin = Actor("张管理", ROLE_MANAGER)
site = svc.register_site(serial_no="S1", actor=admin, name="一号仓")["site_id"]
keeper = Actor("仓管甲", ROLE_KEEPER, site_id=site)

svc.add_batch(serial_no="B1", actor=admin, item="饮用水",
              site_id=site, quantity=50, expiry_date="2026-09-25")
camp = svc.open_campaign(serial_no="C1", actor=admin, code="ACT-2026-09")["campaign_id"]
svc.register_household(serial_no="H1", actor=admin, household_id="HH-001", members=3)
svc.set_quota(serial_no="Q1", actor=admin, campaign_id=camp, household_id="HH-001",
              item="饮用水", amount=12, approver="李审批", reason="按人定量")

svc.issue(serial_no="TERM-0001", actor=keeper, campaign_id=camp,
          household_id="HH-001", item="饮用水", site_id=site, quantity=10)
```

## 设计要点

| 需求 | 实现 |
| --- | --- |
| 批次/有效期/存放点 | `batches` 按到货批次管理，含 `expiry_date`、`site_id`、乐观锁 `version` |
| 领取资格与额度 | `households.status/eligible_until` + `quotas`（行动批次×家庭×物资的剩余可领数） |
| 校验可领数量 | `issue()` 依次校验行动批次有效、家庭资格有效、额度原子扣减（`UPDATE ... amount >= ?`） |
| 先到期先发 | `suggest_outbound()` 给出 FEFO 建议；发放按有效期升序（无有效期排最后）分配批次，过期批次不参与 |
| 审批留痕 | 额度调整/退回/报损/调拨/盘点/资格变更强制 `approver`+`reason`，写入追加式 `audit_log` |
| 库存不足/资格失效 | 抛 `InsufficientStockError` / `EligibilityError` / `InsufficientQuotaError`，事务整体回滚 |
| 断网补传幂等 | 所有写操作要求 `serial_no`；`idempotency_keys` 记录请求哈希与响应，重放返回首次结果（`idempotent_replay: true`），同号不同负载抛 `IdempotencyConflictError`；失败操作不占用流水号 |
| 并发不超发 | 批次行条件更新（`version`+`quantity>=?`）；被抢先时在写锁内按最新库存重规划，仅当真的不足（最后一件被抢）才抛 `ConflictError`——库存充足不会误冲突 |
| 按角色查询 | `manager`：批次库存、家庭领取历史、异常盘点、审计日志、全部在途调拨；`keeper`：仅本存放点库存/发放/调拨 |
| 异常盘点 | `anomaly_report()` 汇总报损记录、账实差异（`stocktakes`）、已过期未发放库存 |
| 重启持久化 | SQLite(WAL) 文件库；`PENDING` 调拨与审计记录重启后继续存在，可继续接收/取消 |

## 接口概览（`src.service.ReliefService`）

- 建档：`register_site` / `add_batch` / `open_campaign` / `close_campaign` /
  `register_household` / `set_household_status` / `set_quota`
- 现场作业：`suggest_outbound`（FEFO 建议）/ `issue` / `return_items` /
  `report_damage` / `initiate_transfer` / `complete_transfer` / `cancel_transfer` /
  `record_stocktake`
- 查询：`batch_inventory` / `quota_view` / `household_history` /
  `pending_transfers` / `anomaly_report` / `audit_trail`

所有写接口均为关键字参数且必须带 `serial_no`；返回值为可 JSON 序列化的 dict，
可直接对接 HTTP 网关或现场终端。错误体系见 `src/errors.py`，均为 `ReliefError`
子类，便于映射为 4xx/409 响应。

## 目录结构

```
src/
  __init__.py   # 包导出
  models.py     # Actor 与角色（manager/keeper）
  errors.py     # 领域错误类型
  db.py         # SQLite(WAL) 连接与事务、建表
  service.py    # ReliefService：全部业务逻辑
tests/test_relief_service.py  # 28 个用例：FEFO/资格/幂等/并发/调拨/盘点/RBAC/重启
demo.py                        # 端到端演示
```
