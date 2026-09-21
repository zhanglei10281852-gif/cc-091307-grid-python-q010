"""领域错误类型。

所有业务拒绝都抛出 ReliefError 的子类，调用方（终端/网关）可据此映射
为 4xx/409 等响应，而不会把库存扣成负数或产生半完成状态。
"""


class ReliefError(Exception):
    """物资领用服务领域错误基类。"""


class ValidationError(ReliefError):
    """请求参数不合法（数量非正、缺少审批人/原因等）。"""


class NotFoundError(ReliefError):
    """引用的实体不存在（批次、家庭、行动批次、调拨单等）。"""


class PermissionDeniedError(ReliefError):
    """角色无权执行该操作或查看该数据。"""


class EligibilityError(ReliefError):
    """领取资格失效：家庭被暂停/过期、行动批次已结束、无额度记录。"""


class InsufficientStockError(ReliefError):
    """库存不足，拒绝发放/报损/调拨。"""


class InsufficientQuotaError(ReliefError):
    """发放额度不足，拒绝发放。"""


class ConflictError(ReliefError):
    """并发冲突：同一批物资被其他仓管员抢先扣减，本次操作未生效。"""


class IdempotencyConflictError(ConflictError):
    """同一流水号被用于不同的请求负载。"""


class StateError(ReliefError):
    """状态机冲突：如对已完成/已取消的调拨单再次操作。"""
