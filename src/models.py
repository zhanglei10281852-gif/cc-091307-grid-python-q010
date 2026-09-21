"""角色与操作者模型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# 角色：管理者（社区/仓库管理岗）与仓管员（现场发放岗）。
ROLE_MANAGER = "manager"
ROLE_KEEPER = "keeper"
ALL_ROLES = (ROLE_MANAGER, ROLE_KEEPER)


@dataclass(frozen=True)
class Actor:
    """一次操作/查询的执行者。

    role:     manager 可查看与操作全部存放点；
              keeper  只能操作、查看本存放点，且无家庭历史/异常盘点权限。
    site_id:  keeper 必须绑定存放点；manager 为 None。
    """

    name: str
    role: str
    site_id: Optional[int] = None

    def __post_init__(self):
        if self.role not in ALL_ROLES:
            raise ValueError(f"未知角色: {self.role!r}")
        if self.role == ROLE_KEEPER and self.site_id is None:
            raise ValueError("仓管员必须绑定存放点 site_id")
