"""一次网。

一次网承接热源，把流量分给各换热站。流量基线可以被重新标定，调度下发前先按
偏差把站的优先级排定；每个站的目标流量必须独立算出来，不允许用上一站的结果
往下滚。抢占下发时，让位批必须按回读到的新总流量重算。
"""

from __future__ import annotations

from .flow import (
    PRIORITY_NORMAL,
    PRIORITY_URGENT,
    StationFlow,
    allocate_flows,
    batch_flows,
)
from .network import PrimaryNetwork
from .order import deviation_order, split_urgent
from .state import NetworkSnapshot

__all__ = [
    "NetworkSnapshot",
    "PRIORITY_NORMAL",
    "PRIORITY_URGENT",
    "PrimaryNetwork",
    "StationFlow",
    "allocate_flows",
    "batch_flows",
    "deviation_order",
    "split_urgent",
]
