"""一次网流量分配。

每个站的目标流量只跟它自己的目标开度、批次总流量与本批站数有关，因此必须在
循环里用局部变量计算；把中间结果写回 ``network.flow_t_h`` 再作为下一站的起点，
越靠后的站流量越大。全网总流量在全部站算完之后一次性写回。

抢占下发把一批拆成"急站批"和"让位批"，两批各自按本批总流量与本批站数独立
分配，不允许把急站批的中间累加值当成让位批的起点——让位批必须按抢占后回读
到的新总流量重算，这一顺序由抢占编排负责保证。
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from .network import PrimaryNetwork

PRIORITY_NORMAL = "normal"
PRIORITY_URGENT = "urgent"


@dataclass(frozen=True)
class StationFlow:
    """一个站的目标流量。"""

    station: str
    target_pct: float
    flow_t_h: float
    priority: str = PRIORITY_NORMAL

    def with_priority(self, priority: str) -> "StationFlow":
        """返回同流量、不同优先级标记的副本。"""

        return replace(self, priority=priority)

    def as_dict(self) -> dict:
        return {
            "station": self.station,
            "target_pct": round(self.target_pct, 2),
            "flow_t_h": round(self.flow_t_h, 2),
            "priority": self.priority,
        }


def allocate_flows(
    total_t_h: float,
    plan: list[tuple[str, float]],
    priority: str = PRIORITY_NORMAL,
    *,
    fill_total: bool = False,
) -> list[StationFlow]:
    """按给定批次总流量给一批站独立分配目标流量，不写网络状态。

    批次总流量由调用方给定：普通批取全网基线，让位批取抢占后回读到的新总流量。
    每个站只拿自己的局部量，与同批其他站互不滚动。两种口径：

    - ``fill_total=False``（默认）：总流量按站数摊薄，每站拿
      ``总流量 × 开度占比 / 站数``，整批实际取用不超过总流量。无抢占的常规
      下发沿用这一口径；
    - ``fill_total=True``：先算各站开度占比之和，再把整批总流量按占比分完，
      整批流量之和恰好等于给定总流量。让位批"按新总流量重算"必须用这一口径，
      否则重算结果与回读总流量对不上账。
    """

    shares = [max(0.0, min(1.0, float(target_pct) / 100.0)) for _, target_pct in plan]
    share_sum = sum(shares)
    count = max(1, len(plan))
    flows: list[StationFlow] = []
    for (station, target_pct), share in zip(plan, shares):
        if fill_total:
            target = max(0.0, float(total_t_h)) * (share / share_sum if share_sum > 0 else 0.0)
        else:
            target = max(0.0, float(total_t_h)) * share / count
        flows.append(
            StationFlow(station=str(station), target_pct=float(target_pct), flow_t_h=target, priority=priority)
        )
    return flows


def batch_flows(network: PrimaryNetwork, plan: list[tuple[str, float]]) -> list[StationFlow]:
    """按站独立计算目标流量，最后一次性写回全网总流量。"""

    flows = allocate_flows(network.baseline_t_h(), plan, PRIORITY_NORMAL)
    network.set_flow(sum(flow.flow_t_h for flow in flows))
    return flows
