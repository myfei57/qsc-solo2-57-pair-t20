"""一次网流量分配。

每个站的目标流量只跟它自己的目标开度与给定的总流量基数有关，因此必须在循环里用
局部变量计算；把中间结果写回 ``network.flow_t_h`` 再作为下一站的起点，越靠后的站
流量越大。全网总流量在一批站全部算完之后一次性写回。

``plan_flows`` 是纯计算（给基数、给站数，不写回网络），``allocate_flows`` 在它
之上把合计写回全网总流量；抢占重算用前者按回读到的新总流量算，顺序下发用后者。
"""

from __future__ import annotations

from dataclasses import dataclass

from .network import PrimaryNetwork


@dataclass(frozen=True)
class StationFlow:
    """一个站的目标流量。"""

    station: str
    target_pct: float
    flow_t_h: float

    def as_dict(self) -> dict:
        return {
            "station": self.station,
            "target_pct": round(self.target_pct, 2),
            "flow_t_h": round(self.flow_t_h, 2),
        }


def plan_flows(
    plan: list[tuple[str, float]], basis_t_h: float, count: int | None = None
) -> list[StationFlow]:
    """按给定总流量基数为计划里的每个站计算目标流量（纯计算，不写回网络）。

    ``count`` 是全网参与分配的站数：急站先发、其余站让位重算时，每一份仍然按
    全网站数折算，保证被抢与让位的两份计划口径一致。
    """

    share_of = max(1, int(count) if count else len(plan))
    flows: list[StationFlow] = []
    for station, target_pct in plan:
        share = max(0.0, min(1.0, float(target_pct) / 100.0))
        target = float(basis_t_h) * share / share_of
        flows.append(
            StationFlow(station=str(station), target_pct=float(target_pct), flow_t_h=target)
        )
    return flows


def allocate_flows(
    network: PrimaryNetwork, plan: list[tuple[str, float]], basis_t_h: float, count: int | None = None
) -> list[StationFlow]:
    """按给定基数分配流量，并把合计一次性写回全网总流量。"""

    flows = plan_flows(plan, basis_t_h, count)
    network.set_flow(sum(flow.flow_t_h for flow in flows))
    return flows


def batch_flows(network: PrimaryNetwork, plan: list[tuple[str, float]]) -> list[StationFlow]:
    """按站独立计算目标流量，最后一次性写回全网总流量。"""

    return allocate_flows(network, plan, network.baseline_t_h())
