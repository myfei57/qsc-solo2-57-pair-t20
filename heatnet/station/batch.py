"""批量调节编排。

先按偏差优先级排出下发顺序并算出计划，再交给一次网按站独立分配流量，最后逐站
调节。流量分配与阀门动作分两步做，是为了让"计划里的流量"与"执行后的阀位"可以
分别核对。

优先级口径与抢占下发保持同一条：按偏差（目标供温与实际供温差距的绝对值）降序，
偏差相同按站名升序；站序不参与。抢占编排见 :mod:`heatnet.station.preempt`。
"""

from __future__ import annotations

from ..primary.network import PrimaryNetwork
from ..primary.order import deviation_order
from ..ns.zone import ZoneRegistry
from .mapping import StationMapping
from .preempt import MODE_URGENT_FIRST, run_preempt
from .service import CancelToken, StationService


def build_plan(
    mapping: StationMapping,
    zones: ZoneRegistry,
    zone_names: list[str],
    demand_pct: float,
    deviations: dict[str, float],
) -> list[tuple[str, float]]:
    """按各站偏差排出调节计划，同时带上开度需求。

    ``deviations`` 必须由调用方在规划瞬间按"目标温度 − 当前供温"的绝对值冻结
    后传入；排序口径在此定死为偏差降序、站名升序兜底，与抢占下发完全一致。
    """

    peak = max((zones.get(name).setpoint_c for name in zone_names), default=0.0)
    rows: list[tuple[str, float, float]] = []
    for zone_name in zone_names:
        share = 1.0 if peak <= 0 else max(0.2, zones.get(zone_name).setpoint_c / peak)
        demand = max(0.0, min(100.0, float(demand_pct) * share))
        for station in mapping.stations_in(zone_name):
            rows.append((station, demand, float(deviations.get(station, 0.0))))
    ordered = deviation_order([(station, deviation) for station, _, deviation in rows])
    demand_by_station = {station: demand for station, demand, _ in rows}
    return [(station, demand_by_station[station]) for station, _ in ordered]


def run_batch(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    network: PrimaryNetwork,
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
) -> dict:
    """算流量、下发、汇总结果（无急站的普通批，委托抢占编排执行）。"""

    return run_preempt(
        service,
        mapping,
        zones,
        network,
        zone_names,
        demand_pct,
        target_c,
        urgent=None,
        mode=MODE_URGENT_FIRST,
        readback=None,
        cancel=cancel,
        dt=dt,
    )
