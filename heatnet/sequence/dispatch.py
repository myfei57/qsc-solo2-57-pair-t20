"""调度下发编排。

调度把目标负荷折算成各站的开度需求，按偏差优先级排出计划，交给一次网按站独立
分配流量，再逐站下发。急站可抢占：急站先算先发，让位批按抢占后回读到的新总流量
重算；抢占模式按工况选择（先发急站再重算，或先全体重算再按优先级发）。总流量
回读缺失属于部分失败，此时不许抢占。流量基线可以按实测重新标定，标定之后联锁
门限同步变化。
"""

from __future__ import annotations

from ..ns.zone import ZoneRegistry
from ..primary.network import PrimaryNetwork
from ..station.batch import run_batch
from ..station.mapping import StationMapping
from ..station.preempt import MODES, MODE_URGENT_FIRST, run_preempt
from ..station.service import CancelToken, StationService


def dispatch_demand(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    network: PrimaryNetwork,
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
    urgent: list[str] | None = None,
    mode: str = MODE_URGENT_FIRST,
    readback=None,
) -> dict:
    """按偏差优先级下发一次调度，急站抢占、让位批按新总流量重算。

    没有点名急站时走单批普通路径；有点名急站时按 ``mode`` 指定的工况时序抢占，
    ``readback`` 为可调用对象，返回总流量回读值，返回 ``None`` 表示回读缺失。
    """

    if mode not in MODES:
        raise ValueError(f"未知抢占模式: {mode}")
    if urgent:
        report = run_preempt(
            service,
            mapping,
            zones,
            network,
            zone_names,
            demand_pct,
            target_c,
            urgent=list(urgent),
            mode=mode,
            readback=readback,
            cancel=cancel,
            dt=dt,
        )
    else:
        report = run_batch(
            service, mapping, zones, network, zone_names, demand_pct, target_c, cancel, dt
        )
    return {
        **report,
        "demand_pct": round(float(demand_pct), 2),
        "target_c": round(float(target_c), 2),
        "flow_t_h": round(network.flow_t_h(), 2),
        "threshold_t_h": round(network.threshold_t_h(), 2),
    }


def recalibrate_flow(network: PrimaryNetwork, measured_t_h: float) -> dict:
    """按实测流量重新标定一次网基线。"""

    baseline = network.recalibrate(measured_t_h)
    return {
        "baseline_t_h": round(baseline, 2),
        "threshold_t_h": round(network.threshold_t_h(), 2),
        "recalibrations": network.recalibrations(),
    }
