"""调度下发编排。

调度把目标负荷折算成各站的开度需求，再按工况决定怎么发：平稳工况走既有顺序
下发；急迫与紧急工况走抢占编排——急的先算先发，被抢的站让位并按新的总流量
重算。工况只看站偏差（规则定死，见 :mod:`heatnet.station.priority`），抢占
时序与回读约束写在 :mod:`heatnet.station.preempt`。流量基线可以按实测重新
标定，标定之后联锁门限同步变化。
"""

from __future__ import annotations

from ..ids import IdFactory
from ..ns.zone import ZoneRegistry
from ..primary.network import PrimaryNetwork
from ..station.batch import run_batch
from ..station.mapping import StationMapping
from ..station.preempt import (
    CRITICAL,
    STEADY,
    URGENT,
    DispatchBatch,
    run_preemptive,
)
from ..station.priority import deviation_c
from ..station.service import CancelToken, StationService
from ..station.station import HeatStation


def dispatch_condition(network: PrimaryNetwork, stations: list[HeatStation]) -> str:
    """按站偏差判定工况：平稳 / 急迫 / 紧急（定死，只看偏差）。"""

    limits = network.limits
    max_dev = max((deviation_c(station) for station in stations), default=0.0)
    if max_dev >= limits.critical_deviation_c:
        return CRITICAL
    if max_dev >= limits.urgent_deviation_c:
        return URGENT
    return STEADY


def dispatch_demand(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    network: PrimaryNetwork,
    stations: dict[str, HeatStation],
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
    run_id: str = "",
    batch_ids: IdFactory | None = None,
    now: float = 0.0,
) -> dict:
    """按分区下发一次调度：先判工况，平稳走顺序下发，急迫/紧急走抢占编排。"""

    names = [station for zone in zone_names for station in mapping.stations_in(zone)]
    condition = dispatch_condition(network, [stations[name] for name in names])
    if condition != STEADY:
        report = run_preemptive(
            service,
            mapping,
            zones,
            network,
            stations,
            zone_names,
            demand_pct,
            target_c,
            condition,
            run_id,
            batch_ids if batch_ids is not None else IdFactory("batch"),
            now,
            cancel,
            dt,
        )
    else:
        report = run_batch(
            service, mapping, zones, network, zone_names, demand_pct, target_c, cancel, dt
        )
        ids = batch_ids if batch_ids is not None else IdFactory("batch")
        batch = DispatchBatch(
            batch_id=ids.next(),
            run_id=run_id,
            seq=0,
            kind="sequential",
            strategy="sequential",
            stations=tuple(flow["station"] for flow in report["flows"]),
            flows=tuple(dict(flow) for flow in report["flows"]),
            flow_basis_t_h=network.baseline_t_h(),
            status="sent" if report["ok"] else "aborted",
            preempted_by="",
            preempts=(),
            reason="平稳工况顺序下发",
            ts=now,
        )
        report.update(
            partial=bool(report["adjusted"]) and not report["ok"],
            condition=STEADY,
            strategy="sequential",
            skipped=[],
            batches=[batch.as_dict()],
            preemptions=[],
            readbacks=[],
            urgent=[],
            audit_events=[
                {
                    "action": "dispatch_batch",
                    "severity": "notice" if report["ok"] else "warning",
                    "message": f"下发批次 {batch.batch_id}（sequential）：平稳工况顺序下发",
                    "payload": {"batch": batch.as_dict()},
                }
            ],
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
