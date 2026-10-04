"""带优先级与抢占的下发编排。

优先级口径定死为**按偏差**：以下发前瞬间各站二次侧供温与目标温度的偏差绝对
值为准，偏差大的先算先发，偏差相同按站名升序兜底；站序（拓扑序／登记序）一律
不参与排序。急站由调用方显式点名，点名后整体组成急站批，排在普通批之前。

抢占支持两种工况模式，顺序不可互换：

- ``urgent_first``（先发急站再重算其余站）：先按旧总流量给急站批算流量并发急
  站，发完回读总流量，再按回读到的新总流量重算让位批，最后发让位批；
- ``recompute_first``（先全体重算再按优先级发）：先回读总流量，按新总流量一次
  性重算两批（急站从总量里先取，让位批分剩余量），重算完成后再按优先级先发急
  站、后发让位批。

两种模式都把"总流量回读缺失"定为部分失败：此时一律不许抢占——``urgent_first``
下急站批已经发出的动作保留，但让位批整批发回（不重算、不下发）；
``recompute_first`` 下任何站都不许动。批次、流量与审计三者口径一致：报告里每
个站的流量必属于且仅属于一个批次；``recompute_first`` 下两批流量之和等于回读
总流量，``urgent_first`` 下让位批按回读新总流量整额重算、管网最终值与之一致。
"""

from __future__ import annotations

from ..primary.flow import (
    PRIORITY_NORMAL,
    PRIORITY_URGENT,
    StationFlow,
    allocate_flows,
)
from ..primary.network import PrimaryNetwork
from ..ns.zone import ZoneRegistry
from .mapping import StationMapping
from .service import CancelToken, StationService

MODE_URGENT_FIRST = "urgent_first"
MODE_RECOMPUTE_FIRST = "recompute_first"
MODES = (MODE_URGENT_FIRST, MODE_RECOMPUTE_FIRST)

READBACK_MISSING = "flow_readback_missing"


def station_deviation(station, target_c: float) -> float:
    """偏差口径：目标温度与当前供温之差的绝对值，在规划瞬间冻结。"""

    return abs(float(target_c) - station.supply_c())


def _ordered_plan(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
) -> list[tuple[str, float, float]]:
    """算出每个站的开度需求，并按冻结偏差排出全站顺序。"""

    peak = max((zones.get(name).setpoint_c for name in zone_names), default=0.0)
    entries: list[tuple[str, float, float]] = []
    for zone_name in zone_names:
        share = 1.0 if peak <= 0 else max(0.2, zones.get(zone_name).setpoint_c / peak)
        demand = max(0.0, min(100.0, float(demand_pct) * share))
        for station_name in mapping.stations_in(zone_name):
            deviation = station_deviation(service.get(station_name), target_c)
            entries.append((station_name, demand, deviation))
    return sorted(entries, key=lambda item: (-item[2], item[0]))


def _demands(plan: list[tuple[str, float, float]]) -> list[tuple[str, float]]:
    return [(station, demand) for station, demand, _ in plan]


def _urgent_basis(baseline: float, urgent_count: int, total_count: int) -> float:
    """急站批的批次总流量：旧总流量里属于这些急站的那一份。

    普通下发时每站拿 ``基线 × 开度占比 / 总站数``；急站批只含若干个站，为了让
    单个急站的流量与普通下发同口径，批次总流量取 ``基线 × 急站数 / 总站数``。
    """

    return baseline * urgent_count / max(1, total_count)


def _batch_record(
    batch_id: str,
    priority: str,
    mode: str,
    flows: list[StationFlow],
    dispatched: bool,
    skipped: list[str] | None = None,
    *,
    preempted: bool = False,
    preempted_by: str | None = None,
    based_on_total_t_h: float | None = None,
) -> dict:
    stations = [flow.station for flow in flows]
    return {
        "batch_id": batch_id,
        "priority": priority,
        "mode": mode,
        "stations": stations,
        "flows": [flow.as_dict() for flow in flows],
        "total_t_h": round(sum(flow.flow_t_h for flow in flows), 2),
        "dispatched": dispatched,
        "skipped": list(skipped or []),
        "preempted": preempted,
        "preempted_by": preempted_by,
        "based_on_total_t_h": None if based_on_total_t_h is None else round(based_on_total_t_h, 2),
    }


def _default_readback(network: PrimaryNetwork):
    def read() -> float | None:
        total = network.flow_t_h()
        return total if total > 0 else None

    return read


def run_preempt(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    network: PrimaryNetwork,
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
    urgent: list[str] | None = None,
    mode: str = MODE_URGENT_FIRST,
    readback=None,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
) -> dict:
    """按偏差优先级下发，急站抢占、让位批按新总流量重算。"""

    if mode not in MODES:
        raise ValueError(f"未知抢占模式: {mode}")
    read_total = readback if callable(readback) else _default_readback(network)

    ordered = _ordered_plan(service, mapping, zones, zone_names, demand_pct, target_c)
    planned_stations = [station for station, _, _ in ordered]
    urgent_names = list(urgent or [])
    unknown = [name for name in urgent_names if name not in set(planned_stations)]
    if unknown:
        raise ValueError(f"急站不在本次下发名单: {unknown}")
    urgent_set = set(urgent_names)
    urgent_plan = [(s, d, dev) for s, d, dev in ordered if s in urgent_set]
    rest_plan = [(s, d, dev) for s, d, dev in ordered if s not in urgent_set]

    steps: list[dict] = []
    violations: list[str] = []
    adjusted: list[str] = []
    batches: list[dict] = []
    preemptions: list[dict] = []
    flows_flat: list[dict] = []
    zone_of = mapping.zones.index()
    readback_t_h: float | None = None
    partial_failure = False

    def send(batch_id: str, priority: str, plan_rows: list[tuple[str, float, float]]) -> dict:
        names = [station for station, _, _ in plan_rows]
        label = "urgent" if priority == PRIORITY_URGENT else "yielded"
        return service.adjust_stations(zone_of, names, target_c, cancel, dt, label)

    def allocate_step(batch_id: str, flows: list[StationFlow]) -> None:
        for flow in flows:
            steps.append(
                {
                    "component": "primary",
                    "action": "allocate_flow",
                    "detail": {**flow.as_dict(), "batch_id": batch_id},
                }
            )

    if not urgent_plan:
        # 没有急站：单批普通下发，沿用基线总流量，不需要回读与抢占。
        flows = allocate_flows(network.baseline_t_h(), _demands(ordered), PRIORITY_NORMAL)
        allocate_step("b1", flows)
        network.set_flow(sum(flow.flow_t_h for flow in flows))
        network.count_dispatch()
        report = send("b1", PRIORITY_NORMAL, ordered)
        steps.extend(report["steps"])
        violations.extend(report["violations"])
        adjusted.extend(report["adjusted"])
        batches.append(
            _batch_record(
                "b1", PRIORITY_NORMAL, mode, flows, bool(report["adjusted"]), report["skipped"],
                based_on_total_t_h=network.baseline_t_h(),
            )
        )
        flows_flat = [flow.as_dict() for flow in flows]
        partial_failure = not report["ok"]
    elif mode == MODE_URGENT_FIRST:
        # 时序：急站按旧总量算 → 发急站 → 回读 → 按新总量重算让位批 → 发让位批。
        baseline = network.baseline_t_h()
        # 急站先算先发：它拿的是旧总流量里按全站摊薄的自己那一份，与普通下发
        # 同口径；它抢的是先发权，不是额外流量。
        urgent_flows = allocate_flows(
            _urgent_basis(baseline, len(urgent_plan), len(ordered)),
            _demands(urgent_plan),
            PRIORITY_URGENT,
        )
        urgent_sum = sum(flow.flow_t_h for flow in urgent_flows)
        allocate_step("b1", urgent_flows)
        network.set_flow(urgent_sum)
        urgent_report = send("b1", PRIORITY_URGENT, urgent_plan)
        steps.extend(urgent_report["steps"])
        violations.extend(urgent_report["violations"])
        adjusted.extend(urgent_report["adjusted"])
        urgent_dispatched = bool(urgent_report["adjusted"])
        batches.append(
            _batch_record(
                "b1", PRIORITY_URGENT, mode, urgent_flows, urgent_dispatched,
                urgent_report["skipped"], based_on_total_t_h=baseline,
            )
        )
        flows_flat = [flow.as_dict() for flow in urgent_flows]

        if not urgent_report["ok"]:
            # 急站批中途取消：回读与让位批一律不允许发生（时序不能乱），
            # 直接以部分失败结束，让位批整批发回。
            partial_failure = True
            blocked = allocate_flows(baseline, _demands(rest_plan), PRIORITY_NORMAL)
            batches.append(
                _batch_record(
                    "b2", PRIORITY_NORMAL, mode, blocked, False,
                    [flow.station for flow in blocked], based_on_total_t_h=baseline,
                )
            )
            if urgent_dispatched:
                network.count_dispatch()
        else:
            measured = read_total()
            readback_t_h = measured
            if measured is None:
                # 部分失败：回读缺失，不许抢占，让位批整批不重算、不下发。
                partial_failure = True
                violations.append(READBACK_MISSING)
                blocked = allocate_flows(baseline, _demands(rest_plan), PRIORITY_NORMAL)
                batches.append(
                    _batch_record(
                        "b2", PRIORITY_NORMAL, mode, blocked, False,
                        [flow.station for flow in blocked], based_on_total_t_h=baseline,
                    )
                )
                network.count_dispatch()
            else:
                # 让位批按新总流量整额分完（急站已在前面单独取走，这里不重复扣减）。
                rest_flows = allocate_flows(
                    float(measured), _demands(rest_plan), PRIORITY_NORMAL, fill_total=True
                )
                allocate_step("b2", rest_flows)
                # 写回管网的总流量只以回读新总流量为准：急站那份已包含在新总流量里，
                # 不能再把抢占前的 urgent_sum 叠上去，否则总量被凭空放大。
                network.set_flow(sum(flow.flow_t_h for flow in rest_flows))
                rest_report = send("b2", PRIORITY_NORMAL, rest_plan)
                steps.extend(rest_report["steps"])
                violations.extend(rest_report["violations"])
                adjusted.extend(rest_report["adjusted"])
                batches.append(
                    _batch_record(
                        "b2", PRIORITY_NORMAL, mode, rest_flows, bool(rest_report["adjusted"]),
                        rest_report["skipped"], preempted=True, preempted_by="b1",
                        based_on_total_t_h=float(measured),
                    )
                )
                flows_flat.extend(flow.as_dict() for flow in rest_flows)
                preemptions.append(
                    {
                        "preempted_batch": "b2",
                        "by_batch": "b1",
                        "mode": mode,
                        "urgent_stations": [row[0] for row in urgent_plan],
                        "yielded_stations": [row[0] for row in rest_plan],
                        "old_total_t_h": round(baseline, 2),
                        "new_total_t_h": round(float(measured), 2),
                    }
                )
                network.count_dispatch()
                partial_failure = bool(rest_report["violations"])
    else:
        # 时序：先回读 → 全体重算（急站先取总量，让位批分剩余量）→ 按优先级发。
        measured = read_total()
        readback_t_h = measured
        if measured is None:
            # 部分失败：回读缺失，重算无法进行，一批都不许发。
            partial_failure = True
            violations.append(READBACK_MISSING)
            blocked_urgent = allocate_flows(network.baseline_t_h(), _demands(urgent_plan), PRIORITY_URGENT)
            blocked_rest = allocate_flows(network.baseline_t_h(), _demands(rest_plan), PRIORITY_NORMAL)
            batches.append(
                _batch_record("b1", PRIORITY_URGENT, mode, blocked_urgent, False,
                              [flow.station for flow in blocked_urgent])
            )
            batches.append(
                _batch_record("b2", PRIORITY_NORMAL, mode, blocked_rest, False,
                              [flow.station for flow in blocked_rest])
            )
        else:
            total = float(measured)
            # 全体重算：总流量按各站开度占比一次性切给两批，急站在排序上先取、
            # 让位批取剩余占比；两批流量之和恰好等于回读总流量。
            plans = _demands(urgent_plan) + _demands(rest_plan)
            all_flows = allocate_flows(total, plans, PRIORITY_NORMAL, fill_total=True)
            urgent_flows = [
                flow.with_priority(PRIORITY_URGENT)
                for flow in all_flows[: len(urgent_plan)]
            ]
            rest_flows = all_flows[len(urgent_plan) :]
            urgent_sum = sum(flow.flow_t_h for flow in urgent_flows)
            remaining = total - urgent_sum
            allocate_step("b1", urgent_flows)
            allocate_step("b2", rest_flows)
            network.set_flow(sum(flow.flow_t_h for flow in all_flows))
            urgent_report = send("b1", PRIORITY_URGENT, urgent_plan)
            steps.extend(urgent_report["steps"])
            violations.extend(urgent_report["violations"])
            adjusted.extend(urgent_report["adjusted"])
            rest_report = send("b2", PRIORITY_NORMAL, rest_plan)
            steps.extend(rest_report["steps"])
            violations.extend(rest_report["violations"])
            adjusted.extend(rest_report["adjusted"])
            urgent_dispatched = bool(urgent_report["adjusted"])
            batches.append(
                _batch_record("b1", PRIORITY_URGENT, mode, urgent_flows, urgent_dispatched,
                              urgent_report["skipped"], based_on_total_t_h=total)
            )
            batches.append(
                _batch_record("b2", PRIORITY_NORMAL, mode, rest_flows, bool(rest_report["adjusted"]),
                              rest_report["skipped"], preempted=True, preempted_by="b1",
                              based_on_total_t_h=remaining)
            )
            flows_flat = [flow.as_dict() for flow in urgent_flows]
            flows_flat.extend(flow.as_dict() for flow in rest_flows)
            preemptions.append(
                {
                    "preempted_batch": "b2",
                    "by_batch": "b1",
                    "mode": mode,
                    "urgent_stations": [row[0] for row in urgent_plan],
                    "yielded_stations": [row[0] for row in rest_plan],
                    "old_total_t_h": round(network.baseline_t_h(), 2),
                    "new_total_t_h": round(total, 2),
                }
            )
            if urgent_dispatched:
                network.count_dispatch()
            partial_failure = bool(urgent_report["violations"] or rest_report["violations"])

    # 管网最终总流量就是下发后的实际总口径。各批流量是在各自基线（旧总量／
    # 回读新总量）上算的，直接跨批相加会把急站那份重复计一次，因此不做求和，
    # 以最后一次写回管网的值为准，保证批次、流量、审计三处对得上账。
    dispatched_total = round(network.flow_t_h(), 2)
    return {
        "ok": not violations,
        "partial_failure": partial_failure,
        "steps": steps,
        "violations": violations,
        "planned": len(ordered),
        "adjusted": adjusted,
        "flows": flows_flat,
        "batches": batches,
        "preemptions": preemptions,
        "mode": mode,
        "priority_rule": "deviation",
        "readback_total_t_h": None if readback_t_h is None else round(readback_t_h, 2),
        "dispatched_total_t_h": dispatched_total,
    }
