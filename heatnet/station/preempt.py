"""一次网批量下发的抢占编排。

优先级定死为按偏差（见 :mod:`heatnet.station.priority`）：急的站先算先发，被抢
的站让位，让位以后它的计划按新的总流量重算。工况决定抢占时序，两条策略也只按
工况选，不开放配置：

- 紧急工况（最大偏差越紧急门限）：**先发急站再重算其余站**——急站按基线流量与
  自己的真实需求先走，回读到新的总流量之后，再给让位的站重算；
- 急迫工况（最大偏差越急迫门限）：**先全体重算再按优先级发**——先回读总流量，
  全体在新总流量上重算，再按偏差顺序发；
- 平稳工况不进入本模块，走既有顺序下发（见 :func:`heatnet.station.batch.run_batch`）。

时序写死：重算要么发生在"急站发完且总流量回读成功"之后（紧急），要么发生在
"任何一站发出之前"（急迫），两者都只在回读成功的前提下成立。总流量回读缺失
属于部分失败：已发出的保持，未发出的不再发，也不许再抢占重算——宁可停在半路
上，也不能拿猜出来的总流量给站改计划。

每一次抢占都落成 :class:`DispatchBatch` 记录：原计划批次（``base``）被谁抢了、
急站批次（``urgent``）按什么基数算的、重算批次（``recomputed``）按什么基数
重算的，控制台与审计读的是同一份记录。

全网总流量的口径：原计划按基线算出各站计划流量；每发完一个批次，该批次各站的
新计划替换旧计划，全网总流量按各站当前计划之和一次性写回。急站按自己的真实
需求（阀门需求开度）折算流量，因此急站先发之后总流量必然变化，让位站的重算
基数（回读值）与原计划基数（基线）一定对得上账。
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from ..ids import IdFactory
from ..ns.zone import ZoneRegistry
from ..primary.flow import StationFlow, plan_flows
from ..primary.network import PrimaryNetwork
from .batch import build_plan
from .mapping import StationMapping
from .priority import urgency_order, urgent_stations
from .service import CancelToken, StationService
from .station import HeatStation

# 工况
STEADY = "steady"
URGENT = "urgent"
CRITICAL = "critical"

# 策略
SEQUENTIAL = "sequential"
URGENT_FIRST = "urgent_first"
RECOMPUTE_FIRST = "recompute_first"

# 批次状态
PLANNED = "planned"
SENT = "sent"
PREEMPTED = "preempted"
ABORTED = "aborted"

READBACK_MISSING = "flow_readback_missing"


@dataclass(frozen=True)
class DispatchBatch:
    """一次下发批次的完整记录：谁发的、按什么总流量算的、被谁抢了、结局如何。"""

    batch_id: str
    run_id: str
    seq: int
    kind: str
    strategy: str
    stations: tuple[str, ...]
    flows: tuple[dict, ...]
    flow_basis_t_h: float
    status: str
    preempted_by: str
    preempts: tuple[str, ...]
    reason: str
    ts: float

    def as_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "seq": self.seq,
            "kind": self.kind,
            "strategy": self.strategy,
            "stations": list(self.stations),
            "flows": [dict(flow) for flow in self.flows],
            "flow_basis_t_h": round(self.flow_basis_t_h, 2),
            "status": self.status,
            "preempted_by": self.preempted_by,
            "preempts": list(self.preempts),
            "reason": self.reason,
            "ts": round(self.ts, 3),
        }


class BatchRegistry:
    """下发批次的内存登记簿：控制台读这里，跨重启从审计流水重建。"""

    def __init__(self, limit: int = 64) -> None:
        self._limit = max(1, int(limit))
        self._batches: list[dict] = []

    def add(self, batch: dict) -> dict:
        """登记（或按 ``batch_id`` 更新）一个批次，返回登记内容。"""

        batch_id = str(batch.get("batch_id", ""))
        self._batches = [item for item in self._batches if item["batch_id"] != batch_id]
        self._batches.append(dict(batch))
        if len(self._batches) > self._limit:
            del self._batches[: len(self._batches) - self._limit]
        return batch

    def recent(self, limit: int = 20) -> list[dict]:
        """返回最近的批次（最新的在前）。"""

        window = self._batches[-max(1, int(limit)):]
        return [dict(item) for item in reversed(window)]

    def get(self, batch_id: str) -> dict:
        """按标识取一个批次，未登记即抛 ``KeyError``。"""

        for item in self._batches:
            if item["batch_id"] == batch_id:
                return dict(item)
        raise KeyError(f"未登记下发批次: {batch_id}")

    def load_records(self, records: list) -> int:
        """从审计记录里的 ``dispatch_batch`` 事件重建登记簿，返回重建条数。"""

        count = 0
        for record in records:
            if getattr(record, "action", "") != "dispatch_batch":
                continue
            batch = record.payload.get("batch")
            if isinstance(batch, dict) and batch.get("batch_id"):
                self.add(batch)
                count += 1
        return count


def _batch_event(batch: DispatchBatch) -> dict:
    """把一个批次的终态包装成审计事件。"""

    payload = batch.as_dict()
    return {
        "action": "dispatch_batch",
        "severity": "warning" if batch.status == ABORTED else "notice",
        "message": f"下发批次 {batch.batch_id}（{batch.kind}）：{batch.reason}",
        "payload": {"batch": payload},
    }


def run_preemptive(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    network: PrimaryNetwork,
    stations: dict[str, HeatStation],
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
    condition: str,
    run_id: str,
    batch_ids: IdFactory,
    now: float,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
) -> dict:
    """抢占式下发：急的先算先发，让位的按新总流量重算，回读缺失即部分失败。"""

    limits = network.limits
    plan = build_plan(mapping, zones, zone_names, demand_pct)
    demand_of = {station: demand for station, demand in plan}
    names = [station for station, _ in plan]
    count = max(1, len(plan))
    baseline = network.baseline_t_h()
    objects = [stations[name] for name in names]
    ordered = [station.name for station in urgency_order(objects)]
    urgent = [station.name for station in urgent_stations(objects, limits.urgent_deviation_c)]
    rest = [name for name in ordered if name not in urgent]
    strategy = URGENT_FIRST if condition == CRITICAL else RECOMPUTE_FIRST

    steps: list[dict] = []
    violations: list[str] = []
    adjusted: list[str] = []
    skipped: list[str] = []
    flows_sent: list[StationFlow] = []
    batches: list[DispatchBatch] = []
    preemptions: list[dict] = []
    readbacks: list[dict] = []
    events: list[dict] = []
    partial = False

    def emit(batch: DispatchBatch) -> None:
        batches.append(batch)
        events.append(_batch_event(batch))

    def note_preemption(preempted_id: str, by_id: str) -> None:
        record = {
            "preempted": preempted_id,
            "by": by_id,
            "urgent_stations": list(urgent),
            "condition": condition,
            "strategy": strategy,
        }
        preemptions.append(record)
        events.append(
            {
                "action": "dispatch_preempt",
                "severity": "notice",
                "message": f"批次 {preempted_id} 被 {by_id} 抢占（急站：{','.join(urgent)}）",
                "payload": dict(record),
            }
        )
        steps.append(
            {
                "component": "dispatch",
                "action": "preempt",
                "detail": {"preempted": preempted_id, "by": by_id, "urgent": len(urgent)},
            }
        )

    def note_readback(stage: str, batch_id: str, flow_t_h: float | None) -> bool:
        if flow_t_h is None:
            events.append(
                {
                    "action": "dispatch_readback",
                    "severity": "warning",
                    "message": "总流量回读缺失，按部分失败处理，禁止抢占重算",
                    "payload": {"batch": batch_id, "stage": stage, "missing": True},
                }
            )
            steps.append(
                {
                    "component": "primary",
                    "action": "readback_flow",
                    "detail": {"batch": batch_id, "stage": stage, "missing": True},
                }
            )
            return False
        readbacks.append({"batch": batch_id, "stage": stage, "flow_t_h": round(flow_t_h, 2)})
        events.append(
            {
                "action": "dispatch_readback",
                "severity": "notice",
                "message": f"总流量回读 {flow_t_h:.2f} t/h",
                "payload": {"batch": batch_id, "stage": stage, "flow_t_h": round(flow_t_h, 2)},
            }
        )
        steps.append(
            {
                "component": "primary",
                "action": "readback_flow",
                "detail": {"batch": batch_id, "stage": stage, "flow_t_h": round(flow_t_h, 2)},
            }
        )
        return True

    def allocate_steps(batch_id: str, flows: list[StationFlow]) -> None:
        for flow in flows:
            steps.append(
                {
                    "component": "primary",
                    "action": "allocate_flow",
                    "detail": {**flow.as_dict(), "batch": batch_id},
                }
            )

    def send_batch(members: list[str]) -> bool:
        report = service.adjust_stations(members, target_c, cancel, dt)
        steps.extend(report["steps"])
        adjusted.extend(report["adjusted"])
        violations.extend(report["violations"])
        return bool(report["ok"])

    network.count_dispatch()

    # 原计划批次：按基线流量算出的名义计划，也是被抢的对象。
    base_flows = plan_flows(plan, baseline, count)
    current = {flow.station: flow.flow_t_h for flow in base_flows}
    base = DispatchBatch(
        batch_id=batch_ids.next(),
        run_id=run_id,
        seq=0,
        kind="base",
        strategy=strategy,
        stations=tuple(names),
        flows=tuple(flow.as_dict() for flow in base_flows),
        flow_basis_t_h=baseline,
        status=PLANNED,
        preempted_by="",
        preempts=(),
        reason="原计划（按基线流量）",
        ts=now,
    )

    if strategy == URGENT_FIRST:
        # 紧急工况：急站按基线与自己的真实需求先算先发。
        urgent_plan = [(name, stations[name].demand_pct()) for name in urgent]
        urgent_flows = plan_flows(urgent_plan, baseline, count)
        urgent_id = batch_ids.next()
        emit(
            replace(
                base,
                status=PREEMPTED,
                preempted_by=urgent_id,
                reason=f"原计划被急站批次 {urgent_id} 抢占",
            )
        )
        note_preemption(base.batch_id, urgent_id)
        allocate_steps(urgent_id, urgent_flows)
        for flow in urgent_flows:
            current[flow.station] = flow.flow_t_h
        network.set_flow(sum(current.values()))
        ok = send_batch(urgent)
        if not ok:
            skipped = [name for name in names if name not in adjusted]
            partial = bool(adjusted)
            emit(
                DispatchBatch(
                    batch_id=urgent_id,
                    run_id=run_id,
                    seq=1,
                    kind="urgent",
                    strategy=strategy,
                    stations=tuple(urgent),
                    flows=tuple(flow.as_dict() for flow in urgent_flows),
                    flow_basis_t_h=baseline,
                    status=ABORTED,
                    preempted_by="",
                    preempts=(base.batch_id,),
                    reason="急站下发被取消" if violations else "急站下发未完成",
                    ts=now,
                )
            )
            return _report(
                condition, strategy, steps, violations, partial, plan, adjusted, skipped,
                flows_sent, batches, preemptions, readbacks, urgent, events,
            )
        flows_sent.extend(urgent_flows)
        emit(
            DispatchBatch(
                batch_id=urgent_id,
                run_id=run_id,
                seq=1,
                kind="urgent",
                strategy=strategy,
                stations=tuple(urgent),
                flows=tuple(flow.as_dict() for flow in urgent_flows),
                flow_basis_t_h=baseline,
                status=SENT,
                preempted_by="",
                preempts=(base.batch_id,),
                reason="急站按真实需求先发",
                ts=now,
            )
        )

        # 急站发完必须回读总流量；回读缺失即部分失败，不许再重算。
        readback = network.readback_flow_t_h()
        if not note_readback("after_urgent", urgent_id, readback):
            violations.append(READBACK_MISSING)
            partial = True
            skipped = [name for name in names if name not in adjusted]
            emit(
                DispatchBatch(
                    batch_id=batch_ids.next(),
                    run_id=run_id,
                    seq=2,
                    kind="recomputed",
                    strategy=strategy,
                    stations=tuple(rest),
                    flows=(),
                    flow_basis_t_h=0.0,
                    status=ABORTED,
                    preempted_by="",
                    preempts=(),
                    reason="总流量回读缺失，让位重算被禁止",
                    ts=now,
                )
            )
            return _report(
                condition, strategy, steps, violations, partial, plan, adjusted, skipped,
                flows_sent, batches, preemptions, readbacks, urgent, events,
            )

        # 让位的站按回读到的新总流量重算，再按优先级发。
        recomputed_id = batch_ids.next()
        rest_plan = [(name, demand_of[name]) for name in rest]
        rest_flows = plan_flows(rest_plan, readback, count)
        events.append(
            {
                "action": "dispatch_recompute",
                "severity": "notice",
                "message": f"让位站按新总流量 {readback:.2f} t/h 重算",
                "payload": {
                    "batch": recomputed_id,
                    "old_basis_t_h": round(baseline, 2),
                    "new_basis_t_h": round(readback, 2),
                    "stations": list(rest),
                },
            }
        )
        steps.append(
            {
                "component": "primary",
                "action": "recompute_flows",
                "detail": {
                    "batch": recomputed_id,
                    "old_basis_t_h": round(baseline, 2),
                    "new_basis_t_h": round(readback, 2),
                },
            }
        )
        allocate_steps(recomputed_id, rest_flows)
        for flow in rest_flows:
            current[flow.station] = flow.flow_t_h
        network.set_flow(sum(current.values()))
        ok = send_batch(rest)
        flows_sent.extend(rest_flows if ok else [])
        skipped = [] if ok else [name for name in names if name not in adjusted]
        partial = not ok
        emit(
            DispatchBatch(
                batch_id=recomputed_id,
                run_id=run_id,
                seq=2,
                kind="recomputed",
                strategy=strategy,
                stations=tuple(rest),
                flows=tuple(flow.as_dict() for flow in rest_flows),
                flow_basis_t_h=readback,
                status=SENT if ok else ABORTED,
                preempted_by="",
                preempts=(),
                reason=f"让位后按新总流量 {readback:.2f} t/h 重算",
                ts=now,
            )
        )
        return _report(
            condition, strategy, steps, violations, partial, plan, adjusted, skipped,
            flows_sent, batches, preemptions, readbacks, urgent, events,
        )

    # 急迫工况：先回读总流量，全体重算，再按优先级发。
    readback = network.readback_flow_t_h()
    if not note_readback("before_recompute", base.batch_id, readback):
        violations.append(READBACK_MISSING)
        partial = True
        skipped = list(names)
        emit(
            replace(
                base,
                status=ABORTED,
                reason="总流量回读缺失，抢占被禁止，原计划不再下发",
            )
        )
        return _report(
            condition, strategy, steps, violations, partial, plan, adjusted, skipped,
            flows_sent, batches, preemptions, readbacks, urgent, events,
        )

    recomputed_id = batch_ids.next()
    emit(
        replace(
            base,
            status=PREEMPTED,
            preempted_by=recomputed_id,
            reason=f"原计划被重算批次 {recomputed_id} 抢占",
        )
    )
    note_preemption(base.batch_id, recomputed_id)
    full_plan = [(name, demand_of[name]) for name in ordered]
    full_flows = plan_flows(full_plan, readback, count)
    events.append(
        {
            "action": "dispatch_recompute",
            "severity": "notice",
            "message": f"全体按新总流量 {readback:.2f} t/h 重算",
            "payload": {
                "batch": recomputed_id,
                "old_basis_t_h": round(baseline, 2),
                "new_basis_t_h": round(readback, 2),
                "stations": list(ordered),
            },
        }
    )
    steps.append(
        {
            "component": "primary",
            "action": "recompute_flows",
            "detail": {
                "batch": recomputed_id,
                "old_basis_t_h": round(baseline, 2),
                "new_basis_t_h": round(readback, 2),
            },
        }
    )
    allocate_steps(recomputed_id, full_flows)
    current = {flow.station: flow.flow_t_h for flow in full_flows}
    network.set_flow(sum(current.values()))
    ok = send_batch(ordered)
    flows_sent.extend(full_flows if ok else [])
    skipped = [] if ok else [name for name in names if name not in adjusted]
    partial = not ok
    emit(
        DispatchBatch(
            batch_id=recomputed_id,
            run_id=run_id,
            seq=1,
            kind="recomputed",
            strategy=strategy,
            stations=tuple(ordered),
            flows=tuple(flow.as_dict() for flow in full_flows),
            flow_basis_t_h=readback,
            status=SENT if ok else ABORTED,
            preempted_by="",
            preempts=(base.batch_id,),
            reason=f"全体按新总流量 {readback:.2f} t/h 重算后按优先级下发",
            ts=now,
        )
    )
    return _report(
        condition, strategy, steps, violations, partial, plan, adjusted, skipped,
        flows_sent, batches, preemptions, readbacks, urgent, events,
    )


def _report(
    condition: str,
    strategy: str,
    steps: list[dict],
    violations: list[str],
    partial: bool,
    plan: list[tuple[str, float]],
    adjusted: list[str],
    skipped: list[str],
    flows_sent: list[StationFlow],
    batches: list[DispatchBatch],
    preemptions: list[dict],
    readbacks: list[dict],
    urgent: list[str],
    events: list[dict],
) -> dict:
    """汇总一次抢占下发的结果（报告里的流量只含已发出批次的流量）。"""

    return {
        "ok": not violations,
        "partial": partial,
        "condition": condition,
        "strategy": strategy,
        "steps": steps,
        "violations": violations,
        "planned": len(plan),
        "adjusted": adjusted,
        "skipped": skipped,
        "flows": [flow.as_dict() for flow in flows_sent],
        "batches": [batch.as_dict() for batch in batches],
        "preemptions": preemptions,
        "readbacks": readbacks,
        "urgent": list(urgent),
        "audit_events": events,
    }
