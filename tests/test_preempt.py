"""一次网批量下发的抢占、让位重算、回读缺失与批次审计。"""

from __future__ import annotations

import pytest

from heatnet.app import build_platform
from heatnet.config import default_config
from heatnet.console.api import Api
from heatnet.sequence.dispatch import dispatch_condition
from heatnet.station.preempt import READBACK_MISSING
from heatnet.station.priority import deviation_c, urgency_order
from heatnet.station.service import CancelToken


def _deviate(platform, station: str, target_c: float) -> None:
    """让某个站偏离自己的设定点（模拟"急的站"）。"""

    platform.stations[station].set_target(target_c)


def test_priority_is_fixed_by_deviation_then_name(platform):
    _deviate(platform, "st04", 66.0)
    _deviate(platform, "st02", 64.0)
    ordered = [station.name for station in urgency_order(list(platform.stations.values()))]
    assert ordered[:2] == ["st04", "st02"]
    assert ordered[2:] == ["st01", "st03", "st05", "st06"]
    assert deviation_c(platform.stations["st04"]) == pytest.approx(6.0)


def test_condition_is_decided_by_deviation_only(platform):
    stations = list(platform.stations.values())
    assert dispatch_condition(platform.network, stations) == "steady"
    _deviate(platform, "st05", 63.0)
    assert dispatch_condition(platform.network, stations) == "urgent"
    _deviate(platform, "st05", 66.0)
    assert dispatch_condition(platform.network, stations) == "critical"


def test_steady_dispatch_stays_sequential(platform):
    report = platform.dispatch(60.0, 60.0)
    assert report["ok"] is True
    assert report["condition"] == "steady"
    assert report["strategy"] == "sequential"
    assert report["adjusted"][0] == "st01"
    assert report["preemptions"] == []
    assert len(report["batches"]) == 1
    batch = report["batches"][0]
    assert batch["kind"] == "sequential"
    assert batch["status"] == "sent"
    assert batch["flow_basis_t_h"] == pytest.approx(platform.network.baseline_t_h())
    assert platform.batches.get(batch["batch_id"])["status"] == "sent"


def test_critical_condition_sends_urgent_first_then_recomputes(platform):
    _deviate(platform, "st03", 68.0)
    report = platform.dispatch(60.0, 60.0)
    assert report["ok"] is True
    assert report["condition"] == "critical"
    assert report["strategy"] == "urgent_first"
    assert report["urgent"] == ["st03"]
    assert report["adjusted"][0] == "st03"

    base, urgent, recomputed = report["batches"]
    assert base["kind"] == "base"
    assert base["status"] == "preempted"
    assert base["preempted_by"] == urgent["batch_id"]
    assert urgent["kind"] == "urgent"
    assert urgent["status"] == "sent"
    assert urgent["stations"] == ["st03"]
    assert urgent["preempts"] == [base["batch_id"]]
    assert recomputed["kind"] == "recomputed"
    assert recomputed["status"] == "sent"
    assert "st03" not in recomputed["stations"]

    # 急站按真实需求（100%）先抽一份：2400 * 1.0 / 6 = 400。
    urgent_flow = urgent["flows"][0]["flow_t_h"]
    assert urgent_flow == pytest.approx(400.0)
    # 急站发完总流量变成 2400 - 240 + 400 的按计划口径，让位站按这个新总流量重算。
    assert recomputed["flow_basis_t_h"] == pytest.approx(1538.06, abs=0.01)
    assert report["readbacks"] == [
        {
            "batch": urgent["batch_id"],
            "stage": "after_urgent",
            "flow_t_h": pytest.approx(1538.06, abs=0.01),
        }
    ]
    # 让位的站流量被重算压低（240 -> 153.81），急站保持 400。
    yielded = {flow["station"]: flow["flow_t_h"] for flow in recomputed["flows"]}
    assert yielded["st01"] == pytest.approx(153.81, abs=0.01)
    final = {flow["station"]: flow["flow_t_h"] for flow in report["flows"]}
    assert final["st03"] == pytest.approx(400.0)
    assert final["st01"] == pytest.approx(153.81, abs=0.01)

    assert report["preemptions"] == [
        {
            "preempted": base["batch_id"],
            "by": urgent["batch_id"],
            "urgent_stations": ["st03"],
            "condition": "critical",
            "strategy": "urgent_first",
        }
    ]
    assert platform.audit.count("dispatch_preempt") == 1
    assert platform.audit.count("dispatch_recompute") == 1
    assert platform.audit.count("dispatch_batch") == 3
    assert platform.network.dispatches() == 1


def test_urgent_condition_recomputes_all_then_sends_by_priority(platform):
    platform.network.set_flow(1500.0)
    _deviate(platform, "st02", 64.0)
    report = platform.dispatch(60.0, 60.0)
    assert report["ok"] is True
    assert report["condition"] == "urgent"
    assert report["strategy"] == "recompute_first"
    assert report["adjusted"][0] == "st02"

    base, recomputed = report["batches"]
    assert base["status"] == "preempted"
    assert base["preempted_by"] == recomputed["batch_id"]
    assert recomputed["kind"] == "recomputed"
    assert recomputed["flow_basis_t_h"] == pytest.approx(1500.0)
    # 全体按回读到的 1500 重算：st01..st03 各 1500*0.6/6 = 150。
    flows = {flow["station"]: flow["flow_t_h"] for flow in recomputed["flows"]}
    assert flows["st01"] == pytest.approx(150.0)
    assert platform.network.flow_t_h() == pytest.approx(sum(flows.values()))
    assert report["preemptions"][0]["by"] == recomputed["batch_id"]
    assert platform.audit.count("dispatch_recompute") == 1


def test_missing_readback_after_urgent_send_is_partial_failure(platform):
    _deviate(platform, "st03", 68.0)
    platform.set_flow_meter(False)
    report = platform.dispatch(60.0, 60.0)
    assert report["ok"] is False
    assert report["partial"] is True
    assert READBACK_MISSING in report["violations"]
    # 急站已发出，其余站不再发也不重算。
    assert report["adjusted"] == ["st03"]
    assert sorted(report["skipped"]) == ["st01", "st02", "st04", "st05", "st06"]
    kinds = [(batch["kind"], batch["status"]) for batch in report["batches"]]
    assert kinds == [("base", "preempted"), ("urgent", "sent"), ("recomputed", "aborted")]
    aborted = report["batches"][2]
    assert aborted["flows"] == []
    assert aborted["stations"] == sorted(["st01", "st02", "st04", "st05", "st06"])
    assert platform.audit.count("dispatch_recompute") == 0
    assert platform.audit.count("dispatch_preempt") == 1
    assert platform.service.valve("st01").moves() == 0


def test_missing_readback_forbids_preemption_entirely(platform):
    _deviate(platform, "st02", 64.0)
    platform.set_flow_meter(False)
    report = platform.dispatch(60.0, 60.0)
    assert report["ok"] is False
    assert report["partial"] is True
    assert report["violations"] == [READBACK_MISSING]
    # 一个站都不许发，抢占关系也不许成立。
    assert report["adjusted"] == []
    assert report["skipped"] == ["st01", "st02", "st03", "st04", "st05", "st06"]
    assert report["preemptions"] == []
    assert report["flows"] == []
    assert len(report["batches"]) == 1
    assert report["batches"][0]["status"] == "aborted"
    assert platform.audit.count("dispatch_preempt") == 0
    assert platform.audit.count("dispatch_recompute") == 0
    assert platform.service.valve("st02").moves() == 0


def test_cancel_aborts_urgent_batch_before_recompute(platform):
    _deviate(platform, "st03", 68.0)
    token = CancelToken("client-timeout")
    token.cancel()
    report = platform.dispatch(60.0, 60.0, None, token)
    assert report["ok"] is False
    assert report["violations"] == ["cancelled:client-timeout"]
    assert report["adjusted"] == []
    kinds = [(batch["kind"], batch["status"]) for batch in report["batches"]]
    assert kinds == [("base", "preempted"), ("urgent", "aborted")]
    assert platform.service.valve("st03").moves() == 0


def test_report_registry_and_audit_stay_consistent(platform):
    _deviate(platform, "st03", 68.0)
    report = platform.dispatch(60.0, 60.0)
    # 报告里的批次与登记簿、审计流水是同一份内容。
    for batch in report["batches"]:
        assert platform.batches.get(batch["batch_id"]) == batch
    audited = [
        entry["payload"]["batch"]
        for entry in platform.audit.tail(50)
        if entry["action"] == "dispatch_batch"
    ]
    assert audited == report["batches"]
    # 报告里的流量只来自已发出的批次。
    sent_flows = [
        flow for batch in report["batches"] if batch["status"] == "sent" for flow in batch["flows"]
    ]
    assert report["flows"] == sent_flows


def test_batches_survive_restart(platform, clock):
    _deviate(platform, "st03", 68.0)
    report = platform.dispatch(60.0, 60.0)
    batch_ids = [batch["batch_id"] for batch in report["batches"]]

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    restored = restarted.batches.recent(10)
    assert [batch["batch_id"] for batch in restored] == list(reversed(batch_ids))
    assert restarted.batches.get(batch_ids[0])["status"] == "preempted"


def test_console_exposes_batches_and_flow_meter(platform):
    api = Api(platform)
    _deviate(platform, "st03", 68.0)
    status, dispatch = api.handle("POST", "/api/dispatch", {}, {"demand_pct": 60.0, "target_c": 60.0})
    assert status == 200
    assert dispatch["condition"] == "critical"

    status, payload = api.handle("GET", "/api/batches", {}, {})
    assert status == 200
    assert payload["count"] == 3
    newest = payload["batches"][0]
    assert newest["kind"] == "recomputed"

    status, detail = api.handle("GET", f"/api/batch/{newest['batch_id']}", {}, {})
    assert status == 200
    assert detail["batch"]["batch_id"] == newest["batch_id"]
    status, missing = api.handle("GET", "/api/batch/nope", {}, {})
    assert status == 404

    status, meter = api.handle("POST", "/api/flow/meter", {}, {"ok": False})
    assert status == 200
    assert meter["flow_meter_ok"] is False
    status, snapshot = api.handle("GET", "/api/status", {}, {})
    assert snapshot["primary"]["flow_meter_ok"] is False
    status, meter = api.handle("POST", "/api/flow/meter", {}, {"ok": True})
    assert meter["flow_meter_ok"] is True
