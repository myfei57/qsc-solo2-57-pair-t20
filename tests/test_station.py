"""换热站批量调节、取消语义、流量分批与抢占下发。"""

from __future__ import annotations

import pytest

from heatnet.config import default_config
from heatnet.ns.zone import Zone, ZoneRegistry
from heatnet.primary.flow import PRIORITY_NORMAL, PRIORITY_URGENT, batch_flows
from heatnet.primary.network import PrimaryNetwork
from heatnet.station.batch import build_plan, run_batch
from heatnet.station.mapping import StationMapping
from heatnet.station.mix import apply_mix
from heatnet.station.preempt import (
    MODE_RECOMPUTE_FIRST,
    MODE_URGENT_FIRST,
    run_preempt,
)
from heatnet.station.service import CancelToken, StationService
from heatnet.station.station import HeatStation
from heatnet.store.jsonfile import JsonStore
from heatnet.store.state import DurableState
from heatnet.valve.valve import Valve


def _stack(tmp_path) -> tuple[ZoneRegistry, StationMapping, StationService, PrimaryNetwork]:
    zones = ZoneRegistry()
    zones.add(Zone(name="north", setpoint_c=62.0))
    zones.add(Zone(name="south", setpoint_c=54.0))
    for station in ("st01", "st02", "st03"):
        zones.assign(station, "north")
    for station in ("st04", "st05"):
        zones.assign(station, "south")

    config = default_config()
    stations = {name: HeatStation(name, zones.zone_of(name), config.station) for name in zones.index()}
    valves = {name: Valve(f"{name}.mix", config.valve) for name in stations}
    service = StationService(stations, valves, DurableState(JsonStore(str(tmp_path))))
    return zones, StationMapping(zones), service, PrimaryNetwork(config.primary)


class _CancelAfter(CancelToken):
    """处理若干个站之后再置位取消，用来验证逐步检查。"""

    def __init__(self, allowed: int) -> None:
        super().__init__("mid-batch")
        self._allowed = allowed
        self._seen = 0

    def cancelled(self) -> bool:
        self._seen += 1
        return self._seen > self._allowed


def test_batch_flows_are_independent_per_station():
    network = PrimaryNetwork(default_config().primary)
    plan = [("st01", 60.0), ("st02", 60.0), ("st03", 60.0)]
    flows = batch_flows(network, plan)
    values = [flow.flow_t_h for flow in flows]
    assert values[0] == pytest.approx(values[1])
    assert values[0] == pytest.approx(values[2])
    assert network.flow_t_h() == pytest.approx(sum(values))
    assert network.flow_t_h() < network.baseline_t_h()
    assert [flow.as_dict()["station"] for flow in flows] == ["st01", "st02", "st03"]
    assert network.dispatches() == 0


def test_adjust_stations_applies_to_every_station(tmp_path):
    zones, mapping, service, _ = _stack(tmp_path)
    report = service.adjust_stations(zones.index(), mapping.stations_in("north"), 61.0, label="north")
    assert report["ok"] is True
    assert report["adjusted"] == ["st01", "st02", "st03"]
    assert report["skipped"] == []
    for name in ("st01", "st02", "st03"):
        assert service.get(name).target_c() == pytest.approx(61.0)
        assert service.valve(name).position_pct() > 0.0
    assert service.batches() == 1
    assert service.aborted() == 0


def test_cancelled_before_start_changes_nothing(tmp_path):
    zones, mapping, service, _ = _stack(tmp_path)
    token = CancelToken("client-timeout")
    token.cancel()
    report = service.adjust_stations(
        zones.index(), mapping.stations_in("north"), 61.0, token, label="north"
    )
    assert report["ok"] is False
    assert report["adjusted"] == []
    assert report["skipped"] == ["st01", "st02", "st03"]
    assert report["violations"] == ["cancelled:client-timeout"]
    assert service.valve("st01").moves() == 0
    assert service.aborted() == 1


def test_cancellation_is_checked_at_every_station(tmp_path):
    zones, mapping, service, _ = _stack(tmp_path)
    report = service.adjust_stations(
        zones.index(), mapping.stations_in("north"), 61.0, _CancelAfter(1), label="north"
    )
    assert report["ok"] is False
    assert report["adjusted"] == ["st01"]
    assert report["skipped"] == ["st02", "st03"]
    assert service.get("st02").target_c() == pytest.approx(service.get("st02").limits.setpoint_c)
    assert service.valve("st02").moves() == 0
    with pytest.raises(ValueError):
        service.adjust_stations(zones.index(), mapping.stations_in("north"), 61.0, None, 0.0)


def test_run_batch_reports_flows_and_adjustments(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    report = run_batch(service, mapping, zones, network, ["north", "south"], 60.0, 60.0)
    assert report["ok"] is True
    assert report["planned"] == 5
    assert set(report["adjusted"]) == {"st01", "st02", "st03", "st04", "st05"}
    assert len(report["flows"]) == 5
    assert network.dispatches() == 1
    assert network.flow_t_h() > 0.0


def test_build_plan_orders_by_deviation_not_station_order(tmp_path):
    zones, mapping, _, _ = _stack(tmp_path)
    # 偏差口径定死：偏差大的站在前，偏差相同按站名升序；站序不参与。
    deviations = {"st01": 1.0, "st02": 9.0, "st03": 9.0, "st04": 4.0, "st05": 4.0}
    plan = build_plan(mapping, zones, ["south", "north"], 60.0, deviations)
    assert [station for station, _ in plan] == ["st02", "st03", "st04", "st05", "st01"]
    demands = dict(plan)
    assert demands["st01"] == pytest.approx(60.0)
    assert demands["st04"] < demands["st01"]


def test_run_batch_reports_single_normal_batch(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    report = run_batch(service, mapping, zones, network, ["north", "south"], 60.0, 60.0)
    assert report["ok"] is True
    assert report["priority_rule"] == "deviation"
    assert len(report["batches"]) == 1
    assert report["batches"][0]["priority"] == PRIORITY_NORMAL
    assert report["batches"][0]["dispatched"] is True


def test_urgent_first_dispatches_urgent_then_recomputes_yielded_batch(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    # 急站 st04 先按旧总流量算并发，发完回读到新总流量，让位批按新总流量重算。
    readback = {"value": 321.0}
    report = run_preempt(
        service, mapping, zones, network,
        ["north", "south"], 60.0, 60.0,
        urgent=["st04"], mode=MODE_URGENT_FIRST,
        readback=lambda: readback["value"],
    )
    assert report["ok"] is True
    assert report["partial_failure"] is False
    urgent_batch, yielded = report["batches"]
    assert urgent_batch["priority"] == PRIORITY_URGENT
    assert urgent_batch["stations"] == ["st04"]
    assert urgent_batch["dispatched"] is True
    assert yielded["priority"] == PRIORITY_NORMAL
    assert yielded["preempted"] is True
    assert yielded["preempted_by"] == "b1"
    assert yielded["based_on_total_t_h"] == pytest.approx(321.0)
    assert set(yielded["stations"]) == {"st01", "st02", "st03", "st05"}
    preemption = report["preemptions"][0]
    assert preemption["preempted_batch"] == "b2"
    assert preemption["by_batch"] == "b1"
    assert preemption["urgent_stations"] == ["st04"]
    assert preemption["new_total_t_h"] == pytest.approx(321.0)
    # 让位批流量必须按回读新总流量算，而不是按旧基线（逐流两位小数取整后求和）。
    assert sum(flow["flow_t_h"] for flow in yielded["flows"]) == pytest.approx(321.0, abs=0.05)
    # 批次、流量一致：让位批重算完成后，管网总流量与回读新总流量对齐；
    # 急站那份流量已经包含在新总流量里，不再重复叠加。
    assert report["dispatched_total_t_h"] == pytest.approx(321.0, abs=0.05)
    assert network.flow_t_h() == pytest.approx(321.0, abs=0.05)
    # 急站先动：它在调整顺序里排在第一位。
    assert report["adjusted"][0] == "st04"


def test_urgent_first_orders_urgent_group_by_deviation(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    # 多个急站时，急站批内部仍按偏差降序：把 st05 的供温拉低，使它偏差最大。
    service.get("st05")._supply_c = 30.0
    assert service.get("st05").supply_c() < service.get("st04").supply_c()
    report = run_preempt(
        service, mapping, zones, network,
        ["north", "south"], 60.0, 60.0,
        urgent=["st04", "st05"], mode=MODE_URGENT_FIRST,
        readback=lambda: 300.0,
    )
    assert report["batches"][0]["stations"] == ["st05", "st04"]


def test_recompute_first_recomputes_all_before_any_dispatch(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    # 先全体重算模式：回读发生在任何站动作之前；急站从总量先取，让位批分剩余量。
    calls: list[str] = []

    def readback() -> float:
        calls.append("read")
        # 回读发生时还没有任何站被调整。
        assert service.valve("st04").moves() == 0
        return 400.0

    report = run_preempt(
        service, mapping, zones, network,
        ["north", "south"], 60.0, 60.0,
        urgent=["st04"], mode=MODE_RECOMPUTE_FIRST,
        readback=readback,
    )
    assert calls == ["read"]
    assert report["ok"] is True
    urgent_batch, yielded = report["batches"]
    assert urgent_batch["dispatched"] is True
    assert yielded["preempted"] is True
    assert yielded["based_on_total_t_h"] == pytest.approx(
        400.0 - urgent_batch["total_t_h"]
    )
    assert report["readback_total_t_h"] == pytest.approx(400.0)
    assert report["adjusted"][0] == "st04"


def test_missing_readback_in_urgent_first_keeps_urgent_blocks_rest(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    report = run_preempt(
        service, mapping, zones, network,
        ["north", "south"], 60.0, 60.0,
        urgent=["st04"], mode=MODE_URGENT_FIRST,
        readback=lambda: None,
    )
    # 部分失败：急站已发，让位批不许抢占、整批发回。
    assert report["ok"] is False
    assert report["partial_failure"] is True
    assert "flow_readback_missing" in report["violations"]
    urgent_batch, blocked = report["batches"]
    assert urgent_batch["dispatched"] is True
    assert blocked["dispatched"] is False
    assert blocked["preempted"] is False
    assert set(blocked["skipped"]) == {"st01", "st02", "st03", "st05"}
    assert service.valve("st04").moves() > 0
    assert service.valve("st01").moves() == 0
    assert report["preemptions"] == []
    # 已发流量只含急站批。
    assert report["dispatched_total_t_h"] == pytest.approx(urgent_batch["total_t_h"])


def test_missing_readback_in_recompute_first_dispatches_nothing(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    report = run_preempt(
        service, mapping, zones, network,
        ["north", "south"], 60.0, 60.0,
        urgent=["st04"], mode=MODE_RECOMPUTE_FIRST,
        readback=lambda: None,
    )
    assert report["ok"] is False
    assert report["partial_failure"] is True
    assert "flow_readback_missing" in report["violations"]
    assert all(batch["dispatched"] is False for batch in report["batches"])
    assert report["adjusted"] == []
    assert service.valve("st04").moves() == 0


def test_preemption_validates_urgent_stations_and_mode(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    with pytest.raises(ValueError):
        run_preempt(
            service, mapping, zones, network,
            ["north"], 60.0, 60.0, urgent=["st04"], mode=MODE_URGENT_FIRST,
        )
    with pytest.raises(ValueError):
        run_preempt(
            service, mapping, zones, network,
            ["north", "south"], 60.0, 60.0, urgent=["st01"], mode="station_order",
        )


def test_cancellation_mid_urgent_batch_blocks_readback_and_yield(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    read_calls = {"n": 0}

    def readback():
        read_calls["n"] += 1
        return 300.0

    report = run_preempt(
        service, mapping, zones, network,
        ["north", "south"], 60.0, 60.0,
        urgent=["st02", "st05"], mode=MODE_URGENT_FIRST,
        readback=readback, cancel=_CancelAfter(1),
    )
    # 急站批只发了一个站就被取消：回读不许发生（时序：发完急站才回读），
    # 让位批不许重算下发。
    assert report["ok"] is False
    assert "cancelled:mid-batch" in report["violations"]
    assert read_calls["n"] == 0
    assert report["readback_total_t_h"] is None
    # b2 作为"被挡住、未下发"的批次仍然列在报告里，供控制台说明哪批发回了。
    urgent_batch, blocked = report["batches"]
    assert urgent_batch["dispatched"] is True
    assert urgent_batch["skipped"]
    assert blocked["dispatched"] is False
    assert blocked["preempted"] is False
    assert len(report["adjusted"]) == 1
    assert service.valve(report["adjusted"][0]).moves() > 0


def test_batch_flow_consistency_between_report_batches_and_audit_payload(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    report = run_preempt(
        service, mapping, zones, network,
        ["north", "south"], 60.0, 60.0,
        urgent=["st02", "st05"], mode=MODE_RECOMPUTE_FIRST,
        readback=lambda: 260.0,
    )
    # recompute_first 把回读总流量一次性切给两批：每个站恰好出现一次，
    # 两批流量之和与报告总口径、管网总流量三处一致。
    seen: dict[str, float] = {}
    for batch in report["batches"]:
        for flow in batch["flows"]:
            assert flow["station"] not in seen
            seen[flow["station"]] = flow["flow_t_h"]
    assert set(seen) == {"st01", "st02", "st03", "st04", "st05"}
    assert round(sum(seen.values()), 2) == pytest.approx(report["dispatched_total_t_h"], abs=0.05)
    assert round(sum(seen.values()), 2) == pytest.approx(260.0, abs=0.05)
    assert len(report["flows"]) == 5


def test_apply_mix_writes_station_state_to_store(tmp_path):
    config = default_config()
    station = HeatStation("st01", "north", config.station)
    valve = Valve("st01.mix", config.valve)
    durable = DurableState(JsonStore(str(tmp_path)))
    detail = apply_mix(station, valve, durable, 58.0, 1.0)
    assert detail["station"] == "st01"
    assert detail["valve_pct"] > 0.0
    assert durable.load("valve/st01.mix")["position_pct"] == pytest.approx(detail["valve_pct"])
    assert station.snapshot().as_dict()["target_c"] == pytest.approx(58.0)
