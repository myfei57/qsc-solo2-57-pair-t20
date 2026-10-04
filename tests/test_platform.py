"""平台装配、控制循环、并发探测与跨重启一致性。"""

from __future__ import annotations

import pytest

from heatnet.app import READBACK_MISSING, build_platform
from heatnet.config import default_config
from heatnet.station.preempt import MODE_RECOMPUTE_FIRST, MODE_URGENT_FIRST
from heatnet.station.service import CancelToken


def test_startup_dispatch_and_cycle(online):
    for _ in range(120):
        online.cycle(dt=1.0)
    status = online.status()
    assert status["source"]["state"] == "online"
    assert status["source"]["quality_confirmed"] is True
    assert status["curve"]["stale"] is False
    assert status["primary"]["flow_t_h"] > 0.0
    assert status["pump"]["running"] is False
    assert status["stations"]["count"] == 6
    assert online.audit.summary()["total"] > 0


def test_dispatch_uses_deviation_order_and_independent_flows(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    report = platform.dispatch(60.0, 60.0)
    assert report["ok"] is True
    assert report["priority_rule"] == "deviation"
    assert len(report["flows"]) == 6
    assert platform.network.dispatches() == 1
    assert platform.network.flow_t_h() < platform.network.baseline_t_h()


def test_dispatch_honours_cancellation(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    token = CancelToken("client-timeout")
    token.cancel()
    report = platform.dispatch(60.0, 60.0, ["north"], token)
    assert report["ok"] is False
    assert report["violations"] == ["cancelled:client-timeout"]
    assert platform.service.valve("st01").moves() == 0


def test_preemptive_dispatch_records_batches_audit_and_priority_rule(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    # 末端站 st05 报急：按偏差点名，先发急站再回读重算让位批。
    report = platform.dispatch(
        60.0, 60.0, urgent=["st05"], mode=MODE_URGENT_FIRST
    )
    assert report["ok"] is True
    assert report["priority_rule"] == "deviation"
    assert report["batches"][0]["priority"] == "urgent"
    assert report["batches"][0]["stations"] == ["st05"]
    yielded = report["batches"][1]
    assert yielded["preempted"] is True
    assert yielded["preempted_by"] == "b1"
    assert "st05" not in yielded["stations"]
    assert report["adjusted"][0] == "st05"
    # 审计里能点清"哪一批被谁抢了"。
    actions = [event.action for event in platform.bus.history("dispatch")]
    assert "dispatch_batch" in actions
    assert "batch_preempted" in actions
    preempted_event = [
        event for event in platform.bus.history("dispatch") if event.action == "batch_preempted"
    ][0]
    assert preempted_event.payload["preempted_batch"] == "b2"
    assert preempted_event.payload["by_batch"] == "b1"
    # 控制台批次台账与状态快照一致。
    ledger = platform.dispatch_batches()
    assert {batch["batch_id"] for batch in ledger} == {"b1", "b2"}
    assert ledger[0]["priority"] == "normal"
    assert platform.status()["dispatch"]["priority_rule"] == "deviation"
    assert {mode["mode"] for mode in ledger} == {MODE_URGENT_FIRST}


def test_recompute_first_mode_recomputes_before_dispatch(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    report = platform.dispatch(
        60.0, 60.0, urgent=["st03"], mode=MODE_RECOMPUTE_FIRST
    )
    assert report["ok"] is True
    assert report["mode"] == MODE_RECOMPUTE_FIRST
    assert report["batches"][0]["stations"] == ["st03"]
    assert report["batches"][1]["preempted"] is True
    assert report["readback_total_t_h"] is not None


def test_missing_flow_readback_is_partial_failure_and_blocks_preemption(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    # urgent_first：急站已发，但回读缺失时让位批整批发回，且不记抢占。
    report = platform.dispatch(
        60.0, 60.0, urgent=["st02"], mode=MODE_URGENT_FIRST, readback=READBACK_MISSING
    )
    assert report["ok"] is False
    assert report["partial_failure"] is True
    assert "flow_readback_missing" in report["violations"]
    assert report["preemptions"] == []
    urgent_batch, blocked = report["batches"]
    assert urgent_batch["dispatched"] is True
    assert blocked["dispatched"] is False
    assert blocked["skipped"]
    assert platform.service.valve("st02").moves() > 0
    assert platform.service.valve("st01").moves() == 0
    assert any(
        event.action == "flow_readback_missing"
        for event in platform.bus.history("dispatch")
    )

    # recompute_first：回读缺失时一批都不许发。
    report2 = platform.dispatch(
        60.0, 60.0, urgent=["st04"], mode=MODE_RECOMPUTE_FIRST, readback=READBACK_MISSING
    )
    assert report2["ok"] is False
    assert all(batch["dispatched"] is False for batch in report2["batches"])
    assert platform.service.valve("st04").moves() == 0


def test_unknown_preempt_mode_is_rejected(platform):
    with pytest.raises(ValueError):
        platform.dispatch(60.0, 60.0, urgent=["st01"], mode="station_order")


def test_dispatch_batches_survive_restart(platform, clock):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    platform.dispatch(60.0, 60.0, urgent=["st02"], mode=MODE_URGENT_FIRST)
    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    batches = restarted.dispatch_batches()
    assert len(batches) == 2
    priorities = sorted(batch["priority"] for batch in batches)
    assert priorities == ["normal", "urgent"]
    assert any(batch["preempted"] for batch in batches)


def test_split_zone_rebuilds_mapping(platform):
    report = platform.split_zone("north", ["st02", "st03"], "north-west")
    assert report["ok"] is True
    assert platform.mapping.zone_of("st02") == "north-west"
    assert platform.mapping.generation() == 2
    assert platform.service.get("st02").zone() == "north-west"
    assert platform.zones.names() == ["east", "north", "north-west", "south"]


def test_replay_valves_skips_applied_commands(platform):
    platform.queue_commands("st01", [40.0, 65.0])
    first = platform.replay_valves(["st01"])
    assert first["replayed"] == 2
    second = platform.replay_valves(["st01"])
    assert second["replayed"] == 0
    assert platform.valves["st01"].position_pct() == pytest.approx(65.0)


def test_collect_heat_and_settle_quota(platform):
    for name in platform.meter_names():
        platform.collect_heat(name, 5.0)
    report = platform.quota_report()
    assert report["ledger"]["total_mwh"] == pytest.approx(30.0)
    assert report["accumulator"]["count"] == 6
    settled = platform.settle_quota()
    assert settled["period"] == 1
    assert len(settled["lines"]) == 6
    assert platform.ledger.periods() == 1


def test_concurrency_probe_reports_no_lost_state(platform):
    report = platform.concurrency_probe(rounds=24)
    assert report["ok"] is True
    assert report["source"]["mixed_pairs"] == 0
    assert report["meter"]["lost_increments"] == pytest.approx(0.0)


def test_trip_state_survives_restart(platform, clock):
    platform.start()
    platform.push_weather(-8.0)
    platform.trip("overtemperature")
    assert platform.source.state() == "tripped"

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    assert restarted.source.state() == "tripped"
    assert restarted.source.trip_reason() == "overtemperature"
    assert restarted.status()["operations"] >= 1


def test_valve_position_survives_restart(platform, clock):
    platform.start()
    platform.queue_commands("st02", [55.0])
    platform.replay_valves(["st02"])

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    assert restarted.valves["st02"].position_pct() == pytest.approx(55.0)
    assert restarted.checkpoint.load("st02") == 1


def test_snapshot_exposes_all_sections(online):
    snapshot = online.snapshot()
    for key in ("status", "topology", "namespaces", "zones", "mapping", "trend", "billing"):
        assert key in snapshot
    assert snapshot["topology"]["order"][0] == "weather"
    assert snapshot["namespaces"]["count"] == 25
    assert snapshot["status"]["stations"]["count"] == 6


def test_trend_collects_signals_after_cycles(online):
    for _ in range(5):
        online.cycle(dt=5.0)
    samples = online.trend.series("supply_c", limit=10)
    assert len(samples) == 5
    assert online.trend.latest("network_flow_t_h") is not None
    filled = online.trend.fill_gaps("supply_c", samples)
    assert len(filled) >= len(samples)


def test_forecast_and_billing_reports(platform):
    forecast = platform.outdoor_forecast(-10.0, steps=4)
    assert forecast["count"] == 4
    assert forecast["max_supply_c"] >= forecast["min_supply_c"]
    platform.collect_heat(platform.meter_names()[0], 40.0)
    lines = platform.billing(10.0)
    assert lines and lines[0]["overage_mwh"] > 0.0


def test_reset_state_clears_counters(platform):
    platform.start()
    platform.cycle(dt=1.0)
    assert platform.reset_state() is True
    assert platform.reset_state() is False


def test_note_error_writes_audit_entry(platform):
    from heatnet.errors import InterlockError

    platform.start()
    record = platform.note_error(InterlockError("联锁拒绝", component="source"))
    assert record["component"] == "source"
    assert platform.audit.summary()["by_severity"]["warning"] == 1
    assert platform.audit.count("error") == 1
