"""控制台接口与 HTTP 服务。"""

from __future__ import annotations

from heatnet.console.api import Api
from heatnet.console.server import ConsoleServer, probe_endpoints


def test_api_returns_status_and_routes(platform):
    api = Api(platform)
    status, payload = api.handle("GET", "/api/status", {}, {})
    assert status == 200
    assert payload["platform"] == platform.config.name
    paths = [route["path"] for route in api.routes()]
    assert "/api/dispatch" in paths
    assert "/api/station/{name}" in paths
    assert "/api/zone/{name}" in paths


def test_api_unknown_path_and_station_are_404(platform):
    api = Api(platform)
    status, payload = api.handle("GET", "/api/does-not-exist", {}, {})
    assert status == 404
    assert payload["error"] == "not_found"
    status, payload = api.handle("GET", "/api/station/nope", {}, {})
    assert status == 404
    assert payload["error"] == "not_found"


def test_api_station_and_zone_details(platform):
    api = Api(platform)
    status, payload = api.handle("GET", "/api/station/st01", {}, {})
    assert status == 200
    assert payload["station"]["name"] == "st01"
    assert payload["zone"] == "north"
    status, zone = api.handle("GET", "/api/zone/north", {}, {})
    assert status == 200
    assert [item["name"] for item in zone["stations"]] == ["st01", "st02", "st03"]


def test_api_bad_request_and_interlock(platform):
    api = Api(platform)
    status, payload = api.handle("POST", "/api/cycle", {}, {"dt": "not-a-number"})
    assert status == 409
    assert payload["component"] == "console"
    status, payload = api.handle("POST", "/api/concurrency", {}, {"rounds": 0})
    assert status == 400
    assert payload["error"] == "bad_request"
    status, payload = api.handle(
        "POST", "/api/zone/split", {}, {"source_zone": "nope", "new_zone": "x", "stations": ["st01"]}
    )
    assert status == 409
    assert payload["component"] == "ns"


def test_api_weather_adjust_and_readiness(platform):
    api = Api(platform)
    status, weather = api.handle("POST", "/api/weather", {}, {"outdoor_c": -9.0})
    assert status == 200
    assert weather["ok"] is True
    status, adjusted = api.handle("POST", "/api/adjust", {}, {"temperature_c": 88.0, "flow_t_h": 1500.0})
    assert status == 200
    assert adjusted["ok"] is True
    status, ready = api.handle("GET", "/api/readiness", {}, {})
    assert status == 200
    assert ready["checks"]["curve_fresh"] is True
    assert "source_state" in ready["info"]


def test_api_forecast_curve_and_reports(platform):
    api = Api(platform)
    status, curve = api.handle("GET", "/api/curve", {}, {})
    assert status == 200
    assert len(curve["table"]) == 3
    status, forecast = api.handle("GET", "/api/forecast", {"outdoor_c": "-10", "steps": "3"}, {})
    assert status == 200
    assert forecast["count"] == 3
    status, pressure = api.handle("GET", "/api/pressure", {}, {})
    assert status == 200
    assert pressure["baseline_kpa"] > 0.0
    status, quota = api.handle("GET", "/api/quota", {}, {})
    assert status == 200
    assert quota["accumulator"]["count"] == 6


def test_api_timeline_journal_and_store(platform):
    api = Api(platform)
    platform.start()
    platform.push_weather(-8.0)
    status, timeline = api.handle("GET", "/api/timeline", {"asset": "st01"}, {})
    assert status == 200
    assert timeline["timeline"] == []
    status, journal = api.handle("GET", "/api/journal", {}, {})
    assert status == 200
    assert journal["count"] > 0
    status, store = api.handle("GET", "/api/store", {}, {})
    assert status == 200
    assert isinstance(store["documents"], list)


def test_api_dispatch_with_priority_preemption_and_batches(platform):
    api = Api(platform)
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    # 急站抢占：st04 先算先发，其余站让位后按新总流量重算。
    status, report = api.handle(
        "POST",
        "/api/dispatch",
        {},
        {"demand_pct": 55.0, "urgent": ["st04"], "mode": "urgent_first"},
    )
    assert status == 200
    assert report["ok"] is True
    assert report["priority_rule"] == "deviation"
    assert report["batches"][0]["priority"] == "urgent"
    assert report["batches"][0]["stations"] == ["st04"]
    assert report["batches"][1]["preempted"] is True
    assert report["batches"][1]["preempted_by"] == "b1"

    # 批次台账供控制台展示。
    status, batches = api.handle("GET", "/api/dispatch/batches", {"limit": "10"}, {})
    assert status == 200
    assert batches["count"] == 2
    assert batches["priority_rule"] == "deviation"
    assert batches["batches"][0]["priority"] == "normal"
    assert {f["station"] for f in batches["batches"][1]["flows"]} == {"st04"}

    # recompute_first 模式同样可达。
    status, report2 = api.handle(
        "POST",
        "/api/dispatch",
        {},
        {"demand_pct": 55.0, "urgent": ["st06"], "mode": "recompute_first"},
    )
    assert status == 200
    assert report2["mode"] == "recompute_first"

    # 非法抢占模式被 409 拦下。
    status, bad = api.handle(
        "POST", "/api/dispatch", {}, {"urgent": ["st01"], "mode": "station_order"}
    )
    assert status == 409
    assert bad["component"] == "console"

    # 总流量回读缺失：部分失败，不许抢占。
    status, missing = api.handle(
        "POST",
        "/api/dispatch",
        {},
        {"urgent": ["st01"], "mode": "urgent_first", "flow_readback": False},
    )
    assert status == 200
    assert missing["ok"] is False
    assert "flow_readback_missing" in missing["violations"]
    assert missing["preemptions"] == []
    assert missing["batches"][1]["dispatched"] is False


def test_api_actions_cover_queue_and_meter(platform):
    api = Api(platform)
    status, queued = api.handle("POST", "/api/dispatch", {}, {"demand_pct": 55.0, "zones": ["north"]})
    assert status == 200
    assert queued["ok"] is True
    status, meter = api.handle("POST", "/api/meter", {}, {"meter": "st01.heat-meter", "delta_mwh": 4.0})
    assert status == 200
    assert meter["total_mwh"] > 0.0
    status, settled = api.handle("POST", "/api/settle", {}, {})
    assert status == 200
    assert settled["period"] == 1
    status, pump = api.handle("POST", "/api/pump", {}, {"action": "start", "speed_pct": 70.0})
    assert status == 200
    assert pump["running"] is True
    status, stopped = api.handle("POST", "/api/pump", {}, {"action": "stop"})
    assert status == 200
    assert stopped["running"] is False
    status, disturbed = api.handle("POST", "/api/disturb", {}, {"heat_fraction": 0.2})
    assert status == 200
    assert disturbed["disturbances"] == 1


def test_api_concurrency_trip_and_reset(platform):
    api = Api(platform)
    status, concurrency = api.handle("POST", "/api/concurrency", {}, {"rounds": 12})
    assert status == 200
    assert concurrency["ok"] is True
    status, trip = api.handle("POST", "/api/trip", {}, {"reason": "console"})
    assert status == 200
    assert trip["state"] == "tripped"
    status, reset = api.handle("POST", "/api/reset", {}, {"operator": "console"})
    assert status == 200
    assert reset["state"] == "idle"


def test_server_serves_api_over_http(platform):
    platform.start()
    platform.push_weather(-8.0)
    server = ConsoleServer(platform, host="127.0.0.1", port=0)
    base_url = server.start()
    try:
        probe = probe_endpoints(
            base_url,
            ["/api/status", "/api/readiness", "/api/topology", "/api/stations", "/api/nope"],
        )
    finally:
        server.stop()
    assert probe["/api/status"]["ok"] is True
    assert probe["/api/readiness"]["ok"] is True
    assert probe["/api/topology"]["ok"] is True
    assert probe["/api/stations"]["ok"] is True
    assert probe["/api/nope"]["ok"] is False
    assert server.port == 0
