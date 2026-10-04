"""抢占优先级：定死为按偏差，不开放配置。

偏差 = |站目标温度 − 当前二次侧供温|。偏差大的站更急，先算先发；偏差相同按站名
升序。任何时刻、任何入口对同一批站只能排出一个顺序，控制台展示、审计回放与
重算结果才能对上。
"""

from __future__ import annotations

from .station import HeatStation


def deviation_c(station: HeatStation) -> float:
    """返回站当前目标温度与二次侧供温的偏差。"""

    return abs(station.target_c() - station.supply_c())


def urgency_order(stations: list[HeatStation]) -> list[HeatStation]:
    """按偏差降序、站名升序排出发送顺序。"""

    return sorted(stations, key=lambda station: (-deviation_c(station), station.name))


def urgent_stations(stations: list[HeatStation], threshold_c: float) -> list[HeatStation]:
    """挑出偏差达到急迫门限的站（按发送顺序）。"""

    return [
        station for station in urgency_order(stations) if deviation_c(station) >= float(threshold_c)
    ]
