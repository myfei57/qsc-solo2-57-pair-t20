"""调节优先级排序。

优先级口径在此定死：**按偏差降序**，偏差大的站先算先发；偏差相同时按站名
升序兜底。绝不按站序（拓扑序／登记序）排——站序只反映离热源远近，不反映
现场紧急程度，按站序排会让末端急站永远排在后面等。

排序只产出顺序，不碰任何设备状态，供调度编排按序下发。急站由调用方显式点名，
点名后整体排在普通站之前，急站内部与普通站内部仍然各自按偏差排序。
"""

from __future__ import annotations


def deviation_order(entries: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """按偏差降序排序，偏差相同时按站名升序。"""

    return sorted(entries, key=lambda item: (-float(item[1]), str(item[0])))


def split_urgent(
    ordered: list[tuple[str, float]], urgent: list[str]
) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
    """把排好序的站拆成急站批与普通批，两批各自保持偏差序。

    急站是显式点名的那一组，整体排在前面；没被点名的站留在普通批。点名了
    但不在本次下发名单里的站直接忽略，不能凭空下发。
    """

    wanted = {str(name) for name in urgent}
    front = [entry for entry in ordered if entry[0] in wanted]
    rest = [entry for entry in ordered if entry[0] not in wanted]
    return front, rest
