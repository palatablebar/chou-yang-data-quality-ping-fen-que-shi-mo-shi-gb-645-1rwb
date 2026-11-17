"""复核队列 CSV：与页面读同一份库内队列，列与页面整改项表一一对应。"""
from __future__ import annotations

from ..m7_export.csv_out import csv_text


def review_csv(queue: dict, items: list[dict]) -> str:
    rows = []
    for item in items:
        bridges = " ".join(
            f"第{bridge['lineno']}行「{bridge['raw']}」"
            for bridge in item["bridges"])
        rows.append([
            item["seq"], item["item_id"], item["column"], item["label"],
            item["start_lineno"], item["end_lineno"], item["length"],
            item["span"], " ".join(str(lineno) for lineno in item["linenos"]),
            bridges, item["basis"], item["code"],
        ])
    return csv_text(
        ["优先级", "整改项", "列名", "列标题", "起始行", "结束行", "缺失格数",
         "覆盖行数", "原始缺失格", "跨接有值格", "连接依据", "原因码"], rows)
