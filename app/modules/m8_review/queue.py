"""复核队列装配：裁定段 → 按优先级排序的整改项队列。

「每条缺失格不能进入多个整改项」在这里强制执行：装配时逐格登记，
撞格直接拒绝，宁可本次发布失败也不让同一格被两个整改项认领。
优先级：缺失格数降序，并列按列名、起始行号，全库唯一确定。
"""
from __future__ import annotations

SEGMENT_KEYS = ("column", "label", "start_lineno", "end_lineno", "length",
                "span", "linenos", "bridges", "bridged", "basis", "code",
                "label_code")


def build_queue(segments: list[dict]) -> dict:
    ordered = sorted(segments,
                     key=lambda item: (-item["length"], item["column"],
                                       item["start_lineno"]))
    seen: set[tuple[str, int]] = set()
    items: list[dict] = []
    for seq, segment in enumerate(ordered, start=1):
        for lineno in segment["linenos"]:
            key = (segment["column"], lineno)
            if key in seen:
                raise ValueError(
                    f"缺失格重复进入整改项：{segment['column']} 第 {lineno} 行")
            seen.add(key)
        items.append({
            "seq": seq,
            "item_id": f"RQ-{seq:03d}",
            **{key: segment[key] for key in SEGMENT_KEYS},
        })
    return {
        "items": items,
        "item_count": len(items),
        "cell_count": sum(item["length"] for item in items),
        "disjoint": True,
    }
