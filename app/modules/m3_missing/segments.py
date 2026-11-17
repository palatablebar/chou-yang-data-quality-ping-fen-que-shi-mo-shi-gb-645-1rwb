"""连续缺失段裁定：底段 → 跨接 → 立段，一段一个整改项。

裁定规则（唯一来源，与 docs/DATA-DICTIONARY.md 第 7 节一致）：
1. 底段：同一列按原始行序的极大连续缺失格序列；
2. 跨接：相邻底段之间的有值格数 ≤ `MISSING_BRIDGE_MAX`（默认 1）时并为一
   段，被跨过的有值格逐格记入「连接依据」；跨接可链式发生。短暂有值不再
   打断长缺口，但间隔超过上限的两段绝不串成一段；
3. 立段：段内原始缺失格总数 ≥ `MISSING_RUN_MIN`（3）才立为整改项，不足
   的仍是单格噪声，不进复核队列；
4. 互斥：段与段互不相交，每条缺失格至多进入一个整改项（装配侧再校验）。
"""
from __future__ import annotations

from ... import config, reasons
from .cells import is_missing


def find_segments(profile: dict, min_len: int = config.MISSING_RUN_MIN,
                  bridge_max: int = config.MISSING_BRIDGE_MAX) -> list[dict]:
    segments: list[dict] = []
    for column in profile["columns"]:
        for linenos, bridges in _merged_runs(profile, column["name"], bridge_max):
            if len(linenos) < min_len:
                continue
            segments.append(_segment(column, linenos, bridges, bridge_max))
    segments.sort(key=lambda item: (item["column"], item["start_lineno"]))
    return segments


def summary(segments: list[dict], min_len: int = config.MISSING_RUN_MIN,
            bridge_max: int = config.MISSING_BRIDGE_MAX) -> dict:
    return {
        "items": len(segments),
        "cells": sum(item["length"] for item in segments),
        "bridged_items": sum(1 for item in segments if item["bridged"]),
        "bridged_cells": sum(len(item["bridges"]) for item in segments),
        "rule": (f"同列连续缺失 ≥{min_len} 格立段；相邻底段间隔 ≤{bridge_max} 格"
                 f"有值时跨接为一段，每条缺失格至多进入一个整改项"),
    }


def _merged_runs(profile: dict, name: str,
                 bridge_max: int) -> list[tuple[list[int], list[dict]]]:
    base_runs, gaps = _base_runs(profile, name)
    merged: list[tuple[list[int], list[dict]]] = []
    for index, run in enumerate(base_runs):
        if merged and 0 < len(gaps[index - 1]) <= bridge_max:
            linenos, bridges = merged[-1]
            linenos.extend(run)
            bridges.extend(gaps[index - 1])
        else:
            merged.append((list(run), []))
    return merged


def _base_runs(profile: dict, name: str) -> tuple[list[list[int]], list[list[dict]]]:
    """极大连续缺失底段；gaps[i] 是底段 i 与底段 i+1 之间的有值格。"""
    base_runs: list[list[int]] = []
    gaps: list[list[dict]] = []
    current: list[int] = []
    gap: list[dict] = []
    for row in profile["rows"]:
        cell = row["cells"][name]
        if is_missing(cell):
            if gap and current:
                base_runs.append(current)
                gaps.append(gap)
                current, gap = [], []
            elif gap:
                gap = []  # 首个底段之前的有值格不构成间隔
            current.append(row["lineno"])
        elif current or base_runs:
            gap.append({"lineno": row["lineno"], "raw": cell["raw"],
                        "state": cell["state"]})
    if current:
        base_runs.append(current)
    return base_runs, gaps


def _segment(column: dict, linenos: list[int], bridges: list[dict],
             bridge_max: int) -> dict:
    code = "K_RUN_BRIDGED" if bridges else "K_MISSING_RUN"
    return {
        "column": column["name"],
        "label": column["label"],
        "start_lineno": linenos[0],
        "end_lineno": linenos[-1],
        "length": len(linenos),
        "span": linenos[-1] - linenos[0] + 1,
        "linenos": list(linenos),
        "bridges": list(bridges),
        "bridged": bool(bridges),
        "basis": _basis(linenos, bridges, bridge_max),
        "code": code,
        "label_code": reasons.label(code),
    }


def _basis(linenos: list[int], bridges: list[dict], bridge_max: int) -> str:
    if not bridges:
        return f"同列连续缺失 {len(linenos)} 格，中间无有值格，无需跨接"
    rows = "、".join(str(item["lineno"]) for item in bridges)
    return (f"第 {rows} 行短暂有值（每处 ≤{bridge_max} 格，共 {len(bridges)} 格），"
            f"按裁定规则跨接为一段；原始缺失 {len(linenos)} 格，"
            f"覆盖第 {linenos[0]}–{linenos[-1]} 行")
