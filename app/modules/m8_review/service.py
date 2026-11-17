"""M8 编排：复核队列的发布、复用与当前生效队列。

- 队列内容来自运行快照里的裁定段，发布即落库，之后页面与导出只读库
  里同一份队列，不再各自重算；
- 同一附件（数据集名 + 输入摘要 + 规则版本）重跑发布直接沿用原版本，
  不新增重复整改项；并发撞同一身份时负一方按复用返回；
- 发布单事务完成：任一步失败整体回滚，上一完整旧版保持 published，
  记录只增不删。
"""
from __future__ import annotations

import datetime as dt
import sqlite3

from fastapi.responses import Response

from ... import config, manifest, reasons
from ...errors import NotFound
from ..m6_score import runstore
from . import csv_out, queue as queue_mod


def publish(store, run_id: str | None = None) -> dict:
    run = runstore.require_run(store, run_id)
    result = store.load_result(run["run_id"])
    if result is None:
        raise NotFound(f"运行快照缺失：{run['run_id']}", code="E_RUN_NOT_FOUND")
    existing = store.find_review_queue(run["name"], run["digest"],
                                       run["rules_version"])
    if existing is not None:
        return _reuse(store, existing, run)
    segments = result.get("missing", {}).get("segments", [])
    payload = queue_mod.build_queue(segments)
    now = _now()
    row = {
        "queue_id": manifest.new_run_id("QR"),
        "name": run["name"], "digest": run["digest"],
        "rules_version": run["rules_version"], "run_id": run["run_id"],
        "status": "published",
        "item_count": payload["item_count"], "cell_count": payload["cell_count"],
        "created_at": now, "published_at": now, "superseded_at": None,
    }
    try:
        row["version"] = store.publish_review_queue(row, payload["items"])
    except sqlite3.IntegrityError:
        # 并发撞同一附件身份：负方按复用返回，绝不新增重复整改项
        existing = store.find_review_queue(run["name"], run["digest"],
                                           run["rules_version"])
        if existing is not None:
            return _reuse(store, existing, run)
        raise
    return {"queue": row, "items": _labeled(payload["items"]),
            "reused": False, "run": runstore.summary(run)}


def current(store, run: dict | None) -> dict:
    """页面与 API 共用的当前视图：生效队列 + 待发布预览 + 版本历史。"""
    if run is None:
        return {"queue": None, "items": [], "preview": None, "history": []}
    queue_row = store.current_review_queue(run["name"])
    items = _labeled(store.list_review_items(queue_row["queue_id"])) \
        if queue_row else []
    preview = None
    published_current = (queue_row is not None
                         and queue_row["digest"] == run["digest"]
                         and queue_row["rules_version"] == run["rules_version"])
    if not published_current:
        result = store.load_result(run["run_id"])
        if result is not None:
            segments = result.get("missing", {}).get("segments", [])
            payload = queue_mod.build_queue(segments)
            preview = {"run_id": run["run_id"], "entries": payload["items"],
                       "item_count": payload["item_count"],
                       "cell_count": payload["cell_count"]}
    return {"queue": queue_row, "items": items, "preview": preview,
            "history": store.list_review_queues(run["name"])}


def export_csv(store, run_id: str | None = None) -> Response:
    run = runstore.require_run(store, run_id)
    queue_row = store.current_review_queue(run["name"])
    if queue_row is None:
        raise NotFound(f"数据集 {run['name']} 还没有已发布的复核队列",
                       code="E_QUEUE_NOT_FOUND")
    items = store.list_review_items(queue_row["queue_id"])
    content = csv_out.review_csv(queue_row, items)
    filename = f"{run['name']}-{queue_row['queue_id']}-review.csv"
    return Response(content=content.encode("utf-8"),
                    media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             f'attachment; filename="{filename}"'})


def _reuse(store, existing: dict, run: dict) -> dict:
    return {"queue": existing,
            "items": _labeled(store.list_review_items(existing["queue_id"])),
            "reused": True, "run": runstore.summary(run)}


def _labeled(items: list[dict]) -> list[dict]:
    return [{**item, "label_code": reasons.label(item["code"])}
            for item in items]


def _now() -> str:
    return dt.datetime.now(config.UTC).isoformat(timespec="seconds")
