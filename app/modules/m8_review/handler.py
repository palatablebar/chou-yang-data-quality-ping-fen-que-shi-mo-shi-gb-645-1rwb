"""M8 入口：复核队列页、发布、当前队列 API 与队列 CSV 导出。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ...web import Ctx, get_ctx, render
from . import service

router = APIRouter(tags=["M8 复核队列"])


@router.get("/api/review")
def api_review(run_id: str | None = None, ctx: Ctx = Depends(get_ctx)) -> dict:
    run = ctx.current_run(run_id)
    view = service.current(ctx.store, run)
    return {"run_id": run["run_id"] if run else None,
            "dataset": run["name"] if run else None, **view}


@router.post("/api/review/publish")
def api_publish(payload: dict | None = None,
                ctx: Ctx = Depends(get_ctx)) -> dict:
    body = payload or {}
    return service.publish(ctx.store, body.get("run_id"))


@router.get("/api/export/review.csv")
def export_review(run_id: str | None = None, ctx: Ctx = Depends(get_ctx)):
    return service.export_csv(ctx.store, run_id)


@router.get("/review")
def page_review(request: Request, run_id: str | None = None,
                ctx: Ctx = Depends(get_ctx)):
    run, result = ctx.current_result(run_id)
    view = service.current(ctx.store, run)
    return render(request, "review.html", page="review", page_title="高缺失复核",
                  run=run, result=result, **view)
