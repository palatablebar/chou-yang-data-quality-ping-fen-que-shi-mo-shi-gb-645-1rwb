"""M8 复核队列：段边界裁定、互斥装配、幂等发布、失败回滚、页面导出同源。"""
from __future__ import annotations

import sqlite3

import pytest

from app.modules.m8_review import queue as queue_mod
from app.modules.m8_review import service as review_service
from fixtures import dirty_cases, expected


def agent_case(states: str) -> str:
    """从第 2 行起按 M（缺失）/V（有值）铺 agent 列。"""
    rows = []
    for index, state in enumerate(states):
        rows.append(dirty_cases.make_row(
            ticket=f"T-2026-91{index:04d}", customer=f"00{index + 100}",
            agent="" if state == "M" else "陈允"))
    return dirty_cases.frame(rows)


def segments_of(settings, text: str) -> list[dict]:
    return dirty_cases.pipeline(settings, text)["missing"]["segments"]


def queue_row(store, name: str = "case.csv") -> dict | None:
    return store.current_review_queue(name)


# ---------- 段边界裁定 ----------

def test_single_filled_cell_is_bridged(settings):
    found = segments_of(settings, agent_case("V" + "MM" + "V" + "MM" + "V"))
    assert len(found) == 1
    segment = found[0]
    assert (segment["start_lineno"], segment["end_lineno"]) == (3, 7)
    assert segment["linenos"] == [3, 4, 6, 7]
    assert [bridge["lineno"] for bridge in segment["bridges"]] == [5]
    assert segment["code"] == "K_RUN_BRIDGED"
    assert "第 5 行" in segment["basis"]


def test_raw_runs_miss_the_brided_gap(settings):
    """对照：旧逻辑把中间补回一格的长缺口拆成两段 <3，整段漏掉。"""
    payload = dirty_cases.pipeline(settings, agent_case("V" + "MM" + "V" + "MM" + "V"))
    assert payload["missing"]["runs"] == []
    assert len(payload["missing"]["segments"]) == 1


def test_gap_wider_than_limit_breaks_segment(settings):
    found = segments_of(settings, agent_case("V" + "MMM" + "VV" + "MMM" + "V"))
    assert [(item["start_lineno"], item["end_lineno"]) for item in found] == [(3, 5), (8, 10)]
    assert all(item["code"] == "K_MISSING_RUN" for item in found)
    assert all(item["bridges"] == [] for item in found)


def test_chained_bridges_merge_into_one_segment(settings):
    found = segments_of(settings, agent_case("V" + "MM" + "V" + "M" + "V" + "MM" + "V"))
    assert len(found) == 1
    assert found[0]["linenos"] == [3, 4, 6, 8, 9]
    assert [bridge["lineno"] for bridge in found[0]["bridges"]] == [5, 7]


def test_merged_segment_below_min_length_excluded(settings):
    assert segments_of(settings, agent_case("V" + "M" + "V" + "M" + "V")) == []


def test_segments_partition_missing_cells(settings):
    """每条缺失格至多进一个整改项：段与段互不相交。"""
    text = agent_case("V" + "MMM" + "VV" + "MMM" + "V" + "MM" + "V" + "MM")
    found = segments_of(settings, text)
    cells = [(item["column"], lineno) for item in found for lineno in item["linenos"]]
    assert len(cells) == len(set(cells))
    payload = queue_mod.build_queue(found)
    assert payload["disjoint"] is True
    assert payload["cell_count"] == len(cells)


def test_queue_rejects_overlapping_segment():
    segment = {"column": "agent", "label": "受理人", "start_lineno": 3,
               "end_lineno": 5, "length": 3, "span": 3, "linenos": [3, 4, 5],
               "bridges": [], "bridged": False, "basis": "",
               "code": "K_MISSING_RUN", "label_code": "连续缺失段"}
    with pytest.raises(ValueError, match="重复进入整改项"):
        queue_mod.build_queue([segment, segment])


def test_queue_priority_length_desc_then_column(settings):
    rows = []
    for index in range(6):
        rows.append(dirty_cases.make_row(
            ticket=f"T-2026-91{index:04d}", customer=f"00{index + 100}",
            agent="" if index < 3 else "陈允",
            region="" if index < 5 else "华东"))
    payload = queue_mod.build_queue(
        segments_of(settings, dirty_cases.frame(rows)))
    assert [(item["column"], item["length"]) for item in payload["items"]] == [
        ("region", 5), ("agent", 3)]
    assert [item["item_id"] for item in payload["items"]] == ["RQ-001", "RQ-002"]


def test_dirty_sample_segments_match_expected(dirty_result):
    found = {item["column"]: (item["start_lineno"], item["end_lineno"],
                              item["length"])
             for item in dirty_result["missing"]["segments"]}
    assert found == expected.MISSING_SEGMENTS
    summary = dirty_result["missing"]["segment_summary"]
    assert summary["items"] == 2 and summary["bridged_items"] == 0


# ---------- 发布与幂等 ----------

def test_publish_then_republish_same_attachment_reuses(store, settings):
    result = dirty_cases.run_text(settings, store, agent_case("V" + "MMM" + "V"),
                                  name="case.csv")
    first = review_service.publish(store, result["run"]["run_id"])
    assert first["reused"] is False
    assert first["queue"]["version"] == 1
    assert first["queue"]["item_count"] == 1

    replay = dirty_cases.run_text(settings, store, agent_case("V" + "MMM" + "V"),
                                  name="case.csv")
    assert replay["replay"] is True
    second = review_service.publish(store, replay["run"]["run_id"])
    assert second["reused"] is True
    assert second["queue"]["queue_id"] == first["queue"]["queue_id"]
    assert second["queue"]["version"] == 1
    with store.connect() as conn:
        total = conn.execute("SELECT COUNT(*) AS n FROM review_items").fetchone()["n"]
        queues = conn.execute("SELECT COUNT(*) AS n FROM review_queues").fetchone()["n"]
    assert total == first["queue"]["item_count"]
    assert queues == 1


def test_new_attachment_version_supersedes_and_keeps_history(store, settings):
    dirty_cases.run_text(settings, store, agent_case("V" + "MMM" + "V"), name="case.csv")
    first = review_service.publish(store)
    dirty_cases.run_text(settings, store, agent_case("V" + "MMMM" + "V"), name="case.csv")
    second = review_service.publish(store)
    assert second["reused"] is False
    assert second["queue"]["version"] == 2

    current = queue_row(store)
    assert current["queue_id"] == second["queue"]["queue_id"]
    history = store.list_review_queues("case.csv")
    assert [(entry["version"], entry["status"]) for entry in history] == [
        (2, "published"), (1, "superseded")]
    old_items = store.list_review_items(first["queue"]["queue_id"])
    assert len(old_items) == first["queue"]["item_count"]


def test_failed_publish_rolls_back_to_previous_version(store):
    base = {"name": "case.csv", "rules_version": "rv", "run_id": "r1",
            "status": "published", "item_count": 1, "cell_count": 3,
            "created_at": "t", "published_at": "t"}
    item = {"seq": 1, "item_id": "RQ-001", "column": "agent", "label": "受理人",
            "start_lineno": 3, "end_lineno": 5, "length": 3, "span": 3,
            "linenos": [3, 4, 5], "bridges": [], "basis": "", "code": "K_MISSING_RUN"}
    version = store.publish_review_queue(
        {**base, "queue_id": "QR-1", "digest": "d1"}, [item])
    assert version == 1
    with pytest.raises(sqlite3.IntegrityError):
        store.publish_review_queue(
            {**base, "queue_id": "QR-2", "digest": "d2"},
            [item, {**item, "item_id": "RQ-002"}])  # (queue_id, seq) 撞主键
    current = queue_row(store)
    assert current["queue_id"] == "QR-1"
    assert current["status"] == "published"
    assert [entry["item_id"] for entry in store.list_review_items("QR-1")] == ["RQ-001"]
    assert store.list_review_items("QR-2") == []
    assert [entry["version"] for entry in store.list_review_queues("case.csv")] == [1]


def test_failed_publish_via_service_keeps_previous(store, settings, monkeypatch):
    dirty_cases.run_text(settings, store, agent_case("V" + "MMM" + "V"), name="case.csv")
    first = review_service.publish(store)
    assert first["queue"]["version"] == 1
    dirty_cases.run_text(settings, store, agent_case("V" + "MMMM" + "V"), name="case.csv")

    original = queue_mod.build_queue

    def broken(segments):
        payload = original(segments)
        payload["items"] = payload["items"] + payload["items"]  # 撞 (queue_id, seq)
        return payload

    monkeypatch.setattr(queue_mod, "build_queue", broken)
    with pytest.raises(sqlite3.IntegrityError):
        review_service.publish(store)
    current = queue_row(store)
    assert current["version"] == 1
    assert current["queue_id"] == first["queue"]["queue_id"]
    assert len(store.list_review_items(current["queue_id"])) == 1


# ---------- 页面与导出同源 ----------

def test_page_and_export_read_same_published_queue(client_with_run):
    published = client_with_run.post("/api/review/publish", json={})
    assert published.status_code == 200, published.text
    assert published.json()["reused"] is False

    api = client_with_run.get("/api/review").json()
    page = client_with_run.get("/review")
    exported = client_with_run.get("/api/export/review.csv")
    assert page.status_code == 200
    assert exported.status_code == 200

    api_ids = [item["item_id"] for item in api["items"]]
    assert api_ids == ["RQ-001", "RQ-002"]
    for item_id in api_ids:
        assert item_id in page.text
        assert item_id in exported.text
    lines = [line for line in exported.text.splitlines() if line.strip()]
    assert len(lines) == len(api_ids) + 1
    assert "原始缺失格" in lines[0] and "连接依据" in lines[0]


def test_export_before_publish_returns_404(client_with_run):
    response = client_with_run.get("/api/export/review.csv")
    assert response.status_code == 404
    assert response.json()["error"] == "E_QUEUE_NOT_FOUND"


def test_review_page_empty_state(client):
    response = client.get("/review")
    assert response.status_code == 200
    assert "还没有运行记录" in response.text


def test_preview_disappears_after_publish(client_with_run):
    before = client_with_run.get("/api/review").json()
    assert before["queue"] is None
    assert before["preview"]["item_count"] == 2
    client_with_run.post("/api/review/publish", json={})
    after = client_with_run.get("/api/review").json()
    assert after["queue"]["version"] == 1
    assert after["preview"] is None
    assert len(after["items"]) == 2
