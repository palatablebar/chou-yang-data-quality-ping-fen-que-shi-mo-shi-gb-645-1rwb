"""SQLite 存储：数据集原文、运行记录与完整结果快照。

约定：
- 数据集按名字唯一：同名文件重新采信即覆盖原文，历史运行仍按 digest 保留；
- 运行记录按 (dataset, digest, rules_version) 幂等：同输入重跑不新增行；
- 复核队列按 (dataset, digest, rules_version) 幂等：同一附件重跑沿用原版本，
  不新增重复整改项；发布单事务完成，失败整体回滚，上一完整旧版原样保留；
- 分数与比率落 TEXT（Decimal 字符串），不落 float；
- 页面永远只展示一条运行记录，导出读的是同一条快照。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    name TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    encoding TEXT NOT NULL,
    declared_encoding TEXT,
    bytes INTEGER NOT NULL,
    content BLOB NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    digest TEXT NOT NULL,
    rules_version TEXT NOT NULL,
    version INTEGER NOT NULL,
    score TEXT NOT NULL,
    grade TEXT NOT NULL,
    rows_in INTEGER NOT NULL,
    rows_accepted INTEGER NOT NULL,
    rows_unique INTEGER NOT NULL,
    rows_dup INTEGER NOT NULL,
    rows_quarantine INTEGER NOT NULL,
    rows_blank INTEGER NOT NULL,
    cells_total INTEGER NOT NULL,
    cells_missing INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    seen_seq INTEGER NOT NULL,
    summary_json TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_identity ON runs(name, digest, rules_version);
CREATE INDEX IF NOT EXISTS idx_runs_version ON runs(name, version DESC);
CREATE TABLE IF NOT EXISTS review_queues (
    queue_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    digest TEXT NOT NULL,
    rules_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    cell_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT NOT NULL,
    superseded_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_review_identity
    ON review_queues(name, digest, rules_version);
CREATE INDEX IF NOT EXISTS idx_review_status
    ON review_queues(name, status, version DESC);
CREATE TABLE IF NOT EXISTS review_items (
    queue_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    item_id TEXT NOT NULL,
    column_name TEXT NOT NULL,
    label TEXT NOT NULL,
    start_lineno INTEGER NOT NULL,
    end_lineno INTEGER NOT NULL,
    length INTEGER NOT NULL,
    span INTEGER NOT NULL,
    linenos_json TEXT NOT NULL,
    bridges_json TEXT NOT NULL,
    basis TEXT NOT NULL,
    code TEXT NOT NULL,
    PRIMARY KEY (queue_id, seq)
);
"""


def _dec(text):
    return None if text is None else Decimal(str(text))


class Store:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------- 数据集 ----------
    def save_dataset(self, *, name: str, digest: str, encoding: str,
                     declared_encoding: str | None, data: bytes, stamp: str) -> None:
        """存原始字节：GB18030 文件重算摘要必须与首次采信逐位相同。"""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO datasets (name,digest,encoding,declared_encoding,bytes,
                        content,updated_at) VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(name) DO UPDATE SET
                     digest=excluded.digest, encoding=excluded.encoding,
                     declared_encoding=excluded.declared_encoding,
                     bytes=excluded.bytes, content=excluded.content,
                     updated_at=excluded.updated_at""",
                (name, digest, encoding, declared_encoding, len(data), data, stamp))

    def get_dataset(self, name: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM datasets WHERE name=?", (name,)).fetchone()
        return dict(row) if row else None

    def list_datasets(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name,digest,encoding,bytes,updated_at FROM datasets ORDER BY name"
            ).fetchall()
        return [dict(row) for row in rows]

    # ---------- 运行记录 ----------
    def latest_version(self, name: str) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT MAX(version) AS v FROM runs WHERE name=?", (name,)).fetchone()
        return int(row["v"] or 0)

    def find_run(self, name: str, digest: str, rules_version: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE name=? AND digest=? AND rules_version=?",
                (name, digest, rules_version)).fetchone()
        return dict(row) if row else None

    def save_run(self, run: dict, summary: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO runs (run_id,name,digest,rules_version,version,score,grade,
                        rows_in,rows_accepted,rows_unique,rows_dup,rows_quarantine,
                        rows_blank,cells_total,cells_missing,created_at,finished_at,
                        last_seen_at,seen_seq,summary_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run["run_id"], run["name"], run["digest"], run["rules_version"],
                 int(run["version"]), str(run["score"]), run["grade"],
                 int(run["rows_in"]), int(run["rows_accepted"]), int(run["rows_unique"]),
                 int(run["rows_dup"]), int(run["rows_quarantine"]),
                 int(run["rows_blank"]), int(run["cells_total"]),
                 int(run["cells_missing"]), run["created_at"], run["finished_at"],
                 run["created_at"], self._next_seq(conn),
                 json.dumps(summary, ensure_ascii=False)))

    def touch_run(self, run_id: str, stamp: str) -> None:
        """命中幂等复用时刷新「最近一次查看」，导航回各页仍停在这条记录上。"""
        with self.connect() as conn:
            conn.execute("UPDATE runs SET last_seen_at=?, seen_seq=? WHERE run_id=?",
                         (stamp, self._next_seq(conn), run_id))

    def _next_seq(self, conn) -> int:
        row = conn.execute("SELECT COALESCE(MAX(seen_seq), 0) + 1 AS n FROM runs").fetchone()
        return int(row["n"])

    def latest_run(self, name: str | None = None) -> dict | None:
        with self.connect() as conn:
            if name:
                row = conn.execute(
                    "SELECT * FROM runs WHERE name=? ORDER BY version DESC LIMIT 1",
                    (name,)).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM runs ORDER BY seen_seq DESC LIMIT 1"
                ).fetchone()
        return dict(row) if row else None

    def get_run(self, run_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def list_runs(self, limit: int = 20) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY seen_seq DESC LIMIT ?",
                (limit,)).fetchall()
        return [dict(row) for row in rows]

    def load_result(self, run_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute("SELECT summary_json FROM runs WHERE run_id=?",
                               (run_id,)).fetchone()
        return json.loads(row["summary_json"]) if row else None

    def count_runs(self) -> int:
        with self.connect() as conn:
            return int(conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"])

    # ---------- 复核队列 ----------
    def find_review_queue(self, name: str, digest: str,
                          rules_version: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM review_queues WHERE name=? AND digest=?"
                " AND rules_version=?",
                (name, digest, rules_version)).fetchone()
        return dict(row) if row else None

    def current_review_queue(self, name: str) -> dict | None:
        """数据集当前生效（published）的复核队列，最多一条。"""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM review_queues WHERE name=? AND status='published'"
                " ORDER BY version DESC LIMIT 1", (name,)).fetchone()
        return dict(row) if row else None

    def list_review_queues(self, name: str, limit: int = 10) -> list[dict]:
        """版本历史含已 superseded 的旧版：记录只增不删。"""
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM review_queues WHERE name=?"
                " ORDER BY version DESC LIMIT ?", (name, limit)).fetchall()
        return [dict(row) for row in rows]

    def list_review_items(self, queue_id: str) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM review_items WHERE queue_id=? ORDER BY seq",
                (queue_id,)).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["column"] = item.pop("column_name")
            item["linenos"] = json.loads(item.pop("linenos_json"))
            item["bridges"] = json.loads(item.pop("bridges_json"))
            items.append(item)
        return items

    def publish_review_queue(self, queue: dict, items: list[dict]) -> int:
        """单事务发布：旧版置 superseded、新版与全部整改项一起落库。

        任一步失败整体回滚，上一完整旧版保持 published 不动；版本号在事务
        内取，并发发布被 SQLite 写锁串行化，不会撞号。
        """
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 AS v FROM review_queues"
                " WHERE name=?", (queue["name"],)).fetchone()
            version = int(row["v"])
            conn.execute(
                "UPDATE review_queues SET status='superseded', superseded_at=?"
                " WHERE name=? AND status='published'",
                (queue["published_at"], queue["name"]))
            conn.execute(
                """INSERT INTO review_queues (queue_id,name,digest,rules_version,
                        run_id,version,status,item_count,cell_count,created_at,
                        published_at,superseded_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL)""",
                (queue["queue_id"], queue["name"], queue["digest"],
                 queue["rules_version"], queue["run_id"], version, "published",
                 int(queue["item_count"]), int(queue["cell_count"]),
                 queue["created_at"], queue["published_at"]))
            conn.executemany(
                """INSERT INTO review_items (queue_id,seq,item_id,column_name,label,
                        start_lineno,end_lineno,length,span,linenos_json,
                        bridges_json,basis,code)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(queue["queue_id"], int(item["seq"]), item["item_id"],
                  item["column"], item["label"], int(item["start_lineno"]),
                  int(item["end_lineno"]), int(item["length"]), int(item["span"]),
                  json.dumps(item["linenos"], ensure_ascii=False),
                  json.dumps(item["bridges"], ensure_ascii=False),
                  item["basis"], item["code"]) for item in items])
        return version
