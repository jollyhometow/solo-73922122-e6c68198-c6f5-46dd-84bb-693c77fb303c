"""SQLite 持久层。

关键约束
--------
* ``records`` 按 OAI identifier 唯一；删除只是置 ``deleted=1``，并且永远保留，
  绝不以"本轮未出现"推断删除。
* ``records.metadata_json`` 永远保存 OAI 源端的**原始** dc 值（含原始 dc:subject）；
  主题词映射只在生成目录时应用，换表/空表都能从源值重算。
* 任意时刻最多有一个未解决的采集中/发布中轮次，由部分唯一索引兜底。
* 任意时刻最多有一个未解决的主题词转换任务（``mapping_jobs`` 同样有部分唯一索引）。
* 每次发布写一条 publications 记录：水位、文件路径、sha256、**主题词映射版本**
  与**发布时刻的活动记录快照**在同一行；下载端只从该行解析，因此"下载内容 ↔
  映射版本 ↔ 采集水位"永远对应同一次完整结果。
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
import sqlite3
import threading
from typing import Any, Iterator

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    identifier    TEXT PRIMARY KEY,
    datestamp     TEXT,
    deleted       INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT,
    metadata_xml  TEXT,
    sets_json     TEXT NOT NULL DEFAULT '[]',
    first_seen_run INTEGER,
    updated_run   INTEGER,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_deleted ON records(deleted);

CREATE TABLE IF NOT EXISTS runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mode            TEXT NOT NULL,              -- full | incremental
    window_from     TEXT,                       -- NULL = 全量(不带 from)
    window_until    TEXT NOT NULL,
    status          TEXT NOT NULL,              -- running|paused|publishing|publish_failed|completed|canceled
    resumption_token TEXT,
    pages_done      INTEGER NOT NULL DEFAULT 0,
    records_seen    INTEGER NOT NULL DEFAULT 0,
    complete_list_size INTEGER,
    cursor          INTEGER,
    token_resets    INTEGER NOT NULL DEFAULT 0,
    error_code      TEXT,
    error_message   TEXT,
    notice          TEXT,
    started_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    finished_at     TEXT
);

-- 同一时间只允许一轮未解决采集；paused/publish_failed 需先继续、重试或放弃。
CREATE UNIQUE INDEX IF NOT EXISTS uq_runs_unresolved
    ON runs((1)) WHERE status IN ('running','paused','publishing','publish_failed');

CREATE TABLE IF NOT EXISTS publications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       INTEGER REFERENCES runs(id),          -- NULL = 主题词转换发布（非采集轮次）
    kind         TEXT NOT NULL DEFAULT 'harvest',      -- harvest | mapping
    watermark    TEXT NOT NULL,
    file_path    TEXT NOT NULL UNIQUE,
    file_name    TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    size_bytes   INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    mapping_version_id INTEGER REFERENCES mapping_versions(id),
    snapshot_json TEXT,
    published_at TEXT NOT NULL
);

-- 上传成功（含校验通过）的主题词映射版本。整表替换：新版本是一份完整映射，
-- 从不在旧版本上增量合并；entry_count=0 表示空表（恢复源词）。
CREATE TABLE IF NOT EXISTS mapping_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_count INTEGER NOT NULL,
    source_csv  TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mapping_entries (
    version_id INTEGER NOT NULL REFERENCES mapping_versions(id) ON DELETE CASCADE,
    source_term TEXT NOT NULL,
    target_term TEXT NOT NULL,
    PRIMARY KEY(version_id, source_term)
);

-- 主题词转换任务：applied 成功后生成一份与采集发布同构的 publications 行。
-- 同期只应用一份：running/applying 未结束时部分唯一索引挡住新任务。
CREATE TABLE IF NOT EXISTS mapping_jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id  INTEGER NOT NULL REFERENCES mapping_versions(id),
    base_publication_id INTEGER REFERENCES publications(id),
    status      TEXT NOT NULL,           -- running|applying|failed|completed
    error_message TEXT,
    publication_id INTEGER REFERENCES publications(id),
    started_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    finished_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_mapping_jobs_unresolved
    ON mapping_jobs((1)) WHERE status IN ('running','applying');

"""

UNRESOLVED = ("running", "paused", "publishing", "publish_failed")
MAPPING_JOB_UNRESOLVED = ("running", "applying")


def _migrate(conn: sqlite3.Connection) -> None:
    """对旧版本数据库做幂等迁移（全新库的表已按新 SCHEMA 建好，自动跳过）。

    旧库的 publications 没有 mapping_version_id/snapshot_json 列且 run_id 为
    NOT NULL；用"建新表 → 拷数据 → 改名"的标准 SQLite 方式重建，已发布的
    水位/文件引用一行不丢。
    """
    cols = {row[1]: row for row in conn.execute("PRAGMA table_info(publications)")}
    if not cols:
        return
    needs_rebuild = (
        "mapping_version_id" not in cols
        or "snapshot_json" not in cols
        or "kind" not in cols
        or bool(cols["run_id"][3])  # notnull 标志位
    )
    if not needs_rebuild:
        return
    conn.executescript(
        """
        CREATE TABLE publications_new (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       INTEGER REFERENCES runs(id),
            kind         TEXT NOT NULL DEFAULT 'harvest',
            watermark    TEXT NOT NULL,
            file_path    TEXT NOT NULL UNIQUE,
            file_name    TEXT NOT NULL,
            record_count INTEGER NOT NULL,
            size_bytes   INTEGER NOT NULL,
            sha256       TEXT NOT NULL,
            mapping_version_id INTEGER,
            snapshot_json TEXT,
            published_at TEXT NOT NULL
        );
        INSERT INTO publications_new
            (id, run_id, kind, watermark, file_path, file_name, record_count, size_bytes,
             sha256, mapping_version_id, snapshot_json, published_at)
        SELECT id, run_id, 'harvest', watermark, file_path, file_name, record_count, size_bytes,
               sha256, NULL, NULL, published_at
        FROM publications;
        DROP TABLE publications;
        ALTER TABLE publications_new RENAME TO publications;
        """
    )


def utcnow() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Run:
    id: int
    mode: str
    window_from: str | None
    window_until: str
    status: str
    resumption_token: str | None
    pages_done: int
    records_seen: int
    complete_list_size: int | None
    cursor: int | None
    token_resets: int
    error_code: str | None
    error_message: str | None
    notice: str | None
    started_at: str
    updated_at: str
    finished_at: str | None

    @property
    def is_unresolved(self) -> bool:
        return self.status in UNRESOLVED

    @property
    def is_resumable(self) -> bool:
        return self.status == "paused"

    @property
    def is_publish_retryable(self) -> bool:
        return self.status == "publish_failed"

    @property
    def has_resumption_token(self) -> bool:
        return bool(self.resumption_token)

    @property
    def progress_fraction(self) -> float | None:
        if self.complete_list_size and self.complete_list_size > 0:
            return min(1.0, self.records_seen / self.complete_list_size)
        return None


class Database:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent or ".", exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.executescript(SCHEMA)
        with self._lock:
            _migrate(self._conn)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def query(self, sql: str, *params: Any) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params))

    def one(self, sql: str, *params: Any) -> sqlite3.Row | None:
        rows = self.query(sql, *params)
        return rows[0] if rows else None

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.one("SELECT value FROM meta WHERE key = ?", key)
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def watermark(self) -> str | None:
        """展示/续采水位只取自最近一次已提交的发布记录，保证与下载内容成对。"""
        latest = self.latest_publication()
        if latest is None:
            return None
        return latest["watermark"]

    def take_publish_fault(self) -> str | None:
        """一次性故障注入（演示用）；读后即清除，避免一直污染后续发布。"""
        with self.tx() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = 'publish_fault'").fetchone()
            if row is None or row["value"] == "none":
                return None
            conn.execute("DELETE FROM meta WHERE key = 'publish_fault'")
            return row["value"]

    # --------------------------------------------------------------- records
    def upsert_record(self, rec: dict[str, Any], run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                INSERT INTO records(identifier, datestamp, deleted, metadata_json, metadata_xml,
                                    sets_json, first_seen_run, updated_run, updated_at)
                VALUES(:identifier, :datestamp, :deleted, :metadata_json, :metadata_xml,
                       :sets_json, :run_id, :run_id, :updated_at)
                ON CONFLICT(identifier) DO UPDATE SET
                    datestamp     = excluded.datestamp,
                    deleted       = excluded.deleted,
                    metadata_json = excluded.metadata_json,
                    metadata_xml  = excluded.metadata_xml,
                    sets_json     = excluded.sets_json,
                    updated_run   = excluded.updated_run,
                    updated_at    = excluded.updated_at
                """,
                {
                    "identifier": rec["identifier"],
                    "datestamp": rec.get("datestamp"),
                    "deleted": 1 if rec.get("deleted") else 0,
                    "metadata_json": (
                        json.dumps(rec["metadata"], ensure_ascii=False)
                        if rec.get("metadata") is not None
                        else None
                    ),
                    "metadata_xml": rec.get("metadata_xml"),
                    "sets_json": json.dumps(rec.get("sets") or [], ensure_ascii=False),
                    "run_id": run_id,
                    "updated_at": utcnow(),
                },
            )

    def active_records(self) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM records WHERE deleted = 0 ORDER BY identifier")

    def record_stats(self) -> tuple[int, int]:
        row = self.one("SELECT COUNT(*) AS total, COALESCE(SUM(deleted), 0) AS deleted FROM records")
        total = int(row["total"]) if row else 0
        deleted = int(row["deleted"]) if row else 0
        return total - deleted, deleted

    def records_changed_in_run(self, run_id: int) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM records WHERE updated_run = ? ORDER BY updated_at DESC, identifier",
            run_id,
        )

    # ------------------------------------------------------------------ runs
    @staticmethod
    def _run(row: sqlite3.Row) -> Run:
        return Run(**{key: row[key] for key in row.keys()})

    def get_run(self, run_id: int) -> Run | None:
        row = self.one("SELECT * FROM runs WHERE id = ?", run_id)
        return self._run(row) if row else None

    def unresolved_run(self) -> Run | None:
        row = self.one(
            f"SELECT * FROM runs WHERE status IN ({','.join('?' * len(UNRESOLVED))}) ORDER BY id DESC LIMIT 1",
            *UNRESOLVED,
        )
        return self._run(row) if row else None

    def create_run(self, mode: str, window_from: date | None, window_until: date) -> Run:
        with self.tx() as conn:
            existing = conn.execute(
                f"SELECT id, status FROM runs WHERE status IN ({','.join('?' * len(UNRESOLVED))}) LIMIT 1",
                UNRESOLVED,
            ).fetchone()
            if existing is not None:
                raise RuntimeError(f"已有未结束的轮次 #{existing['id']}（{existing['status']}）")
            now = utcnow()
            cur = conn.execute(
                """
                INSERT INTO runs(mode, window_from, window_until, status, pages_done, records_seen,
                                 started_at, updated_at)
                VALUES(?, ?, ?, 'running', 0, 0, ?, ?)
                """,
                (mode, window_from.isoformat() if window_from else None, window_until.isoformat(), now, now),
            )
            run_id = int(cur.lastrowid)
        run = self.get_run(run_id)
        assert run is not None
        return run

    def update_progress(
        self,
        run_id: int,
        *,
        token: str | None,
        pages_done: int,
        records_seen: int,
        complete_list_size: int | None = None,
        cursor: int | None = None,
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET resumption_token = ?, pages_done = ?, records_seen = ?,
                       complete_list_size = COALESCE(?, complete_list_size),
                       cursor = COALESCE(?, cursor), updated_at = ?
                 WHERE id = ? AND status = 'running'
                """,
                (token, pages_done, records_seen, complete_list_size, cursor, utcnow(), run_id),
            )

    def add_notice(self, run_id: int, notice: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE runs SET notice = ?, updated_at = ? WHERE id = ?",
                (notice, utcnow(), run_id),
            )

    def complete_harvest(self, run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'publishing', resumption_token = NULL,
                       error_code = NULL, error_message = NULL, updated_at = ?
                 WHERE id = ? AND status = 'running'
                """,
                (utcnow(), run_id),
            )

    def pause_run(self, run_id: int, code: str, message: str, *, notice: str | None = None) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'paused', error_code = ?, error_message = ?,
                       notice = COALESCE(?, notice), updated_at = ?
                 WHERE id = ? AND status IN ('running','publishing')
                """,
                (code, message[:3000], notice, utcnow(), run_id),
            )

    def mark_publish_failed(self, run_id: int, message: str) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'publish_failed', error_code = 'publish_failed',
                       error_message = ?, updated_at = ?
                 WHERE id = ? AND status IN ('publishing','running')
                """,
                (message[:3000], utcnow(), run_id),
            )

    def begin_publish_retry(self, run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'publishing', error_code = NULL, error_message = NULL, updated_at = ?
                 WHERE id = ? AND status = 'publish_failed'
                """,
                (utcnow(), run_id),
            )

    def mark_completed(self, run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs SET status = 'completed', updated_at = ?, finished_at = ?
                 WHERE id = ? AND status = 'publishing'
                """,
                (utcnow(), utcnow(), run_id),
            )

    def cancel_run(self, run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'canceled', resumption_token = NULL, updated_at = ?, finished_at = ?
                 WHERE id = ? AND status IN ('running','paused','publishing','publish_failed')
                """,
                (utcnow(), utcnow(), run_id),
            )

    def reset_for_relist(self, run_id: int, reason: str) -> None:
        """badResumptionToken：保留原窗口，从第一页重采，不改进度水位。"""
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET resumption_token = NULL, pages_done = 0, records_seen = 0,
                       cursor = NULL, token_resets = token_resets + 1,
                       notice = ?, error_code = NULL, error_message = NULL,
                       status = 'running', updated_at = ?
                 WHERE id = ? AND status IN ('running','paused')
                """,
                (reason, utcnow(), run_id),
            )

    def recent_runs(self, limit: int = 20) -> list[Run]:
        rows = self.query("SELECT * FROM runs ORDER BY id DESC LIMIT ?", limit)
        return [self._run(r) for r in rows]

    def recover_after_restart(self) -> list[Run]:
        """进程启动时处理上次遗留状态。

        running -> paused（保留 token，可续采）
        publishing -> 保持，由启动流程根据 publications 判断补完成或重新发布
        """
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'paused', error_code = 'interrupted',
                       error_message = '进程在采集途中退出',
                       notice = '服务重启，本轮已中断；分页进度已保存，可继续采集。',
                       updated_at = ?
                 WHERE status = 'running'
                """,
                (utcnow(),),
            )
        return self.recent_runs(50)

    # ---------------------------------------------------------- publications
    def latest_publication(self) -> sqlite3.Row | None:
        return self.one("SELECT * FROM publications ORDER BY id DESC LIMIT 1")

    def publication_for_run(self, run_id: int) -> sqlite3.Row | None:
        return self.one("SELECT * FROM publications WHERE run_id = ? ORDER BY id DESC LIMIT 1", run_id)

    def record_publication(
        self,
        *,
        run_id: int | None,
        watermark: str,
        file_path: str,
        file_name: str,
        record_count: int,
        size_bytes: int,
        sha256: str,
        kind: str = "harvest",
        mapping_version_id: int | None = None,
        snapshot_json: str | None = None,
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                INSERT INTO publications(run_id, kind, watermark, file_path, file_name, record_count,
                                         size_bytes, sha256, mapping_version_id, snapshot_json,
                                         published_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, kind, watermark, file_path, file_name, record_count, size_bytes, sha256,
                 mapping_version_id, snapshot_json, utcnow()),
            )

    def publications(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM publications ORDER BY id DESC LIMIT ?", limit)

    # --------------------------------------------------------- 主题词映射版本
    def create_mapping_version(self, entries: list[tuple[str, str]], source_csv: str) -> int:
        """整表写入一份新映射版本（entries 已通过校验/去重），返回版本 id。"""
        digest = hashlib.sha256(source_csv.encode("utf-8")).hexdigest()
        with self.tx() as conn:
            now = utcnow()
            cur = conn.execute(
                "INSERT INTO mapping_versions(entry_count, source_csv, sha256, created_at) "
                "VALUES(?, ?, ?, ?)",
                (len(entries), source_csv, digest, now),
            )
            version_id = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO mapping_entries(version_id, source_term, target_term) VALUES(?, ?, ?)",
                [(version_id, s, t) for s, t in entries],
            )
        return version_id

    def latest_mapping_version(self) -> sqlite3.Row | None:
        return self.one("SELECT * FROM mapping_versions ORDER BY id DESC LIMIT 1")

    def get_mapping_version(self, version_id: int) -> sqlite3.Row | None:
        return self.one("SELECT * FROM mapping_versions WHERE id = ?", version_id)

    def mapping_entries(self, version_id: int) -> list[sqlite3.Row]:
        return self.query(
            "SELECT source_term, target_term FROM mapping_entries "
            "WHERE version_id = ? ORDER BY source_term",
            version_id,
        )

    def mapping_dict(self, version_id: int) -> dict[str, str]:
        return {
            row["source_term"]: row["target_term"]
            for row in self.mapping_entries(version_id)
        }

    def active_snapshot(self) -> list[dict[str, Any]]:
        """发布时刻活动记录的原始值快照（dc:subject 保持源端原文，不做映射）。"""
        rows = self.query(
            "SELECT identifier, datestamp, sets_json, metadata_json FROM records "
            "WHERE deleted = 0 ORDER BY identifier"
        )
        return [
            {
                "identifier": row["identifier"],
                "datestamp": row["datestamp"],
                "sets": json.loads(row["sets_json"] or "[]"),
                "metadata": json.loads(row["metadata_json"] or "{}"),
            }
            for row in rows
        ]

    @staticmethod
    def decode_snapshot(snapshot_json: str | None) -> list[dict[str, Any]] | None:
        if not snapshot_json:
            return None
        try:
            data = json.loads(snapshot_json)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, list) else None

    # ----------------------------------------------------------- 映射转换任务
    def unresolved_mapping_job(self) -> sqlite3.Row | None:
        return self.one(
            f"SELECT * FROM mapping_jobs WHERE status IN "
            f"({','.join('?' * len(MAPPING_JOB_UNRESOLVED))}) ORDER BY id DESC LIMIT 1",
            *MAPPING_JOB_UNRESOLVED,
        )

    def create_mapping_job(self, version_id: int, base_publication_id: int) -> int:
        with self.tx() as conn:
            existing = conn.execute(
                f"SELECT id FROM mapping_jobs WHERE status IN "
                f"({','.join('?' * len(MAPPING_JOB_UNRESOLVED))}) LIMIT 1",
                MAPPING_JOB_UNRESOLVED,
            ).fetchone()
            if existing is not None:
                raise RuntimeError(f"已有未结束的主题词转换任务 #{existing['id']}")
            now = utcnow()
            cur = conn.execute(
                "INSERT INTO mapping_jobs(version_id, base_publication_id, status, started_at, updated_at) "
                "VALUES(?, ?, 'running', ?, ?)",
                (version_id, base_publication_id, now, now),
            )
            return int(cur.lastrowid)

    def mark_mapping_job_applying(self, job_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE mapping_jobs SET status='applying', updated_at=? "
                "WHERE id=? AND status IN ('running','applying')",
                (utcnow(), job_id),
            )

    def mark_mapping_job_failed(self, job_id: int, message: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE mapping_jobs SET status='failed', error_message=?, updated_at=?, finished_at=? "
                "WHERE id=? AND status IN ('running','applying')",
                (message[:3000], utcnow(), utcnow(), job_id),
            )

    def complete_mapping_job(self, job_id: int, publication_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE mapping_jobs SET status='completed', publication_id=?, error_message=NULL, "
                "updated_at=?, finished_at=? WHERE id=? AND status IN ('running','applying')",
                (publication_id, utcnow(), utcnow(), job_id),
            )

    def get_mapping_job(self, job_id: int) -> sqlite3.Row | None:
        return self.one("SELECT * FROM mapping_jobs WHERE id = ?", job_id)

    def recent_mapping_jobs(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM mapping_jobs ORDER BY id DESC LIMIT ?", limit)

    def recover_mapping_jobs_after_restart(self) -> None:
        """进程退出后：未提交的转换任务一律转失败，已提交的目录一行不动。

        发布文件与 publications 行在同一事务里落地，因此 running/applying 的任务
        不可能留下"半套"新目录；重启后最近已提交结果自然保持可用。
        """
        with self.tx() as conn:
            rows = conn.execute(
                f"SELECT id, publication_id FROM mapping_jobs "
                f"WHERE status IN ({','.join('?' * len(MAPPING_JOB_UNRESOLVED))})",
                MAPPING_JOB_UNRESOLVED,
            ).fetchall()
            now = utcnow()
            for row in rows:
                if row["publication_id"] is not None:
                    conn.execute(
                        "UPDATE mapping_jobs SET status='completed', updated_at=?, finished_at=? WHERE id=?",
                        (now, now, row["id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE mapping_jobs SET status='failed', error_message=?, "
                        "updated_at=?, finished_at=? WHERE id=?",
                        ("进程在主题词转换过程中退出；已提交目录保持不变，请重新应用。", now, now, row["id"]),
                    )

    def commit_mapping_publication(
        self,
        *,
        job_id: int,
        base_publication_id: int,
        watermark: str,
        file_path: str,
        file_name: str,
        record_count: int,
        size_bytes: int,
        sha256: str,
        mapping_version_id: int,
        snapshot_json: str,
    ) -> tuple[int, int]:
        """转换结果落库的关键事务：先复查"基准目录"是否仍是最新发布。

        若采集在此期间发布了新目录（id 变化），返回 (None, latest_id) 让调用方
        用新快照重算，绝不让迟到的转换覆盖更新的源值。成功则返回 (publication_id, latest_id)。
        """
        with self.tx() as conn:
            latest = conn.execute(
                "SELECT id FROM publications ORDER BY id DESC LIMIT 1"
            ).fetchone()
            latest_id = int(latest["id"]) if latest else 0
            if latest_id != base_publication_id:
                return None, latest_id
            now = utcnow()
            cur = conn.execute(
                """
                INSERT INTO publications(run_id, kind, watermark, file_path, file_name, record_count,
                                         size_bytes, sha256, mapping_version_id, snapshot_json,
                                         published_at)
                VALUES(NULL, 'mapping', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (watermark, file_path, file_name, record_count, size_bytes, sha256,
                 mapping_version_id, snapshot_json, now),
            )
            publication_id = int(cur.lastrowid)
            conn.execute(
                "UPDATE mapping_jobs SET status='completed', publication_id=?, error_message=NULL, "
                "updated_at=?, finished_at=? WHERE id=? AND status IN ('running','applying')",
                (publication_id, now, now, job_id),
            )
            return publication_id, latest_id
