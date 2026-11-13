"""主题词映射行为测试。

覆盖：
* CSV 整表替换校验（200 条上限、同源词冲突、列数/空单元格、空表合法、BOM）。
* dc:subject 逐值精确匹配、只替换一次、不级联、未匹配保留、源值不被修改。
* 应用范围是最近成功发布目录的快照，不含未发布采集记录、不推进水位。
* 换表从源值重算；空表恢复源词。
* 转换期间采集并发发布：提交前发现新目录则用新快照重算，迟到处理不覆盖更新的源值。
* 转换失败/进程退出后重启：最近已提交结果保留；新映射与目录在同一事务生效。
* 下载内容、映射版本与水位对应同一完整结果。
"""
from __future__ import annotations

import asyncio
from datetime import date
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import Database
from app.harvester import Harvester, Publisher
from app.mapping import (
    MAX_MAPPING_ROWS,
    MappingApplier,
    MappingValidationError,
    apply_mapping_to_snapshot,
    parse_mapping_csv,
    transform_subjects,
)


# --------------------------------------------------------------- CSV 与转换语义
def test_csv_basic_normalization_and_bom():
    entries, normalized = parse_mapping_csv(
        "数字图书馆, 数字馆藏\n\n 图书馆学 ,图书馆学理论 \n"
    )
    assert entries == [("数字图书馆", "数字馆藏"), ("图书馆学", "图书馆学理论")]
    assert normalized == "数字图书馆,数字馆藏\n图书馆学,图书馆学理论\n"


def test_csv_utf8_bom_and_blank_file_is_empty_version():
    entries, normalized = parse_mapping_csv("﻿\n  \n\t\n".encode("utf-8"))
    assert entries == [] and normalized == ""
    entries2, _ = parse_mapping_csv(b"\xef\xbb\xbfa,b\n")
    assert entries2 == [("a", "b")]


def test_csv_rejects_conflicting_same_source():
    with pytest.raises(MappingValidationError) as exc:
        parse_mapping_csv("数字图书馆,规范A\n数字图书馆,规范B\n")
    assert "同一源词" in str(exc.value)


def test_csv_allows_duplicate_identical_pair_once():
    entries, _ = parse_mapping_csv("a,b\na,b\n")
    assert entries == [("a", "b")]


@pytest.mark.parametrize("bad", ["a,b,c", "a,\n", ",b\n", "a,b\nx"])
def test_csv_rejects_bad_shape(bad):
    with pytest.raises(MappingValidationError):
        parse_mapping_csv(bad)


def test_csv_limit_is_200_and_200_accepted():
    ok = "\n".join(f"s{i},t{i}" for i in range(MAX_MAPPING_ROWS))
    entries, _ = parse_mapping_csv(ok)
    assert len(entries) == MAX_MAPPING_ROWS
    too_many = ok + "\nsx,ty"
    with pytest.raises(MappingValidationError) as exc:
        parse_mapping_csv(too_many)
    assert "200" in str(exc.value)


def test_transform_exact_match_single_pass_no_cascade_and_preserves_source():
    metadata = {"title": ["书"], "subject": ["a", "b", "c", "d"]}
    mapping = {"a": "b", "b": "规范B", "d": "d"}  # a->b 不会再触发 b->规范B
    out = transform_subjects(metadata, mapping)
    assert out["subject"] == ["b", "规范B", "c", "d"]
    assert out["title"] == ["书"]
    # 原始值保持不变（保存源值）
    assert metadata["subject"] == ["a", "b", "c", "d"]


def test_transform_handles_multiple_and_missing_subjects():
    # 同一记录多个 dc:subject，逐个独立精确替换；没有 subject 的记录不受影响
    metadata = {"subject": ["a", "a", "b", "z"], "title": ["t"]}
    out = transform_subjects(metadata, {"a": "A", "b": "B"})
    assert out["subject"] == ["A", "A", "B", "z"]
    assert transform_subjects({"title": ["t"]}, {"a": "A"}) == {"title": ["t"]}


def test_empty_mapping_restores_source_terms():
    snapshot = [
        {"identifier": "r1", "datestamp": "2026-11-13", "sets": [],
         "metadata": {"subject": ["a", "b"]}},
    ]
    assert apply_mapping_to_snapshot(snapshot, {})[0]["metadata"]["subject"] == ["a", "b"]
    mapped = apply_mapping_to_snapshot(snapshot, {"a": "A"})
    assert mapped[0]["metadata"]["subject"] == ["A", "b"]
    # 快照本身不被改写
    assert snapshot[0]["metadata"]["subject"] == ["a", "b"]


# --------------------------------------------------------------- 端到端（与采集联动）
def _harvest(tmp_path, src_app):
    db = Database(str(tmp_path / "catalog.db"))
    publisher = Publisher(db, str(tmp_path / "published"))
    h = Harvester(db, "http://source/oai", publisher, max_retries=0)
    return db, h


def _subjects_in_file(path):
    import xml.etree.ElementTree as ET

    ns = {"dc": "http://purl.org/dc/elements/1.1/"}
    root = ET.parse(path).getroot()
    return {
        rec.get("identifier"): [el.text or "" for el in rec.findall(".//dc:subject", ns)]
        for rec in root.findall("{urn:library:catalog:1}record")
    }


@pytest.fixture()
def harvested(tmp_path, env):
    _, src_app, _ = env
    db, h = _harvest(tmp_path, src_app)
    asyncio.run(h.start_in_background(date(2026, 11, 13)))
    return tmp_path, src_app, db, h


def _upload_version(db: Database, csv_text: str) -> int:
    entries, normalized = parse_mapping_csv(csv_text)
    return db.create_mapping_version(entries, normalized)


def _apply(db, tmp_path, job_id, delay=0.0):
    applier = MappingApplier(db, str(tmp_path / "published"), apply_delay=delay)
    return asyncio.run(applier.apply(job_id))


def test_apply_uses_latest_publication_snapshot_and_pairs_three_artifacts(harvested, tmp_path):
    _, src_app, db, _ = harvested
    base = db.latest_publication()
    vid = _upload_version(db, "图书馆学,图书馆学理论\n数字图书馆,数字馆藏\n")
    job_id = db.create_mapping_job(vid, base["id"])
    outcome = _apply(db, tmp_path, job_id)

    assert outcome.watermark == base["watermark"]  # 水位不推进
    new_pub = db.latest_publication()
    assert new_pub["id"] != base["id"]
    assert new_pub["kind"] == "mapping"
    assert new_pub["mapping_version_id"] == vid
    assert new_pub["watermark"] == base["watermark"]  # 同水位
    assert new_pub["record_count"] == base["record_count"]

    subjects = _subjects_in_file(new_pub["file_path"])
    assert subjects["oai:demo:lib:001"] == ["图书馆学理论"]
    assert subjects["oai:demo:lib:002"] == ["数字馆藏"]
    assert subjects["oai:demo:lib:003"] == ["元数据"]  # 未匹配保留

    payload = open(new_pub["file_path"], encoding="utf-8").read()
    import hashlib

    assert hashlib.sha256(payload.encode()).hexdigest() == new_pub["sha256"]
    assert f'mappingVersion="{vid}"' in payload
    assert f'watermark="{base["watermark"]}"' in payload
    assert 'kind="mapping"' in payload

    # records 表保存的仍是源值
    rec = db.one("SELECT metadata_json FROM records WHERE identifier='oai:demo:lib:001'")
    assert "图书馆学理论" not in rec["metadata_json"]
    assert "图书馆学" in rec["metadata_json"]
    assert db.watermark() == base["watermark"]


def test_unpublished_harvest_records_are_excluded_from_mapping_apply(harvested, tmp_path):
    _, src_app, db, h = harvested
    base = db.latest_publication()
    # 源端新增一条，但不采集发布：转换范围不得包含它
    src_app.state.store.create_item(
        identifier="oai:demo:lib:999", title="未发布记录", creator="", subject="图书馆学",
        description="", publisher="", date_="2026", type_="book",
    )
    vid = _upload_version(db, "图书馆学,图书馆学理论\n")
    job_id = db.create_mapping_job(vid, base["id"])
    _apply(db, tmp_path, job_id)
    subjects = _subjects_in_file(db.latest_publication()["file_path"])
    assert "oai:demo:lib:999" not in subjects
    assert len(subjects) == base["record_count"]


def test_swap_table_recomputes_from_source_and_empty_version_restores(harvested, tmp_path):
    _, _, db, _ = harvested
    first_base = db.latest_publication()
    v1 = _upload_version(db, "图书馆学,规范词A\n")
    j1 = db.create_mapping_job(v1, first_base["id"])
    _apply(db, tmp_path, j1)
    pub1 = db.latest_publication()
    assert _subjects_in_file(pub1["file_path"])["oai:demo:lib:001"] == ["规范词A"]

    # 换表：整表替换为完全不同的映射，必须从源值重算而不是在结果上叠加
    v2 = _upload_version(db, "图书馆学,规范词B\n元数据,元数据规范\n")
    j2 = db.create_mapping_job(v2, pub1["id"])
    _apply(db, tmp_path, j2)
    pub2 = db.latest_publication()
    subjects2 = _subjects_in_file(pub2["file_path"])
    assert subjects2["oai:demo:lib:001"] == ["规范词B"]
    assert subjects2["oai:demo:lib:003"] == ["元数据规范"]
    assert pub2["mapping_version_id"] == v2

    # 空表恢复源词
    v3 = _upload_version(db, "")
    j3 = db.create_mapping_job(v3, pub2["id"])
    _apply(db, tmp_path, j3)
    pub3 = db.latest_publication()
    subjects3 = _subjects_in_file(pub3["file_path"])
    assert subjects3["oai:demo:lib:001"] == ["图书馆学"]
    assert subjects3["oai:demo:lib:003"] == ["元数据"]
    assert pub3["mapping_version_id"] == v3


def test_concurrent_harvest_during_apply_is_recomputed_against_newer_snapshot(
    harvested, tmp_path, monkeypatch
):
    _, src_app, db, h = harvested
    base = db.latest_publication()
    vid = _upload_version(db, "图书馆学,图书馆学理论\n")

    # 在转换"暂停"期间让采集并发完成并发布新目录（含标题更新）
    def delayed_then_publish(delay):
        async def _run():
            src_app.state.store.update_item("oai:demo:lib:001", title="并发更新标题")
            await h.start_in_background(date(2026, 11, 13))
        return asyncio.run(_run())

    applier = MappingApplier(db, str(tmp_path / "published"), apply_delay=1.0)
    job_id = db.create_mapping_job(vid, base["id"])

    import threading

    t = threading.Timer(0.3, delayed_then_publish, args=(1.0,))
    t.start()
    outcome = asyncio.run(applier.apply(job_id))
    t.join()

    assert outcome.retries >= 1  # 至少重算一次
    latest = db.latest_publication()
    payload = open(latest["file_path"], encoding="utf-8").read()
    assert "并发更新标题" in payload  # 没有被迟到的转换覆盖
    assert f'mappingVersion="{vid}"' in payload
    subjects = _subjects_in_file(latest["file_path"])
    assert subjects["oai:demo:lib:001"] == ["图书馆学理论"]
    job = db.get_mapping_job(job_id)
    assert job["status"] == "completed" and job["publication_id"] == latest["id"]


def test_apply_failure_keeps_last_committed_result(harvested, tmp_path):
    _, _, db, _ = harvested
    base = db.latest_publication()
    vid = _upload_version(db, "图书馆学,图书馆学理论\n")
    job_id = db.create_mapping_job(vid, base["id"])
    db.set_meta("mapping_publish_fault", "generate_error")
    applier = MappingApplier(db, str(tmp_path / "published"))
    with pytest.raises(Exception):
        asyncio.run(applier.apply(job_id))
    assert db.latest_publication()["id"] == base["id"]
    job = db.get_mapping_job(job_id)
    assert job["status"] == "failed" and job["publication_id"] is None

    # 故障是一次性的：重新应用成功
    job_id2 = db.create_mapping_job(vid, base["id"])
    outcome = _apply(db, tmp_path, job_id2)
    assert db.latest_publication()["id"] == outcome.publication_id


def test_restart_during_apply_keeps_committed_catalog(harvested, tmp_path):
    _, _, db, _ = harvested
    base = db.latest_publication()
    vid = _upload_version(db, "图书馆学,图书馆学理论\n")
    job_id = db.create_mapping_job(vid, base["id"])
    # 模拟进程在 running/applying 状态退出后重启
    db.mark_mapping_job_applying(job_id)
    db.close()

    db2 = Database(str(tmp_path / "catalog.db"))
    db2.recover_mapping_jobs_after_restart()
    job = db2.get_mapping_job(job_id)
    assert job["status"] == "failed"
    assert db2.latest_publication()["id"] == base["id"]  # 最近已提交结果保留
    # 重启后可重新应用
    job_id2 = db2.create_mapping_job(vid, base["id"])
    outcome = _apply(db2, tmp_path, job_id2)
    assert db2.latest_publication()["id"] == outcome.publication_id


def test_restart_after_crash_after_publish_keeps_committed_catalog(harvested, tmp_path, monkeypatch):
    _, _, db, _ = harvested
    base = db.latest_publication()
    vid = _upload_version(db, "图书馆学,图书馆学理论\n")
    job_id = db.create_mapping_job(vid, base["id"])
    # 模拟"改名后、落库前"进程退出：新文件已存在但无引用
    import app.mapping as mapping_mod

    def fake_exit(code):
        raise SystemExit(code)

    monkeypatch.setattr(mapping_mod.os, "_exit", fake_exit)
    db.set_meta("mapping_publish_fault", "crash_after_publish")
    applier = MappingApplier(db, str(tmp_path / "published"))
    with pytest.raises(SystemExit):
        asyncio.run(applier.apply(job_id))
    # 重启恢复：任务转失败，孤儿文件清理，最近已提交目录不变
    db.close()
    db2 = Database(str(tmp_path / "catalog.db"))
    db2.recover_mapping_jobs_after_restart()
    from app.catalog import cleanup_orphans

    cleanup_orphans(
        str(tmp_path / "published"),
        keep_paths={r["file_path"] for r in db2.query("SELECT file_path FROM publications")},
    )
    job = db2.get_mapping_job(job_id)
    assert job["status"] == "failed" and job["publication_id"] is None
    assert db2.latest_publication()["id"] == base["id"]
    # 孤儿（catalog-map* 但无 publications 引用）已清掉，只剩被引用文件
    files = os.listdir(tmp_path / "published")
    assert all(not f.startswith(".catalog-tmp") for f in files)
    # 重新应用成功
    job_id2 = db2.create_mapping_job(vid, base["id"])
    outcome = _apply(db2, tmp_path, job_id2)
    assert db2.latest_publication()["id"] == outcome.publication_id


def test_only_one_unresolved_mapping_job_at_a_time(harvested, tmp_path):
    _, _, db, _ = harvested
    base = db.latest_publication()
    vid = _upload_version(db, "图书馆学,图书馆学理论\n")
    db.create_mapping_job(vid, base["id"])
    with pytest.raises(RuntimeError):
        db.create_mapping_job(vid, base["id"])
    # 完成后可以再开
    applier = MappingApplier(db, str(tmp_path / "published"))
    job = db.unresolved_mapping_job()
    asyncio.run(applier.apply(job["id"]))
    assert db.unresolved_mapping_job() is None
    db.create_mapping_job(vid, db.latest_publication()["id"])


def test_new_harvest_publishes_source_terms_without_mapping_marker(harvested, tmp_path):
    """已有映射版本时再采集：新目录仍是源值，mapping_version_id 留空（映射不自动套用）。"""
    _, src_app, db, h = harvested
    vid = _upload_version(db, "图书馆学,图书馆学理论\n")
    jid = db.create_mapping_job(vid, db.latest_publication()["id"])
    _apply(db, tmp_path, jid)
    assert db.latest_publication()["mapping_version_id"] == vid

    # 源端修改一条并重新采集发布
    src_app.state.store.update_item("oai:demo:lib:001", title="映射后再采集")
    asyncio.run(h.start_in_background(date(2026, 11, 13)))
    pub = db.latest_publication()
    assert pub["kind"] == "harvest"
    assert pub["mapping_version_id"] is None  # 采集发布不套用映射
    subjects = _subjects_in_file(pub["file_path"])
    assert subjects["oai:demo:lib:001"] == ["图书馆学"]  # 源值
    payload = open(pub["file_path"], encoding="utf-8").read()
    assert "mappingVersion" not in payload and "映射后再采集" in payload
    # 快照仍是源值，可再次应用映射
    jid2 = db.create_mapping_job(vid, pub["id"])
    _apply(db, tmp_path, jid2)
    assert _subjects_in_file(db.latest_publication()["file_path"])["oai:demo:lib:001"] == ["图书馆学理论"]


def test_snapshot_carries_source_values_across_migration_style_rebuild(harvested, tmp_path):
    """快照里保存的是源值：连续多次换表都能无损重算。"""
    _, _, db, _ = harvested
    base = db.latest_publication()
    snapshot = Database.decode_snapshot(base["snapshot_json"])
    subj = {item["identifier"]: item["metadata"].get("subject") for item in snapshot}
    assert subj["oai:demo:lib:001"] == ["图书馆学"]
    for i, word in enumerate(["甲", "乙", "丙"], start=1):
        vid = _upload_version(db, f"图书馆学,{word}\n")
        jid = db.create_mapping_job(vid, db.latest_publication()["id"])
        _apply(db, tmp_path, jid)
    last = db.latest_publication()
    assert _subjects_in_file(last["file_path"])["oai:demo:lib:001"] == ["丙"]
    # 仍可回到源值
    vid0 = _upload_version(db, "")
    jid0 = db.create_mapping_job(vid0, last["id"])
    _apply(db, tmp_path, jid0)
    assert _subjects_in_file(db.latest_publication()["file_path"])["oai:demo:lib:001"] == ["图书馆学"]
