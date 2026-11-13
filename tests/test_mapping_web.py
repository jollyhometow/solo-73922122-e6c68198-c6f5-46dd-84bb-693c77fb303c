"""主题词映射网页端测试：上传/校验、应用、当前映射查看、下载三件套一致与并发。"""
from __future__ import annotations

import os
import sys
import time

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _wait_harvest(client: TestClient):
    db = client.app.state.db
    deadline = time.time() + 30
    while time.time() < deadline:
        run = db.recent_runs(1)[0] if db.recent_runs(1) else None
        if run is not None and run.status in {"completed", "paused", "publish_failed", "canceled"}:
            return
        time.sleep(0.1)
    raise AssertionError("采集未在限定时间内结束")


def _wait_mapping(client: TestClient):
    db = client.app.state.db
    deadline = time.time() + 30
    while time.time() < deadline:
        job = db.unresolved_mapping_job()
        if job is None:
            return db.recent_mapping_jobs(1)[0]
        time.sleep(0.1)
    raise AssertionError("转换任务未在限定时间内结束")


def test_mapping_page_upload_apply_view_and_download(web):
    client, app, _ = web
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait_harvest(client)
    db = app.state.db

    # 页面可打开
    assert client.get("/mapping").status_code == 200

    # 冲突 CSV 被拒绝，不产生新版本
    bad = {"file": ("m.csv", b"a,b\na,c\n", "text/csv")}
    r = client.post("/mapping/upload", files=bad, follow_redirects=True)
    assert "同一源词" in r.text
    assert db.latest_mapping_version() is None

    # 超过 200 条被拒绝
    big = "\n".join(f"s{i},t{i}" for i in range(201)).encode()
    r = client.post("/mapping/upload", files={"file": ("m.csv", big, "text/csv")}, follow_redirects=True)
    assert "200" in r.text

    # 合法上传
    csv_bytes = "图书馆学,图书馆学理论\n数字图书馆,数字馆藏\n".encode("utf-8")
    r = client.post(
        "/mapping/upload", files={"file": ("m.csv", csv_bytes, "text/csv")}, follow_redirects=True
    )
    version = db.latest_mapping_version()
    assert version is not None and version["entry_count"] == 2
    assert "图书馆学理论" in r.text and "图书馆学理论" in client.get("/mapping").text

    # 应用
    r = client.post("/mapping/apply", data={"version_id": version["id"]}, follow_redirects=True)
    assert r.status_code == 200
    job = _wait_mapping(client)
    assert job["status"] == "completed"

    latest = db.latest_publication()
    assert latest["kind"] == "mapping" and latest["mapping_version_id"] == version["id"]

    # 下载内容、映射版本头、水位头对应同一结果
    dl = client.get("/catalog/download")
    assert dl.status_code == 200
    assert dl.headers["x-catalog-mapping-version"] == str(version["id"])
    assert dl.headers["x-catalog-kind"] == "mapping"
    assert dl.headers["x-catalog-watermark"] == latest["watermark"]
    assert f'mappingVersion="{version["id"]}"' in dl.text
    assert "图书馆学理论" in dl.text and "数字馆藏" in dl.text
    assert "图书馆学</dc:subject>" not in dl.text  # 源词已被替换
    assert "元数据" in dl.text  # 未匹配保留

    # 版本 CSV 可下载
    dl_csv = client.get(f"/mapping/versions/{version['id']}/csv")
    assert dl_csv.status_code == 200 and "图书馆学,图书馆学理论" in dl_csv.text


def test_empty_mapping_application_restores_source_terms(web):
    client, app, _ = web
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait_harvest(client)
    db = app.state.db

    client.post(
        "/mapping/upload",
        files={"file": ("m.csv", "图书馆学,规范A\n".encode(), "text/csv")},
        follow_redirects=True,
    )
    v1 = db.latest_mapping_version()
    client.post("/mapping/apply", data={"version_id": v1["id"]}, follow_redirects=True)
    _wait_mapping(client)
    assert "规范A" in client.get("/catalog/download").text

    # 空表上传并应用：恢复源词
    client.post(
        "/mapping/upload", files={"file": ("empty.csv", b"", "text/csv")}, follow_redirects=True
    )
    v_empty = db.latest_mapping_version()
    assert v_empty["entry_count"] == 0
    client.post("/mapping/apply", data={"version_id": v_empty["id"]}, follow_redirects=True)
    _wait_mapping(client)
    dl = client.get("/catalog/download").text
    assert "图书馆学</dc:subject>" in dl and "规范A" not in dl
    assert db.latest_publication()["mapping_version_id"] == v_empty["id"]


def test_apply_blocked_without_publication_and_single_job(web):
    client, app, _ = web
    db = app.state.db
    # 没有目录时上传后应用被拒绝
    client.post(
        "/mapping/upload", files={"file": ("m.csv", b"a,b\n", "text/csv")}, follow_redirects=True
    )
    version = db.latest_mapping_version()
    r = client.post("/mapping/apply", data={"version_id": version["id"]}, follow_redirects=True)
    assert "先完成一轮采集" in r.text
    assert db.unresolved_mapping_job() is None


def test_harvest_can_publish_during_mapping_and_recompute_wins(web):
    client, app, src_app = web
    # build_app 读环境变量时该测试未设延迟；直接给路由闭包使用的 applier 加暂停窗口
    app.state.applier.apply_delay = 1.2
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait_harvest(client)
    db = app.state.db
    base = db.latest_publication()

    client.post(
        "/mapping/upload",
        files={"file": ("m.csv", "图书馆学,图书馆学理论\n".encode(), "text/csv")},
        follow_redirects=True,
    )
    version = db.latest_mapping_version()
    client.post("/mapping/apply", data={"version_id": version["id"]}, follow_redirects=True)
    # 转换暂停期间完成一轮新采集（源端先改一条记录）
    time.sleep(0.3)
    src_app.state.store.update_item("oai:demo:lib:001", title="转换期间新发布")
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait_harvest(client)

    job = _wait_mapping(client)
    assert job["status"] == "completed"
    latest = db.latest_publication()
    payload = open(latest["file_path"], encoding="utf-8").read()
    assert "转换期间新发布" in payload  # 更新的源值未被迟到转换覆盖
    assert "图书馆学理论" in payload
    assert latest["mapping_version_id"] == version["id"]
    assert latest["watermark"] == base["watermark"]
