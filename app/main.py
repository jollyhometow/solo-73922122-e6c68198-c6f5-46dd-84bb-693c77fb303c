"""馆藏目录采集网页（FastAPI 主应用）。

环境变量
--------
CATALOG_DATA_DIR      SQLite/输出目录（默认 <repo>/data）
OAI_ENDPOINT          固定 OAI-PMH 地址（默认 http://oai-source:8001/oai）
OAI_SOURCE_HOME       演示源首页（默认 http://localhost:8001/）
PUBLISH_FAILURE_MODE  启动时预置一次性发布故障：none|generate_error|commit_error|
                      crash_before_commit|crash_after_publish
MAX_PAGES_PER_CALL    单次后台采集最多处理页数（演示分批与续采）
MAPPING_APPLY_DELAY   主题词转换在"生成前"暂停秒数（演示转换期间采集并发发布，默认 0）
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
import asyncio
import logging
import os

from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .catalog import KNOWN_FAULTS, cleanup_orphans
from .db import Database
from .harvester import HarvestBlocked, Harvester, Publisher
from .mapping import MAX_MAPPING_ROWS, MappingApplier, MappingValidationError, parse_mapping_csv

log = logging.getLogger("catalog")

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("CATALOG_DATA_DIR", os.path.join(HERE, "..", "data"))
DB_PATH = os.path.join(DATA_DIR, "catalog.db")
OUTPUT_DIR = os.path.join(DATA_DIR, "published")
ENDPOINT = os.environ.get("OAI_ENDPOINT", "http://oai-source:8001/oai")
SOURCE_HOME = os.environ.get("OAI_SOURCE_HOME", "http://localhost:8001/")
FAILURE_MODE = os.environ.get("PUBLISH_FAILURE_MODE", "none").strip() or "none"
if FAILURE_MODE not in KNOWN_FAULTS:
    FAILURE_MODE = "none"
_raw_max = os.environ.get("MAX_PAGES_PER_CALL", "").strip()
MAX_PAGES = int(_raw_max) if _raw_max.isdigit() and int(_raw_max) > 0 else None
_raw_delay = os.environ.get("MAPPING_APPLY_DELAY", "0").strip()
MAPPING_DELAY = float(_raw_delay) if _raw_delay.replace(".", "", 1).isdigit() else 0.0
MAX_UPLOAD_BYTES = 2 * 1024 * 1024

_STATUS_LABELS = {
    "running": "采集中",
    "publishing": "发布中",
    "paused": "已暂停·可续采",
    "publish_failed": "发布失败·可重试",
    "completed": "已完成",
    "canceled": "已放弃",
}


def _fmt_local(value: str | None) -> str:
    if not value:
        return "—"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def _status_label(value: str | None) -> str:
    return _STATUS_LABELS.get(value or "", value or "")


def _outcome_message(outcome) -> str:
    run = outcome.run
    if outcome.published:
        pub = outcome.publication or {}
        return (
            f"第 {run.id} 轮完成并发布：{pub.get('record_count', 0)} 条有效记录，"
            f"水位 {pub.get('watermark')}。"
        )
    if run.status == "paused":
        if run.error_code == "chunk_limit":
            return f"第 {run.id} 轮已按页数上限暂停，进度已保存，可继续采集。"
        return f"第 {run.id} 轮已暂停（{run.error_code}）：进度已保存，可继续采集。"
    if run.status == "publish_failed":
        return f"第 {run.id} 轮采集完成但发布失败，旧目录仍可下载；可重试发布。"
    if run.status == "running":
        return f"第 {run.id} 轮已在后台开始采集。"
    return f"第 {run.id} 轮状态：{run.status}"


def build_app() -> FastAPI:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    db = Database(DB_PATH)

    # 一次性故障注入：环境变量只在"尚未消费过"时预置，避免重启后永远失败。
    if FAILURE_MODE != "none" and db.get_meta("publish_fault_seeded") is None:
        db.set_meta("publish_fault", FAILURE_MODE)
        db.set_meta("publish_fault_seeded", "1")

    # 启动恢复：不改动任何"已发布"引用，只把中断状态变成可续采/可重试。
    db.recover_after_restart()
    # 主题词转换任务：未提交的转失败（最近已提交的 publications 一行不动）。
    db.recover_mapping_jobs_after_restart()
    cleanup_orphans(
        OUTPUT_DIR,
        keep_paths={row["file_path"] for row in db.query("SELECT file_path FROM publications")},
    )
    for run in db.recent_runs(50):
        if run.status != "publishing":
            continue
        if db.publication_for_run(run.id) is not None:
            db.mark_completed(run.id)  # 发布记录已落库，仅状态没来得及写
        else:
            db.mark_publish_failed(run.id, "进程在发布过程中退出；旧目录仍可用，请重试发布。")

    harvester = Harvester(
        db,
        ENDPOINT,
        Publisher(db, OUTPUT_DIR, fault="none"),
        max_pages_per_call=MAX_PAGES,
    )
    applier = MappingApplier(db, OUTPUT_DIR, apply_delay=MAPPING_DELAY)

    tasks: dict[str, asyncio.Task] = {}

    def spawn(key: str, coro) -> None:
        existing = tasks.get(key)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(coro, name=key)

        def _done(t: asyncio.Task) -> None:
            tasks.pop(key, None)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log.error("background task %s failed: %r", key, exc)

        task.add_done_callback(_done)
        tasks[key] = task

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        for task in list(tasks.values()):
            task.cancel()
        db.close()

    app = FastAPI(title="机构库 → 馆藏目录采集", lifespan=lifespan)
    app.state.db = db
    app.state.harvester = harvester
    app.state.applier = applier

    templates = Jinja2Templates(directory=os.path.join(HERE, "templates"))
    templates.env.filters["localtime"] = _fmt_local
    templates.env.filters["status_label"] = _status_label
    static_dir = os.path.join(HERE, "static")
    if os.path.isdir(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        # 有未结束轮次时聚焦它；否则展示最近一轮的结果（进度与错误仍可见）。
        recent = db.recent_runs(1)
        run = db.unresolved_run() or (recent[0] if recent else None)
        latest = db.latest_publication()
        active_count, deleted_count = db.record_stats()
        busy = any(not t.done() for t in tasks.values()) or bool(
            run and run.status in ("running", "publishing")
        )
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "run": run,
                "latest": latest,
                "watermark": db.watermark(),
                "active_count": active_count,
                "deleted_count": deleted_count,
                "runs": db.recent_runs(12),
                "publications": db.publications(8),
                "today": datetime.now(timezone.utc).date().isoformat(),
                "endpoint": ENDPOINT,
                "source_home": SOURCE_HOME,
                "fault": db.get_meta("publish_fault") or "none",
                "busy": busy,
                "changed": db.records_changed_in_run(run.id)[:30] if run else [],
                "run_is_active": bool(run and run.is_unresolved),
                "message": request.query_params.get("message"),
                "error": request.query_params.get("error"),
                "mapping_version": db.latest_mapping_version(),
                "mapping_job": db.unresolved_mapping_job(),
            },
        )

    @app.post("/harvest/start")
    async def harvest_start(request: Request):
        form = await request.form()
        raw = str(form.get("until", "") or "").strip()
        try:
            until = date.fromisoformat(raw) if raw else datetime.now(timezone.utc).date()
        except ValueError:
            return RedirectResponse(url="/?error=截止日格式无效", status_code=303)
        try:
            run = await harvester.start(until)
        except (HarvestBlocked, RuntimeError) as exc:
            return RedirectResponse(url=f"/?error={exc}", status_code=303)
        spawn(f"harvest-{run.id}", harvester.resume(run.id))
        return RedirectResponse(url=f"/?message=第 {run.id} 轮已在后台开始采集", status_code=303)

    @app.post("/harvest/{run_id}/resume")
    async def harvest_resume(run_id: int):
        run = db.get_run(run_id)
        if run is None or not run.is_unresolved:
            return RedirectResponse(url="/?error=该轮次无需继续", status_code=303)
        spawn(f"harvest-{run_id}", harvester.resume(run_id))
        return RedirectResponse(url=f"/?message=第 {run_id} 轮正在后台继续", status_code=303)

    @app.post("/harvest/{run_id}/retry-publish")
    async def harvest_retry_publish(run_id: int):
        run = db.get_run(run_id)
        if run is None or run.status != "publish_failed":
            return RedirectResponse(url="/?error=该轮次当前不需要重试发布", status_code=303)
        db.begin_publish_retry(run_id)
        spawn(f"harvest-{run_id}", harvester.retry_publish_after_begin(run_id))
        return RedirectResponse(url=f"/?message=第 {run_id} 轮正在重新发布", status_code=303)

    @app.post("/harvest/{run_id}/cancel")
    def harvest_cancel(run_id: int):
        run = db.get_run(run_id)
        if run is None or not run.is_unresolved:
            return RedirectResponse(url="/?error=该轮次不可放弃", status_code=303)
        task = tasks.get(f"harvest-{run_id}")
        if task is not None and not task.done():
            return RedirectResponse(url="/?error=采集任务仍在运行，请等其暂停后再放弃", status_code=303)
        db.cancel_run(run_id)
        return RedirectResponse(url="/?message=已放弃本轮，水位与已发布目录保持不变", status_code=303)

    @app.post("/config/fault")
    async def config_fault(request: Request):
        form = await request.form()
        mode = str(form.get("fault", "none"))
        if mode not in KNOWN_FAULTS:
            return RedirectResponse(url="/?error=未知故障模式", status_code=303)
        db.set_meta("publish_fault", mode)
        return RedirectResponse(url=f"/?message=下一次发布将使用故障注入：{mode}", status_code=303)

    # --------------------------------------------------------- 主题词映射页
    def _mapping_context(request: Request) -> dict:
        latest_pub = db.latest_publication()
        latest_version = db.latest_mapping_version()
        entries = db.mapping_entries(latest_version["id"]) if latest_version else []
        versions = db.query(
            """
            SELECT mv.*,
                   (SELECT COUNT(*) FROM mapping_jobs j WHERE j.version_id = mv.id) AS job_count
              FROM mapping_versions mv ORDER BY mv.id DESC LIMIT 20
            """
        )
        return {
            "request": request,
            "latest_pub": latest_pub,
            "latest_version": latest_version,
            "entries": entries,
            "versions": versions,
            "jobs": db.recent_mapping_jobs(20),
            "unresolved_job": db.unresolved_mapping_job(),
            "max_rows": MAX_MAPPING_ROWS,
            "mapping_fault": db.get_meta("mapping_publish_fault") or "none",
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
            "apply_delay": MAPPING_DELAY,
        }

    @app.get("/mapping", response_class=HTMLResponse)
    def mapping_page(request: Request):
        return templates.TemplateResponse(request, "mapping.html", _mapping_context(request))

    @app.post("/mapping/upload")
    async def mapping_upload(request: Request):
        form = await request.form()
        upload: UploadFile | None = form.get("file")
        if upload is None or not upload.filename:
            return RedirectResponse(url="/mapping?error=请选择要上传的 CSV 文件", status_code=303)
        raw = await upload.read(MAX_UPLOAD_BYTES + 1)
        if len(raw) > MAX_UPLOAD_BYTES:
            return RedirectResponse(url="/mapping?error=CSV 文件过大（上限 2MB）", status_code=303)
        try:
            entries, normalized = parse_mapping_csv(raw)
        except MappingValidationError as exc:
            return RedirectResponse(url=f"/mapping?error={exc}", status_code=303)
        except UnicodeDecodeError:
            return RedirectResponse(url="/mapping?error=CSV 必须是 UTF-8 编码", status_code=303)
        version_id = db.create_mapping_version(entries, normalized)
        return RedirectResponse(
            url=f"/mapping?message=映射版本+{version_id}+已保存（{len(entries)} 条，整表替换），点“应用到当前目录”生效",
            status_code=303,
        )

    @app.post("/mapping/apply")
    async def mapping_apply(request: Request):
        form = await request.form()
        raw_id = str(form.get("version_id", "")).strip()
        if not raw_id.isdigit() or db.get_mapping_version(int(raw_id)) is None:
            return RedirectResponse(url="/mapping?error=映射版本不存在", status_code=303)
        if db.unresolved_mapping_job() is not None:
            return RedirectResponse(url="/mapping?error=已有主题词转换任务在进行，同期只能应用一份", status_code=303)
        latest = db.latest_publication()
        if latest is None:
            return RedirectResponse(url="/mapping?error=尚无成功发布的目录，请先完成一轮采集", status_code=303)
        job_id = db.create_mapping_job(int(raw_id), latest["id"])
        spawn(f"mapping-{job_id}", _run_mapping_job(job_id))
        return RedirectResponse(
            url=f"/mapping?message=转换任务+{job_id}+已在后台开始（暂停 {MAPPING_DELAY:g} 秒模拟转换窗口）",
            status_code=303,
        )

    async def _run_mapping_job(job_id: int) -> None:
        try:
            await applier.apply(job_id)
        except Exception as exc:  # 失败状态已在 applier 内落库
            log.error("mapping job %s failed: %r", job_id, exc)

    @app.post("/mapping/fault")
    async def mapping_fault(request: Request):
        form = await request.form()
        mode = str(form.get("fault", "none"))
        if mode not in KNOWN_FAULTS:
            return RedirectResponse(url="/mapping?error=未知故障模式", status_code=303)
        db.set_meta("mapping_publish_fault", mode)
        return RedirectResponse(url=f"/mapping?message=下一次转换将使用故障注入：{mode}", status_code=303)

    @app.get("/mapping/versions/{version_id}/csv")
    def mapping_version_csv(version_id: int):
        version = db.get_mapping_version(version_id)
        if version is None:
            raise HTTPException(status_code=404, detail="映射版本不存在")
        return PlainTextResponse(
            version["source_csv"],
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="mapping-v{version_id}.csv"'},
        )

    @app.get("/catalog/download")
    def catalog_download():
        latest = db.latest_publication()
        if latest is None:
            raise HTTPException(status_code=404, detail="尚无可下载的目录")
        path = latest["file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=410, detail="已登记的目录文件缺失")
        headers = {
            "X-Catalog-Watermark": latest["watermark"],
            "X-Catalog-Run": str(latest["run_id"] or ""),
            "X-Catalog-Kind": latest["kind"],
            "X-Catalog-Mapping-Version": str(latest["mapping_version_id"] or ""),
        }
        return FileResponse(
            path,
            media_type="application/xml",
            filename=latest["file_name"],
            headers=headers,
        )

    @app.get("/catalog.xml")
    def catalog_latest():
        return catalog_download()

    @app.get("/healthz", response_class=PlainTextResponse)
    def healthz():
        return "ok"

    return app


app = build_app()
