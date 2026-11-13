"""主题词映射：CSV 校验、dc:subject 精确替换与"换表即重算"的转换编排。

业务规则
--------
* 上传 CSV 最多 200 条"源词,规范词"；整表替换——每次上传都是一份完整新版本，
  从不与旧版本合并。同一源词对应不同规范词直接拒绝；空表（0 条）合法，
  应用后恢复源词。
* 只对 ``dc:subject`` 逐值**精确匹配**，每个值最多替换一次；未命中的值原样保留，
  不会因"规范词恰好等于另一个源词"而级联二次替换。
* ``records.metadata_json`` 永远保存源端原始值。应用范围是**最近一次成功发布的
  目录快照**（``publications.snapshot_json``），不含尚未发布的采集记录，也不推进
  采集水位。
* 转换期间采集照常发布；提交时复查基准目录是否仍是最新发布，若已被新采集取代，
  则用新快照重算，保证迟到处理不覆盖更新的源值或映射。
"""
from __future__ import annotations

from dataclasses import dataclass
import asyncio
import csv
import hashlib
import io
import json
import os
import secrets
import time

from .catalog import (
    FAULT_COMMIT_ERROR,
    FAULT_CRASH_AFTER_PUBLISH,
    FAULT_CRASH_BEFORE_COMMIT,
    FAULT_GENERATE_ERROR,
    KNOWN_FAULTS,
    build_catalog_xml,
    cleanup_orphans,
)
from .db import Database

MAX_MAPPING_ROWS = 200


class MappingValidationError(ValueError):
    """CSV 内容不符合"源词,规范词"整表约束。"""


class MappingBlocked(RuntimeError):
    """当前状态不允许应用（例如没有已发布目录、已有转换在跑）。"""


# --------------------------------------------------------------- CSV 解析校验
def parse_mapping_csv(raw: bytes | str) -> tuple[list[tuple[str, str]], str]:
    """解析上传内容，返回 (entries, 规范化后的 CSV 文本)。

    每条非空行必须恰好两列且两列都非空；同源词冲突或超过 200 条一律拒绝。
    空内容返回空 entries——空表是合法版本，应用即恢复源词。
    """
    if isinstance(raw, bytes):
        text = raw.decode("utf-8-sig")
    else:
        text = raw.lstrip("﻿")
    reader = csv.reader(io.StringIO(text))
    entries: list[tuple[str, str]] = []
    seen: dict[str, str] = {}
    line_no = 0
    try:
        for row in reader:
            line_no += 1
            cells = [cell.strip() for cell in row]
            if not any(cells):
                continue  # 跳过完全空白的行
            if len(cells) != 2:
                raise MappingValidationError(
                    f"第 {line_no} 行不是恰好两列（源词,规范词），实际 {len(cells)} 列"
                )
            source_term, target_term = cells
            if not source_term or not target_term:
                raise MappingValidationError(f"第 {line_no} 行存在空单元格，源词与规范词都必填")
            if source_term in seen and seen[source_term] != target_term:
                raise MappingValidationError(
                    f"源词 {source_term!r} 同时映射到 {seen[source_term]!r} 与 {target_term!r}，"
                    "同一源词只能对应一个规范词"
                )
            if source_term not in seen:
                seen[source_term] = target_term
                entries.append((source_term, target_term))
    except csv.Error as exc:
        raise MappingValidationError(f"CSV 解析失败（第 {line_no} 行附近）：{exc}") from exc
    if len(entries) > MAX_MAPPING_ROWS:
        raise MappingValidationError(
            f"映射条数 {len(entries)} 超过上限 {MAX_MAPPING_ROWS} 条；请整表替换上传"
        )
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for pair in entries:
        writer.writerow(pair)
    return entries, buffer.getvalue()


# --------------------------------------------------------------- subject 转换
def transform_subjects(metadata: dict, mapping: dict[str, str]) -> dict:
    """对 dc:subject 逐值精确替换一次；其他 dc 元素与未命中值原样保留。"""
    transformed = {name: list(values) for name, values in metadata.items()}
    subjects = transformed.get("subject")
    if subjects:
        # dict.get 只作用一次：即使规范词等于另一个源词也不会级联替换。
        transformed["subject"] = [mapping.get(value, value) for value in subjects]
    return transformed


def apply_mapping_to_snapshot(snapshot: list[dict], mapping: dict[str, str]) -> list[dict]:
    """从源值快照重算整份目录数据；空映射等价于全部保留（恢复源词）。"""
    result = []
    for item in snapshot:
        result.append(
            {
                "identifier": item["identifier"],
                "datestamp": item.get("datestamp"),
                "sets": list(item.get("sets") or []),
                "metadata": transform_subjects(item.get("metadata") or {}, mapping),
                "deleted": False,
            }
        )
    return result


@dataclass
class MappingApplyOutcome:
    job_id: int
    publication_id: int | None
    retries: int
    watermark: str
    mapping_version_id: int
    record_count: int


class MappingApplier:
    """把一份映射版本应用到最近成功目录，产出与采集发布同构的 publications 行。"""

    def __init__(self, db: Database, output_dir: str, *, apply_delay: float = 0.0):
        self.db = db
        self.output_dir = output_dir
        self.apply_delay = max(0.0, apply_delay)
        self._lock = asyncio.Lock()

    def _take_fault(self) -> str | None:
        with self.db.tx() as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key='mapping_publish_fault'"
            ).fetchone()
            if row is None or row["value"] == "none":
                return None
            conn.execute("DELETE FROM meta WHERE key='mapping_publish_fault'")
            return row["value"]

    async def apply(self, job_id: int) -> MappingApplyOutcome:
        async with self._lock:
            return await asyncio.to_thread(self._apply_blocking, job_id)

    def _apply_blocking(self, job_id: int) -> MappingApplyOutcome:
        job = self.db.get_mapping_job(job_id)
        if job is None:
            raise MappingBlocked(f"转换任务 #{job_id} 不存在")
        if job["status"] not in ("running", "applying"):
            raise MappingBlocked(f"转换任务 #{job_id} 已结束（{job['status']}），不能重复应用")
        version = self.db.get_mapping_version(job["version_id"])
        if version is None:
            self.db.mark_mapping_job_failed(job_id, "映射版本不存在")
            raise MappingBlocked("映射版本不存在")
        mapping = self.db.mapping_dict(version["id"])
        fault = self._take_fault() or "none"
        if fault not in KNOWN_FAULTS:
            fault = "none"

        base_id = job["base_publication_id"]
        retries = 0
        # 最多重算次数：每次重算都说明期间有新采集成功发布；正常演示只会发生 0~1 次。
        for _ in range(5):
            base_pub = self.db.latest_publication()
            if base_pub is None:
                self.db.mark_mapping_job_failed(job_id, "尚无已发布的目录，无法应用主题词映射")
                raise MappingBlocked("尚无已发布的目录")
            snapshot = Database.decode_snapshot(base_pub["snapshot_json"])
            if snapshot is None:
                self.db.mark_mapping_job_failed(
                    job_id, f"目录 #{base_pub['id']} 缺少记录快照，无法从源值重算"
                )
                raise MappingBlocked("基准目录缺少快照")
            base_id = base_pub["id"]

            if self.apply_delay > 0:
                # 演示窗口：转换"暂停"期间，采集可以并发完成并发布。
                self.db.mark_mapping_job_applying(job_id)
                time.sleep(self.apply_delay)

            items = apply_mapping_to_snapshot(snapshot, mapping)
            try:
                staged = self._stage(items, base_pub["watermark"], version["id"], job_id, fault)
            except _MappingFault as exc:
                self._safe_cleanup()
                self.db.mark_mapping_job_failed(job_id, str(exc))
                raise MappingBlocked(str(exc)) from exc

            publication_id, latest_id = self.db.commit_mapping_publication(
                job_id=job_id,
                base_publication_id=base_id,
                watermark=base_pub["watermark"],
                file_path=staged["path"],
                file_name=staged["name"],
                record_count=staged["record_count"],
                size_bytes=staged["size_bytes"],
                sha256=staged["sha256"],
                mapping_version_id=version["id"],
                snapshot_json=json.dumps(snapshot, ensure_ascii=False),
            )
            if publication_id is not None:
                self._safe_cleanup()
                return MappingApplyOutcome(
                    job_id=job_id,
                    publication_id=publication_id,
                    retries=retries,
                    watermark=base_pub["watermark"],
                    mapping_version_id=version["id"],
                    record_count=staged["record_count"],
                )
            # 期间有新采集发布：删除本次暂存文件，用最新快照重算，绝不覆盖更新的源值。
            retries += 1
            try:
                os.remove(staged["path"])
            except OSError:
                pass
            staged = None

        if staged is not None:
            try:
                os.remove(staged["path"])
            except OSError:
                pass
        self.db.mark_mapping_job_failed(
            job_id, "转换期间目录被多次更新，重算次数超限；请重新应用当前映射。"
        )
        raise MappingBlocked("转换期间目录频繁更新，请重新应用")

    def _stage(self, items: list[dict], watermark: str, mapping_version_id: int,
               job_id: int, fault: str) -> dict:
        os.makedirs(self.output_dir, exist_ok=True)
        try:
            xml_text = build_catalog_xml(
                items,
                watermark=watermark,
                mapping_version_id=mapping_version_id,
                kind="mapping",
            )
            payload = xml_text.encode("utf-8")
        except Exception as exc:  # 生成阶段失败：旧目录完全不动
            raise _MappingFault(f"目录生成失败：{exc}") from exc
        if fault == FAULT_GENERATE_ERROR:
            raise _MappingFault("演示：主题词目录生成阶段失败（旧目录保持可下载）")

        digest = hashlib.sha256(payload).hexdigest()
        final_name = f"catalog-map{job_id}-{digest[:12]}.xml"
        final_path = os.path.abspath(os.path.join(self.output_dir, final_name))
        tmp_path = os.path.join(self.output_dir, f".catalog-tmp-{secrets.token_hex(8)}.xml")
        try:
            with open(tmp_path, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
        except OSError as exc:
            raise _MappingFault(f"目录写入失败：{exc}") from exc

        if fault == FAULT_CRASH_BEFORE_COMMIT:
            os._exit(70)  # 演示：暂存后、改名前进程退出，旧引用不受影响
        try:
            os.replace(tmp_path, final_path)
            dir_fd = os.open(self.output_dir, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError as exc:
            raise _MappingFault(f"目录发布改名失败：{exc}") from exc

        if fault == FAULT_CRASH_AFTER_PUBLISH:
            os._exit(70)  # 演示：改名后、落库前退出，新文件无引用，启动时按孤儿清理
        if fault == FAULT_COMMIT_ERROR:
            try:
                os.remove(final_path)
            except OSError:
                pass
            raise _MappingFault("演示：转换目录已写出但发布登记失败（旧目录仍为当前版本）")

        return {
            "path": final_path,
            "name": final_name,
            "sha256": digest,
            "size_bytes": len(payload),
            "record_count": len(items),
        }

    def _safe_cleanup(self) -> None:
        try:
            cleanup_orphans(
                self.output_dir,
                keep_paths={row["file_path"] for row in self.db.query("SELECT file_path FROM publications")},
            )
        except OSError:
            pass


class _MappingFault(RuntimeError):
    pass
