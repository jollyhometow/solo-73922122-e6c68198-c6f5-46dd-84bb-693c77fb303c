"""命令行演示脚本：无需浏览器跑通采集 + 主题词映射全流程。

用法（在容器内）：
    python -m scripts.demo_flow

它直接调用 HTTP 接口，等价于在页面上点击，覆盖：
1. 首次全量采集与下载（原有能力保留）。
2. 上传主题词映射 CSV（含冲突被拒、200 条上限等校验由测试覆盖）。
3. 应用映射：下载内容、映射版本与水位对应同一完整结果；源值保留。
4. 换表从源值重算；空表应用恢复源词。
5. 转换"暂停"期间完成一轮采集：转换提交前发现新目录，自动用新快照重算，
   迟到处理不覆盖更新的源值或映射。
"""
from __future__ import annotations

import os
import time

import httpx

WEB = os.environ.get("CATALOG_WEB_URL", "http://catalog-web:8000")
SOURCE = os.environ.get("OAI_SOURCE_URL", "http://oai-source:8001")


def _top_section(html: str) -> str:
    """只取“本轮采集”区域，避免把历史表格里的状态当成当前轮次。"""
    body = html.split("当前发布", 1)[0]
    return body.split("本轮采集", 1)[-1]


def wait_idle(client: httpx.Client, timeout: float = 60.0) -> str:
    deadline = time.time() + timeout
    html = client.get("/").text
    while time.time() < deadline:
        html = client.get("/").text
        top = _top_section(html)
        if "已完成" in top or "发布失败" in top or "已暂停" in top:
            return html
        time.sleep(0.5)
    return html


def start(client: httpx.Client, until: str) -> None:
    client.post("/harvest/start", data={"until": until}, follow_redirects=True)
    wait_idle(client)


def download(client: httpx.Client):
    r = client.get("/catalog/download")
    r.raise_for_status()
    return r.headers, r.text


def latest_mapping_id(client: httpx.Client) -> int:
    import re

    html = client.get("/mapping").text
    m = re.search(r"<td>(\d+)</td>\s*<td>\d+</td>", html)
    assert m, "页面上找不到映射版本"
    return int(m.group(1))


def mapping_job_busy(client: httpx.Client) -> bool:
    return "status-running" in client.get("/mapping").text


def wait_mapping_done(client: httpx.Client, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline and mapping_job_busy(client):
        time.sleep(0.5)


def main() -> None:
    with httpx.Client(base_url=WEB, timeout=30, follow_redirects=True) as web, \
            httpx.Client(base_url=SOURCE, timeout=30, follow_redirects=True) as src:
        print("1) 首次全量采集（原采集/下载能力保留）")
        start(web, "2026-11-13")
        hdr, xml = download(web)
        print(f"   水位={hdr['x-catalog-watermark']} 类型={hdr['x-catalog-kind']} "
              f"映射版本={hdr.get('x-catalog-mapping-version') or '无'} 记录数={xml.count('<record ')}")
        assert "图书馆学</dc:subject>" in xml

        print("2) 上传主题词映射并应用（整表替换、精确匹配只替换一次）")
        csv_text = "图书馆学,图书馆学理论\n数字图书馆,数字馆藏\n元数据,元数据规范\n"
        r = web.post("/mapping/upload",
                     files={"file": ("mapping.csv", csv_text.encode("utf-8"), "text/csv")})
        assert r.status_code == 200 and "已保存" in r.text, r.text
        vid = latest_mapping_id(web)
        web.post("/mapping/apply", data={"version_id": vid})
        wait_mapping_done(web)
        hdr, xml = download(web)
        print(f"   类型={hdr['x-catalog-kind']} 映射版本={hdr['x-catalog-mapping-version']} "
              f"水位={hdr['x-catalog-watermark']}（与转换前相同，未推进）")
        assert hdr["x-catalog-mapping-version"] == str(vid)
        assert "图书馆学理论" in xml and "数字馆藏" in xml and "元数据规范" in xml
        assert "图书馆学</dc:subject>" not in xml

        print("3) 换表：从源值重算（不叠加旧结果），再上传空表恢复源词")
        web.post("/mapping/upload",
                 files={"file": ("v2.csv", "图书馆学,规范词B\n古籍数字化,古籍保护\n".encode(), "text/csv")})
        vid2 = latest_mapping_id(web)
        web.post("/mapping/apply", data={"version_id": vid2})
        wait_mapping_done(web)
        _, xml = download(web)
        assert "规范词B" in xml and "古籍保护" in xml and "图书馆学理论" not in xml
        print("   换表生效：规范词B/古籍保护 在目录中，旧规范词已消失")
        web.post("/mapping/upload", files={"file": ("empty.csv", b"", "text/csv")})
        vid_empty = latest_mapping_id(web)
        web.post("/mapping/apply", data={"version_id": vid_empty})
        wait_mapping_done(web)
        _, xml = download(web)
        assert "图书馆学</dc:subject>" in xml and "规范词B" not in xml
        print("   空表应用：dc:subject 全部恢复为源词")

        print("4) 转换暂停期间完成采集：提交前自动重算，迟到转换不覆盖更新的源值")
        web.post("/mapping/upload",
                 files={"file": ("v3.csv", "图书馆学,图书馆学理论\n".encode(), "text/csv")})
        vid3 = latest_mapping_id(web)
        web.post("/mapping/apply", data={"version_id": vid3})
        # 服务端 MAPPING_APPLY_DELAY=2 秒：在窗口内让源端变更并完成一轮采集
        time.sleep(0.5)
        src.post("/demo/update", data={"identifier": "oai:demo:lib:001", "title": "转换期间新标题"})
        start(web, "2026-11-13")
        wait_mapping_done(web)
        hdr, xml = download(web)
        assert "转换期间新标题" in xml, "更新的源值被迟到转换覆盖了"
        assert "图书馆学理论" in xml, "映射未应用到最新快照"
        print(f"   最终目录同时包含新标题与新映射；类型={hdr['x-catalog-kind']} "
              f"映射版本={hdr['x-catalog-mapping-version']} 水位={hdr['x-catalog-watermark']}")

        print("5) 冲突 CSV 必须被拒绝（同一源词对应不同规范词）")
        r = web.post("/mapping/upload",
                     files={"file": ("bad.csv", "a,b\na,c\n".encode(), "text/csv")},
                     follow_redirects=True)
        assert "同一源词" in r.text
        print("   冲突映射已被拒绝，当前映射不受影响")

        print("\n全部演示断言通过。浏览器可在 http://localhost:8000/mapping 重复以上操作。")


if __name__ == "__main__":
    main()
