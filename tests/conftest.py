import os
import sys

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import oai as oai_mod
from source.app import create_source_app


@pytest.fixture()
def env(tmp_path):
    """真实 ASGI 演示源 + 把 OAI HTTP 客户端直连到该源（无网络依赖）。"""
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)
    orig_enter = oai_mod.OAIHttpClient.__aenter__

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    oai_mod.OAIHttpClient.__aenter__ = patched_enter
    try:
        yield tmp_path, src_app, transport
    finally:
        oai_mod.OAIHttpClient.__aenter__ = orig_enter


@pytest.fixture()
def web(tmp_path, monkeypatch):
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    monkeypatch.setattr(oai_mod.OAIHttpClient, "__aenter__", patched_enter)
    monkeypatch.setenv("CATALOG_DATA_DIR", str(tmp_path / "web"))
    monkeypatch.setenv("OAI_ENDPOINT", "http://source/oai")
    monkeypatch.delenv("PUBLISH_FAILURE_MODE", raising=False)
    monkeypatch.delenv("MAX_PAGES_PER_CALL", raising=False)
    monkeypatch.delenv("MAPPING_APPLY_DELAY", raising=False)

    import importlib

    import app.main as main_mod

    importlib.reload(main_mod)
    app = main_mod.build_app()
    with TestClient(app) as client:
        yield client, app, src_app
