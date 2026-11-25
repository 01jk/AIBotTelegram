from contextlib import asynccontextmanager

import aiohttp
import aiosqlite
import pytest
from aiohttp import web
from pydantic import BaseModel, ConfigDict, SecretStr

from aibottelegram.config import Mode
from aibottelegram.control.commands import ControlConfig, handle_update
from aibottelegram.llm.config import ModelsConfig
from aibottelegram.llm.credentials import KeyFileError, install_key, load_key
from aibottelegram.llm.router import OpenRouter, RouterUnavailable, reset_provider_quota, usage
from aibottelegram.storage.database import initialize
from aibottelegram.telegram.worker import WorkerStatus

PRIMARY = "nvidia/example:free"
FALLBACK = "dots/example:free"
PAID = "vendor/example"
FREE = {"prompt": "0", "completion": "0"}


class Answer(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    text: str


def completion(model, text='{"text":"ok"}'):
    return {
        "model": model, "choices": [{"message": {"content": text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "cost": 0},
    }


@asynccontextmanager
async def mock_router(tmp_path, respond, *, primary_price=FREE, limit=50, role="conversation",
                      key_file=None, request_keys=None, mixed=False):
    calls = []

    async def handle(request):
        if request.path == "/key":
            bearer = request.headers.get("Authorization")
            owners = {"Bearer test-key": "owner", "Bearer replacement": "owner", "Bearer other-key": "other"}
            if bearer not in owners:
                return web.json_response({"error": {"code": 401}}, status=401)
            return web.json_response({"data": {"creator_user_id": owners[bearer], "is_management_key": False}})
        if request.path == "/models":
            assert "Authorization" not in request.headers
            return web.json_response({"data": [
                {"id": model, "pricing": FREE, "context_length": 16000,
                 "architecture": {"input_modalities": ["text"], "output_modalities": ["text"]}}
                for model in ((PRIMARY, FALLBACK, PAID) if mixed else (PRIMARY, FALLBACK))
            ]})
        if request.path.endswith("/endpoints"):
            assert "Authorization" not in request.headers
            model = request.path.removeprefix("/models/").removesuffix("/endpoints")
            return web.json_response({"data": {"id": model, "endpoints": [{
                "tag": "mock-provider", "pricing": (
                    {"prompt": "0.0000001", "completion": "0.0000002"} if model == PAID
                    else primary_price if model == PRIMARY else FREE
                ),
                "supported_parameters": ["max_tokens"] + (
                    ["structured_outputs", "response_format", "temperature", "reasoning"]
                    if model != PRIMARY else ["reasoning"]
                ),
            }]}})
        assert request.path == "/chat/completions"
        if request_keys is None:
            assert request.headers["Authorization"] == "Bearer test-key"
        else:
            request_keys.append(request.headers["Authorization"])
        payload = await request.json()
        calls.append(payload)
        return respond(payload, len(calls))

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handle)
    server = web.AppRunner(app, access_log=None)
    await server.setup()
    try:
        site = web.TCPSite(server, "127.0.0.1", 0)
        await site.start()
        port = server.addresses[0][1]
        raw_config = {
            "max_requests_per_day": limit,
            "models": {role: {"primary": PRIMARY, "fallbacks": [FALLBACK]}},
        }
        if mixed:
            raw_config.update(
                free_only=False, paid_requests_per_day=2, foreground_reserve_per_day=0,
                paid_models={PAID: {"provider": "mock-provider", "prompt_per_million": 0.1,
                                   "completion_per_million": 0.2, "temperature": 0.3, "reasoning": "none"}},
                models={"style_review": {"primary": PRIMARY, "fallbacks": [FALLBACK, PAID]},
                        "conversation": {"primary": PAID}, "analysis": {"primary": FALLBACK}},
            )
        config = ModelsConfig.model_validate(raw_config)
        async with aiosqlite.connect(tmp_path / "llm.sqlite3") as db:
            await initialize(db, Mode.OBSERVE)
            async with aiohttp.ClientSession() as session:
                router = OpenRouter(session, db, config, SecretStr("test-key"), key_file=key_file,
                                    base_url=f"http://127.0.0.1:{port}")
                yield router, calls, db
    finally:
        await server.cleanup()


async def run(router):
    return await router.complete("conversation", system="Respond.", public_data={"example": "synthetic"}, schema=Answer)


@pytest.mark.asyncio
async def test_cross_account_replacement_recovers_after_a_rejected_key_without_resetting_usage(tmp_path):
    path = tmp_path / "openrouter-key.json"
    keys = []

    def respond(payload, index):
        if index == 1:
            return web.json_response({"error": {"code": 401}}, status=401)
        return web.json_response(completion(payload["model"]))

    async with mock_router(tmp_path, respond, limit=2, key_file=path, request_keys=keys) as (router, _, db):
        await install_key(router.http, router.url, path, SecretStr("test-key"), initialize=True)
        with pytest.raises(RouterUnavailable, match="key_rejected"):
            await run(router)
        with pytest.raises(RouterUnavailable, match="key_rejected"):
            await run(router)
        await install_key(router.http, router.url, path, SecretStr("other-key"))
        assert load_key(path).secret == SecretStr("other-key")
        assert (await run(router)).text == "ok"
        with pytest.raises(RouterUnavailable, match="local_quota"):
            await run(router)
        assert keys == ["Bearer test-key", "Bearer other-key"]
        assert await db.execute_fetchall("SELECT outcome FROM provider_usage ORDER BY id") == [("http_401",), ("ok",)]


@pytest.mark.asyncio
async def test_key_replacement_does_not_clear_an_account_quota_block(tmp_path):
    path = tmp_path / "openrouter-key.json"
    keys = []

    def respond(_, __):
        return web.json_response({"error": {"code": 429}}, status=429, headers={"Retry-After": "30"})

    async with mock_router(tmp_path, respond, key_file=path, request_keys=keys) as (router, _, _):
        await install_key(router.http, router.url, path, SecretStr("test-key"), initialize=True)
        with pytest.raises(RouterUnavailable, match="account_quota"):
            await run(router)
        await install_key(router.http, router.url, path, SecretStr("replacement"))
        with pytest.raises(RouterUnavailable, match="account_blocked"):
            await run(router)
        assert keys == ["Bearer test-key"]


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["conversation", "summarization"])
async def test_pause_blocks_foreground_and_background_model_requests(tmp_path, role):
    async with mock_router(
        tmp_path, lambda payload, _: web.json_response(completion(payload["model"])), role=role,
    ) as (router, calls, db):
        await db.execute("UPDATE runtime_state SET paused=1")
        await db.commit()
        with pytest.raises(RouterUnavailable, match="paused"):
            await router.complete(role, system="Reply.", public_data={}, schema=Answer)
        assert not calls
        assert await db.execute_fetchall("SELECT COUNT(*) FROM provider_usage") == [(0,)]
        await db.execute("UPDATE runtime_state SET paused=0")
        await db.commit()
        assert (await router.complete(role, system="Reply.", public_data={}, schema=Answer)).text == "ok"


@pytest.mark.asyncio
async def test_paid_endpoint_is_skipped_and_request_has_a_zero_price_ceiling(tmp_path):
    def respond(payload, _):
        return web.json_response(completion(payload["model"]))

    async with mock_router(tmp_path, respond, primary_price={**FREE, "request": "0.01"}) as (router, calls, db):
        assert (await run(router)).text == "ok"
        assert [payload["model"] for payload in calls] == [FALLBACK]
        payload = calls[0]
        assert payload["provider"] == {
            "only": ["mock-provider"], "allow_fallbacks": False, "require_parameters": True,
            "max_price": {"prompt": 0, "completion": 0, "request": 0},
        }
        assert payload["response_format"]["type"] == "json_schema"
        assert payload["messages"][0]["role"] == "system"
        assert await db.execute_fetchall(
            "SELECT outcome, prompt_tokens, completion_tokens, estimated FROM provider_usage"
        ) == [("ok", 10, 5, 0)]


@pytest.mark.asyncio
async def test_owner_quota_reset_is_durable_and_keeps_usage_history(tmp_path):
    config = ControlConfig(SecretStr("123:test"), 11, 11, True)
    worker = WorkerStatus("connected")
    update = {
        "update_id": 5,
        "message": {"chat": {"type": "private", "id": 11}, "from": {"id": 11, "is_bot": False},
                    "text": "/reset_quota"},
    }
    async with mock_router(
        tmp_path, lambda payload, _: web.json_response(completion(payload["model"])), limit=1,
    ) as (router, calls, db):
        now = int(router.clock())
        router.clock = lambda: now
        assert (await run(router)).text == "ok"
        with pytest.raises(RouterUnavailable, match="local_quota"):
            await run(router)
        assert await usage(db, now) == (1, 1)
        async with aiosqlite.connect(tmp_path / "llm.sqlite3") as control:
            assert await handle_update(control, config, worker, 123, "testbot", update)
        assert await usage(db, now) == (0, 0)
        assert (await run(router)).text == "ok"
        async with aiosqlite.connect(tmp_path / "llm.sqlite3") as reopened:
            assert not await handle_update(reopened, config, worker, 123, "testbot", update)
            assert await usage(reopened, now) == (1, 1)
        assert len(calls) == 2
        assert await db.execute_fetchall("SELECT outcome FROM provider_usage ORDER BY id") == [("ok",), ("ok",)]


@pytest.mark.asyncio
async def test_account_rate_limit_stops_fallbacks_and_next_request(tmp_path):
    def respond(_, __):
        return web.json_response({"error": {"code": 429}}, status=429, headers={"Retry-After": "30"})

    async with mock_router(tmp_path, respond) as (router, calls, db):
        with pytest.raises(RouterUnavailable, match="account_quota"):
            await run(router)
        await reset_provider_quota(db)
        await db.commit()
        with pytest.raises(RouterUnavailable, match="account_blocked"):
            await run(router)
        assert [payload["model"] for payload in calls] == [PRIMARY]
        assert await db.execute_fetchall("SELECT outcome FROM provider_usage") == [("http_429",)]


@pytest.mark.asyncio
async def test_revoked_context_is_not_sent_after_reservation(tmp_path):
    def respond(payload, _):
        return web.json_response(completion(payload["model"]))

    checks = iter((True, False))

    async def authorize():
        return next(checks)

    async with mock_router(tmp_path, respond) as (router, calls, db):
        with pytest.raises(RouterUnavailable, match="context_revoked"):
            await router.complete(
                "conversation", system="Respond.", public_data={"example": "synthetic"},
                schema=Answer, authorize=authorize,
            )
        assert not calls
        assert await db.execute_fetchall("SELECT outcome FROM provider_usage") == [("context_revoked",)]


@pytest.mark.asyncio
async def test_provider_rate_limit_falls_back_without_disabling_the_account(tmp_path):
    def respond(payload, index):
        if index == 1:
            return web.json_response(
                {"error": {"code": 429, "metadata": {"provider_name": "Upstream"}}}, status=429,
            )
        return web.json_response(completion(payload["model"]))

    async with mock_router(tmp_path, respond) as (router, calls, _):
        assert (await run(router)).text == "ok"
        assert [payload["model"] for payload in calls] == [PRIMARY, FALLBACK]


@pytest.mark.asyncio
async def test_invalid_json_gets_one_correction_and_local_quota_is_enforced(tmp_path):
    def respond(payload, index):
        return web.json_response(completion(payload["model"], "not json" if index == 1 else '{"text":"ok"}'))

    async with mock_router(tmp_path, respond, limit=2) as (router, calls, db):
        assert (await run(router)).text == "ok"
        assert [payload["model"] for payload in calls] == [PRIMARY, PRIMARY]
        assert all("response_format" not in payload for payload in calls)
        assert "prior output was invalid" in calls[1]["messages"][0]["content"]
        with pytest.raises(RouterUnavailable, match="local_quota"):
            await run(router)
        assert await db.execute_fetchall("SELECT outcome FROM provider_usage ORDER BY id") == [
            ("invalid_json",), ("ok",),
        ]


@pytest.mark.asyncio
async def test_truncated_participation_goes_directly_to_structured_fallback(tmp_path):
    def respond(payload, index):
        body = completion(payload["model"])
        if index == 1:
            body["choices"][0].update(finish_reason="length", message={"content": None})
        return web.json_response(body)

    async with mock_router(tmp_path, respond, role="participation") as (router, calls, db):
        assert (await router.complete(
            "participation", system="Decide.", public_data={}, schema=Answer, max_tokens=768,
        )).text == "ok"
        assert [payload["model"] for payload in calls] == [PRIMARY, FALLBACK]
        assert calls[0]["reasoning"] == {"effort": "none"}
        assert calls[1]["response_format"]["type"] == "json_schema"
        assert await db.execute_fetchall("SELECT outcome FROM provider_usage ORDER BY id") == [
            ("truncated",), ("ok",),
        ]


def test_configuration_rejects_unqualified_or_paid_routes():
    with pytest.raises(ValueError, match="explicit paid_models"):
        ModelsConfig.model_validate({"models": {"conversation": {"primary": "nvidia/example"}}})


@pytest.mark.asyncio
async def test_reasoning_switch_changes_paid_conversation_payload_without_restart(tmp_path):
    async with mock_router(
        tmp_path, lambda payload, _: web.json_response(completion(payload["model"])), mixed=True,
    ) as (router, calls, db):
        assert (await run(router)).text == "ok"
        update = {
            "update_id": 1,
            "message": {"chat": {"type": "private", "id": 11}, "from": {"id": 11, "is_bot": False},
                        "text": "/reasoning on"},
        }
        async with aiosqlite.connect(tmp_path / "llm.sqlite3") as control:
            assert "включён" in await handle_update(
                control, ControlConfig(SecretStr("123:test"), 11, 11, True), WorkerStatus("connected"),
                123, "testbot", update,
            )
        assert (await run(router)).text == "ok"
        assert [request["reasoning"] for request in calls] == [{"effort": "none"}, {"enabled": True}]
        assert [request["max_tokens"] for request in calls] == [384, 1536]


@pytest.mark.asyncio
@pytest.mark.parametrize("free_429", [False, True])
async def test_free_quota_falls_back_to_paid_and_keeps_paid_count_separate(tmp_path, free_429):
    def respond(payload, _):
        model = payload["model"]
        if model == PRIMARY and free_429:
            return web.json_response({"error": {"code": 429,
                "metadata": {"limit_source": "openrouter_free_tier_daily"}}}, status=429)
        body = completion(model)
        if model == PAID:
            body["usage"]["cost"] = 0.000002
        return web.json_response(body)

    async with mock_router(tmp_path, respond, mixed=True, limit=1) as (router, calls, db):
        if not free_429:
            await router.complete("analysis", system="Classify.", public_data={}, schema=Answer)
        assert (await router.complete("style_review", system="Edit.", public_data={}, schema=Answer)).text == "ok"
        assert (await run(router)).text == "ok"
        assert [item["model"] for item in calls] == [PRIMARY if free_429 else FALLBACK, PAID, PAID]
        assert calls[1]["temperature"] == 0.3
        assert calls[1]["provider"]["max_price"] == {"prompt": 0.1, "completion": 0.2, "request": 0}
        await reset_provider_quota(db)
        await db.commit()
        assert await usage(db, int(router.clock())) == (0, 0)
        assert await usage(db, int(router.clock()), paid=True) == (2, 2)
        with pytest.raises(RouterUnavailable, match="local_quota"):
            await run(router)
