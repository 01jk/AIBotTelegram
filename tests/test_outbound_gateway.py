import asyncio
from datetime import datetime
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import aiosqlite
import pytest
import pytest_asyncio
from pydantic import SecretStr

from aibottelegram.config import Mode
from aibottelegram.control.commands import ControlConfig, apply_command
from aibottelegram.policy.limits import is_quiet, local_day
from aibottelegram.policy.outbound import Candidate, OutboundGateway, Queued, TransportRejected, budget_usage
from aibottelegram.storage.database import initialize
from aibottelegram.telegram.worker import WorkerStatus
from aibottelegram.telegram.outbound import TelethonOutbound
from telethon import errors, types

ZONE = ZoneInfo("Europe/Kyiv")
ACCOUNT = 42
CHAT = -1000000000011
OTHER = -1000000000012


@pytest_asyncio.fixture
async def setup(tmp_path):
    path = tmp_path / "gateway.sqlite3"
    async with aiosqlite.connect(path) as db:
        await initialize(db, Mode.AUTO)
        await db.executemany(
            "INSERT INTO chats (account_id, chat_id, mode) VALUES (?, ?, 'AUTO_REPLY')",
            [(ACCOUNT, CHAT), (ACCOUNT, OTHER)],
        )
        await db.executemany(
            "INSERT INTO send_limits VALUES (?, ?, ?, ?)",
            [
                (ACCOUNT, 0, "global_daily", 2), (ACCOUNT, 0, "global_rolling", 2),
                (ACCOUNT, 0, "min_interval", 0), (ACCOUNT, 0, "candidate_ttl", 900),
                (ACCOUNT, CHAT, "min_interval", 0), (ACCOUNT, OTHER, "min_interval", 0),
            ],
        )
        await db.commit()
        clock = [int(datetime(2026, 11, 6, 14, tzinfo=ZONE).timestamp())]
        transport = AsyncMock()
        transport.send.return_value = 80
        gateway = OutboundGateway(db, ACCOUNT, transport, dry_run=False, zone=ZONE,
                                  lock=asyncio.Lock(), clock=lambda: clock[0])
        await gateway.observe(CHAT, 0, 50, False)
        await gateway.observe(OTHER, 0, 50, False)
        yield path, db, gateway, transport, clock


def candidate(key="a", chat=CHAT, context=50, **kwargs):
    return Candidate(chat, context, kwargs.pop("text", "Можна уточнити?"), key, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["dry_run", "observe", "paused", "read_only"])
async def test_enqueue_refuses_closed_policy(setup, gate):
    _, db, gateway, transport, _ = setup
    if gate == "dry_run":
        gateway.dry_run = True
    elif gate == "observe":
        await db.execute("UPDATE runtime_state SET mode = 'OBSERVE'")
    elif gate == "paused":
        await db.execute("UPDATE runtime_state SET paused = 1")
    else:
        await db.execute("UPDATE chats SET mode = 'READ_ONLY'")
    await db.commit()
    assert not await gateway.enqueue(candidate())
    assert await gateway.dispatch_once() is None
    transport.prepare.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "args", "reason"), [
    ("pause", [], "paused"), ("deny", [str(CHAT)], "chat_denied"), ("mode", ["observe"], "mode_changed"),
])
async def test_owner_changes_during_preflight_stop_send(setup, name, args, reason):
    _, db, gateway, transport, _ = setup
    assert await gateway.enqueue(candidate())

    async def prepare(_):
        async with gateway.transaction():
            await apply_command(
                db, ControlConfig(SecretStr("test"), ACCOUNT, ACCOUNT, False),
                WorkerStatus("connected"), name, args,
            )
        return object()

    transport.prepare.side_effect = prepare
    assert await gateway.dispatch_once() is None
    transport.send.assert_not_awaited()
    assert await db.execute_fetchall("SELECT status, reason FROM outbox") == [("failed", reason)]


@pytest.mark.asyncio
async def test_manual_reply_and_expiry_cancel_candidates(setup):
    _, db, gateway, transport, clock = setup
    assert await gateway.enqueue(candidate())
    await gateway.observe(CHAT, 0, 51, True)
    assert await gateway.enqueue(candidate("b", chat=OTHER))
    clock[0] += 901
    assert await gateway.dispatch_once() is None
    transport.send.assert_not_awaited()
    assert await db.execute_fetchall("SELECT status, reason FROM outbox ORDER BY id") == [
        ("failed", "manual_reply"), ("failed", "expired"),
    ]


@pytest.mark.asyncio
async def test_context_changed_during_preflight_is_not_sent(setup):
    _, db, gateway, transport, _ = setup
    assert await gateway.enqueue(candidate())

    async def prepare(_):
        await gateway.observe(CHAT, 0, 51, False)

    transport.prepare.side_effect = prepare
    assert await gateway.dispatch_once() is None
    assert await db.execute_fetchall("SELECT reason FROM outbox") == [("stale_context",)]
    transport.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_reservation_and_lowered_limit_keep_usage(setup):
    path, db, gateway, transport, clock = setup
    await db.execute("UPDATE send_limits SET value = 1 WHERE name = 'global_daily'")
    await db.commit()
    assert await gateway.enqueue(candidate())
    assert not await gateway.enqueue(candidate())
    assert not await gateway.enqueue(candidate("b"))
    assert await gateway.enqueue(candidate("b", text="І ще одне питання"))
    started, finish = asyncio.Event(), asyncio.Event()

    async def send(_):
        started.set()
        await finish.wait()
        return 80

    transport.send.side_effect = send
    async with aiosqlite.connect(path) as second_db:
        second = OutboundGateway(second_db, ACCOUNT, transport, dry_run=False, zone=ZONE,
                                 lock=asyncio.Lock(), clock=lambda: clock[0])
        task = asyncio.create_task(gateway.dispatch_once())
        await asyncio.wait_for(started.wait(), 2)
        try:
            assert await second.dispatch_once() is None
            assert (await budget_usage(second_db, ACCOUNT, 0, clock[0], ZONE))[:2] == (1, 1)
        finally:
            finish.set()
            assert await task == "sent"
        await db.execute("UPDATE send_limits SET value = 0 WHERE name = 'global_daily'")
        await db.commit()
        assert await second.dispatch_once() is None
        assert (await budget_usage(second_db, ACCOUNT, 0, clock[0], ZONE))[:2] == (1, 1)
    transport.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_one_conversation_and_switch_cooldown(setup):
    _, _, gateway, transport, clock = setup
    assert await gateway.enqueue(candidate())
    assert await gateway.enqueue(candidate("b", chat=OTHER))
    assert await gateway.dispatch_once() == "sent"
    assert await gateway.dispatch_once() is None
    clock[0] += 301
    assert await gateway.dispatch_once() is None
    clock[0] += 119
    assert await gateway.dispatch_once() == "sent"
    assert [call.args[0].chat_id for call in transport.prepare.await_args_list] == [CHAT, OTHER]


@pytest.mark.asyncio
async def test_unknown_delivery_is_not_retried_and_reserves_budget(setup):
    _, db, gateway, transport, clock = setup
    assert await gateway.enqueue(candidate())
    transport.send.side_effect = TimeoutError
    assert await gateway.dispatch_once() == "unknown"
    assert await gateway.dispatch_once() is None
    assert not await gateway.enqueue(candidate())
    assert not await gateway.enqueue(candidate("new-key"))
    assert await db.execute_fetchall("SELECT status, notify_owner FROM outbox") == [("unknown", 1)]
    assert (await budget_usage(db, ACCOUNT, 0, clock[0], ZONE))[:2] == (1, 1)
    transport.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_restart_recovers_interrupted_send(setup):
    _, db, gateway, transport, clock = setup
    assert await gateway.enqueue(candidate())
    await db.execute("UPDATE outbox SET status = 'sending', reserved_at = ?", (clock[0],))
    await db.commit()
    await gateway.recover()
    assert await db.execute_fetchall("SELECT status, reason FROM outbox") == [("unknown", "interrupted")]
    assert await gateway.dispatch_once() is None
    transport.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_flood_wait_and_second_flood_pause_are_durable(setup):
    path, db, gateway, transport, clock = setup
    assert await gateway.enqueue(candidate())
    transport.send.side_effect = TransportRejected("flood_wait", 40)
    assert await gateway.dispatch_once() == "flood_wait"
    async with aiosqlite.connect(path) as reopened:
        assert await reopened.execute_fetchall("SELECT blocked_until FROM outbound_state") == [(clock[0] + 40,)]
    assert await gateway.enqueue(candidate("b"))
    assert await gateway.dispatch_once() is None
    clock[0] += 40
    assert await gateway.dispatch_once() == "repeated_flood_wait"
    assert await db.execute_fetchall("SELECT paused, pause_reason FROM runtime_state") == [(1, "repeated_flood_wait")]
    assert transport.send.await_count == 2


@pytest.mark.asyncio
async def test_proactive_permission_and_budgets_are_independent(setup):
    _, db, gateway, transport, _ = setup
    assert not await gateway.enqueue(candidate(proactive=True))
    await db.execute("UPDATE chats SET proactive_enabled = 1 WHERE chat_id = ?", (CHAT,))
    await db.commit()
    assert await gateway.enqueue(candidate(proactive=True))
    assert await gateway.dispatch_once() is None
    await db.executemany(
        "INSERT INTO send_limits VALUES (?, ?, 'proactive_daily', 1)", [(ACCOUNT, 0), (ACCOUNT, CHAT)],
    )
    await db.commit()
    assert await gateway.dispatch_once() == "sent"
    assert await gateway.enqueue(candidate("b", text="Ще одне питання", proactive=True))
    assert await gateway.dispatch_once() is None
    transport.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_slow_mode_is_scoped_and_write_forbidden_revokes_sending(setup):
    _, db, gateway, transport, clock = setup
    transport.prepare.side_effect = TransportRejected("slow_mode", 12)
    assert await gateway.enqueue(candidate())
    assert await gateway.dispatch_once() == "slow_mode"
    assert await gateway.enqueue(candidate("b"))
    assert await gateway.dispatch_once() is None
    assert await db.execute_fetchall("SELECT chat_id, blocked_until FROM chat_send_state") == [
        (CHAT, clock[0] + 12),
    ]
    clock[0] += 12
    transport.prepare.side_effect = TransportRejected("write_forbidden")
    assert await gateway.dispatch_once() == "write_forbidden"
    assert await db.execute_fetchall("SELECT mode FROM chats WHERE chat_id = ?", (CHAT,)) == [
        ("READ_ONLY",),
    ]
    transport.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_telethon_routes_forum_reply_and_uses_stored_random_id():
    from types import SimpleNamespace
    from unittest.mock import Mock

    client = AsyncMock()
    client.is_connected = Mock(return_value=True)
    client.get_entity.return_value = types.Channel(
        id=11, access_hash=123, title="Forum", photo=types.ChatPhotoEmpty(),
        date=datetime(2026, 11, 6, tzinfo=ZONE), megagroup=True, forum=True,
    )
    client.get_messages.return_value = [types.Message(
        id=50, peer_id=types.PeerChannel(11),
        reply_to=types.MessageReplyHeader(reply_to_msg_id=60, reply_to_top_id=40, forum_topic=True),
    )]
    transport = TelethonOutbound(client)
    queued = Queued(1, CHAT, 50, "Можна уточнити?", 40, 50, False, 763, 100)
    client.return_value = SimpleNamespace(full_chat=SimpleNamespace(send_paid_messages_stars=3))
    with pytest.raises(TransportRejected, match="paid_messages"):
        await transport.prepare(queued)
    client.get_messages.assert_not_awaited()
    client.return_value = SimpleNamespace(full_chat=SimpleNamespace(send_paid_messages_stars=None))
    request = await transport.prepare(queued)
    assert request.random_id == 763
    assert request.allow_paid_stars is None
    assert request.reply_to.reply_to_msg_id == 50
    assert request.reply_to.top_msg_id == 40
    client.return_value = types.Updates(
        [types.UpdateMessageID(id=99, random_id=763)], [], [], datetime(2026, 11, 6, tzinfo=ZONE), 0,
    )
    assert await transport.send(request) == 99
    client.side_effect = errors.ForbiddenError(request, "ALLOW_PAYMENT_REQUIRED_3", 403)
    with pytest.raises(TransportRejected, match="paid_messages"):
        await transport.send(request)
    client.side_effect = errors.RandomIdDuplicateError(request=request)
    with pytest.raises(TimeoutError):
        await transport.send(request)


@pytest.mark.asyncio
async def test_outgoing_update_before_receipt_is_recognized(setup):
    _, db, gateway, transport, _ = setup
    started, finish = asyncio.Event(), asyncio.Event()

    async def send(_):
        started.set()
        await finish.wait()
        return 80

    transport.send.side_effect = send
    assert await gateway.enqueue(candidate())
    sending = asyncio.create_task(gateway.dispatch_once())
    await asyncio.wait_for(started.wait(), 2)
    observing = asyncio.create_task(gateway.observe(CHAT, 0, 80, True))
    await asyncio.sleep(0)
    assert not observing.done()
    finish.set()
    await asyncio.wait_for(asyncio.gather(sending, observing), 2)
    assert not await db.execute_fetchall("SELECT manual_until FROM chat_send_state")
    assert await db.execute_fetchall(
        "SELECT message_id FROM outbound_context WHERE chat_id = ?", (CHAT,),
    ) == [(80,)]


@pytest.mark.asyncio
@pytest.mark.parametrize("text", ["# Заголовок", "Текст — текст", "https://t.me/example", "a" * 501])
async def test_text_policy_fails_closed(setup, text):
    _, db, gateway, transport, _ = setup
    assert await gateway.enqueue(candidate(text=text))
    assert await gateway.dispatch_once() is None
    assert await db.execute_fetchall("SELECT reason FROM outbox") == [("invalid_text",)]
    transport.prepare.assert_not_awaited()


def test_kyiv_day_windows_and_quiet_hours_cross_dst():
    spring = int(datetime(2026, 3, 29, 12, tzinfo=ZONE).timestamp())
    autumn = int(datetime(2026, 10, 25, 12, tzinfo=ZONE).timestamp())
    assert local_day(spring, ZONE)[1] - local_day(spring, ZONE)[0] == 23 * 3600
    assert local_day(autumn, ZONE)[1] - local_day(autumn, ZONE)[0] == 25 * 3600
    after_midnight = int(datetime(2026, 10, 25, 3, 30, tzinfo=ZONE).timestamp())
    assert is_quiet(after_midnight, ZONE, 23 * 60, 7 * 60)
    assert not is_quiet(autumn, ZONE, 23 * 60, 7 * 60)
