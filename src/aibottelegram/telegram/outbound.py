import math
import re
import time
from contextlib import contextmanager

from telethon import TelegramClient, errors, functions, types, utils

from ..policy.outbound import Queued, TransportRejected
from .session import INVALID_AUTH

WRITE_ERRORS = (
    errors.ChatWriteForbiddenError, errors.UserBannedInChannelError,
    errors.ChannelPrivateError, errors.ChatAdminRequiredError, errors.ChatRestrictedError,
)
PAYMENT_REQUIRED = re.compile(r"ALLOW_PAYMENT_REQUIRED(?:_\d+)?")


def message_thread(message: object) -> int:
    header = getattr(message, "reply_to", None)
    if header and getattr(header, "forum_topic", False):
        return header.reply_to_top_id or header.reply_to_msg_id or 0
    return 0


@contextmanager
def telegram_errors():
    try:
        yield
    except errors.SlowModeWaitError as error:
        raise TransportRejected("slow_mode", error.seconds) from None
    except errors.FloodWaitError as error:
        raise TransportRejected("flood_wait", error.seconds) from None
    except errors.PeerFloodError:
        raise TransportRejected("peer_flood") from None
    except WRITE_ERRORS:
        raise TransportRejected("write_forbidden") from None
    except INVALID_AUTH:
        raise TransportRejected("session_unauthorized") from None
    except (errors.RandomIdDuplicateError, errors.ServerError, errors.TimedOutError, errors.RpcCallFailError):
        raise TimeoutError from None
    except errors.RPCError as error:
        if PAYMENT_REQUIRED.fullmatch(error.message or ""):
            raise TransportRejected("paid_messages") from None
        if error.code >= 500:
            raise TimeoutError from None
        raise TransportRejected(type(error).__name__) from None


class TelethonOutbound:
    def __init__(self, client: TelegramClient):
        self.client = client

    async def prepare(self, candidate: Queued) -> functions.messages.SendMessageRequest:
        if not self.client.is_connected():
            raise TransportRejected("disconnected")
        with telegram_errors():
            try:
                entity = await self.client.get_entity(candidate.chat_id)
            except (TypeError, ValueError):
                raise TransportRejected("peer_unresolved") from None
            if utils.get_peer_id(entity) != candidate.chat_id:
                raise TransportRejected("peer_mismatch")
            if not isinstance(entity, (types.User, types.Chat, types.Channel)):
                raise TransportRejected("write_forbidden")
            try:
                input_peer = utils.get_input_peer(entity)
            except (TypeError, ValueError):
                raise TransportRejected("peer_unresolved") from None
            if any(getattr(entity, field, None) for field in ("deleted", "left", "deactivated", "migrated_to")):
                raise TransportRejected("write_forbidden")
            admin = bool(getattr(entity, "creator", False) or getattr(entity, "admin_rights", None))
            if getattr(entity, "broadcast", False) and not (
                getattr(entity, "creator", False) or getattr(entity.admin_rights, "post_messages", False)
            ):
                raise TransportRejected("write_forbidden")
            rights = getattr(entity, "banned_rights", None)
            default_rights = getattr(entity, "default_banned_rights", None) if not admin else None
            if any(
                getattr(rule, "send_messages", False) or getattr(rule, "send_plain", False)
                for rule in (rights, default_rights)
            ):
                raise TransportRejected("write_forbidden")
            if getattr(entity, "send_paid_messages_stars", 0):
                raise TransportRejected("paid_messages")
            if isinstance(entity, types.Channel) and (entity.megagroup or entity.monoforum or entity.slowmode_enabled):
                full = (await self.client(functions.channels.GetFullChannelRequest(entity))).full_chat
                if getattr(full, "send_paid_messages_stars", None) is not None:
                    raise TransportRejected("paid_messages")
                if not admin:
                    next_send = getattr(full, "slowmode_next_send_date", None)
                    wait = math.ceil(next_send.timestamp() - time.time()) if next_send else 0
                    if wait > 0:
                        raise TransportRejected("slow_mode", wait)
            if candidate.thread_id and not getattr(entity, "forum", False):
                raise TransportRejected("thread_mismatch")

            reply_id = candidate.reply_to_id
            check_ids = list(dict.fromkeys((candidate.context_id, reply_id or candidate.context_id)))
            messages = await self.client.get_messages(entity, ids=check_ids)
            if len(messages) != len(check_ids) or any(
                not isinstance(message, types.Message)
                or message.id != expected
                or (message_thread(message) or (1 if candidate.thread_id == 1 else 0)) != candidate.thread_id
                for expected, message in zip(check_ids, messages)
            ):
                raise TransportRejected("stale_context")
            if any(getattr(message, "paid_message_stars", 0) for message in messages):
                raise TransportRejected("paid_messages")
            if reply_id is None and candidate.thread_id > 1:
                reply_id = candidate.thread_id
            reply = None if reply_id is None else types.InputReplyToMessage(
                reply_to_msg_id=reply_id,
                top_msg_id=candidate.thread_id if candidate.thread_id > 1 and reply_id != candidate.thread_id else None,
            )
            return functions.messages.SendMessageRequest(
                peer=input_peer, message=candidate.text, no_webpage=True,
                random_id=candidate.random_id, reply_to=reply,
            )

    async def send(self, request: functions.messages.SendMessageRequest) -> int | None:
        if not self.client.is_connected():
            raise TransportRejected("disconnected")
        with telegram_errors():
            try:
                result = await self.client(request)
            except ValueError:
                # Telethon can report an exhausted request without an RPC result.
                raise TimeoutError from None
        if isinstance(result, types.UpdateShortSentMessage):
            return result.id
        for update in getattr(result, "updates", ()):
            if isinstance(update, types.UpdateMessageID) and update.random_id == request.random_id:
                return update.id
        return None
