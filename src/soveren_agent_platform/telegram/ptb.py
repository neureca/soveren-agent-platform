"""Optional python-telegram-bot adapter.

This module intentionally uses duck typing for PTB objects so importing
`soveren_agent_platform` does not require `python-telegram-bot` as a core dependency.
"""

from __future__ import annotations

import asyncio
import inspect
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.agent.contracts import AgentHandler
from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.batching.rules import (
    DEFAULT_MAX_COUNT,
    DEFAULT_MAX_WINDOW_S,
    DEFAULT_QUIET_WINDOW_S,
)
from soveren_agent_platform.outbound.contracts import OutboundMessage, SendResult
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.queue.contracts import DurableQueue
from soveren_agent_platform.queue.sqlite import SQLiteEventQueue
from soveren_agent_platform.telegram.contracts import TelegramChatRegistry, TelegramInboundMessage
from soveren_agent_platform.telegram.ingress import enqueue_telegram_message
from soveren_agent_platform.telegram.outbound import TELEGRAM_TEXT_LIMIT
from soveren_agent_platform.telegram.sqlite import SQLiteTelegramChatRegistry

Hook = Callable[..., Any]


@dataclass(slots=True)
class TelegramRuntimeHooks:
    on_update_enqueued: Hook | None = None
    on_callback_query: Hook | None = None
    on_chat_registered: Hook | None = None


@dataclass(frozen=True, slots=True)
class TelegramAccessPolicy:
    allowed_chat_ids: frozenset[int] | None = None
    allowed_user_ids: frozenset[int] | None = None

    def __post_init__(self) -> None:
        if self.allowed_chat_ids is None and self.allowed_user_ids is None:
            raise ValueError(
                "TelegramAccessPolicy requires a chat or user allowlist; "
                "use allow_all_updates=True for unrestricted access"
            )

    def allows(self, *, chat_id: int, user_id: int | None) -> bool:
        if self.allowed_chat_ids is not None and chat_id not in self.allowed_chat_ids:
            return False
        if self.allowed_user_ids is not None and user_id not in self.allowed_user_ids:
            return False
        return True


@dataclass(frozen=True, slots=True)
class TelegramChatRegistrationPolicy:
    trusted_user_ids: frozenset[int]
    commands: frozenset[str] = field(default_factory=lambda: frozenset({"/start", "/register"}))

    def can_register(self, message: TelegramInboundMessage) -> bool:
        if message.user_id not in self.trusted_user_ids:
            return False
        command = _telegram_command(message.text)
        return command in self.commands


class TelegramSender:
    """Outbound `ChannelSender` implementation for Telegram bots."""

    def __init__(self, bot: Any) -> None:
        self.bot = bot

    async def send(self, message: OutboundMessage) -> SendResult:
        payload = message.payload or {}
        if len(message.text) > TELEGRAM_TEXT_LIMIT and payload.get("parse_mode") is None:
            return SendResult.permanent_failure(
                f"Telegram text exceeds the {TELEGRAM_TEXT_LIMIT}-character limit"
            )
        try:
            sent = await self.bot.send_message(
                chat_id=_coerce_chat_id(message.destination_id),
                text=message.text,
                parse_mode=payload.get("parse_mode"),
                reply_markup=payload.get("reply_markup")
                or build_telegram_inline_keyboard(payload.get("buttons")),
                disable_web_page_preview=payload.get("disable_web_page_preview"),
            )
        except Exception as exc:
            failure = _telegram_send_failure(exc)
            if failure is not None:
                return failure
            raise
        return SendResult.sent(
            {
                "message_id": getattr(sent, "message_id", None),
                "chat_id": message.destination_id,
            }
        )


def _telegram_send_failure(exc: Exception) -> SendResult | None:
    try:
        from telegram.error import BadRequest, Forbidden, RetryAfter
    except ImportError:
        return None
    error = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, RetryAfter):
        return SendResult.retryable_failure(
            error,
            retry_after_s=_retry_after_seconds(exc.retry_after),
        )
    if isinstance(exc, (BadRequest, Forbidden)):
        return SendResult.permanent_failure(error)
    return None


def _retry_after_seconds(value: int | timedelta) -> int:
    if isinstance(value, timedelta):
        return max(0, math.ceil(value.total_seconds()))
    return max(0, value)


@dataclass(slots=True)
class TelegramAgentApp:
    """High-level polling runtime for Telegram-backed agent applications."""

    platform: AgentPlatformApp
    telegram_app: Any
    event_queue: SQLiteEventQueue
    tenant_id: str | None = None
    chat_registry: TelegramChatRegistry | None = None
    _started: bool = False
    _closed: bool = False

    async def revoke_registered_chat(self, chat_id: int) -> bool:
        """Revoke one dynamically registered chat for this app's tenant."""
        if self.tenant_id is None or self.chat_registry is None:
            raise RuntimeError("Telegram chat access management is not configured")
        return await self.chat_registry.revoke(tenant_id=self.tenant_id, chat_id=chat_id)

    async def start(self) -> None:
        if self._started:
            return
        if self._closed:
            raise RuntimeError("Telegram agent app cannot be restarted after stop")
        initialized = False
        telegram_started = False
        polling_start_attempted = False
        updater = None
        try:
            await self.platform.start()
            await _call_method(self.telegram_app, "initialize")
            initialized = True
            await _call_method(self.telegram_app, "start")
            telegram_started = True
            updater = getattr(self.telegram_app, "updater", None)
            if updater is None:
                raise RuntimeError("Telegram polling application does not expose an updater")
            polling_start_attempted = True
            await _call_method(updater, "start_polling")
        except BaseException as start_error:
            errors: list[BaseException] = [start_error]
            if polling_start_attempted:
                await _call_method_collecting(errors, updater, "stop")
            if telegram_started:
                await _call_method_collecting(errors, self.telegram_app, "stop")
            if initialized:
                await _call_method_collecting(errors, self.telegram_app, "shutdown")
            try:
                await self.platform.stop()
            except BaseException as exc:
                errors.append(exc)
            finally:
                await self.event_queue.close()
                self._closed = True
            if len(errors) == 1:
                raise
            raise BaseExceptionGroup("telegram agent startup failed", errors) from None
        self._started = True

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        if self._closed:
            return
        if not self._started:
            await self.event_queue.close()
            self._closed = True
            return
        errors: list[BaseException] = []
        try:
            updater = getattr(self.telegram_app, "updater", None)
            if updater is not None:
                await _call_method_collecting(errors, updater, "stop")
            await _call_method_collecting(errors, self.telegram_app, "stop")
            await _call_method_collecting(errors, self.telegram_app, "shutdown")
        finally:
            try:
                await self.platform.stop(timeout_s=timeout_s)
            except BaseException as exc:
                errors.append(exc)
            finally:
                await self.event_queue.close()
                self._started = False
                self._closed = True
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("telegram agent shutdown failed", errors)

    async def run(self, stop_event: Any | None = None) -> None:
        await self.start()
        platform_wait = asyncio.create_task(self.platform.wait())
        external_stop = asyncio.create_task(_never_stop() if stop_event is None else _maybe_await(stop_event.wait()))
        errors: list[BaseException] = []
        try:
            done, _ = await asyncio.wait(
                (platform_wait, external_stop),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if platform_wait in done:
                await platform_wait
            if external_stop in done:
                await external_stop
        except BaseException as exc:
            errors.append(exc)
        finally:
            for task in (platform_wait, external_stop):
                if not task.done():
                    task.cancel()
            await asyncio.gather(platform_wait, external_stop, return_exceptions=True)
            try:
                await self.stop()
            except BaseException as exc:
                errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("telegram agent runtime failed", errors)

    async def __aenter__(self) -> "TelegramAgentApp":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.stop()


async def create_telegram_agent_app(
    *,
    token: str,
    db_path: Path,
    tenant_id: str,
    handler: AgentHandler,
    actions: ActionRegistry | None = None,
    outbound: OutboundRegistry | None = None,
    hooks: TelegramRuntimeHooks | None = None,
    access_policy: TelegramAccessPolicy | None = None,
    registration_policy: TelegramChatRegistrationPolicy | None = None,
    allowed_chat_ids: Iterable[int] | None = None,
    allowed_user_ids: Iterable[int] | None = None,
    registration_user_ids: Iterable[int] | None = None,
    allow_all_updates: bool = False,
    quiet_window_s: int = DEFAULT_QUIET_WINDOW_S,
    max_window_s: int = DEFAULT_MAX_WINDOW_S,
    max_count: int = DEFAULT_MAX_COUNT,
    bootstrap_storage: bool = True,
    application_builder: Any | None = None,
    message_handler_cls: Any | None = None,
    callback_query_handler_cls: Any | None = None,
    message_filter: Any | None = None,
) -> TelegramAgentApp:
    event_queue = await SQLiteEventQueue.open(db_path)
    chat_registry = SQLiteTelegramChatRegistry._from_connection(event_queue._conn)
    try:
        effective_access_policy = _telegram_access_policy(
            access_policy,
            allowed_chat_ids=allowed_chat_ids,
            allowed_user_ids=allowed_user_ids,
        )
        effective_registration_policy = _telegram_registration_policy(
            registration_policy,
            registration_user_ids=registration_user_ids,
        )
        telegram_app = build_telegram_polling_application(
            token=token,
            queue=event_queue,
            tenant_id=tenant_id,
            chat_registry=chat_registry,
            hooks=hooks,
            access_policy=effective_access_policy,
            registration_policy=effective_registration_policy,
            allow_all_updates=allow_all_updates,
            application_builder=application_builder,
            message_handler_cls=message_handler_cls,
            callback_query_handler_cls=callback_query_handler_cls,
            message_filter=message_filter,
        )
    except BaseException:
        await event_queue.close()
        raise
    action_registry = actions or ActionRegistry()
    outbound_registry = outbound or OutboundRegistry()
    outbound_registry.register("telegram", TelegramSender(telegram_app.bot))
    platform = (
        AgentPlatformApp(db_path=db_path, bootstrap_storage=bootstrap_storage)
        .use_batching(
            tenant_id=tenant_id,
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        )
        .use_agent(handler=handler, tenant_id=tenant_id)
        .use_actions(registry=action_registry, tenant_id=tenant_id)
        .use_outbound(
            registry=outbound_registry,
            channels=["telegram"],
            tenant_id=tenant_id,
        )
    )
    return TelegramAgentApp(
        platform=platform,
        telegram_app=telegram_app,
        event_queue=event_queue,
        tenant_id=tenant_id,
        chat_registry=chat_registry,
    )


def update_to_inbound_message(
    update: Any,
    *,
    tenant_id: str,
    access_policy: TelegramAccessPolicy | None = None,
) -> TelegramInboundMessage | None:
    """Normalize a Telegram Update-like object into a platform TelegramInboundMessage."""
    message = getattr(update, "effective_message", None)
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    if message is None or chat is None:
        return None
    chat_id = int(getattr(chat, "id"))
    user_id = getattr(user, "id", None)
    if access_policy is not None and not access_policy.allows(chat_id=chat_id, user_id=user_id):
        return None

    text = getattr(message, "text", None) or getattr(message, "caption", None)
    date = _timestamp(getattr(message, "date", None))
    raw_payload = update.to_dict() if hasattr(update, "to_dict") else {}
    payload = {
        "date": date,
        "message_id": getattr(message, "message_id", None),
        "from_first_name": getattr(user, "first_name", None),
        "from_last_name": getattr(user, "last_name", None),
        "from_username": getattr(user, "username", None),
        "raw": raw_payload,
    }
    return TelegramInboundMessage(
        tenant_id=tenant_id,
        chat_id=chat_id,
        update_id=int(getattr(update, "update_id")),
        user_id=user_id,
        username=getattr(user, "username", None),
        text=text,
        payload=payload,
    )


async def enqueue_telegram_update(
    queue: DurableQueue,
    update: Any,
    *,
    tenant_id: str,
    access_policy: TelegramAccessPolicy | None = None,
    allow_all_updates: bool = False,
) -> str | None:
    _validate_telegram_security(
        access_policy=access_policy,
        registration_policy=None,
        allow_all_updates=allow_all_updates,
    )
    message = update_to_inbound_message(update, tenant_id=tenant_id, access_policy=access_policy)
    if message is None:
        return None
    return await enqueue_telegram_message(queue, message)


async def handle_telegram_message_update(
    queue: DurableQueue,
    update: Any,
    context: Any,
    *,
    tenant_id: str,
    hooks: TelegramRuntimeHooks | None = None,
    access_policy: TelegramAccessPolicy | None = None,
    registration_policy: TelegramChatRegistrationPolicy | None = None,
    chat_registry: TelegramChatRegistry | None = None,
    allow_all_updates: bool = False,
) -> str | None:
    _validate_telegram_security(
        access_policy=access_policy,
        registration_policy=registration_policy,
        chat_registry=chat_registry,
        allow_all_updates=allow_all_updates,
    )
    message = update_to_inbound_message(update, tenant_id=tenant_id)
    if message is None:
        return None
    registered = await _register_telegram_chat_if_requested(
        chat_registry,
        message,
        registration_policy=registration_policy,
    )
    if registered:
        await _call_hook(
            hooks.on_chat_registered if hooks else None,
            message=message,
            update=update,
            context=context,
        )
        return None
    if not await _telegram_message_allowed(
        chat_registry,
        message,
        access_policy=access_policy,
        registration_policy=registration_policy,
        allow_all_updates=allow_all_updates,
    ):
        return None
    event_id = await enqueue_telegram_message(queue, message)
    await _call_hook(
        hooks.on_update_enqueued if hooks else None,
        event_id=event_id,
        message=message,
        update=update,
        context=context,
    )
    return event_id


async def handle_telegram_callback_query(
    chat_registry: TelegramChatRegistry | None,
    update: Any,
    context: Any,
    *,
    tenant_id: str,
    hooks: TelegramRuntimeHooks | None = None,
    access_policy: TelegramAccessPolicy | None = None,
    registration_policy: TelegramChatRegistrationPolicy | None = None,
    allow_all_updates: bool = False,
) -> Any:
    _validate_telegram_security(
        access_policy=access_policy,
        registration_policy=registration_policy,
        chat_registry=chat_registry,
        allow_all_updates=allow_all_updates,
    )
    query = getattr(update, "callback_query", None)
    if query is not None and hasattr(query, "answer"):
        await _maybe_await(query.answer())
    if not await _telegram_callback_allowed(
        chat_registry,
        update,
        tenant_id=tenant_id,
        access_policy=access_policy,
        registration_policy=registration_policy,
        allow_all_updates=allow_all_updates,
    ):
        return None
    return await _call_hook(
        hooks.on_callback_query if hooks else None,
        query=query,
        update=update,
        context=context,
        data=getattr(query, "data", None) if query is not None else None,
    )


def build_telegram_polling_application(
    *,
    token: str,
    queue: DurableQueue,
    tenant_id: str,
    chat_registry: TelegramChatRegistry | None = None,
    hooks: TelegramRuntimeHooks | None = None,
    access_policy: TelegramAccessPolicy | None = None,
    registration_policy: TelegramChatRegistrationPolicy | None = None,
    allow_all_updates: bool = False,
    application_builder: Any | None = None,
    message_handler_cls: Any | None = None,
    callback_query_handler_cls: Any | None = None,
    message_filter: Any | None = None,
) -> Any:
    _validate_telegram_security(
        access_policy=access_policy,
        registration_policy=registration_policy,
        chat_registry=chat_registry,
        allow_all_updates=allow_all_updates,
    )
    if application_builder is None or message_handler_cls is None or callback_query_handler_cls is None:
        try:
            from telegram.ext import (
                Application,
                CallbackQueryHandler,
                MessageHandler,
                filters,
            )
        except ImportError as exc:
            raise RuntimeError(
                "Telegram adapter dependencies are required to build a Telegram polling application"
            ) from exc
        application_builder = application_builder or Application.builder()
        message_handler_cls = message_handler_cls or MessageHandler
        callback_query_handler_cls = callback_query_handler_cls or CallbackQueryHandler
        message_filter = message_filter or filters.ALL

    async def on_message(update: Any, context: Any) -> str | None:
        return await handle_telegram_message_update(
            queue,
            update,
            context,
            tenant_id=tenant_id,
            hooks=hooks,
            access_policy=access_policy,
            registration_policy=registration_policy,
            chat_registry=chat_registry,
            allow_all_updates=allow_all_updates,
        )

    async def on_callback(update: Any, context: Any) -> Any:
        return await handle_telegram_callback_query(
            chat_registry,
            update,
            context,
            tenant_id=tenant_id,
            hooks=hooks,
            access_policy=access_policy,
            registration_policy=registration_policy,
            allow_all_updates=allow_all_updates,
        )

    app = application_builder.token(token).build()
    app.add_handler(message_handler_cls(message_filter, on_message))
    app.add_handler(callback_query_handler_cls(on_callback))
    return app


def build_telegram_inline_keyboard(buttons: list[list[dict[str, str]]] | None) -> Any:
    if not buttons:
        return None
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    except ImportError as exc:
        raise RuntimeError("Telegram adapter dependencies are required to build inline keyboard markup") from exc
    rows = [
        [InlineKeyboardButton(text=button["text"], callback_data=button["callback_data"]) for button in row]
        for row in buttons
    ]
    return InlineKeyboardMarkup(rows)


PtbRuntimeHooks = TelegramRuntimeHooks
PtbTelegramAccessPolicy = TelegramAccessPolicy
PtbTelegramChatRegistrationPolicy = TelegramChatRegistrationPolicy
PtbTelegramSender = TelegramSender
PtbTelegramAgentApp = TelegramAgentApp
build_ptb_application = build_telegram_polling_application
build_ptb_inline_keyboard = build_telegram_inline_keyboard
create_ptb_agent_app = create_telegram_agent_app
enqueue_ptb_update = enqueue_telegram_update
handle_ptb_callback_query = handle_telegram_callback_query
handle_ptb_message_update = handle_telegram_message_update


async def _telegram_message_allowed(
    chat_registry: TelegramChatRegistry | None,
    message: TelegramInboundMessage,
    *,
    access_policy: TelegramAccessPolicy | None,
    registration_policy: TelegramChatRegistrationPolicy | None,
    allow_all_updates: bool,
) -> bool:
    if access_policy is not None and access_policy.allows(chat_id=message.chat_id, user_id=message.user_id):
        return True
    if registration_policy is not None:
        assert chat_registry is not None
        return await chat_registry.is_registered(tenant_id=message.tenant_id, chat_id=message.chat_id)
    return allow_all_updates


async def _telegram_callback_allowed(
    chat_registry: TelegramChatRegistry | None,
    update: Any,
    *,
    tenant_id: str,
    access_policy: TelegramAccessPolicy | None,
    registration_policy: TelegramChatRegistrationPolicy | None,
    allow_all_updates: bool,
) -> bool:
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    if chat is None:
        return False
    chat_id = int(getattr(chat, "id"))
    user_id = getattr(user, "id", None)
    if access_policy is not None and access_policy.allows(chat_id=chat_id, user_id=user_id):
        return True
    if registration_policy is not None:
        assert chat_registry is not None
        return await chat_registry.is_registered(tenant_id=tenant_id, chat_id=chat_id)
    return allow_all_updates


def _validate_telegram_security(
    *,
    access_policy: TelegramAccessPolicy | None,
    registration_policy: TelegramChatRegistrationPolicy | None,
    chat_registry: TelegramChatRegistry | None = None,
    allow_all_updates: bool,
) -> None:
    if allow_all_updates and (access_policy is not None or registration_policy is not None):
        raise ValueError("allow_all_updates=True cannot be combined with Telegram access or registration policies")
    if access_policy is None and registration_policy is None and not allow_all_updates:
        raise ValueError(
            "Telegram runtime requires an access policy, a registration policy, or explicit allow_all_updates=True"
        )
    if registration_policy is not None and chat_registry is None:
        raise ValueError("Telegram chat registration requires a TelegramChatRegistry")


async def _register_telegram_chat_if_requested(
    chat_registry: TelegramChatRegistry | None,
    message: TelegramInboundMessage,
    *,
    registration_policy: TelegramChatRegistrationPolicy | None,
) -> bool:
    if registration_policy is None or message.user_id is None or not registration_policy.can_register(message):
        return False
    assert chat_registry is not None
    await chat_registry.register(
        tenant_id=message.tenant_id,
        chat_id=message.chat_id,
        registered_by_user_id=message.user_id,
    )
    return True


def _telegram_access_policy(
    access_policy: TelegramAccessPolicy | None,
    *,
    allowed_chat_ids: Iterable[int] | None,
    allowed_user_ids: Iterable[int] | None,
) -> TelegramAccessPolicy | None:
    if access_policy is not None and (allowed_chat_ids is not None or allowed_user_ids is not None):
        raise ValueError("pass either access_policy or allowed_chat_ids/allowed_user_ids")
    if access_policy is not None:
        return access_policy
    if allowed_chat_ids is None and allowed_user_ids is None:
        return None
    chat_ids = frozenset(int(chat_id) for chat_id in allowed_chat_ids) if allowed_chat_ids is not None else None
    user_ids = frozenset(int(user_id) for user_id in allowed_user_ids) if allowed_user_ids is not None else None
    return TelegramAccessPolicy(
        allowed_chat_ids=chat_ids,
        allowed_user_ids=user_ids,
    )


def _telegram_registration_policy(
    registration_policy: TelegramChatRegistrationPolicy | None,
    *,
    registration_user_ids: Iterable[int] | None,
) -> TelegramChatRegistrationPolicy | None:
    if registration_policy is not None and registration_user_ids is not None:
        raise ValueError("pass either registration_policy or registration_user_ids")
    if registration_policy is not None:
        return registration_policy
    if registration_user_ids is None:
        return None
    return TelegramChatRegistrationPolicy(
        trusted_user_ids=frozenset(int(user_id) for user_id in registration_user_ids),
    )


def _telegram_command(text: str | None) -> str | None:
    if not text:
        return None
    first = text.strip().split(maxsplit=1)[0].lower()
    if "@" in first:
        first = first.split("@", 1)[0]
    return first if first.startswith("/") else None


def _timestamp(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_chat_id(value: str) -> int | str:
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


async def _call_hook(hook: Hook | None, **kwargs: Any) -> Any:
    if hook is None:
        return None
    return await _maybe_await(hook(**kwargs))


async def _call_method(target: Any, name: str) -> Any:
    method = getattr(target, name)
    return await _maybe_await(method())


async def _call_method_collecting(errors: list[BaseException], target: Any, name: str) -> None:
    try:
        await _call_method(target, name)
    except BaseException as exc:
        errors.append(exc)


async def _never_stop() -> None:
    await asyncio.Event().wait()


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
