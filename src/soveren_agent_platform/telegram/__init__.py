"""Telegram communication interface for the platform."""

import soveren_agent_platform.telegram.ptb as _ptb
from soveren_agent_platform.telegram.contracts import TelegramInboundMessage
from soveren_agent_platform.telegram.ingress import enqueue_telegram_message

TelegramRuntimeHooks = _ptb.TelegramRuntimeHooks
TelegramAgentApp = _ptb.TelegramAgentApp
TelegramSender = _ptb.TelegramSender
build_telegram_inline_keyboard = _ptb.build_telegram_inline_keyboard
build_telegram_polling_application = _ptb.build_telegram_polling_application
create_telegram_agent_app = _ptb.create_telegram_agent_app
enqueue_telegram_update = _ptb.enqueue_telegram_update
handle_telegram_callback_query = _ptb.handle_telegram_callback_query
handle_telegram_message_update = _ptb.handle_telegram_message_update
update_to_inbound_message = _ptb.update_to_inbound_message

PtbRuntimeHooks = _ptb.PtbRuntimeHooks
PtbTelegramAgentApp = _ptb.PtbTelegramAgentApp
PtbTelegramSender = _ptb.PtbTelegramSender
build_ptb_application = _ptb.build_ptb_application
create_ptb_agent_app = _ptb.create_ptb_agent_app
enqueue_ptb_update = _ptb.enqueue_ptb_update
handle_ptb_callback_query = _ptb.handle_ptb_callback_query
handle_ptb_message_update = _ptb.handle_ptb_message_update

__all__ = [
    "TelegramAgentApp",
    "TelegramInboundMessage",
    "TelegramRuntimeHooks",
    "TelegramSender",
    "build_telegram_inline_keyboard",
    "build_telegram_polling_application",
    "create_telegram_agent_app",
    "enqueue_telegram_message",
    "enqueue_telegram_update",
    "handle_telegram_callback_query",
    "handle_telegram_message_update",
    "update_to_inbound_message",
]
