"""Registry of outbound channel senders."""
from __future__ import annotations

from dataclasses import dataclass, field

from soveren_agent_platform.outbound.contracts import ChannelSender


@dataclass(slots=True)
class OutboundRegistry:
    senders: dict[str, ChannelSender] = field(default_factory=dict)

    def register(self, channel: str, sender: ChannelSender) -> None:
        self.senders[channel] = sender

    def get(self, channel: str) -> ChannelSender:
        try:
            return self.senders[channel]
        except KeyError as exc:
            raise KeyError(f"no outbound sender registered for channel={channel!r}") from exc

