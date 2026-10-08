"""Shared Squid routing policy; also the dependency-free egress image entrypoint."""

from __future__ import annotations

import ipaddress
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

UPSTREAM_PROXY_ENV = "SOVEREN_EGRESS_UPSTREAM_PROXY"
UPSTREAM_DESTINATIONS_ENV = "SOVEREN_EGRESS_UPSTREAM_DESTINATIONS"
EGRESS_ROUTING_LABEL = "soveren.egress_routing"
EGRESS_ROUTING_VERSION = "1"
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")


def _hostname(value: str) -> str:
    host = value.lower().removesuffix(".")
    if len(host) > 253 or not all(_HOST_LABEL.fullmatch(label) for label in host.split(".")):
        raise ValueError("egress hostnames must be ASCII DNS names without URLs, wildcards, or suffix selectors")
    return host


@dataclass(frozen=True, slots=True)
class SandboxEgressUpstream:
    """Send exact destination hostnames through one unauthenticated HTTP parent.

    This is host-wide infrastructure policy, never model/tenant input. Destinations
    not listed here remain direct. Selected destinations cannot fall back to direct.
    """

    proxy_url: str
    destination_hosts: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.proxy_url, str) or any(ord(c) <= 32 for c in self.proxy_url):
            raise ValueError("egress upstream must be an HTTP proxy URL with an explicit port")
        try:
            parsed = urlsplit(self.proxy_url)
            port = parsed.port
        except ValueError:
            raise ValueError("egress upstream proxy URL is invalid") from None
        if (
            parsed.scheme != "http"
            or parsed.hostname is None
            or port is None
            or not 1 <= port <= 65535
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("egress upstream requires http://host:port without credentials, path, query, or fragment")
        host = _hostname(parsed.hostname)
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            if host == "localhost" or host.endswith(".localhost"):
                raise ValueError("egress upstream loopback is container-local; use a guarded host bridge listener")
        else:
            if address.is_loopback or address.is_unspecified or address.is_multicast or address.is_link_local:
                raise ValueError("egress upstream requires a reachable unicast address, not container loopback")
        object.__setattr__(self, "proxy_url", f"http://{host}:{port}")
        if (
            not isinstance(self.destination_hosts, (tuple, list))
            or not 1 <= len(self.destination_hosts) <= 256
            or any(not isinstance(item, str) for item in self.destination_hosts)
        ):
            raise ValueError("egress destinations must contain 1-256 exact hostnames")
        hosts = tuple(sorted({_hostname(item) for item in self.destination_hosts}))
        for destination in hosts:
            try:
                ipaddress.ip_address(destination)
            except ValueError:
                continue
            raise ValueError("egress destinations must be hostnames, not IP literals")
        object.__setattr__(self, "destination_hosts", hosts)

    def environment(self) -> dict[str, str]:
        return {
            UPSTREAM_PROXY_ENV: self.proxy_url,
            UPSTREAM_DESTINATIONS_ENV: json.dumps(self.destination_hosts, separators=(",", ":")),
        }


def upstream_from_environment(environment: Mapping[str, str]) -> SandboxEgressUpstream | None:
    proxy = environment.get(UPSTREAM_PROXY_ENV, "")
    destinations = environment.get(UPSTREAM_DESTINATIONS_ENV, "")
    if not proxy and not destinations:
        return None
    if not proxy or not destinations:
        raise ValueError("egress upstream proxy and destination hostnames must be configured together")
    try:
        hosts = json.loads(destinations)
    except json.JSONDecodeError as exc:
        raise ValueError("egress destinations must be a JSON array of exact hostnames") from exc
    if not isinstance(hosts, list):
        raise ValueError("egress destinations must be a JSON array of exact hostnames")
    return SandboxEgressUpstream(proxy_url=proxy, destination_hosts=tuple(hosts))


def render_squid_config(base_config: str, upstream: SandboxEgressUpstream | None) -> str:
    if upstream is None:
        return base_config
    parsed = urlsplit(upstream.proxy_url)
    if base_config.count("http_access allow all\n") != 1:
        raise ValueError("Squid base configuration must have exactly one final http_access allow all rule")
    # A parent may resolve a name Squid could not resolve locally. Do not let
    # that skip the private-address checks already performed by http_access.
    config = base_config.replace(
        "http_access allow all\n",
        f"acl upstream_destination dstdomain -n {' '.join(upstream.destination_hosts)}\n"
        "acl resolved_destination dst 0.0.0.0/0 ::/0\n"
        "http_access deny upstream_destination !resolved_destination\n"
        "http_access allow all\n",
    )
    return config + "\n" + "\n".join(
        (
            "# Selected destinations use the parent exclusively; all others stay direct.",
            f"cache_peer {parsed.hostname} parent {parsed.port} 0 "
            "no-query no-digest default name=selected_upstream connect-timeout=5",
            "cache_peer_access selected_upstream allow upstream_destination",
            "cache_peer_access selected_upstream deny all",
            "always_direct deny upstream_destination",
            "always_direct allow all",
            "never_direct allow upstream_destination",
            "never_direct deny all",
            "",
        )
    )


def main() -> None:
    upstream = upstream_from_environment(os.environ)
    base = Path("/etc/squid/squid.conf").read_text()
    output = Path("/run/soveren-squid.conf")
    output.write_text(render_squid_config(base, upstream))
    # Retain the pinned image's initialization and Docker log forwarding.
    os.execvp("entrypoint.sh", ["entrypoint.sh", "-f", str(output), "-NYC"])


if __name__ == "__main__":
    main()
