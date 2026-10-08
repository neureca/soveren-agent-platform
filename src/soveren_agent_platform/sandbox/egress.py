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

UPSTREAM_ROUTES_ENV = "SOVEREN_EGRESS_UPSTREAM_ROUTES"
EGRESS_ROUTING_LABEL = "soveren.egress_routing"
EGRESS_ROUTING_VERSION = "2"
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")


def _hostname(value: str) -> str:
    host = value.lower().removesuffix(".")
    if len(host) > 253 or not all(_HOST_LABEL.fullmatch(label) for label in host.split(".")):
        raise ValueError("egress hostnames must be ASCII DNS names without URLs, wildcards, or suffix selectors")
    return host


@dataclass(frozen=True, slots=True)
class SandboxEgressUpstream:
    """Send exact destination hostnames through one unauthenticated HTTP parent.

    Bootstrap supplies a tuple of these groups as host-wide infrastructure policy,
    never model/tenant input. Unlisted destinations remain direct. Selected hosts
    cannot fall back to direct or another group's parent.
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
        hosts = tuple(sorted(_hostname(item) for item in self.destination_hosts))
        if len(set(hosts)) != len(hosts):
            raise ValueError("egress destination hostnames must not be duplicated")
        for destination in hosts:
            try:
                ipaddress.ip_address(destination)
            except ValueError:
                continue
            raise ValueError("egress destinations must be hostnames, not IP literals")
        object.__setattr__(self, "destination_hosts", hosts)

def normalize_upstreams(upstreams: tuple[SandboxEgressUpstream, ...]) -> tuple[SandboxEgressUpstream, ...]:
    if not isinstance(upstreams, (tuple, list)) or len(upstreams) > 16:
        raise ValueError("egress upstreams must contain at most 16 groups")
    by_proxy: dict[str, list[str]] = {}
    seen: set[str] = set()
    for upstream in upstreams:
        if not isinstance(upstream, SandboxEgressUpstream):
            raise TypeError("egress upstream groups must be SandboxEgressUpstream values")
        for host in upstream.destination_hosts:
            if host in seen:
                raise ValueError(f"egress destination hostname {host!r} is assigned more than once")
            seen.add(host)
        by_proxy.setdefault(upstream.proxy_url, []).extend(upstream.destination_hosts)
    if len(seen) > 256:
        raise ValueError("egress upstreams must contain at most 256 total destination hostnames")
    return tuple(
        SandboxEgressUpstream(proxy_url=url, destination_hosts=tuple(hosts))
        for url, hosts in sorted(by_proxy.items())
    )


def upstreams_environment(upstreams: tuple[SandboxEgressUpstream, ...]) -> dict[str, str]:
    routes = normalize_upstreams(upstreams)
    if not routes:
        return {}
    return {UPSTREAM_ROUTES_ENV: json.dumps([
        {"proxy_url": route.proxy_url, "destination_hosts": route.destination_hosts}
        for route in routes
    ], separators=(",", ":"))}


def upstreams_from_environment(environment: Mapping[str, str]) -> tuple[SandboxEgressUpstream, ...]:
    if environment.get("SOVEREN_EGRESS_UPSTREAM_PROXY") or environment.get("SOVEREN_EGRESS_UPSTREAM_DESTINATIONS"):
        raise ValueError("single-upstream environment is unsupported; use SOVEREN_EGRESS_UPSTREAM_ROUTES")
    serialized = environment.get(UPSTREAM_ROUTES_ENV, "")
    if not serialized:
        return ()
    try:
        groups = json.loads(serialized)
    except json.JSONDecodeError as exc:
        raise ValueError("egress upstream routes must be a JSON array of groups") from exc
    if not isinstance(groups, list) or len(groups) > 16:
        raise ValueError("egress upstream routes must be a JSON array of at most 16 groups")
    routes: list[SandboxEgressUpstream] = []
    for group in groups:
        if (
            not isinstance(group, dict)
            or set(group) != {"proxy_url", "destination_hosts"}
            or not isinstance(group["destination_hosts"], list)
        ):
            raise ValueError("each egress group requires only proxy_url and a destination_hosts array")
        routes.append(SandboxEgressUpstream(
            proxy_url=group["proxy_url"], destination_hosts=tuple(group["destination_hosts"]),
        ))
    return normalize_upstreams(tuple(routes))


def render_squid_config(base_config: str, upstreams: tuple[SandboxEgressUpstream, ...]) -> str:
    routes = normalize_upstreams(upstreams)
    if not routes:
        return base_config
    if base_config.count("http_access allow all\n") != 1:
        raise ValueError("Squid base configuration must have exactly one final http_access allow all rule")
    # A parent may resolve a name Squid could not resolve locally. Do not let
    # that skip the private-address checks already performed by http_access.
    selected_hosts = " ".join(sorted(host for route in routes for host in route.destination_hosts))
    acls = [f"acl upstream_destination dstdomain -n {selected_hosts}"]
    peers: list[str] = []
    for index, route in enumerate(routes):
        parsed = urlsplit(route.proxy_url)
        name = f"selected_upstream_{index}"
        acl = f"upstream_group_{index}"
        acls.append(f"acl {acl} dstdomain -n {' '.join(route.destination_hosts)}")
        peers.extend([
            f"cache_peer {parsed.hostname} parent {parsed.port} 0 "
            f"no-query no-digest name={name} connect-timeout=5",
            f"cache_peer_access {name} allow {acl}",
            f"cache_peer_access {name} deny all",
        ])
    config = base_config.replace(
        "http_access allow all\n",
        "\n".join(acls) + "\n"
        "acl resolved_destination dst 0.0.0.0/0 ::/0\n"
        "http_access deny upstream_destination !resolved_destination\n"
        "http_access allow all\n",
    )
    return config + "\n" + "\n".join(
        (
            "# Selected destinations use their assigned parent exclusively; all others stay direct.",
            *peers,
            "always_direct deny upstream_destination",
            "always_direct allow all",
            "never_direct allow upstream_destination",
            "never_direct deny all",
            "",
        )
    )


def main() -> None:
    upstreams = upstreams_from_environment(os.environ)
    base = Path("/etc/squid/squid.conf").read_text()
    output = Path("/run/soveren-squid.conf")
    output.write_text(render_squid_config(base, upstreams))
    # Retain the pinned image's initialization and Docker log forwarding.
    os.execvp("entrypoint.sh", ["entrypoint.sh", "-f", str(output), "-NYC"])


if __name__ == "__main__":
    main()
