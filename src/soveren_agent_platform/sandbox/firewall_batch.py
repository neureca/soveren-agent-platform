"""One bounded helper for exact platform network rules, without flushing host chains.

Executed with stdlib Python in the existing privileged egress helper image.
Only trusted DockerSandboxManager supplies these rules; this is not a tenant API.
"""

from __future__ import annotations

import json
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Sequence

MAX_INPUT_BYTES = 64 * 1024
MAX_CHAIN_BYTES = 1024 * 1024


def _canonical(rule: Sequence[str]) -> list[str]:
    result = list(rule)
    # iptables -S renders this implicit protocol module explicitly.
    while ["-m", "tcp"] in [result[i : i + 2] for i in range(len(result))]:
        index = next(i for i in range(len(result)) if result[i : i + 2] == ["-m", "tcp"])
        del result[index : index + 2]
    if "--ctstate" in result:
        index = result.index("--ctstate") + 1
        result[index] = ",".join(sorted(result[index].split(",")))
    return result


def plan(
    rules: list[tuple[list[str], bool]], removals: list[list[str]], snapshots: dict[str, str]
) -> tuple[bytes, list[bool]]:
    """Plan exact deletes/inserts; unrelated rules retain their relative order."""
    initial: dict[str, list[list[str]]] = {}
    for chain, snapshot in snapshots.items():
        if chain not in {"INPUT", "DOCKER-USER"} or not snapshot.strip():
            raise ValueError("required host firewall chain is unavailable")
        initial[chain] = []
        for line in snapshot.splitlines():
            tokens = shlex.split(line)
            if tokens[:2] == ["-A", chain]:
                initial[chain].append(_canonical(tokens[2:]))
            elif tokens[:2] not in (["-P", chain], ["-N", chain]):
                raise ValueError("unexpected host firewall chain response")
    current = {chain: [list(rule) for rule in entries] for chain, entries in initial.items()}
    commands = []
    created = []
    for rule, first in rules:
        chain, *body = _canonical(rule)
        present = body in current[chain]
        created.append(not present)
        if present and not first:
            continue
        while body in current[chain]:
            current[chain].remove(body)
            commands.append("-D " + shlex.join([chain, *body]))
        current[chain].insert(0, body)
        commands.append("-I " + shlex.join([chain, "1", *body]))
    for rule in removals:
        chain, *body = _canonical(rule)
        while body in current[chain]:
            current[chain].remove(body)
            commands.append("-D " + shlex.join([chain, *body]))
    if current == initial:
        return b"", created
    return ("*filter\n" + "\n".join(commands) + "\nCOMMIT\n").encode(), created


def _run(command: list[str], payload: bytes | None = None) -> bytes:
    with tempfile.TemporaryFile() as output:
        subprocess.run(command, input=payload, stdout=output, stderr=subprocess.DEVNULL, check=True, timeout=5)
        if output.tell() > MAX_CHAIN_BYTES:
            raise ValueError("host firewall response exceeds observation limit")
        output.seek(0)
        return output.read(MAX_CHAIN_BYTES)


def main() -> None:
    payload = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(payload) > MAX_INPUT_BYTES:
        raise ValueError("firewall batch exceeds limit")
    value = json.loads(payload)
    rules = value["rules"]
    removals = value["removals"]
    if not 1 <= len(rules) <= 16 or len(removals) > 16:
        raise ValueError("invalid firewall rule count")
    for rule in [entry[0] for entry in rules] + removals:
        if not rule or rule[0] not in {"DOCKER-USER", "INPUT"}:
            raise ValueError("invalid firewall chain")
        if any(not isinstance(token, str) or not token or any(c.isspace() for c in token) for token in rule):
            raise ValueError("invalid firewall token")
    chains = sorted({rule[0] for rule, _ in rules} | {rule[0] for rule in removals})

    def snapshots() -> dict[str, str]:
        return {chain: _run(["iptables", "--wait", "5", "-S", chain]).decode() for chain in chains}

    update, created = plan(rules, removals, snapshots())
    if update:
        _run(["iptables-restore", "--wait", "5", "--noflush"], update)
    remaining, _ = plan(rules, removals, snapshots())
    if remaining:
        raise RuntimeError("host firewall verification failed")
    print(json.dumps(created))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        raise SystemExit("host firewall reconciliation failed") from None
