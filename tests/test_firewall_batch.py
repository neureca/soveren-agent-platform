"""Batch rule semantics and the actual Docker helper fencing entrypoint."""

import asyncio
import json
import shlex

import pytest

from soveren_agent_platform.sandbox import CommandResult, DockerEgressSpec, DockerSandboxManager
from soveren_agent_platform.sandbox.docker import _DockerNetworkPolicy
from soveren_agent_platform.sandbox.firewall_batch import _canonical, plan


def policy():
    return _DockerNetworkPolicy("private-a", "172.30.0.0/24", "172.30.0.2/32")


def entries(value):
    return {
        chain: "-N " + chain + "\n" + "\n".join("-A " + chain + " " + shlex.join(rule) for rule in rules)
        for chain, rules in value.items()
    }


def apply(value, payload):
    result = {chain: [_canonical(rule) for rule in rules] for chain, rules in value.items()}
    for line in payload.decode().splitlines()[1:-1]:
        tokens = shlex.split(line)
        operation, chain, *rule = tokens
        if operation == "-D":
            result[chain].remove(rule)
        elif operation == "-I":
            assert rule[0] == "1"
            result[chain].insert(0, rule[1:])
        else:
            raise AssertionError(tokens)
    return result


def test_batch_preserves_foreign_rules_orders_guards_and_converges_after_drift():
    p = policy()
    rules = [
        (p.drop_rule(), False),
        (p.host_input_drop_rule(), False),
        (p.allow_rule(), True),
        (p.proxy_response_rule(), True),
    ]
    foreign = ["-s", "192.0.2.1/32", "-j", "ACCEPT"]
    before = {
        "DOCKER-USER": [foreign, p.legacy_proxy_egress_rule()[1:]],
        "INPUT": [["-p", "tcp", "--dport", "22", "-j", "ACCEPT"]],
    }
    payload, created = plan(rules, [p.legacy_proxy_egress_rule()], entries(before))
    assert created == [True] * 4
    assert payload.count(b"COMMIT") == 1 and b"-F" not in payload
    after = apply(before, payload)
    assert after["DOCKER-USER"] == [
        _canonical(p.proxy_response_rule()[1:]),
        p.allow_rule()[1:],
        p.drop_rule()[1:],
        foreign,
    ]
    assert after["INPUT"][-1] == before["INPUT"][0]
    assert plan(rules, [p.legacy_proxy_egress_rule()], entries(after)) == (b"", [False] * 4)
    # iptables renders implicit -m tcp and normalizes conntrack state order.
    after["DOCKER-USER"][0] = p.proxy_response_rule()[1:]
    after["DOCKER-USER"][0][2:2] = ["-m", "tcp"]
    after["DOCKER-USER"][0][-3] = "RELATED,ESTABLISHED"
    assert plan(rules, [], entries(after))[0] == b""
    after["DOCKER-USER"].remove(p.allow_rule()[1:])
    repaired, created = plan(rules, [], entries(after))
    assert created == [False, False, True, False]
    assert apply(after, repaired)["DOCKER-USER"][1] == p.allow_rule()[1:]


class HelperEdge:
    def __init__(self):
        self.calls = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.mode = "normal"
        self.helper = False
        self.foreign = False
        self.fail_cleanup = False

    async def run(self, args, *, input_data=None):
        self.calls.append((args, input_data))
        if args[1] == "inspect":
            if not self.helper:
                return CommandResult(1, stderr="No such object")
            return CommandResult(
                0,
                stdout=json.dumps(
                    {"soveren.managed": "true", "soveren.network_firewall_helper": "foreign" if self.foreign else "1"}
                ),
            )
        if args[1] == "rm":
            if self.fail_cleanup:
                return CommandResult(1, stderr="fixture lost cleanup response")
            self.helper = False
            return CommandResult(0)
        assert args[1] == "run" and "soveren-network-firewall-apply" in args
        self.helper = True
        self.entered.set()
        if self.mode == "lost":
            return CommandResult(1, stderr="fixture lost apply response")
        if self.mode == "hold":
            await self.release.wait()
        self.helper = False
        return CommandResult(0, stdout=json.dumps([False] * len(json.loads(input_data)["rules"])))


@pytest.mark.parametrize("failure", ["cancel", "timeout", "lost", "foreign", "cleanup"])
def test_network_firewall_named_writer_drains_or_fences_before_next_writer(failure, monkeypatch):
    from soveren_agent_platform.sandbox import docker

    async def run():
        edge = HelperEdge()
        manager = DockerSandboxManager(runner=edge, egress=DockerEgressSpec(image="egress:test"))
        if failure == "foreign":
            edge.helper = edge.foreign = True
        elif failure in {"lost", "cleanup"}:
            edge.mode = "lost"
            edge.fail_cleanup = failure == "cleanup"
        else:
            edge.mode = "hold"
        if failure == "timeout":
            monkeypatch.setattr(docker, "HOST_RELAY_UPDATE_TIMEOUT_S", 0.02)
        task = asyncio.create_task(manager._ensure_network_policy(policy=policy()))
        if failure == "cancel":
            await edge.entered.wait()
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and manager._firewall_lock.locked()
            second = asyncio.create_task(manager._ensure_network_policy(policy=policy()))
            await asyncio.sleep(0)
            assert len([call for call, _ in edge.calls if call[1] == "run"]) == 1
            edge.mode = "normal"
            edge.release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            await second
        else:
            with pytest.raises((RuntimeError, TimeoutError, BaseExceptionGroup)):
                await task
        assert not manager._firewall_lock.locked()
        if failure == "foreign":
            assert not any(call[1] in {"run", "rm"} for call, _ in edge.calls)
        elif failure != "cleanup":
            assert not edge.helper
        else:
            assert edge.helper  # unknown removal is a surfaced failure
            edge.fail_cleanup = False
            edge.mode = "normal"
            await manager._ensure_network_policy(policy=policy())
            assert not edge.helper  # next writer fences the same name

    asyncio.run(run())
