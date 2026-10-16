#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2026 the Spiral authors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.

"""Spiral patch 0003: `/versions` advertises v1.13 (decision 167)."""

from twisted.internet.testing import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, reporting, room, versions
from synapse.server import HomeServer
from synapse.synapse_rust.http_client import HttpClient
from synapse.util.clock import Clock

from tests import unittest


class SpiralVersionsTestCase(unittest.HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
        reporting.register_servlets,
        versions.register_servlets,
    ]

    def make_homeserver(self, reactor: MemoryReactor, clock: Clock) -> HomeServer:
        hs = self.setup_test_homeserver()
        # As in test_versions.py: the Rust handler needs the Tokio pool, which
        # starts on `reactor.run()` once a Rust HTTP client exists.
        self._http_client = hs.get_proxied_http_client()
        _ = HttpClient(
            reactor=hs.get_reactor(),
            user_agent=self._http_client.user_agent.decode("utf8"),
        )
        reactor.run()
        return hs

    def tearDown(self) -> None:
        shutdown_triggers = self.reactor.triggers.get("shutdown", {})
        for phase in ["before", "during", "after"]:
            for callable_, args, kwargs in shutdown_triggers.get(phase, []):
                callable_(*args, **kwargs)

    def _versions(self, access_token: str | None = None) -> list[str]:
        channel = self.make_request(
            "GET", "/_matrix/client/versions", access_token=access_token
        )
        self.assertEqual(channel.code, 200, channel.result)
        return channel.json_body["versions"]

    def test_unauthenticated_lists_v1_13_once(self) -> None:
        versions_list = self._versions()
        self.assertEqual(versions_list.count("v1.13"), 1, versions_list)
        # Nothing the Rust handler lists is lost.
        self.assertIn("v1.12", versions_list)
        self.assertIn("r0.6.1", versions_list)

    def test_authenticated_lists_v1_13_once(self) -> None:
        self.register_user("user", "pass")
        tok = self.login("user", "pass")
        versions_list = self._versions(tok)
        self.assertEqual(versions_list.count("v1.13"), 1, versions_list)
        self.assertIn("v1.12", versions_list)

    def test_repeated_requests_do_not_grow_the_list(self) -> None:
        first = self._versions()
        second = self._versions()
        self.assertEqual(first, second)

    def test_report_room_is_served_at_the_v1_13_stable_path(self) -> None:
        # What a v1.13 client switches to; the unstable MSC4151 path is gone.
        user = self.register_user("reporter", "pass")
        tok = self.login("reporter", "pass")
        room_id = self.helper.create_room_as(user, tok=tok)

        channel = self.make_request(
            "POST",
            f"/_matrix/client/v3/rooms/{room_id}/report",
            {"reason": "spam"},
            access_token=tok,
        )
        self.assertEqual(channel.code, 200, channel.result)

        channel = self.make_request(
            "POST",
            f"/_matrix/client/unstable/org.matrix.msc4151/rooms/{room_id}/report",
            {"reason": "spam"},
            access_token=tok,
        )
        self.assertEqual(channel.code, 404, channel.result)
