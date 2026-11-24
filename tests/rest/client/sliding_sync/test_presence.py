#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
#
import logging

from parameterized import parameterized_class

from twisted.internet.testing import MemoryReactor

import synapse.rest.admin
from synapse.api.constants import PresenceState
from synapse.rest.client import login, room, sync
from synapse.server import HomeServer
from synapse.types import UserID
from synapse.util.clock import Clock

from tests.rest.client.sliding_sync.test_sliding_sync import SlidingSyncBase
from tests.unittest import override_config

logger = logging.getLogger(__name__)


# FIXME: This can be removed once we bump `SCHEMA_COMPAT_VERSION` and run the
# foreground update for
# `sliding_sync_joined_rooms`/`sliding_sync_membership_snapshots` (tracked by
# https://github.com/element-hq/synapse/issues/17623)
@parameterized_class(
    ("use_new_tables",),
    [
        (True,),
        (False,),
    ],
    class_name_func=lambda cls,
    num,
    params_dict: f"{cls.__name__}_{'new' if params_dict['use_new_tables'] else 'fallback'}",
)
class SlidingSyncPresenceTestCase(SlidingSyncBase):
    """
    The `set_presence` query parameter of Sliding Sync (MSC4186): "Same as in
    `/v3/sync`, controls whether the client is automatically marked as online
    by polling this API."
    """

    servlets = [
        synapse.rest.admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
        sync.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.presence_handler = hs.get_presence_handler()
        super().prepare(reactor, clock, hs)

    def _presence(self, user_id: str) -> str:
        return self.get_success(
            self.presence_handler.get_state(UserID.from_string(user_id))
        ).state

    def _sync(self, tok: str, set_presence: str | None = None) -> None:
        sync_path = self.sync_endpoint
        if set_presence is not None:
            sync_path += f"?set_presence={set_presence}"
        channel = self.make_request(
            method="POST",
            path=sync_path,
            content={"lists": {}},
            access_token=tok,
        )
        self.assertEqual(channel.code, 200, channel.json_body)

    def test_sync_marks_user_online_by_default(self) -> None:
        """A sliding sync with no `set_presence` marks the client online."""
        user_id = self.register_user("user1", "pass")
        tok = self.login(user_id, "pass")
        self.assertEqual(self._presence(user_id), PresenceState.OFFLINE)

        self._sync(tok)

        self.assertEqual(self._presence(user_id), PresenceState.ONLINE)

    def test_sync_set_presence_offline_does_not_mark_online(self) -> None:
        """`set_presence=offline` is "do not touch presence", as in `/v3/sync`."""
        user_id = self.register_user("user1", "pass")
        tok = self.login(user_id, "pass")

        self._sync(tok, set_presence="offline")

        self.assertEqual(self._presence(user_id), PresenceState.OFFLINE)

    def test_sync_set_presence_unavailable_marks_idle(self) -> None:
        user_id = self.register_user("user1", "pass")
        tok = self.login(user_id, "pass")

        self._sync(tok, set_presence="unavailable")

        self.assertEqual(self._presence(user_id), PresenceState.UNAVAILABLE)

    def test_sync_set_presence_unknown_value_is_rejected(self) -> None:
        user_id = self.register_user("user1", "pass")
        tok = self.login(user_id, "pass")

        channel = self.make_request(
            method="POST",
            path=self.sync_endpoint + "?set_presence=away",
            content={"lists": {}},
            access_token=tok,
        )
        self.assertEqual(channel.code, 400, channel.json_body)
        self.assertEqual(channel.json_body["errcode"], "M_INVALID_PARAM")

    def test_polling_keeps_user_online_past_the_sync_online_timeout(self) -> None:
        """
        A client that polls stays online; one that stops is marked offline
        after `sync_online_timeout` — the same state machine as `/v3/sync`.
        """
        user_id = self.register_user("user1", "pass")
        tok = self.login(user_id, "pass")

        self._sync(tok)
        self.assertEqual(self._presence(user_id), PresenceState.ONLINE)

        # Poll every 20 s for two minutes: well past the 30 s timeout.
        for _ in range(6):
            self.reactor.advance(20)
            self._sync(tok)
            self.assertEqual(self._presence(user_id), PresenceState.ONLINE)

        # Stop polling: offline after the timeout.
        self.reactor.advance(60)
        self.assertEqual(self._presence(user_id), PresenceState.OFFLINE)

    @override_config(
        {"rc_presence": {"per_user": {"per_second": 0.1, "burst_count": 1}}}
    )
    def test_set_presence_is_ratelimited_like_sync(self) -> None:
        """Over the limit the presence update is ignored; the sync still succeeds."""
        user_id = self.register_user("user1", "pass")
        tok = self.login(user_id, "pass")

        self._sync(tok, set_presence="online")
        self.assertEqual(self._presence(user_id), PresenceState.ONLINE)

        # Immediately again, now idle: ratelimited, so still online.
        self._sync(tok, set_presence="unavailable")
        self.assertEqual(self._presence(user_id), PresenceState.ONLINE)

        self.reactor.advance(30)
        self._sync(tok, set_presence="unavailable")
        self.assertEqual(self._presence(user_id), PresenceState.UNAVAILABLE)

    @override_config({"presence": {"enabled": False}})
    def test_presence_disabled_is_a_no_op(self) -> None:
        user_id = self.register_user("user1", "pass")
        tok = self.login(user_id, "pass")

        self._sync(tok)

        self.assertEqual(self._presence(user_id), PresenceState.OFFLINE)
