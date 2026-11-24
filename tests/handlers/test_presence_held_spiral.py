#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2026 the Spiral authors.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""Spiral: an application service holds its users' presence.

A user of a service with `com.spiral.hold_presence: true` has no client that
syncs. What the service sets stays, with `currently_active`, until it sets
something else; nobody else's presence changes.
"""

from twisted.internet.testing import MemoryReactor

import synapse.rest.admin
from synapse.api.constants import PresenceState
from synapse.api.presence import UserPresenceState
from synapse.appservice import ApplicationService
from synapse.config.appservice import _load_appservice
from synapse.handlers.presence import PresenceHandler
from synapse.rest.client import login, presence, register, room
from synapse.server import HomeServer
from synapse.storage.database import LoggingDatabaseConnection
from synapse.types import JsonDict, UserID
from synapse.util.clock import Clock

from tests import unittest

DAY_S = 24 * 60 * 60
AS_TOKEN = "held_as_token"
OTHER_AS_TOKEN = "other_as_token"


def _service(token: str, name: str, regex: str, *, held: bool) -> ApplicationService:
    return ApplicationService(
        token=token,
        id=name,
        sender=UserID.from_string(f"@{name}-bot:test"),
        namespaces={
            ApplicationService.NS_USERS: [{"regex": regex, "exclusive": True}],
        },
        rate_limited=False,
        spiral_hold_presence=held,
    )


class SpiralHeldPresenceRegistrationTestCase(unittest.TestCase):
    def _load(self) -> ApplicationService:
        as_info = {
            "id": "svc",
            "url": "http://localhost",
            "as_token": "a",
            "hs_token": "h",
            "sender_localpart": "bot",
            "namespaces": {
                "users": [{"exclusive": True, "regex": "@agent-.*:test"}],
            },
        }
        return _load_appservice("test", as_info, "svc.yaml")

    def test_off_unless_the_registration_says_so(self) -> None:
        self.assertFalse(self._load().spiral_hold_presence)

    def test_on_when_the_registration_says_so(self) -> None:
        service = _load_appservice(
            "test",
            {
                "id": "svc",
                "url": "http://localhost",
                "as_token": "a",
                "hs_token": "h",
                "sender_localpart": "bot",
                "namespaces": {
                    "users": [{"exclusive": True, "regex": "@agent-.*:test"}],
                },
                "com.spiral.hold_presence": True,
            },
            "svc.yaml",
        )
        self.assertTrue(service.spiral_hold_presence)

    def test_a_value_that_is_not_a_bool_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            _load_appservice(
                "test",
                {
                    "id": "svc",
                    "url": "http://localhost",
                    "as_token": "a",
                    "hs_token": "h",
                    "sender_localpart": "bot",
                    "namespaces": {
                        "users": [{"exclusive": True, "regex": "@agent-.*:test"}],
                    },
                    "com.spiral.hold_presence": "yes",
                },
                "svc.yaml",
            )


class SpiralHeldPresenceTestCase(unittest.HomeserverTestCase):
    servlets = [
        synapse.rest.admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
        presence.register_servlets,
        register.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.held = _service(AS_TOKEN, "held", "@agent-.*:test", held=True)
        self.other = _service(OTHER_AS_TOKEN, "other", "@bridge_.*:test", held=False)
        self.store.services_cache = [self.held, self.other]
        self.alice = self.register_user("alice", "pass")
        self.alice_tok = self.login("alice", "pass")
        self.room_id = self.helper.create_room_as(self.alice, tok=self.alice_tok)
        self.agent = self._register_in_room("agent-1", self.held, AS_TOKEN)
        self.puppet = self._register_in_room("bridge_1", self.other, OTHER_AS_TOKEN)

    def _register_in_room(
        self, localpart: str, service: ApplicationService, token: str
    ) -> str:
        user_id = self.register_appservice_user(localpart, token)[0]
        self.helper.invite(self.room_id, self.alice, user_id, tok=self.alice_tok)
        channel = self.make_request(
            "POST",
            f"/_matrix/client/v3/join/{self.room_id}?user_id={user_id}",
            {},
            access_token=token,
        )
        self.assertEqual(channel.code, 200, channel.json_body)
        return user_id

    def _put(self, user_id: str, token: str, state: str) -> None:
        channel = self.make_request(
            "PUT",
            f"/_matrix/client/v3/presence/{user_id}/status?user_id={user_id}",
            {"presence": state},
            access_token=token,
        )
        self.assertEqual(channel.code, 200, channel.json_body)

    def _seen_by_alice(self, user_id: str) -> JsonDict:
        channel = self.make_request(
            "GET",
            f"/_matrix/client/v3/presence/{user_id}/status",
            access_token=self.alice_tok,
        )
        self.assertEqual(channel.code, 200, channel.json_body)
        return channel.json_body

    def _advance(self, seconds: int) -> None:
        # The timers run on a 5 s loop.
        self.reactor.pump([5] * (seconds // 5))

    def test_a_held_user_stays_online_and_active(self) -> None:
        self._put(self.agent, AS_TOKEN, PresenceState.ONLINE)

        # Past the sync timeout, the idle timer and the active window.
        self._advance(600)
        seen = self._seen_by_alice(self.agent)
        self.assertEqual(seen["presence"], PresenceState.ONLINE)
        self.assertIs(seen["currently_active"], True)

    def test_a_day_later_still_online(self) -> None:
        self._put(self.agent, AS_TOKEN, PresenceState.ONLINE)

        self.reactor.advance(DAY_S)
        self._advance(60)
        seen = self._seen_by_alice(self.agent)
        self.assertEqual(seen["presence"], PresenceState.ONLINE)
        self.assertIs(seen["currently_active"], True)

    def test_offline_is_at_once_and_stays(self) -> None:
        self._put(self.agent, AS_TOKEN, PresenceState.ONLINE)
        self._advance(120)

        self._put(self.agent, AS_TOKEN, PresenceState.OFFLINE)
        self.assertEqual(
            self._seen_by_alice(self.agent)["presence"], PresenceState.OFFLINE
        )

        self._advance(120)
        self.assertEqual(
            self._seen_by_alice(self.agent)["presence"], PresenceState.OFFLINE
        )

    def test_online_again_after_offline(self) -> None:
        self._put(self.agent, AS_TOKEN, PresenceState.OFFLINE)
        self._put(self.agent, AS_TOKEN, PresenceState.ONLINE)
        self._advance(120)
        seen = self._seen_by_alice(self.agent)
        self.assertEqual(seen["presence"], PresenceState.ONLINE)
        self.assertIs(seen["currently_active"], True)

    def test_activity_does_not_bring_an_offline_user_back(self) -> None:
        # A paused agent's device can still act: a to-device reply, a receipt.
        self._put(self.agent, AS_TOKEN, PresenceState.OFFLINE)
        handler = self.hs.get_presence_handler()
        self.get_success(
            handler.bump_presence_active_time(UserID.from_string(self.agent), "DEV")
        )
        self._advance(60)
        self.assertEqual(
            self._seen_by_alice(self.agent)["presence"], PresenceState.OFFLINE
        )

    def test_a_device_acting_does_not_take_a_held_user_offline(self) -> None:
        self._put(self.agent, AS_TOKEN, PresenceState.ONLINE)
        handler = self.hs.get_presence_handler()
        self.get_success(
            handler.bump_presence_active_time(UserID.from_string(self.agent), "DEV")
        )
        self._advance(600)
        self.assertEqual(
            self._seen_by_alice(self.agent)["presence"], PresenceState.ONLINE
        )

    def test_a_service_that_did_not_opt_in_times_out(self) -> None:
        self._put(self.puppet, OTHER_AS_TOKEN, PresenceState.ONLINE)
        self._advance(120)
        self.assertEqual(
            self._seen_by_alice(self.puppet)["presence"], PresenceState.OFFLINE
        )

    def test_a_person_times_out(self) -> None:
        self.get_success(
            self.hs.get_presence_handler().set_state(
                UserID.from_string(self.alice), None, {"presence": PresenceState.ONLINE}
            )
        )
        self._advance(120)
        state = self.get_success(
            self.hs.get_presence_handler().get_state(UserID.from_string(self.alice))
        )
        self.assertEqual(state.state, PresenceState.OFFLINE)


class SpiralHeldPresenceRestartTestCase(unittest.HomeserverTestCase):
    """The state comes back from the database and is still held."""

    def default_config(self) -> JsonDict:
        config = super().default_config()
        # As in PresenceHandlerInitTestCase: the handler is loaded by the test.
        config["run_background_tasks_on"] = "other"
        return config

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.user_id = "@agent-1:test"
        self.person = "@alice:test"
        self.reactor.advance(1000)
        now = self.clock.time_msec()

        main_store = hs.get_datastores().main
        main_store.services_cache = [
            _service(AS_TOKEN, "held", "@agent-.*:test", held=True)
        ]
        self.get_success(
            main_store.update_presence(
                [
                    UserPresenceState(
                        user_id=user_id,
                        state=PresenceState.ONLINE,
                        last_active_ts=now,
                        last_federation_update_ts=now,
                        last_user_sync_ts=now,
                        status_msg=None,
                        currently_active=True,
                    )
                    for user_id in (self.user_id, self.person)
                ]
            )
        )

        def refill_presence(db_conn: LoggingDatabaseConnection) -> None:
            main_store._presence_on_startup = main_store._get_active_presence(db_conn)

        self.get_success(main_store.db_pool.runWithConnection(refill_presence))

    def test_restored_presence_is_held(self) -> None:
        handler = self.hs.get_presence_handler()
        assert isinstance(handler, PresenceHandler)

        self.reactor.pump([5] * 120)

        held = self.get_success(handler.get_state(UserID.from_string(self.user_id)))
        self.assertEqual(held.state, PresenceState.ONLINE)
        self.assertIs(held.currently_active, True)
        person = self.get_success(handler.get_state(UserID.from_string(self.person)))
        self.assertEqual(person.state, PresenceState.OFFLINE)
