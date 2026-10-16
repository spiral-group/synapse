#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""Spiral (ADR-0084): presence privacy as Telegram does it.

`com.spiral.presence` `{"share": false}` makes the server show others only
"offline" and a coarse `com.spiral.last_seen` bucket, reciprocally, and keeps
a hidden pair's presence off every stream.
"""

from twisted.internet.testing import MemoryReactor

import synapse.rest.admin
from synapse.api.constants import PresenceState
from synapse.handlers.presence import (
    SPIRAL_LAST_SEEN_FIELD,
    SPIRAL_PRESENCE_ACCOUNT_DATA_TYPE,
    spiral_last_seen_bucket,
)
from synapse.rest.client import account_data, login, presence, room, sync
from synapse.server import HomeServer
from synapse.types import JsonDict, UserID
from synapse.util.clock import Clock

from tests import unittest

DAY_MS = 24 * 60 * 60 * 1000


class SpiralLastSeenBucketTestCase(unittest.TestCase):
    def test_buckets_count_whole_days(self) -> None:
        now = 1_800_000_000_000
        today = now - now % DAY_MS
        self.assertEqual(spiral_last_seen_bucket(now - 1000, now), "recently")
        self.assertEqual(spiral_last_seen_bucket(today - 2 * DAY_MS, now), "recently")
        self.assertEqual(spiral_last_seen_bucket(today - 3 * DAY_MS, now), "within_week")
        self.assertEqual(spiral_last_seen_bucket(today - 6 * DAY_MS, now), "within_week")
        self.assertEqual(spiral_last_seen_bucket(today - 7 * DAY_MS, now), "within_month")
        self.assertEqual(spiral_last_seen_bucket(today - 29 * DAY_MS, now), "within_month")
        self.assertEqual(spiral_last_seen_bucket(today - 30 * DAY_MS, now), "long_ago")
        self.assertEqual(spiral_last_seen_bucket(0, now), "long_ago")

    def test_the_hour_does_not_move_the_boundary(self) -> None:
        # Active at 00:01 or 23:59 of the same day: the same bucket at any
        # moment, so the switch does not date the activity within the day.
        day = 1_800_000_000_000 // DAY_MS * DAY_MS
        early, late = day + 60_000, day + DAY_MS - 60_000
        for now in (day + 3 * DAY_MS - 1, day + 3 * DAY_MS, day + 3 * DAY_MS + 1):
            self.assertEqual(
                spiral_last_seen_bucket(early, now), spiral_last_seen_bucket(late, now)
            )


class SpiralPresencePrivacyTestCase(unittest.HomeserverTestCase):
    servlets = [
        synapse.rest.admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
        presence.register_servlets,
        account_data.register_servlets,
        sync.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.presence_handler = hs.get_presence_handler()
        self.alice = self.register_user("alice", "pass")
        self.alice_tok = self.login("alice", "pass")
        self.bob = self.register_user("bob", "pass")
        self.bob_tok = self.login("bob", "pass")
        room_id = self.helper.create_room_as(self.alice, tok=self.alice_tok)
        self.helper.join(room_id, self.bob, tok=self.bob_tok)

    def _set_online(self, user_id: str) -> None:
        self.get_success(
            self.presence_handler.set_state(
                UserID.from_string(user_id), None, {"presence": PresenceState.ONLINE}
            )
        )

    def _share(self, user_id: str, tok: str, share: bool) -> None:
        channel = self.make_request(
            "PUT",
            f"/_matrix/client/v3/user/{user_id}/account_data/{SPIRAL_PRESENCE_ACCOUNT_DATA_TYPE}",
            {"share": share},
            access_token=tok,
        )
        self.assertEqual(channel.code, 200, channel.json_body)

    def _get(self, observed: str, tok: str) -> JsonDict:
        channel = self.make_request(
            "GET", f"/_matrix/client/v3/presence/{observed}/status", access_token=tok
        )
        self.assertEqual(channel.code, 200, channel.json_body)
        return channel.json_body

    def _synced_presence_senders(self, tok: str) -> set[str]:
        channel = self.make_request("GET", "/_matrix/client/v3/sync", access_token=tok)
        self.assertEqual(channel.code, 200, channel.json_body)
        return {
            event["sender"]
            for event in channel.json_body.get("presence", {}).get("events", [])
        }

    def test_a_sharing_user_is_seen_exactly(self) -> None:
        self._set_online(self.alice)

        body = self._get(self.alice, self.bob_tok)

        self.assertEqual(body["presence"], PresenceState.ONLINE)
        self.assertIn("last_active_ago", body)
        self.assertTrue(body["currently_active"])
        self.assertNotIn(SPIRAL_LAST_SEEN_FIELD, body)

    def test_a_hiding_user_is_seen_only_coarsely(self) -> None:
        self._set_online(self.alice)
        self._share(self.alice, self.alice_tok, False)

        body = self._get(self.alice, self.bob_tok)

        self.assertEqual(
            body, {"presence": PresenceState.OFFLINE, SPIRAL_LAST_SEEN_FIELD: "recently"}
        )

    def test_a_hiding_user_sees_others_only_coarsely(self) -> None:
        self._set_online(self.alice)
        self._share(self.bob, self.bob_tok, False)

        body = self._get(self.alice, self.bob_tok)

        self.assertEqual(
            body, {"presence": PresenceState.OFFLINE, SPIRAL_LAST_SEEN_FIELD: "recently"}
        )

    def test_everyone_sees_themselves_exactly(self) -> None:
        self._set_online(self.alice)
        self._share(self.alice, self.alice_tok, False)

        body = self._get(self.alice, self.alice_tok)

        self.assertEqual(body["presence"], PresenceState.ONLINE)
        self.assertNotIn(SPIRAL_LAST_SEEN_FIELD, body)

    def test_sharing_again_restores_the_exact_view(self) -> None:
        self._set_online(self.alice)
        self._share(self.alice, self.alice_tok, False)
        self._share(self.alice, self.alice_tok, True)

        body = self._get(self.alice, self.bob_tok)

        self.assertEqual(body["presence"], PresenceState.ONLINE)
        self.assertNotIn(SPIRAL_LAST_SEEN_FIELD, body)

    def test_the_bucket_follows_the_last_activity(self) -> None:
        self._set_online(self.alice)
        self._share(self.alice, self.alice_tok, False)

        self.reactor.advance(10 * DAY_MS / 1000)

        body = self._get(self.alice, self.bob_tok)
        self.assertEqual(body[SPIRAL_LAST_SEEN_FIELD], "within_month")
        self.assertNotIn("last_active_ago", body)

    def test_a_hiding_user_is_absent_from_others_sync(self) -> None:
        self._set_online(self.alice)
        self.assertIn(self.alice, self._synced_presence_senders(self.bob_tok))

        self._share(self.alice, self.alice_tok, False)

        self.assertNotIn(self.alice, self._synced_presence_senders(self.bob_tok))

    def test_a_hiding_observer_gets_no_one_elses_presence_in_sync(self) -> None:
        self._set_online(self.alice)
        self._share(self.bob, self.bob_tok, False)

        self.assertNotIn(self.alice, self._synced_presence_senders(self.bob_tok))
