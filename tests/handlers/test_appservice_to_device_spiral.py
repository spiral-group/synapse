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
#
"""Spiral patch 0004: an appservice gets every to-device message in the range
it acknowledges, whichever send's notification covers it."""

from unittest.mock import AsyncMock, Mock

from twisted.internet.testing import MemoryReactor

import synapse.rest.admin
from synapse.appservice import ApplicationService
from synapse.rest.client import login, sendtodevice
from synapse.server import HomeServer
from synapse.types import JsonDict, StreamKeyType, UserID
from synapse.util.clock import Clock
from synapse.util.json import json_encoder
from synapse.util.stringutils import random_string

from tests import unittest


class AppserviceToDeviceOutOfOrderTestCase(unittest.HomeserverTestCase):
    servlets = [
        synapse.rest.admin.register_servlets_for_client_rest_resource,
        login.register_servlets,
        sendtodevice.register_servlets,
    ]

    def default_config(self) -> JsonDict:
        config = super().default_config()
        config["experimental_features"] = {"msc2409_to_device_messages_enabled": True}
        return config

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.send_mock = AsyncMock()
        hs.get_application_service_handler().scheduler.txn_ctrl.send = self.send_mock  # type: ignore[method-assign]

        self.store = hs.get_datastores().main
        self.appservice = ApplicationService(
            token=random_string(10),
            id=random_string(10),
            sender=UserID.from_string("@as:example.com"),
            rate_limited=False,
            namespaces={
                ApplicationService.NS_USERS: [
                    {"regex": "@agent:.+", "exclusive": True}
                ],
            },
            supports_ephemeral=True,
        )

        # The owner (a plain user), the peer (another plain user) and the
        # agent (in the appservice's namespace).
        self.owner = self.register_user("owner", "password")
        self.owner_token = self.login("owner", "password", "OWNER_DEVICE")
        self.peer = self.register_user("peer", "password")
        self.login("peer", "password", "PEER_DEVICE")
        self.agent = self.register_user("agent", "password")
        self.login("agent", "password", "AGENT_DEVICE")

        # Registered after the users: the namespace is exclusive.
        self.store.get_app_services = Mock(return_value=[self.appservice])  # type: ignore[method-assign]

    def _to_device_messages_sent(self) -> list[JsonDict]:
        return [
            message
            for call in self.send_mock.call_args_list
            for message in call[0][3]
        ]

    def _send(self, txn_id: str, to_user: str, to_device: str) -> None:
        channel = self.make_request(
            "PUT",
            f"/_matrix/client/v3/sendToDevice/io.element.call.encryption_keys/{txn_id}",
            content={"messages": {to_user: {to_device: {"keys": txn_id}}}},
            access_token=self.owner_token,
        )
        self.assertEqual(channel.code, 200, channel.result)

    def test_message_committed_ahead_of_a_lower_stream_id_reaches_the_appservice(
        self,
    ) -> None:
        """The walk's F1 (calls-in-app, 2026-10-15): the owner's app sends its
        key to the peer and to the agent in two concurrent requests. The
        peer's send takes the lower stream id but commits last.

        The agent's send therefore notifies with the persisted-up-to token,
        which is still below its own stream id: the appservice reads the
        agent's messages up to that token and finds none. The peer's send
        then notifies with a token past both, for the peer only — no user
        of this appservice — and the appservice's position moves past the
        agent's key, which is never sent.
        """
        id_gen = self.store.get_to_device_id_generator()

        # The peer's send: a stream id taken, the row not yet written.
        peer_send = id_gen.get_next()
        peer_stream_id = self.get_success(peer_send.__aenter__())

        # The agent's send commits first.
        self._send("agent-key", self.agent, "AGENT_DEVICE")

        # Now the peer's send finishes, exactly as
        # `DeviceMessageHandler.send_device_message` would.
        self.get_success(
            self.store.db_pool.simple_insert(
                table="device_inbox",
                values={
                    "user_id": self.peer,
                    "device_id": "PEER_DEVICE",
                    "stream_id": peer_stream_id,
                    "message_json": json_encoder.encode(
                        {
                            "type": "io.element.call.encryption_keys",
                            "sender": self.owner,
                            "content": {"keys": "peer-key"},
                        }
                    ),
                    "instance_name": self.hs.get_instance_name(),
                },
                desc="spiral_test_peer_send",
            )
        )
        self.store._device_inbox_stream_cache.entity_has_changed(
            self.peer, peer_stream_id
        )
        self.get_success(peer_send.__aexit__(None, None, None))
        self.hs.get_notifier().on_new_event(
            StreamKeyType.TO_DEVICE, id_gen.get_current_token(), users=[self.peer]
        )
        self.pump()

        sent = self._to_device_messages_sent()
        self.assertEqual(
            [(m["to_user_id"], m["to_device_id"], m["content"]) for m in sent],
            [(self.agent, "AGENT_DEVICE", {"keys": "agent-key"})],
        )

    def test_messages_in_order_are_each_sent_once(self) -> None:
        """Two sends to the agent in order: both arrive, neither twice."""
        self._send("first", self.agent, "AGENT_DEVICE")
        self._send("to-peer", self.peer, "PEER_DEVICE")
        self._send("second", self.agent, "AGENT_DEVICE")
        self.pump()

        sent = self._to_device_messages_sent()
        self.assertEqual(
            [m["content"]["keys"] for m in sent],
            ["first", "second"],
        )

    def test_a_stale_notification_does_not_move_the_position_back(self) -> None:
        """A notification whose token is behind the appservice's position
        neither re-sends what was sent nor lowers the position."""
        self._send("first", self.agent, "AGENT_DEVICE")
        self.pump()
        position = self.get_success(
            self.store.get_type_stream_id_for_appservice(self.appservice, "to_device")
        )
        self.assertGreater(position, 0)

        self.hs.get_notifier().on_new_event(
            StreamKeyType.TO_DEVICE, position - 1, users=[self.agent]
        )
        self.pump()

        self.assertEqual(
            [m["content"]["keys"] for m in self._to_device_messages_sent()],
            ["first"],
        )
        self.assertEqual(
            self.get_success(
                self.store.get_type_stream_id_for_appservice(
                    self.appservice, "to_device"
                )
            ),
            position,
        )
