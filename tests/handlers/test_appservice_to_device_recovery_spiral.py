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
"""Spiral patch 0006: an appservice's to-device messages (and device list
changes) survive the time Synapse treats it as down."""

from typing import Any
from unittest.mock import AsyncMock, Mock

from twisted.internet.testing import MemoryReactor

import synapse.rest.admin
from synapse.appservice import ApplicationService, ApplicationServiceState
from synapse.rest.client import login, room, sendtodevice
from synapse.server import HomeServer
from synapse.types import JsonDict, UserID
from synapse.util.clock import Clock
from synapse.util.json import json_encoder
from synapse.util.stringutils import random_string

from tests import unittest


class AppserviceToDeviceRecoveryTestCase(unittest.HomeserverTestCase):
    servlets = [
        synapse.rest.admin.register_servlets_for_client_rest_resource,
        login.register_servlets,
        room.register_servlets,
        sendtodevice.register_servlets,
    ]

    def default_config(self) -> JsonDict:
        config = super().default_config()
        config["experimental_features"] = {
            "msc2409_to_device_messages_enabled": True,
            "msc3202_transaction_extensions": True,
        }
        return config

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        # Every transaction the appservice is offered, and whether it took it.
        self.as_up = True
        self.pushes: list[tuple[bool, dict[str, Any]]] = []

        async def push_bulk(**kwargs: Any) -> bool:
            self.pushes.append((self.as_up, kwargs))
            return self.as_up

        hs.get_application_service_api().push_bulk = AsyncMock(  # type: ignore[method-assign]
            side_effect=push_bulk
        )

        self.store = hs.get_datastores().main
        self.appservice = ApplicationService(
            token=random_string(10),
            id=random_string(10),
            sender=UserID.from_string("@as:test"),
            rate_limited=False,
            namespaces={
                ApplicationService.NS_USERS: [
                    {"regex": "@agent:.+", "exclusive": True}
                ],
            },
            supports_ephemeral=True,
            msc3202_transaction_extensions=True,
        )

        self.owner = self.register_user("owner", "password")
        self.owner_token = self.login("owner", "password", "OWNER_DEVICE")
        self.peer = self.register_user("peer", "password")
        self.login("peer", "password", "PEER_DEVICE")
        self.agent = self.register_user("agent", "password")
        self.agent_token = self.login("agent", "password", "AGENT_DEVICE")

        # Registered after the users: the namespace is exclusive.
        self.store.get_app_services = Mock(return_value=[self.appservice])  # type: ignore[method-assign]

    # ---- helpers -----------------------------------------------------------

    def _send(self, key: str, to_user: str | None = None, device: str = "") -> None:
        to_user = to_user or self.agent
        device = device or ("AGENT_DEVICE" if to_user == self.agent else "PEER_DEVICE")
        channel = self.make_request(
            "PUT",
            f"/_matrix/client/v3/sendToDevice/m.room.encrypted/{random_string(8)}",
            content={"messages": {to_user: {device: {"k": key}}}},
            access_token=self.owner_token,
        )
        self.assertEqual(channel.code, 200, channel.result)
        self.pump()

    def _delivered(self) -> list[str]:
        """The to-device keys the appservice received, in order."""
        return [
            message["content"]["k"]
            for ok, push in self.pushes
            if ok
            for message in push["to_device_messages"]
        ]

    def _state(self) -> ApplicationServiceState | None:
        return self.get_success(self.store.get_appservice_state(self.appservice))

    def _go_down(self) -> None:
        """The appservice stops answering; the next transaction fails."""
        self.as_up = False

    def _come_back(self) -> None:
        """The appservice answers again, and the recoverer's first retry
        (after its two-second backoff) runs."""
        self.as_up = True
        self.reactor.advance(2)
        self.pump()

    # ---- tests -------------------------------------------------------------

    def test_messages_sent_while_down_arrive_after_recovery_in_order_once(
        self,
    ) -> None:
        """The dev verification of 2026-10-22: a room key sent to an agent in
        the seconds after an orchestrator restart never arrived. Stock Synapse
        sends nothing but room events while the appservice is down and stores
        the position past the messages, and retries the failed transaction
        without its to-device messages."""
        self._go_down()
        self._send("k1")  # the transaction that fails
        self.assertEqual(self._state(), ApplicationServiceState.DOWN)
        self._send("k2")
        self._send("to-peer", to_user=self.peer)
        self._send("k3")
        self.assertEqual(self._delivered(), [])

        self._come_back()

        self.assertEqual(self._state(), ApplicationServiceState.UP)
        self.assertEqual(self._delivered(), ["k1", "k2", "k3"])

    def test_a_message_after_recovery_is_not_sent_again(self) -> None:
        self._go_down()
        self._send("k1")
        self._send("k2")
        self._come_back()
        self._send("k3")
        self._send("k4")

        self.assertEqual(self._delivered(), ["k1", "k2", "k3", "k4"])
        # Nothing waits in the device inbox past the stored position.
        self.assertEqual(
            self.get_success(
                self.store.get_type_stream_id_for_appservice(
                    self.appservice, "to_device"
                )
            ),
            self.store.get_to_device_stream_token(),
        )

    def test_room_keys_arrive_before_the_events_they_decrypt(self) -> None:
        """The recoverer sends the to-device backlog before the stored
        transactions: the orchestrator decrypts on the first try."""
        room_id = self.helper.create_room_as(self.owner, tok=self.owner_token)
        self.helper.invite(room_id, self.owner, self.agent, tok=self.owner_token)
        self.helper.join(room_id, self.agent, tok=self.agent_token)
        self.pump()

        self._go_down()
        self._send("room-key")
        event_id = self.helper.send(room_id, "hello", tok=self.owner_token)["event_id"]
        self.pump()
        self._come_back()

        received = [push for ok, push in self.pushes if ok]
        key_at = next(
            i
            for i, push in enumerate(received)
            if any(m["content"]["k"] == "room-key" for m in push["to_device_messages"])
        )
        event_at = next(
            i
            for i, push in enumerate(received)
            if any(e.event_id == event_id for e in push["events"])
        )
        self.assertLess(key_at, event_at)
        self.assertEqual(self._delivered(), ["room-key"])

    def test_a_long_backlog_goes_in_bounded_transactions(self) -> None:
        self._go_down()
        for n in range(150):
            self._send(f"k{n}")
        self._come_back()

        received = [
            len(push["to_device_messages"])
            for ok, push in self.pushes
            if ok and push["to_device_messages"]
        ]
        self.assertEqual(received, [100, 50])
        self.assertEqual(self._delivered(), [f"k{n}" for n in range(150)])

    def test_device_list_changes_and_key_counts_after_recovery(self) -> None:
        """A device list change the appservice missed comes with the backlog,
        and so do the one-time key counts of the backlog's recipients."""
        self._go_down()
        self._send("k1")
        self.login("agent", "password", "AGENT_SECOND_DEVICE")  # a device list change
        self.pump()
        self._come_back()

        first = next(push for ok, push in self.pushes if ok)
        self.assertEqual(
            [m["content"]["k"] for m in first["to_device_messages"]], ["k1"]
        )
        self.assertIn(self.agent, first["device_list_summary"].changed)
        self.assertIn(self.agent, first["one_time_keys_count"])

    def test_a_healthy_appservice_gets_each_message_once_as_it_is_sent(
        self,
    ) -> None:
        self._send("k1")
        self._send("to-peer", to_user=self.peer)
        self._send("k2")

        received = [
            [m["content"]["k"] for m in push["to_device_messages"]]
            for ok, push in self.pushes
            if push["to_device_messages"]
        ]
        self.assertEqual(received, [["k1"], ["k2"]])
        self.assertTrue(all(ok for ok, _ in self.pushes))
        self.assertNotEqual(self._state(), ApplicationServiceState.DOWN)
        self.assertEqual(
            self.hs.get_application_service_scheduler().txn_ctrl.recoverers, {}
        )

    def test_messages_left_in_the_inbox_are_sent_at_start(self) -> None:
        """The position is where the appservice has received messages up to,
        so what was still in memory when Synapse stopped is sent when it
        starts again."""
        id_gen = self.store.get_to_device_id_generator()

        async def write_without_notifying() -> None:
            async with id_gen.get_next() as stream_id:
                await self.store.db_pool.simple_insert(
                    table="device_inbox",
                    values={
                        "user_id": self.agent,
                        "device_id": "AGENT_DEVICE",
                        "stream_id": stream_id,
                        "message_json": json_encoder.encode(
                            {
                                "type": "m.room.encrypted",
                                "sender": self.owner,
                                "content": {"k": "left-behind"},
                            }
                        ),
                        "instance_name": self.hs.get_instance_name(),
                    },
                    desc="spiral_test_left_behind",
                )
                self.store._device_inbox_stream_cache.entity_has_changed(
                    self.agent, stream_id
                )

        self.get_success(write_without_notifying())
        self.pump()
        self.assertEqual(self._delivered(), [])

        self.get_success(self.hs.get_application_service_scheduler().start())
        self.pump()

        self.assertEqual(self._delivered(), ["left-behind"])
