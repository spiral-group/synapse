#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2015, 2016 OpenMarket Ltd
# Copyright (C) 2023, 2025 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#
"""
This module controls the reliability for application service transactions.

The nominal flow through this module looks like:
              __________
1---ASa[e]-->|  Service |--> Queue ASa[f]
2----ASb[e]->|  Queuer  |
3--ASa[f]--->|__________|-----------+ ASa[e], ASb[e]
                                    V
      -````````-            +------------+
      |````````|<--StoreTxn-|Transaction |
      |Database|            | Controller |---> SEND TO AS
      `--------`            +------------+
What happens on SEND TO AS depends on the state of the Application Service:
 - If the AS is marked as DOWN, do nothing.
 - If the AS is marked as UP, send the transaction.
     * SUCCESS : Increment where the AS is up to txn-wise and nuke the txn
                 contents from the db.
     * FAILURE : Marked AS as DOWN and start Recoverer.

Recoverer attempts to recover ASes who have died. The flow for this looks like:
                ,--------------------- backoff++ --------------.
               V                                               |
  START ---> Wait exp ------> Get oldest txn ID from ----> FAILURE
             backoff           DB and try to send it
                                 ^                |___________
Mark AS as                       |                            V
UP & quit           +---------- YES                       SUCCESS
    |               |                                         |
    NO <--- Have more txns? <------ Mark txn success & nuke <-+
                                      from db; incr AS pos.
                                         Reset backoff.

This is all tied together by the AppServiceScheduler which DIs the required
components.
"""

import logging
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    Collection,
    Iterable,
    Sequence,
)

from synapse.appservice import (
    ApplicationService,
    ApplicationServiceState,
    TransactionOneTimeKeysCount,
    TransactionUnusedFallbackKeys,
)
from synapse.appservice.api import ApplicationServiceApi
from synapse.events import EventBase
from synapse.logging.context import run_in_background
from synapse.storage.databases.main import DataStore
from synapse.types import DeviceListUpdates, JsonMapping
from synapse.util.async_helpers import Linearizer
from synapse.util.clock import Clock, DelayedCallWrapper
from synapse.util.duration import Duration

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)


# Maximum number of events to provide in an AS transaction.
MAX_PERSISTENT_EVENTS_PER_TRANSACTION = 100

# Maximum number of ephemeral events to provide in an AS transaction.
MAX_EPHEMERAL_EVENTS_PER_TRANSACTION = 100

# Maximum number of to-device messages to provide in an AS transaction.
MAX_TO_DEVICE_MESSAGES_PER_TRANSACTION = 100


class ApplicationServiceScheduler:
    """
    Public facing API for this module. Does the required dependency injection (DI) to
    tie the components together. This also serves as the "event_pool", which in this
    case is a simple array.
    """

    def __init__(self, hs: "HomeServer"):
        self.txn_ctrl = _TransactionController(hs)
        self.store = hs.get_datastores().main
        self.queuer = _ServiceQueuer(self.txn_ctrl, hs)

        # Spiral patch 0006: what an appservice must not lose while it is down.
        self.backlog = _AppserviceBacklog(hs, self.queuer)
        self.queuer.backlog = self.backlog
        self.txn_ctrl.backlog = self.backlog

    async def start(self) -> None:
        logger.info("Starting appservice scheduler")

        # check for any DOWN ASes and start recoverers for them.
        services = await self.store.get_appservices_by_state(
            ApplicationServiceState.DOWN
        )

        for service in services:
            self.txn_ctrl.start_recoverer(service)

        # Spiral patch 0006: the to-device position is where the appservice
        # has *received* messages up to, so whatever was still queued in
        # memory when we stopped is in the device inbox past it.
        down = {service.id for service in services}
        for service in self.store.get_app_services():
            if service.id not in down and self.backlog.wants_to_device(service):
                self.queuer.wake_for_to_device(service)

    def enqueue_for_appservice(
        self,
        appservice: ApplicationService,
        events: Collection[EventBase] | None = None,
        ephemeral: Collection[JsonMapping] | None = None,
        device_list_summary: DeviceListUpdates | None = None,
    ) -> None:
        """
        Enqueue some data to be sent off to an application service.

        To-device messages are not queued here: see `wake_for_to_device`.

        Args:
            appservice: The application service to create and send a transaction to.
            events: The persistent room events to send.
            ephemeral: The ephemeral events to send.
            device_list_summary: A summary of users that the application service either needs
                to refresh the device lists of, or those that the application service need no
                longer track the device lists of.
        """
        # We purposefully allow this method to run with empty events/ephemeral
        # collections, so that callers do not need to check iterable size themselves.
        if not events and not ephemeral and not device_list_summary:
            return

        if events:
            self.queuer.queued_events.setdefault(appservice.id, []).extend(events)
        if ephemeral:
            self.queuer.queued_ephemeral.setdefault(appservice.id, []).extend(ephemeral)
        if device_list_summary:
            self.queuer.queued_device_list_summaries.setdefault(
                appservice.id, []
            ).append(device_list_summary)

        # Kick off a new application service transaction
        self.queuer.start_background_request(appservice)

    def wake_for_to_device(self, appservice: ApplicationService) -> None:
        """Spiral patch 0006: there may be new to-device messages for this
        application service's users in the device inbox. The sender reads them
        from the position the application service has received them up to."""
        self.queuer.wake_for_to_device(appservice)


class _ServiceQueuer:
    """Queue of events waiting to be sent to appservices.

    Groups events into transactions per-appservice, and sends them on to the
    TransactionController. Makes sure that we only have one transaction in flight per
    appservice at a given time.
    """

    def __init__(self, txn_ctrl: "_TransactionController", hs: "HomeServer"):
        # dict of {service_id: [events]}
        self.queued_events: dict[str, list[EventBase]] = {}
        # dict of {service_id: [events]}
        self.queued_ephemeral: dict[str, list[JsonMapping]] = {}
        # Spiral patch 0006: the services with to-device messages to read
        # from the device inbox, and the reader (set by the scheduler).
        self.to_device_wakeups: set[str] = set()
        self.backlog: "_AppserviceBacklog | None" = None
        # dict of {service_id: [device_list_summary]}
        self.queued_device_list_summaries: dict[str, list[DeviceListUpdates]] = {}

        # the appservices which currently have a transaction in flight
        self.requests_in_flight: set[str] = set()
        self.txn_ctrl = txn_ctrl
        self._msc3202_transaction_extensions_enabled: bool = (
            hs.config.experimental.msc3202_transaction_extensions
        )
        self.server_name = hs.hostname
        self.clock = hs.get_clock()
        self.hs = hs
        self._store = hs.get_datastores().main

    def start_background_request(self, service: ApplicationService) -> None:
        # start a sender for this appservice if we don't already have one
        if service.id in self.requests_in_flight:
            return

        self.hs.run_as_background_process("as-sender", self._send_request, service)

    def wake_for_to_device(self, service: ApplicationService) -> None:
        self.to_device_wakeups.add(service.id)
        self.start_background_request(service)

    async def _send_request(self, service: ApplicationService) -> None:
        # sanity-check: we shouldn't get here if this service already has a sender
        # running.
        assert service.id not in self.requests_in_flight

        self.requests_in_flight.add(service.id)
        try:
            while True:
                all_events = self.queued_events.get(service.id, [])
                events = all_events[:MAX_PERSISTENT_EVENTS_PER_TRANSACTION]
                del all_events[:MAX_PERSISTENT_EVENTS_PER_TRANSACTION]

                all_events_ephemeral = self.queued_ephemeral.get(service.id, [])
                ephemeral = all_events_ephemeral[:MAX_EPHEMERAL_EVENTS_PER_TRANSACTION]
                del all_events_ephemeral[:MAX_EPHEMERAL_EVENTS_PER_TRANSACTION]

                read_to_device = service.id in self.to_device_wakeups
                self.to_device_wakeups.discard(service.id)

                # Consolidate any pending device list summaries into a single, up-to-date
                # summary.
                # Note: this code assumes that in a single DeviceListUpdates, a user will
                # never be in both "changed" and "left" sets.
                device_list_summary = DeviceListUpdates()
                for summary in self.queued_device_list_summaries.get(service.id, []):
                    # For every user in the incoming "changed" set:
                    #   * Remove them from the existing "left" set if necessary
                    #     (as we need to start tracking them again)
                    #   * Add them to the existing "changed" set if necessary.
                    device_list_summary.left.difference_update(summary.changed)
                    device_list_summary.changed.update(summary.changed)

                    # For every user in the incoming "left" set:
                    #   * Remove them from the existing "changed" set if necessary
                    #     (we no longer need to track them)
                    #   * Add them to the existing "left" set if necessary.
                    device_list_summary.changed.difference_update(summary.left)
                    device_list_summary.left.update(summary.left)
                self.queued_device_list_summaries.clear()

                if (
                    not events
                    and not ephemeral
                    and not read_to_device
                    # DeviceListUpdates is True if either the 'changed' or 'left' sets have
                    # at least one entry, otherwise False
                    and not device_list_summary
                ):
                    return

                assert self.backlog is not None
                # Spiral patch 0006: one reader of an appservice's to-device
                # stream at a time (this sender or its recoverer), from the
                # read to the stored position.
                async with self.backlog.lock.queue(service.id):
                    await self._send_one(
                        service, events, ephemeral, read_to_device, device_list_summary
                    )
        finally:
            self.requests_in_flight.discard(service.id)

    async def _send_one(
        self,
        service: ApplicationService,
        events: list[EventBase],
        ephemeral: list[JsonMapping],
        read_to_device: bool,
        device_list_summary: DeviceListUpdates,
    ) -> None:
        assert self.backlog is not None
        to_device_messages_to_send: list[JsonMapping] = []
        to_device_upto: int | None = None
        # While the service is down its recoverer sends the backlog,
        # and wakes us once it is up again.
        if read_to_device and await self.txn_ctrl._is_service_up(service):
            (
                to_device_messages_to_send,
                to_device_upto,
                more,
            ) = await self.backlog.read_to_device(service)
            if more:
                self.to_device_wakeups.add(service.id)
            if not to_device_messages_to_send and to_device_upto is not None:
                # Nothing for this service in the range: no need to
                # read it again.
                await self.backlog.ack_to_device(service, to_device_upto)
                to_device_upto = None

        if (
            not events
            and not ephemeral
            and not to_device_messages_to_send
            and not device_list_summary
        ):
            return

        one_time_keys_count: TransactionOneTimeKeysCount | None = None
        unused_fallback_keys: TransactionUnusedFallbackKeys | None = None

        if (
            self._msc3202_transaction_extensions_enabled
            and service.msc3202_transaction_extensions
        ):
            # Compute the one-time key counts and fallback key usage states
            # for the users which are mentioned in this transaction,
            # as well as the appservice's sender.
            (
                one_time_keys_count,
                unused_fallback_keys,
            ) = await self._compute_msc3202_otk_counts_and_fallback_keys(
                service, events, ephemeral, to_device_messages_to_send
            )

        try:
            delivered = await self.txn_ctrl.send(
                service,
                events,
                ephemeral,
                to_device_messages_to_send,
                one_time_keys_count,
                unused_fallback_keys,
                device_list_summary,
            )
            # Spiral patch 0006: the position moves only past what the
            # service has received; the rest stays in the device inbox.
            if delivered and to_device_upto is not None:
                await self.backlog.ack_to_device(service, to_device_upto)
        except Exception:
            logger.exception("AS request failed")

    async def _compute_msc3202_otk_counts_and_fallback_keys(
        self,
        service: ApplicationService,
        events: Iterable[EventBase],
        ephemerals: Iterable[JsonMapping],
        to_device_messages: Iterable[JsonMapping],
    ) -> tuple[TransactionOneTimeKeysCount, TransactionUnusedFallbackKeys]:
        """
        Given a list of the events, ephemeral messages and to-device messages,
        - first computes a list of application services users that may have
          interesting updates to the one-time key counts or fallback key usage.
        - then computes one-time key counts and fallback key usages for those users.
        Given a list of application service users that are interesting,
        compute one-time key counts and fallback key usages for the users.
        """

        # Set of 'interesting' users who may have updates
        users: set[str] = set()

        # The sender is always included
        users.add(service.sender.to_string())

        # All AS users that would receive the PDUs or EDUs sent to these rooms
        # are classed as 'interesting'.
        rooms_of_interesting_users: set[str] = set()
        # PDUs
        rooms_of_interesting_users.update(event.room_id for event in events)
        # EDUs
        rooms_of_interesting_users.update(
            ephemeral["room_id"]
            for ephemeral in ephemerals
            if ephemeral.get("room_id") is not None
        )

        # Look up the AS users in those rooms
        for room_id in rooms_of_interesting_users:
            users.update(
                await self._store.get_app_service_users_in_room(room_id, service)
            )

        # Add recipients of to-device messages.
        users.update(
            device_message["to_user_id"] for device_message in to_device_messages
        )

        # Compute and return the counts / fallback key usage states
        otk_counts = await self._store.count_bulk_e2e_one_time_keys_for_as(users)
        unused_fbks = await self._store.get_e2e_bulk_unused_fallback_key_types(users)
        return otk_counts, unused_fbks


class _TransactionController:
    """Transaction manager.

    Builds AppServiceTransactions and runs their lifecycle. Also starts a Recoverer
    if a transaction fails.

    (Note we have only have one of these in the homeserver.)
    """

    def __init__(self, hs: "HomeServer"):
        self.server_name = hs.hostname
        self.clock = hs.get_clock()
        self.hs = hs
        self.store = hs.get_datastores().main
        self.as_api = hs.get_application_service_api()

        # map from service id to recoverer instance
        self.recoverers: dict[str, "_Recoverer"] = {}

        # Spiral patch 0006 (set by the scheduler).
        self.backlog: "_AppserviceBacklog | None" = None

        # for UTs
        self.RECOVERER_CLASS = _Recoverer

    async def send(
        self,
        service: ApplicationService,
        events: Sequence[EventBase],
        ephemeral: list[JsonMapping] | None = None,
        to_device_messages: list[JsonMapping] | None = None,
        one_time_keys_count: TransactionOneTimeKeysCount | None = None,
        unused_fallback_keys: TransactionUnusedFallbackKeys | None = None,
        device_list_summary: DeviceListUpdates | None = None,
    ) -> bool:
        """
        Create a transaction with the given data and send to the provided
        application service.

        Args:
            service: The application service to send the transaction to.
            events: The persistent events to include in the transaction.
            ephemeral: The ephemeral events to include in the transaction.
            to_device_messages: The to-device messages to include in the transaction.
            one_time_keys_count: Counts of remaining one-time keys for relevant
                appservice devices in the transaction.
            unused_fallback_keys: Lists of unused fallback keys for relevant
                appservice devices in the transaction.
            device_list_summary: The device list summary to include in the transaction.

        Returns:
            Whether the service received the transaction (Spiral patch 0006).
        """
        delivered = False
        try:
            service_is_up = await self._is_service_up(service)
            # Don't create empty txns when in recovery mode (ephemeral events are dropped)
            if not service_is_up and not events:
                return False

            txn = await self.store.create_appservice_txn(
                service=service,
                events=events,
                ephemeral=ephemeral or [],
                to_device_messages=to_device_messages or [],
                one_time_keys_count=one_time_keys_count or {},
                unused_fallback_keys=unused_fallback_keys or {},
                device_list_summary=device_list_summary or DeviceListUpdates(),
            )
            if service_is_up:
                sent = await txn.send(self.as_api)
                if sent:
                    delivered = True
                    await txn.complete(self.store)
                else:
                    run_in_background(self._on_txn_fail, service)
        except Exception:
            logger.exception("Error creating appservice transaction")
            run_in_background(self._on_txn_fail, service)
        finally:
            # Spiral patch 0006: a stored transaction is retried without its
            # device list changes, so the recoverer sends them.
            if not delivered and device_list_summary and self.backlog is not None:
                self.backlog.keep_device_lists(service, device_list_summary)
        return delivered

    async def on_recovered(self, recoverer: "_Recoverer") -> None:
        logger.info(
            "Successfully recovered application service AS ID %s", recoverer.service.id
        )
        self.recoverers.pop(recoverer.service.id)
        logger.info("Remaining active recoverers: %s", len(self.recoverers))
        await self.store.set_appservice_state(
            recoverer.service, ApplicationServiceState.UP
        )
        # Spiral patch 0006: to-device messages that arrived after the
        # recoverer read the backlog are waiting in the device inbox.
        if self.backlog is not None and self.backlog.wants_to_device(recoverer.service):
            self.backlog.queuer.wake_for_to_device(recoverer.service)

    async def _on_txn_fail(self, service: ApplicationService) -> None:
        try:
            await self.store.set_appservice_state(service, ApplicationServiceState.DOWN)
            self.start_recoverer(service)
        except Exception:
            logger.exception("Error starting AS recoverer")

    def start_recoverer(self, service: ApplicationService) -> None:
        """Start a Recoverer for the given service

        Args:
            service:
        """
        logger.info("Starting recoverer for AS ID %s", service.id)
        assert service.id not in self.recoverers
        recoverer = self.RECOVERER_CLASS(
            self.server_name,
            self.clock,
            self.hs,
            self.store,
            self.as_api,
            service,
            self.on_recovered,
        )
        recoverer.backlog = self.backlog
        self.recoverers[service.id] = recoverer
        recoverer.recover()
        logger.info("Now %i active recoverers", len(self.recoverers))

    def force_retry(self, service: ApplicationService) -> None:
        """Forces a Recoverer to attempt delivery of transations immediately.

        Args:
            service:
        """
        recoverer = self.recoverers.get(service.id)
        if not recoverer:
            # No need to force a retry on a happy AS.
            logger.info("%s is not in recovery, not forcing retry", service.id)
            return

        recoverer.force_retry()

    async def _is_service_up(self, service: ApplicationService) -> bool:
        state = await self.store.get_appservice_state(service)
        return state == ApplicationServiceState.UP or state is None


class _Recoverer:
    """Manages retries and backoff for a DOWN appservice.

    We have one of these for each appservice which is currently considered DOWN.

    Args:
        server_name: the homeserver name (used to label metrics) (this should be `hs.hostname`).
        clock:
        store:
        as_api:
        service: the service we are managing
        callback: called once the service recovers.
    """

    def __init__(
        self,
        server_name: str,
        clock: Clock,
        hs: "HomeServer",
        store: DataStore,
        as_api: ApplicationServiceApi,
        service: ApplicationService,
        callback: Callable[["_Recoverer"], Awaitable[None]],
    ):
        self.server_name = server_name
        self.clock = clock
        self.hs = hs
        self.store = store
        self.as_api = as_api
        self.service = service
        self.callback = callback
        self.backoff_counter = 1
        self.scheduled_recovery: DelayedCallWrapper | None = None
        # Spiral patch 0006 (set by the transaction controller).
        self.backlog: "_AppserviceBacklog | None" = None

    def recover(self) -> None:
        delay = Duration(seconds=2**self.backoff_counter)
        logger.info("Scheduling retries on %s in %fs", self.service.id, delay.as_secs())
        self.scheduled_recovery = self.clock.call_later(
            delay,
            self.hs.run_as_background_process,
            "as-recoverer",
            self.retry,
        )

    def _backoff(self) -> None:
        # cap the backoff to be around 8.5min => (2^9) = 512 secs
        if self.backoff_counter < 9:
            self.backoff_counter += 1
        self.recover()

    def force_retry(self) -> None:
        """Cancels the existing timer and forces an immediate retry in the background.

        Args:
            service:
        """
        # Prevent the existing backoff from occuring
        if self.scheduled_recovery:
            self.clock.cancel_call_later(self.scheduled_recovery)
        # Run a retry, which will resechedule a recovery if it fails.
        self.hs.run_as_background_process(
            "retry",
            self.retry,
        )

    async def retry(self) -> None:
        logger.info("Starting retries on %s", self.service.id)
        try:
            # Spiral patch 0006: first what the service missed that a stored
            # transaction does not hold (to-device messages, device list
            # changes), so that room keys arrive before the events they
            # decrypt.
            backlog_sent = self.backlog is None or await self.backlog.send(
                self.service, self.as_api
            )
            while backlog_sent:
                txn = await self.store.get_oldest_unsent_txn(self.service)
                if not txn:
                    # nothing left: we're done!
                    await self.callback(self)
                    return

                logger.info(
                    "Retrying transaction %s for AS ID %s", txn.id, txn.service.id
                )
                sent = await txn.send(self.as_api)
                if not sent:
                    break

                await txn.complete(self.store)

                # reset the backoff counter and then process the next transaction
                self.backoff_counter = 1

        except Exception:
            logger.exception("Unexpected error running retries")

        # we didn't manage to send all of the transactions before we got an error of
        # some flavour: reschedule the next retry.
        self._backoff()


class _AppserviceBacklog:
    """Spiral patch 0006: what an application service must not lose while
    Synapse treats it as down.

    A stored transaction holds only its room events, so a transaction that
    failed is retried without its to-device messages and device list changes,
    and nothing at all is sent while the service is down except room events.
    The to-device messages (Megolm room keys among them) were lost for good:
    the appservice's `to_device` stream position had already moved past them.

    Here the position moves only past messages the service has received: the
    sender reads them from the device inbox when it sends, and while the
    service is down its recoverer sends them first, in new transactions of at
    about `MAX_TO_DEVICE_MESSAGES_PER_TRANSACTION` messages (a page ends on a
    whole stream id), each device's in stream order. One reader per service at
    a time (`lock`), so each message is sent once. Device list changes the service did not receive
    are kept in memory (a set of users, merged) and go with the first of
    those transactions.
    """

    def __init__(self, hs: "HomeServer", queuer: _ServiceQueuer):
        self.store = hs.get_datastores().main
        self.queuer = queuer
        self.lock = Linearizer(name="appservice_backlog", clock=hs.get_clock())
        self._to_device_enabled = (
            hs.config.experimental.msc2409_to_device_messages_enabled
        )
        self._msc3202_enabled = hs.config.experimental.msc3202_transaction_extensions
        # {service_id: device list changes the service has not received}
        self._device_lists: dict[str, DeviceListUpdates] = {}

    def wants_to_device(self, service: ApplicationService) -> bool:
        return self._to_device_enabled and (
            service.supports_ephemeral or service.supports_unstable_ephemeral
        )

    def keep_device_lists(
        self, service: ApplicationService, summary: DeviceListUpdates
    ) -> None:
        """Keeps device list changes the service did not receive, after any
        kept before."""
        kept = self._device_lists.setdefault(service.id, DeviceListUpdates())
        _merge_device_lists(kept, summary)

    def _put_back_device_lists(
        self, service: ApplicationService, summary: DeviceListUpdates
    ) -> None:
        """Puts back changes taken for sending, before any kept since."""
        since = self._device_lists.pop(service.id, None)
        if since:
            _merge_device_lists(summary, since)
        self._device_lists[service.id] = summary

    async def read_to_device(
        self, service: ApplicationService
    ) -> tuple[list[JsonMapping], int | None, bool]:
        """The next to-device messages for the service's users, from the
        position it has received them up to.

        Returns:
            The messages (with `to_user_id` / `to_device_id`, per MSC2409), the
            stream id to store once the service has them (None: nothing new),
            and whether more are waiting past it.
        """
        from_key = await self.store.get_type_stream_id_for_appservice(
            service, "to_device"
        )
        to_key = self.store.get_to_device_stream_token()
        if to_key <= from_key:
            return [], None, False

        recipients = await self.store.get_local_to_device_recipients_between(
            from_key, to_key
        )
        users = [user for user in recipients if service.is_interested_in_user(user)]
        if not users:
            return [], to_key, False

        upto = await self.store.get_to_device_page_end_for_users(
            users, from_key, to_key, MAX_TO_DEVICE_MESSAGES_PER_TRANSACTION
        )
        by_device = await self.store.get_messages_for_user_devices(
            users, from_key, upto
        )
        messages: list[JsonMapping] = [
            {"to_user_id": user_id, "to_device_id": device_id, **message}
            for (user_id, device_id), device_messages in by_device.items()
            for message in device_messages
        ]
        return messages, upto, upto < to_key

    async def ack_to_device(self, service: ApplicationService, upto: int) -> None:
        await self.store.set_appservice_stream_type_pos(service, "to_device", upto)

    async def send(
        self, service: ApplicationService, as_api: ApplicationServiceApi
    ) -> bool:
        """Sends what the service missed, in new transactions (the service
        deduplicates by transaction id, so a stored one cannot carry it).

        Returns:
            Whether the service has received all of it.
        """
        async with self.lock.queue(service.id):
            device_lists = self._device_lists.pop(service.id, DeviceListUpdates())
            try:
                more = True
                while more:
                    to_device: list[JsonMapping] = []
                    upto: int | None = None
                    more = False
                    if self.wants_to_device(service):
                        to_device, upto, more = await self.read_to_device(service)
                    if not to_device and not device_lists:
                        if upto is not None:
                            await self.ack_to_device(service, upto)
                        continue

                    otks: TransactionOneTimeKeysCount = {}
                    fallback_keys: TransactionUnusedFallbackKeys = {}
                    if self._msc3202_enabled and service.msc3202_transaction_extensions:
                        compute = (
                            self.queuer._compute_msc3202_otk_counts_and_fallback_keys
                        )
                        otks, fallback_keys = await compute(service, [], [], to_device)

                    txn = await self.store.create_appservice_txn(
                        service=service,
                        events=[],
                        ephemeral=[],
                        to_device_messages=to_device,
                        one_time_keys_count=otks,
                        unused_fallback_keys=fallback_keys,
                        device_list_summary=device_lists,
                    )
                    try:
                        sent = await txn.send(as_api)
                    finally:
                        # Its content stays here until it is sent, so the stored
                        # copy (an empty transaction) is never needed.
                        await txn.complete(self.store)
                    if not sent:
                        return False

                    device_lists = DeviceListUpdates()
                    if upto is not None:
                        await self.ack_to_device(service, upto)
                return True
            finally:
                if device_lists:
                    self._put_back_device_lists(service, device_lists)


def _merge_device_lists(into: DeviceListUpdates, summary: DeviceListUpdates) -> None:
    """Merges a later summary into an earlier one, as the sender does."""
    into.left.difference_update(summary.changed)
    into.changed.update(summary.changed)
    into.changed.difference_update(summary.left)
    into.left.update(summary.left)
