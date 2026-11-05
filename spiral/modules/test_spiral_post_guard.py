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
"""Tests for `spiral_post_guard`, standard library only.

    python3 -I -B -m unittest discover -s docker/synapse/modules -p 'test_*.py'

(`just synapse-modules-test`, part of `just ci`.) Without Synapse installed
the module's two imports are stubbed and events are plain objects. Inside the
Synapse image the same tests run against the real thing — events built by
`make_event_from_dict` (whose `content` is Synapse's Rust `JsonObject`, not a
dict), the real `Codes`, the module loader and the spam-checker runner whose
answer the send handler turns into a 403 (checked end to end against a running
Synapse on 2026-10-20, see the commit):

    docker run --rm --platform linux/amd64 --entrypoint python \\
      -v "$PWD/docker/synapse/modules:/m:ro" matrixdotorg/synapse:v1.158.0 \\
      -I -B -m unittest discover -s /m -p 'test_*.py'
"""

import asyncio
import enum
import logging
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import synapse  # noqa: F401

    REAL_SYNAPSE = True
except ImportError:
    REAL_SYNAPSE = False

    class _Codes(str, enum.Enum):
        FORBIDDEN = "M_FORBIDDEN"
        BAD_JSON = "M_BAD_JSON"
        TOO_LARGE = "M_TOO_LARGE"

    class _ModuleApi:
        pass

    _errors = types.ModuleType("synapse.api.errors")
    _errors.Codes = _Codes
    _module_api = types.ModuleType("synapse.module_api")
    _module_api.NOT_SPAM = "NOT_SPAM"
    _module_api.ModuleApi = _ModuleApi
    for _name, _mod in {
        "synapse": types.ModuleType("synapse"),
        "synapse.api": types.ModuleType("synapse.api"),
        "synapse.api.errors": _errors,
        "synapse.module_api": _module_api,
    }.items():
        sys.modules[_name] = _mod

import spiral_post_guard as guard  # noqa: E402
from synapse.api.errors import Codes  # noqa: E402
from synapse.module_api import NOT_SPAM  # noqa: E402

# Refusals log a warning; keep a passing run quiet (assertLogs still sees them).
logging.getLogger("spiral_post_guard").addHandler(logging.NullHandler())

SERVER = "spiral.test"
# As `deploy/config/synapse-homeserver.yaml.tmpl` renders them.
CONFIG = {
    "draft_sender_regexes": [
        f"@agent-.*:{SERVER}",
        f"@assistant-.*:{SERVER}",
        f"@agent-platform-bot:{SERVER}",
    ]
}
POST = "$post"
PERSON = f"@alice:{SERVER}"
MALLORY = f"@mallory:{SERVER}"


class StubEvent:
    def __init__(self, event_type, content, sender, room_id, event_id):
        self.type = event_type
        self.content = content
        self.sender = sender
        self.room_id = room_id
        self.event_id = event_id


def make_event(event_type, content, sender=PERSON, state_key=None):
    if REAL_SYNAPSE:
        from synapse.api.room_versions import RoomVersions
        from synapse.events import make_event_from_dict

        d = {
            "type": event_type,
            "sender": sender,
            "room_id": f"!room:{SERVER}",
            "content": content,
            "auth_events": [],
            "prev_events": [],
            "depth": 1,
            "origin_server_ts": 0,
            "hashes": {"sha256": "x"},
            "signatures": {},
        }
        if state_key is not None:
            d["state_key"] = state_key
        return make_event_from_dict(d, RoomVersions.V10)
    return StubEvent(event_type, content, sender, f"!room:{SERVER}", "$event")


def action(**extra):
    content = {
        "m.relates_to": {"rel_type": "m.reference", "event_id": POST},
        "op": {"kind": "set", "path": ["count"], "value": 3},
        "seq": 7,
        "summary": {"line": "Alice played", "notify": "none"},
    }
    content.update(extra)
    return content


def draft(**extra):
    content = {
        "msgtype": guard.DRAFT_MSGTYPE,
        "body": "<title>T</title>",
        "title": "T",
        "target_room_id": f"!victim:{SERVER}",
        "sha256": "abc",
    }
    content.update(extra)
    return content


def padded_to(size, base):
    """`base` with a filler grown until its content is exactly `size` bytes."""
    content = dict(base, pad="")
    content["pad"] = "x" * (size - guard.content_size(content))
    assert guard.content_size(content) == size
    return content


class FakeApi:
    def __init__(self):
        self.callbacks = {}

    def register_spam_checker_callbacks(self, **callbacks):
        self.callbacks.update(callbacks)


def new_guard(config=CONFIG):
    return guard.SpiralPostGuard(guard.SpiralPostGuard.parse_config(config), FakeApi())


def check(event, g=None):
    return asyncio.run((g or new_guard()).check_event_for_spam(event))


class Refuses:
    def assertRefused(self, event, code, reason):
        result = check(event)
        self.assertEqual(result, (code, {guard.REASON_FIELD: reason}))
        self.assertIsInstance(result[0], Codes)

    def assertPasses(self, event):
        self.assertEqual(check(event), NOT_SPAM)


class ActionSeq(unittest.TestCase, Refuses):
    def test_good_action_passes(self):
        self.assertPasses(make_event(guard.ACTION_EVENT_TYPE, action()))

    def test_bounds_pass(self):
        for seq in (1, 100_000):
            with self.subTest(seq=seq):
                self.assertPasses(make_event(guard.ACTION_EVENT_TYPE, action(seq=seq)))

    def test_out_of_range_refused(self):
        for seq in (0, -1, 100_001, 2**53 - 1):
            with self.subTest(seq=seq):
                self.assertRefused(make_event(guard.ACTION_EVENT_TYPE, action(seq=seq)), Codes.BAD_JSON, "action_seq")

    def test_huge_integer_refused(self):
        # Synapse's own JSON checks refuse integers past ±(2**53-1) at the
        # REST layer in current room versions; the guard does not rely on it.
        for seq in (2**63 - 1, 2**63, -(2**63)):
            with self.subTest(seq=seq):
                try:
                    event = make_event(guard.ACTION_EVENT_TYPE, action(seq=seq))
                except (OverflowError, ValueError, TypeError):
                    continue  # Synapse cannot even hold it as an event
                self.assertRefused(event, Codes.BAD_JSON, "action_seq")

    def test_not_an_integer_refused(self):
        for seq in (1.5, 3.0, "5", True, None, [1], {"n": 1}):
            with self.subTest(seq=seq):
                self.assertRefused(make_event(guard.ACTION_EVENT_TYPE, action(seq=seq)), Codes.BAD_JSON, "action_seq")

    def test_missing_seq_refused(self):
        content = action()
        del content["seq"]
        self.assertRefused(make_event(guard.ACTION_EVENT_TYPE, content), Codes.BAD_JSON, "action_seq")

    def test_state_event_of_the_type_checked_too(self):
        # Relations can point from a state event; the type is what 1.0.1 reads.
        event = make_event(guard.ACTION_EVENT_TYPE, action(seq=0), state_key="")
        self.assertRefused(event, Codes.BAD_JSON, "action_seq")


class ActionSize(unittest.TestCase, Refuses):
    def test_at_the_limit_passes(self):
        content = padded_to(guard.ACTION_CONTENT_MAX_BYTES, action())
        self.assertPasses(make_event(guard.ACTION_EVENT_TYPE, content))

    def test_one_byte_over_refused(self):
        content = padded_to(guard.ACTION_CONTENT_MAX_BYTES + 1, action())
        self.assertRefused(make_event(guard.ACTION_EVENT_TYPE, content), Codes.TOO_LARGE, "action_size")

    def test_counted_in_utf8_bytes_not_characters(self):
        # 20 000 two-byte characters: under the limit in characters, over it in bytes.
        content = action(op={"kind": "set", "path": ["t"], "value": "é" * 20_000})
        self.assertLess(len(str(content)), guard.ACTION_CONTENT_MAX_BYTES)
        self.assertRefused(make_event(guard.ACTION_EVENT_TYPE, content), Codes.TOO_LARGE, "action_size")

    def test_nested_content_measured(self):
        content = action(op={"kind": "batch", "ops": [{"kind": "set", "path": ["a", str(i)], "value": "y" * 100} for i in range(400)]})
        self.assertRefused(make_event(guard.ACTION_EVENT_TYPE, content), Codes.TOO_LARGE, "action_size")


class SummaryLine(unittest.TestCase, Refuses):
    """Not limited: 1.0.1 has no write-time cap, so a long line is a real post's."""

    def test_long_line_passes(self):
        summary = {"line": {"en": "a" * 2_000, "uk": "я" * 2_000}}
        self.assertPasses(make_event(guard.ACTION_EVENT_TYPE, action(summary=summary)))

class Drafts(unittest.TestCase, Refuses):
    def test_agents_pass(self):
        for sender in (f"@assistant-x:{SERVER}", f"@agent-019e51fb:{SERVER}", f"@agent-platform-bot:{SERVER}"):
            with self.subTest(sender=sender):
                self.assertPasses(make_event(guard.MESSAGE_EVENT_TYPE, draft(), sender=sender))

    def test_people_refused(self):
        for sender in (MALLORY, PERSON, f"@telegram_123:{SERVER}"):
            with self.subTest(sender=sender):
                self.assertRefused(make_event(guard.MESSAGE_EVENT_TYPE, draft(), sender=sender), Codes.FORBIDDEN, "draft_sender")

    def test_matched_in_full(self):
        for sender in (
            f"@xassistant-x:{SERVER}",  # not at the start
            f"@assistant-x:{SERVER}.evil",  # not at the end
            "@assistant-x:evil.test",  # another server
        ):
            with self.subTest(sender=sender):
                self.assertRefused(make_event(guard.MESSAGE_EVENT_TYPE, draft(), sender=sender), Codes.FORBIDDEN, "draft_sender")

    def test_edit_into_a_draft_refused(self):
        # A timeline shows an edit's new content: a person's own text edited
        # into a draft would be a draft card.
        content = {
            "msgtype": "m.text",
            "body": "* hi",
            "m.new_content": draft(),
            "m.relates_to": {"rel_type": "m.replace", "event_id": "$mine"},
        }
        self.assertRefused(make_event(guard.MESSAGE_EVENT_TYPE, content, sender=MALLORY), Codes.FORBIDDEN, "draft_sender")
        self.assertPasses(make_event(guard.MESSAGE_EVENT_TYPE, content, sender=f"@assistant-x:{SERVER}"))

    def test_text_with_draft_like_fields_passes(self):
        content = {"msgtype": "m.text", "body": "hi", "target_room_id": "!x", "m.new_content": "not a mapping"}
        self.assertPasses(make_event(guard.MESSAGE_EVENT_TYPE, content, sender=MALLORY))

    def test_draft_msgtype_on_another_event_type_passes(self):
        # Only `m.room.message` renders as a draft card.
        self.assertPasses(make_event("m.sticker", draft(), sender=MALLORY))


class Unrelated(unittest.TestCase, Refuses):
    def test_untouched(self):
        cases = [
            ("m.room.encrypted", {"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "x" * 40_000,
                                  "m.relates_to": {"rel_type": "m.reference", "event_id": POST}}),
            ("m.room.message", {"msgtype": "m.text", "body": "a" * 40_000}),
            ("m.room.message", {"msgtype": "com.spiral.app", "body": "post"}),
            ("m.room.message", {}),
            ("m.reaction", {"m.relates_to": {"rel_type": "m.annotation", "event_id": POST, "key": "👍"}}),
            ("com.spiral.app.actions", {"seq": 0}),
            ("com.spiral.tools.changed", {"version": 1, "tools": []}),
        ]
        for event_type, content in cases:
            with self.subTest(event_type=event_type, keys=sorted(content)):
                self.assertPasses(make_event(event_type, content, sender=MALLORY))

    def test_state_event_untouched(self):
        self.assertPasses(make_event("m.room.member", {"membership": "join"}, sender=MALLORY, state_key=MALLORY))


class FailurePosture(unittest.TestCase, Refuses):
    def test_unclassifiable_event_fails_open(self):
        class Broken:
            event_id = "$broken"

            @property
            def type(self):
                raise RuntimeError("boom")

        with self.assertLogs("spiral_post_guard", logging.WARNING):
            self.assertEqual(check(Broken()), NOT_SPAM)

    def test_message_with_odd_content_passes(self):
        for content in (None, [], "text", 5):
            with self.subTest(content=content):
                self.assertEqual(check(StubEvent("m.room.message", content, MALLORY, "!r", "$e")), NOT_SPAM)

    def test_action_with_odd_content_refused(self):
        for content in (None, [], "text", 5):
            with self.subTest(content=content):
                event = StubEvent(guard.ACTION_EVENT_TYPE, content, PERSON, "!r", "$e")
                self.assertEqual(check(event), (Codes.BAD_JSON, {guard.REASON_FIELD: "action_content"}))

    def test_action_that_cannot_be_measured_fails_closed(self):
        event = StubEvent(guard.ACTION_EVENT_TYPE, action(op={1, 2}), PERSON, "!r", "$e")
        with self.assertLogs("spiral_post_guard", logging.WARNING):
            self.assertEqual(check(event), (Codes.BAD_JSON, {guard.REASON_FIELD: "action_unreadable"}))

    def test_draft_whose_sender_cannot_be_read_fails_closed(self):
        class NoSender:
            type = guard.MESSAGE_EVENT_TYPE
            content = draft()
            event_id = "$e"

            @property
            def sender(self):
                raise RuntimeError("boom")

        self.assertEqual(check(NoSender()), (Codes.BAD_JSON, {guard.REASON_FIELD: "draft_unreadable"}))

    def test_non_string_sender_refused(self):
        self.assertEqual(
            check(StubEvent(guard.MESSAGE_EVENT_TYPE, draft(), None, "!r", "$e")),
            (Codes.FORBIDDEN, {guard.REASON_FIELD: "draft_sender"}),
        )


class Logs(unittest.TestCase):
    def test_refusal_logs_ids_not_content_or_sender(self):
        secret = "SECRET-LINE-" + "s" * 200
        event = make_event(guard.ACTION_EVENT_TYPE, action(seq=0, summary={"line": secret}), sender=MALLORY)
        with self.assertLogs("spiral_post_guard", logging.WARNING) as logs:
            check(event)
        text = "\n".join(logs.output)
        self.assertIn("action_seq", text)
        self.assertIn(event.event_id, text)
        self.assertIn(event.room_id, text)
        self.assertNotIn("SECRET-LINE", text)
        self.assertNotIn("mallory", text)

    def test_passing_event_logs_nothing(self):
        logger = logging.getLogger("spiral_post_guard")
        with self.assertNoLogs(logger, logging.DEBUG):
            check(make_event(guard.ACTION_EVENT_TYPE, action()))
            check(make_event(guard.MESSAGE_EVENT_TYPE, {"msgtype": "m.text", "body": "hi"}))


class Config(unittest.TestCase):
    def test_registers_the_callback(self):
        api = FakeApi()
        g = guard.SpiralPostGuard(guard.SpiralPostGuard.parse_config(CONFIG), api)
        self.assertEqual(api.callbacks, {"check_event_for_spam": g.check_event_for_spam})

    def test_bad_configs_refused(self):
        for config in (
            {},
            {"draft_sender_regexes": []},
            {"draft_sender_regexes": "@agent-.*"},
            {"draft_sender_regexes": [""]},
            {"draft_sender_regexes": [5]},
            {"draft_sender_regexes": ["@agent-(:x"]},
            {"draft_sender_regexes": ["@agent-.*:x"], "typo": 1},
        ):
            with self.subTest(config=config):
                with self.assertRaises(Exception):
                    guard.SpiralPostGuard.parse_config(config)


@unittest.skipUnless(REAL_SYNAPSE, "needs Synapse (run inside the image)")
class RealSynapse(unittest.TestCase):
    """The guard behind Synapse's own module loader and spam-checker runner."""

    def runner(self):
        from synapse.module_api.callbacks.spamchecker_callbacks import SpamCheckerModuleApiCallbacks

        class Clock:
            def time(self):
                return 0.0

            def time_msec(self):
                return 0

        hs = types.SimpleNamespace(hostname=SERVER, get_clock=lambda: Clock())
        callbacks = SpamCheckerModuleApiCallbacks(hs)

        class Api:
            def register_spam_checker_callbacks(self, **kw):
                callbacks.register_callbacks(**kw)

        from synapse.util.module_loader import load_module

        cls, config = load_module({"module": "spiral_post_guard.SpiralPostGuard", "config": CONFIG}, ("modules", "<item 0>"))
        cls(config, Api())
        return callbacks

    def run_check(self, callbacks, event):
        from twisted.internet import defer

        d = defer.ensureDeferred(callbacks.check_event_for_spam(event))
        results = []
        d.addBoth(results.append)
        self.assertEqual(len(results), 1, "the check must not wait on anything")
        if isinstance(results[0], Exception) or hasattr(results[0], "raiseException"):
            results[0].raiseException()
        return results[0]

    def test_runner_passes_refusals_through(self):
        callbacks = self.runner()
        self.assertEqual(self.run_check(callbacks, make_event(guard.ACTION_EVENT_TYPE, action())), callbacks.NOT_SPAM)
        self.assertEqual(
            self.run_check(callbacks, make_event(guard.ACTION_EVENT_TYPE, action(seq=0))),
            (Codes.BAD_JSON, {guard.REASON_FIELD: "action_seq"}),
        )
        self.assertEqual(
            self.run_check(callbacks, make_event(guard.MESSAGE_EVENT_TYPE, draft(), sender=MALLORY)),
            (Codes.FORBIDDEN, {guard.REASON_FIELD: "draft_sender"}),
        )

    def test_loader_turns_a_bad_config_into_a_config_error(self):
        from synapse.config._base import ConfigError
        from synapse.util.module_loader import load_module

        with self.assertRaises(ConfigError):
            load_module({"module": "spiral_post_guard.SpiralPostGuard", "config": {}}, ("modules", "<item 0>"))

    def test_content_is_synapses_own_mapping(self):
        # What made `json.dumps(event.content)` fail without `_plain`.
        content = make_event(guard.ACTION_EVENT_TYPE, action()).content
        self.assertNotIsInstance(content, dict)


if __name__ == "__main__":
    unittest.main()
