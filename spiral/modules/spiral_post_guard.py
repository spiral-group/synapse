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
"""Refuse the Interactive Post events a store build cannot defend against.

Security pass 2 (`~/Projects/docs/DESIGNS/security-pass-2-posts.md`, H1, M1,
M4, Plan § 2). The iOS 1.0.1 build in the App Store reads any room member's
`com.spiral.app.action` and `com.spiral.app.draft` as written; its successors
validate them, but 1.0.1 stays on phones until people update. So the
homeserver refuses the two shapes that hurt it, at send time:

* a `com.spiral.app.action` whose `seq` is not an integer in
  1..=100_000 (1.0.1 iterates `1...seq`: a huge number hangs every reader
  of the post, `Int.max` traps), or whose content is larger than 32 768
  bytes of JSON (1.0.1 folds every action unbounded on every open);
* an `m.room.message` with `msgtype: com.spiral.app.draft` (or an edit whose
  `m.new_content` has it) from anyone but the platform's agents: 1.0.1 draws
  the Publish card for anyone's draft, with the sender's `target_room_id`.

It sees only what the server sees. In an end-to-end encrypted room both
arrive as `m.room.encrypted` and pass untouched; this guards unencrypted
rooms only.

A spam-checker module (`check_event_for_spam`), not a patch: it rides on a
stable module API and needs no rebase on a Synapse bump. Synapse answers a
refusal with HTTP 403 and the errcode returned here.

Failure posture: an event that is neither guarded shape is never refused,
even if this code breaks on it (fail open — a bug here must not stop people
sending messages). An event that IS one of the two shapes and cannot be
checked is refused (fail closed — the only legitimate senders, the Spiral app
and the platform's agents, never send one that cannot be checked).

Logs carry the refused event's id (it is never stored), the room id and a
reason word; never content or the sender's MXID. Who sent it is in Synapse's
access log, on the line with the same request id (`PUT-27`) and a 403.
"""

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from synapse.api.errors import Codes
from synapse.module_api import NOT_SPAM, ModuleApi

logger = logging.getLogger(__name__)

ACTION_EVENT_TYPE = "com.spiral.app.action"
MESSAGE_EVENT_TYPE = "m.room.message"
DRAFT_MSGTYPE = "com.spiral.app.draft"

# The app's own limits, the same numbers: agent-x-ios
# `NSE/Sources/SpiralAppActionLimits.swift`
# (security pass 2, iOS lane `fix/posts-pass2`). Change both together.
#
# `seq` of a valid action: 1...ACTION_SEQ_MAX, a JSON integer (not a bool).
ACTION_SEQ_MIN = 1
ACTION_SEQ_MAX = 100_000
# The whole `content` as compact JSON, UTF-8 bytes (Synapse's own canonical
# encoding; the app's JSONSerialization escapes `/`, so it never measures less).
ACTION_CONTENT_MAX_BYTES = 32_768
# `summary.line` is not limited here on purpose: nothing caps it at write
# time in 1.0.1 (the shell checks its shape, not its length), so a post that
# echoes a person's text into it would have its taps refused. The app caps
# what it shows instead.

# Returned in the error body beside the errcode, so a refusal says which rule.
REASON_FIELD = "com.spiral.refusal"


@dataclass(frozen=True)
class GuardConfig:
    """The parsed `config:` block."""

    # Who may send a draft: the application service's agent namespaces, as in
    # `deploy/config/synapse-registration.yaml.tmpl`. Matched in full.
    draft_senders: tuple[re.Pattern[str], ...]


Refusal = tuple[Codes, str]


def _plain(value: Any) -> Any:
    """`json.dumps` fallback for Synapse's immutable mappings."""
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError(type(value).__name__)


def content_size(content: Mapping) -> int:
    """`content` as compact JSON, in UTF-8 bytes."""
    encoded = json.dumps(content, ensure_ascii=False, separators=(",", ":"), default=_plain)
    return len(encoded.encode("utf-8"))



def check_action(content: Any) -> Refusal | None:
    """Why a `com.spiral.app.action` is refused, or None when it passes."""
    if not isinstance(content, Mapping):
        return Codes.BAD_JSON, "action_content"
    seq = content.get("seq")
    # `type() is int`: a JSON `true` is a Python bool, an int subclass.
    if type(seq) is not int or not ACTION_SEQ_MIN <= seq <= ACTION_SEQ_MAX:
        return Codes.BAD_JSON, "action_seq"
    if content_size(content) > ACTION_CONTENT_MAX_BYTES:
        return Codes.TOO_LARGE, "action_size"
    return None


def is_draft(content: Any) -> bool:
    """Whether an `m.room.message` renders as a draft card: its own msgtype,
    or that of the edit it carries (a timeline shows an edit's new content)."""
    if not isinstance(content, Mapping):
        return False
    if content.get("msgtype") == DRAFT_MSGTYPE:
        return True
    new_content = content.get("m.new_content")
    return isinstance(new_content, Mapping) and new_content.get("msgtype") == DRAFT_MSGTYPE


def check_draft(sender: Any, config: GuardConfig) -> Refusal | None:
    """Why a draft is refused, or None when an agent sent it."""
    if isinstance(sender, str) and any(p.fullmatch(sender) for p in config.draft_senders):
        return None
    return Codes.FORBIDDEN, "draft_sender"


def guarded_shape(event: Any) -> str | None:
    """`action`, `draft`, or None for every other event."""
    event_type = event.type
    if event_type == ACTION_EVENT_TYPE:
        return "action"
    if event_type == MESSAGE_EVENT_TYPE and is_draft(event.content):
        return "draft"
    return None


class SpiralPostGuard:
    """`modules: - module: spiral_post_guard.SpiralPostGuard`."""

    def __init__(self, config: GuardConfig, api: ModuleApi):
        self._config = config
        api.register_spam_checker_callbacks(check_event_for_spam=self.check_event_for_spam)

    @staticmethod
    def parse_config(config: Mapping[str, Any]) -> GuardConfig:
        unknown = set(config) - {"draft_sender_regexes"}
        if unknown:
            raise ValueError(f"unknown keys: {sorted(unknown)}")
        regexes = config.get("draft_sender_regexes")
        valid = isinstance(regexes, list) and regexes and all(isinstance(r, str) and r for r in regexes)
        if not valid:
            raise ValueError("draft_sender_regexes: a non-empty list of regexes")
        return GuardConfig(draft_senders=tuple(re.compile(r) for r in regexes))

    async def check_event_for_spam(self, event: Any) -> str | tuple[Codes, dict]:
        try:
            shape = guarded_shape(event)
        except Exception as e:  # fail open: not known to be ours
            logger.warning(
                "spiral_post_guard: could not classify event=%s (%s); let through",
                getattr(event, "event_id", None),
                type(e).__name__,
            )
            return NOT_SPAM
        if shape is None:
            return NOT_SPAM

        try:
            if shape == "action":
                refusal = check_action(event.content)
            else:
                refusal = check_draft(event.sender, self._config)
        except Exception as e:  # fail closed: a guarded shape we cannot check
            logger.warning(
                "spiral_post_guard: could not check %s event=%s (%s); refused",
                shape,
                getattr(event, "event_id", None),
                type(e).__name__,
            )
            refusal = Codes.BAD_JSON, f"{shape}_unreadable"

        if refusal is None:
            return NOT_SPAM
        code, reason = refusal
        logger.warning(
            "spiral_post_guard: refused %s event=%s room=%s",
            reason,
            getattr(event, "event_id", None),
            getattr(event, "room_id", None),
        )
        return code, {REASON_FIELD: reason}
