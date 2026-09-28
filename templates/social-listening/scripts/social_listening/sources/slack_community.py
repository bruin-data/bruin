"""Interface for a Slack community source. Not implemented in this version.

Only implement this if you administer the workspace, or have written approval
from its administrators, and members have been told their public-channel
messages are monitored. A compliant implementation would:

* use a bot token scoped to ``channels:history`` for explicitly listed public
  channels (never DMs, private channels or ``*:read`` on users);
* page ``conversations.history`` with ``oldest``/``latest`` set to the window
  and ``cursor`` pagination, honouring ``Retry-After`` on HTTP 429;
* emit ``RawRecord(source="slack_community", external_id=f"{channel}:{ts}", ...)``
  with the message text, channel ID, ``ts`` and permalink only;
* honour message deletion events by removing the row (see the redaction steps
  in docs/sources-and-privacy.md).

``settings.validate_policy`` rejects ``slack_community_enabled=true`` until the
collector below is implemented and wired into an asset.
"""

from __future__ import annotations

from typing import Iterator

from ..settings import Window
from .base import RawRecord, SourceCollector, SourceDisabled


class SlackCommunityCollector(SourceCollector):
    source = "slack_community"

    def collect(self, window: Window) -> Iterator[RawRecord]:
        raise SourceDisabled(
            "slack_community is an interface only; see the module docstring"
        )
