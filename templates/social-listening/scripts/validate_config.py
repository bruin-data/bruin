#!/usr/bin/env python3
"""Fail before external collection when safe source-policy invariants are violated."""
import json
import os
import sys


def value(name, default):
    return json.loads(os.getenv("BRUIN_VARS", "{}")).get(name, default)


def main():
    errors = []
    poll = value("reddit_poll_minutes", 15)
    if not isinstance(poll, int) or not 5 <= poll <= 1440:
        errors.append("reddit_poll_minutes must be an integer from 5 through 1440")
    for name in ("min_relevance", "min_priority", "min_confidence"):
        score = value(name, 0)
        if not isinstance(score, (int, float)) or not 0 <= score <= 1:
            errors.append(f"{name} must be between 0 and 1")
    if value("reply_drafts_enabled", False) and not value("require_human_approval", True):
        errors.append("reply_drafts_enabled requires require_human_approval=true")
    if value("allow_mutations", False):
        errors.append("allow_mutations is outside this template's scope and must remain false")
    if value("reddit_enabled", False) and not os.getenv("REDDIT_CLIENT_ID"):
        errors.append("REDDIT_CLIENT_ID is required when reddit_enabled=true")
    destination = value("notification_destination", "none")
    if destination == "webhook" and not os.getenv("SOCIAL_LISTENING_WEBHOOK_URL"):
        errors.append("SOCIAL_LISTENING_WEBHOOK_URL is required for webhook delivery")
    if destination == "slack_webhook" and not os.getenv("SLACK_WEBHOOK_URL"):
        errors.append("SLACK_WEBHOOK_URL is required for Slack webhook delivery")
    if errors:
        print("Invalid social-listening configuration:\n- " + "\n- ".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
