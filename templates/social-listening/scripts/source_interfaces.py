"""Explicit source contracts. Disabled sources require authorised access before implementation."""

SOURCE_INTERFACES = {
    "reddit": {"enabled_by_default": False, "access": "authorised OAuth or public API", "mutation": False},
    "hackernews": {"enabled_by_default": True, "access": "Algolia HN Search API", "mutation": False},
    "github": {"enabled_by_default": False, "access": "public or user-authorised API", "mutation": False},
    "stackoverflow": {"enabled_by_default": False, "access": "Stack Exchange API", "mutation": False},
    "slack_community": {"enabled_by_default": False, "access": "authorised workspace API", "mutation": False},
    "linkedin": {"enabled_by_default": False, "access": "authorised API, export, or approved provider", "mutation": False},
    "x": {"enabled_by_default": False, "access": "authorised API, export, or approved provider", "mutation": False},
    "quora": {"enabled_by_default": False, "access": "authorised API, export, or approved provider", "mutation": False},
}


def require_read_only(source):
    if source not in SOURCE_INTERFACES:
        raise ValueError(f"unsupported source: {source}")
    if SOURCE_INTERFACES[source]["mutation"]:
        raise ValueError("mutation-capable adapters are outside this template's scope")
    return SOURCE_INTERFACES[source]
