from pathlib import Path

from sqlglot import exp, parse, tokenize
from sqlglot.tokens import TokenType


SNOWFLAKE_BLOCKED_FUNCTIONS = frozenset(
    Path(__file__).with_name("snowflake_blocked_functions.txt").read_text().split()
)


class ReadOnlyFunctionError(Exception):
    pass


def parse_read_only_statements(query: str, dialect: str | None):
    statements = parse(query, dialect=dialect)
    if dialect != "snowflake":
        return statements

    alias_positions = {
        node.this.meta.get("start")
        for statement in statements
        if statement is not None
        for node in statement.find_all(exp.TableAlias)
        if isinstance(node.this, exp.Identifier)
    }
    tokens = tokenize(query, dialect=dialect)
    for token, following in zip(tokens, tokens[1:]):
        if following.token_type != TokenType.L_PAREN or token.start in alias_positions:
            continue
        name = token.text.upper()
        if any(
            name.startswith(blocked[:-1]) if blocked.endswith("*") else name == blocked
            for blocked in SNOWFLAKE_BLOCKED_FUNCTIONS
        ):
            raise ReadOnlyFunctionError(
                "function is not allowed on a read-only connection"
            )
    return statements
