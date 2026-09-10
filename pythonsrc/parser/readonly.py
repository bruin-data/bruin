from pathlib import Path

from sqlglot import exp, parse, tokenize
from sqlglot.parsers.snowflake import SnowflakeParser
from sqlglot.tokens import TokenType


SNOWFLAKE_READ_ONLY_FUNCTIONS = frozenset(
    Path(__file__).with_name("snowflake_read_only_functions.txt").read_text().split()
)

SNOWFLAKE_READ_ONLY_SYNTAX = frozenset(
    {"ALL", "ANY", "CASE", "EXISTS", "ILIKE", "LIKE", "RLIKE", "SOME", "TABLE", "UNION"}
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
    type_parameters_end = -1
    for index, token in enumerate(tokens[:-1]):
        if index <= type_parameters_end:
            continue
        if tokens[index + 1].token_type != TokenType.L_PAREN:
            continue
        name = token.text.upper()
        if (
            token.token_type not in SnowflakeParser.FUNC_TOKENS
            and name not in SnowflakeParser.NO_PAREN_FUNCTION_PARSERS
        ):
            continue
        if token.start in alias_positions:
            continue
        previous = tokens[index - 1].token_type if index else None
        if token.token_type in SnowflakeParser.TYPE_TOKENS and previous in (
            TokenType.ALIAS,
            TokenType.DCOLON,
        ):
            depth = 0
            for end in range(index + 1, len(tokens)):
                if tokens[end].token_type == TokenType.L_PAREN:
                    depth += 1
                elif tokens[end].token_type == TokenType.R_PAREN:
                    depth -= 1
                    if depth == 0:
                        type_parameters_end = end
                        break
            continue
        if (
            token.token_type == TokenType.IDENTIFIER
            or previous == TokenType.DOT
            or (
                name not in SNOWFLAKE_READ_ONLY_FUNCTIONS
                and name not in SNOWFLAKE_READ_ONLY_SYNTAX
            )
        ):
            raise ReadOnlyFunctionError(
                "function is not allowed on a read-only connection"
            )
    return statements
