# QA checks for the `murder-mystery` template

## THIS DIRECTORY IS A COMPLETE SPOILER

The query text in `checks/` **is** the deduction path, stage by stage. Reading it will
ruin the game. It is here, and not in `templates/murder-mystery/`, for exactly that
reason, and it must never be copied into the shipped template.

## What these check

The template ships no verdict mechanism: nothing in a player's project tells them whether
their accusation is right. These checks are therefore the *only* guarantee that the case
has exactly one answer. Without them, an innocuous change to a generator quietly makes
the case unsolvable or trivial and nobody finds out until a player complains.

Each asset materializes one stage's candidate set into a `qa` schema and asserts its
size with a `custom_checks` entry. The last asset asserts that the four candidate sets
converge on the four residents the generator actually chose — which is the part that
proves the stages lead somewhere rather than merely returning the right *number* of rows.

## How they run

They need the generation scaffolding alive, which the template's last asset drops. So the
run excludes it by tag:

```bash
bruin init murder-mystery ashmont-case
cp -R integration-tests/templates/qa-murder-mystery/checks ashmont-case/assets/checks
bruin run --exclude-tag scaffolding ashmont-case
```

Any failing `custom_check` fails the run, which is the point. `integration_test.go` drives
exactly this sequence under `make integration-test-light`.

Reading `_gen.actor_assignments` on a scaffolding-preserved run is the only sanctioned way
to learn the answer, and it is why the teardown is its own asset rather than a trailing
statement inside another one.
