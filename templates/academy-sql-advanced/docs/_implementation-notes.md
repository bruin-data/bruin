# Implementation notes - contributor-only

The specification has a deliberate tension: the template must arrive working, yet lesson 12 needs
one runtime failure. The shipped decision is that `bruin validate` passes and `bruin run` builds every
upstream asset, then fails only on the blocking `churn_risk` custom check. The setup prompt names
this failure, tells the agent to stop on any other error, and does not permit silent continuation.
