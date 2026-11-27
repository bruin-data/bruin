# Agent Access

Every agent has its own identity, like a service account: its own permissions, set by its [Cloud CLI access](/cloud/ai-agents/configure#_6-optional-cloud-cli-access), and an owner who answers for it. People can use that identity on purpose: act as the agent from the CLI, or run a scheduled agent as the agent.

Chatting with an agent doesn't change: a chat still runs as the person asking, limited by the agent's permissions.

::: info
Acting as agents is being rolled out team by team. If you don't see the options below, ask us to turn it on for your team.
:::

## Manage access

Open an agent and click **Manage access**. One window shows everything about who can do what with the agent:

- **Owner**: whoever created the agent. The owner can always use and act as it, and chooses who else can.
- **Who can use it**: **Team** (everyone in the team can chat with it) or **Private** (only the people you set).
- **People**: one choice per person.
  - **No access**: private agents only.
  - **Can use**: chat with the agent.
  - **Can act as**: also work as the agent from the Bruin CLI, with the agent's permissions instead of their own.

Org Admins can always use and act as every agent.

### Who can change it

| | Owner | Org Admin |
|---|---|---|
| Pick who can use it and who can act as it | ✅ | ✅ |
| Team / Private | | ✅ |
| Which API tokens can use it | | ✅ |
| Transfer ownership, or take the agent over | | ✅ |

Nobody can hand out permissions they don't have: to set someone to **Can act as**, your own role must cover everything the agent can do. Nobody can add themselves.

### When the agent can do more than its owner

If the agent's permissions go beyond its owner's role, a yellow note lists them (**See all**). Until an Org Admin clicks **Approve**, only Org Admins can act as the agent.

### When the owner leaves

If the owner leaves the team, only Org Admins can act as the agent until one of them picks a new owner or clicks **Take it over** (no owner; Org Admins manage it). Agents created before owners existed have no owner and are managed by Org Admins.

## Act as an agent from the CLI

1. Run `bruin cloud login --global` and keep **Allow acting as agents** ticked. It is ticked for you if you can act as one of your team's agents; a normal login (for example Bruin Desktop) leaves it unticked.
2. Start acting as the agent:

```bash
bruin cloud act-as --agent-id 12
```

Cloud commands now use the agent's permissions and reach only the agent's own dashboards, scheduled agents and data apps. The session lasts an hour and renews on its own. If the agent isn't allowed to do something, the command fails and names the missing permission: ask the agent's owner to add it rather than switching back to your own login.

```bash
bruin cloud act-as --stop   # use your own login again
```

If your login doesn't allow acting as agents yet, `act-as` opens the login page for you. See [`bruin cloud act-as`](/commands/cloud#act-as) for all options.

## Run a scheduled agent as the agent

A scheduled agent runs as its owner by default. To run it with only the agent's permissions and connections, open the scheduled agent and change **Runs as** to the agent. See [Scheduled Agents](/cloud/ai-agents/scheduled#run-as-the-agent).

## Next

- [Configure Agents](/cloud/ai-agents/configure) to set the agent's Cloud CLI permissions.
- [Scheduled Agents](/cloud/ai-agents/scheduled) to put the agent on a schedule.
