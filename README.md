# workhorse-ai-mcp

[![npm](https://img.shields.io/npm/v/workhorse-ai-mcp)](https://www.npmjs.com/package/workhorse-ai-mcp)
[![skills.sh](https://img.shields.io/badge/skills.sh-3_skills-8A2BE2)](https://skills.sh/workhorse-ai/workhorse-ai-mcp)
<!-- TODO: когда каталог проиндексирует установки, вернуть живой счётчик: https://skills.sh/b/workhorse-ai/workhorse-ai-mcp -->

**Your AI is the workhorse. You are the orchestrator.**

A delegation journal for orchestrator/worker AI workflows: an append-only
event log over SQLite (tasks, reports, artifacts, incidents, full-text
search) exposed as an MCP server. Zero dependencies — only Node.js >= 22.5
with the built-in `node:sqlite`.

The journal enforces a simple discipline: `DRAFT → DELEGATED → REPORTED →
ACCEPTED | REWORK | FAILED`, where *reported* (the worker thinks it is done)
is never the same as *accepted* (the orchestrator verified it).

## Quick start

**Claude Code — two commands** (installs the MCP server *and* the skill):

```bash
claude plugin marketplace add https://github.com/workhorse-ai/workhorse-ai-mcp
claude plugin install workhorse-ai@workhorse-ai
```

**Any other agent** — install the skill, then add the MCP server to your
agent's MCP config:

```bash
npx skills add workhorse-ai/workhorse-ai-mcp
```

```json
{ "mcpServers": { "workhorse": { "command": "npx", "args": ["-y", "workhorse-ai-mcp"] } } }
```

Optionally, pin the rule at the project level — copy this into your
`AGENTS.md` / `CLAUDE.md`:

```markdown
Delegation goes through the `workhorse` MCP journal. Worker duties:
`search_precedents` before starting, `record_artifact` (progress notes)
along the way, `submit_report` at the end. Never commit and never accept
your own work — acceptance requires the orchestrator's own test run.
```

## Install

Add the server to your `.mcp.json`:

```json
{
  "mcpServers": {
    "workhorse": {
      "command": "npx",
      "args": ["-y", "workhorse-ai-mcp"]
    }
  }
}
```

Or run it straight from a checkout:

```json
{
  "mcpServers": {
    "workhorse": {
      "command": "node",
      "args": ["apps/mcp/server.mjs"]
    }
  }
}
```

Data lives in `~/.workhorse-ai/journal.db` (override with `WORKHORSE_DB`).
On first start the server creates the directory and the database itself.
`sync.json` always sits next to the database.

## Connect to the cloud (optional)

`connect` needs only a token — the managed Workhorse AI cloud is the default.
The token is **personal** (a PAT, like on GitHub): one token covers every
workspace you are a member of, and the journal is routed between them by
`sync_scope`:

```
connect { "token": "wh_..." }
```

### Self-hosted (on-premise)

Pass the **base URL** of your instance; endpoint paths are derived by the server,
so a reverse-proxy prefix works as-is:

```
connect { "url": "https://workhorse.acme.internal", "token": "wh_..." }
connect { "url": "https://tools.acme.com/workhorse", "token": "wh_..." }
```

The resolved base is stored in `sync.json` next to the database. To point every
run at your instance without passing a URL, set `WORKHORSE_CLOUD_URL`.

### Configure from `.mcp.json` instead

If you would rather keep the credentials with the rest of your MCP config —
no `connect` call, no `sync.json` — pass them as environment variables. They
take precedence over the file:

```json
{
  "mcpServers": {
    "workhorse": {
      "command": "npx",
      "args": ["-y", "workhorse-ai-mcp"],
      "env": {
        "WORKHORSE_SYNC_URL": "https://app.workhorse-ai.dev",
        "WORKHORSE_SYNC_TOKEN": "wh_..."
      }
    }
  }
}
```

`WORKHORSE_SYNC_URL` takes the same **base URL** as `connect`. The journal id
defaults to `<user>-<host>`; override it with `WORKHORSE_SYNC_JOURNAL_ID` when
one machine feeds several journals.

### Several workspaces at once

One machine, one journal — but projects may belong to different teams. List the
targets in `sync.json` and the journal is pushed to every one of them, each with
its own cursor and its own scope:

```json
{
  "targets": [
    { "alias": "acme", "url": "https://wh.acme.internal", "token": "wh_...", "journalId": "kv-mac" },
    { "alias": "lab",  "url": "https://app.workhorse-ai.dev", "token": "wh_...", "journalId": "kv-mac" }
  ]
}
```

`connect { "alias": "lab", "token": "wh_..." }` adds a target instead of
replacing the config. On a flat config without an `alias` it overwrites, exactly
as before; once a `targets` list exists it replaces only its own entry — matched
by alias, or by url plus journal id — and leaves the neighbours alone. The
flat single-target form (`{url, token, journalId}`) keeps working untouched, and
so do the `WORKHORSE_SYNC_*` variables — they describe one target, so when a
`targets` list is present they are ignored with a line on stderr rather than
silently adding a third destination.

A target that is down does not hold up the others: the push reports per target,
and a failing one is a line on stderr, never a crash.

### Which projects are pushed (sync scope)

The journal is one per machine and holds every project you work on, while a
cloud workspace belongs to a team. The token is personal, so the cloud reports
every workspace you are a member of, and each push fans the journal out across
them by the project mapping. Bind projects to a workspace by its **slug**:

```
sync_scope {}                                                  # what would be pushed where, and why
sync_scope { "workspace": "acme", "projects": ["acme-web"] }   # bind these to that workspace
```

`sync_scope` asks the cloud for the workspace list itself and records the id as
`cloud_workspace_id` in the project registry — no manual ids, no SQLite editing.
With a single workspace the `workspace` argument may be omitted. With several
targets (servers), binding also names the target by its alias:
`sync_scope { "target": "acme", "workspace": "team", "projects": ["acme-web"] }`.
`WORKHORSE_SYNC_PROJECTS="acme-web,acme-api"` overrides the registry for one
process, but only while you have a single workspace — with several it names the
projects yet not the destination, so it is ignored with a warning. With no
mapping anywhere: a single workspace receives everything (with a warning, as
before); with several workspaces nothing is pushed until you bind projects —
privacy over convenience.

While a scope is active, events that belong to no project (journal-level
incidents, `_general`) and `ProjectRegistered` stay local. Widening the scope
re-pushes from seq 0 so previously filtered events catch up; the cloud drops
duplicates by seq. The last scope is remembered per target in `sync-state.json`
next to the database (keyed by workspace id), so widening the scope of one target
does not re-push everything to the others.

If your instance uses a certificate from an internal CA, give Node the root
certificate — otherwise the TLS handshake fails and `connect` refuses to write
the config:

```
NODE_EXTRA_CA_CERTS=/etc/ssl/certs/acme-root.pem
```

Sync is one-way: the journal is pushed up, the cloud never rewrites it. The
cursor request is sent with `Cache-Control: no-store`, so a caching proxy in
front of an on-premise instance cannot serve a stale cursor.

## Skills

The package ships three skills — pick the one that matches your setup:

| Skill | Install it on | What it teaches |
|---|---|---|
| `workhorse-ai-orchestrator` | the agent that assigns and accepts work | the full discipline: bootstrap, drafting assignments, line-by-line review, acceptance by your own test run |
| `workhorse-ai-worker` | the agent that executes delegated tasks | the three journal duties, the report format, and the prohibitions (never accept your own work) |
| `workhorse-ai-all` | a single agent playing both roles | the solo discipline: the report and the acceptance stay separate acts with separate evidence |

Install via the Claude Code plugin (all three come along), or pick one:

```bash
npx skills add workhorse-ai/workhorse-ai-mcp --skill workhorse-ai-orchestrator
```

## License

MIT
