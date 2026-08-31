# workhorse-ai-mcp

A delegation journal for orchestrator/worker AI workflows: an append-only
event log over SQLite (tasks, reports, artifacts, incidents, full-text
search) exposed as an MCP server. Zero dependencies — only Node.js >= 22.5
with the built-in `node:sqlite`.

The journal enforces a simple discipline: `DRAFT → DELEGATED → REPORTED →
ACCEPTED | REWORK | FAILED`, where *reported* (the worker thinks it is done)
is never the same as *accepted* (the orchestrator verified it).

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

The journal works fully offline. To sync it with a Planado workspace, issue
an MCP token on the workspace's MCP page and ask your agent to call the
`connect` tool:

```json
connect {
  "url": "https://<your-workspace>/api/mcp/journal-sync",
  "token": "<mcp token>"
}
```

`connect` verifies the connection first and only then writes `sync.json`
next to the database — no manual configuration. The journal id defaults to
a normalized `<username>-<hostname>`. After that, every journal write is
auto-pushed; `sync` forces a push, and `inbox`/`take` pull task intents
from the cloud.

## Connecting to a cloud

`connect` needs only a token — the managed Workhorse AI cloud is the default:

```
connect { "token": "pln_..." }
```

### Self-hosted (on-premise)

Pass the **base URL** of your instance; endpoint paths are derived by the server,
so a reverse-proxy prefix works as-is:

```
connect { "url": "https://workhorse.acme.internal", "token": "pln_..." }
connect { "url": "https://tools.acme.com/workhorse", "token": "pln_..." }
```

The resolved base is stored in `sync.json` next to the database. To point every
run at your instance without passing a URL, set `WORKHORSE_CLOUD_URL`.

If your instance uses a certificate from an internal CA, give Node the root
certificate — otherwise the TLS handshake fails and `connect` refuses to write
the config:

```
NODE_EXTRA_CA_CERTS=/etc/ssl/certs/acme-root.pem
```

Sync is one-way: the journal is pushed up, the cloud never rewrites it. The
cursor request is sent with `Cache-Control: no-store`, so a caching proxy in
front of an on-premise instance cannot serve a stale cursor.

## Orchestration skill

The package ships `skills/workhorse-ai/SKILL.md` (in Russian) — the delegation
discipline the journal is built around: orchestrator/worker roles, the
`REPORTED != ACCEPTED` invariant, the mandatory project bootstrap, and the
working order from `search_precedents` to `accept`.

Copy it into your agent's skill directory (for Claude Code:
`~/.claude/skills/workhorse-ai/SKILL.md`), or just hand the file to the agent
as instructions.

## License

MIT
