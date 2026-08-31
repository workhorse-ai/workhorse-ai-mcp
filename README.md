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
