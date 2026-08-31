---
name: workhorse-ai
description: Use when delegating substantive implementation work (feature, bugfix, refactor, tests) to another agent and accepting the result — the discipline of the workhorse delegation journal (draft → delegate → report → own verification → accept).
---

# Workhorse AI — orchestration discipline

## Overview

Work splits into two roles: the **orchestrator** (plans, writes the assignment,
verifies, accepts) and the **worker** (writes code against a finished assignment).
The core of the discipline: *zero trust in the worker's report, full trust in your
own verification*. A worker can be honestly wrong about its own success — its test
run is information, not proof.

The delegation journal is the `workhorse` MCP server (`mcp__workhorse__*` tools):
an append-only event log over SQLite, with task and incident projections and
full-text search. The server validates status transitions, so you never need — and
must never try — to write to the database by hand. Do not create task files in the
project repository: state lives in the journal only.

## Requirements

This skill drives the **workhorse MCP server** — without it, none of the tools
referenced below exist. If your agent has no `workhorse` server connected, add
it first (or ask the user to):

```json
{ "mcpServers": { "workhorse": { "command": "npx", "args": ["-y", "workhorse-ai-mcp"] } } }
```

The server needs Node.js >= 22.5, stores data in `~/.workhorse-ai/journal.db`,
and creates the database itself on first start. It also announces its protocol
in the MCP `initialize` instructions, so the status machine below is always
available to the agent even without this skill.

Tool inventory (names may be prefixed by your client, e.g. `mcp__workhorse__*`):

| Group | Tools |
|---|---|
| Registry | `resolve_project`, `register_project`, `list_projects` |
| Task lifecycle | `draft_task`, `delegate`, `submit_report`, `accept`, `request_rework`, `mark_failed` |
| Knowledge | `search_precedents`, `record_artifact`, `record_incident`, `list_artifacts` |
| Overview | `get_task`, `list_tasks`, `link_tasks` |
| Cloud (optional) | `connect`, `sync`, `inbox`, `take` |

## Roles

| Stage | Who | Why |
|---|---|---|
| Diagnosis, root cause | orchestrator | An assignment without a proven cause sends the worker into open-ended research and drift |
| Plan and decomposition | orchestrator | Scope control before the work, not after |
| Writing code | worker | Against a finished assignment, with explicit prohibitions |
| Diff review | orchestrator | Line by line, before any test run |
| Build and full test run | **orchestrator only** | Never delegated |
| Commit / push / deploy | **orchestrator only** | Only after your own green run |

A one-to-three-line fix with a diagnosis already in hand is done inline:
delegating costs more than the edit itself.

The roles are **acts, not separate programs**. Any of these configurations
works, as long as writing the code and accepting it stay separate acts:

| Configuration | Orchestrator | Worker |
|---|---|---|
| Two CLIs | one agent (e.g. Claude Code) | another agent (e.g. a coding CLI) |
| One CLI | the main session | a subagent it spawns |
| Solo | the same single agent | the same single agent |

In the **solo** configuration the invariant does not disappear — it becomes a
guard against self-deception. The rule: *the report and the acceptance are
separate acts with separate evidence*. Set `REPORTED` when the work seems done;
`ACCEPTED` only after a fresh, complete test run over the final state of the
code, a line-by-line diff review against the assignment, and a `verify_commit`.
"I just wrote this myself" is never evidence.

## Statuses and the key invariant

```
DRAFT → DELEGATED → REPORTED → ACCEPTED | REWORK (→ DELEGATED …) | FAILED
```

`REPORTED` ("the worker believes it is done") **≠** `ACCEPTED`
("the orchestrator verified it personally"). This is the journal's central
invariant: between those two statuses there must be a full test run performed
by the orchestrator.

## Bootstrap — a hard precondition

Until a project has been bootstrapped, do not work on it at all: no `draft_task`,
no delegation, no artifacts. Bootstrap is two steps:

1. `register_project` — namespace name and `root_path`. Always call
   `resolve_project` first (by path and/or name): the server rejects near-duplicates
   (`foo` ~ `foo-app`, nested paths); deliberate override is `force: true`.
2. An artifact titled `Project baseline: <project>` (kind `spec`): stack and
   conventions, build and test commands, **the baseline number of a full test run**,
   and prohibitions for workers.

Check on every entry into delegation: the project is in `list_projects` **and** the
baseline is in `list_artifacts`. If not — stop and bootstrap (with a real full test
run), then continue. "It's a small task" is not an exception: without a reference
number, acceptance cannot be proven. A project that does not need the journal simply
should not be registered.

After substantial changes to the project, update the baseline: the same `title`
creates a new version, the old one stays in history.

## Working order

1. **`search_precedents`** — mandatory BEFORE writing the assignment: similar tasks,
   artifacts and incidents across all projects. Remembering is cheaper than repeating.
2. **`record_artifact`** — if the decision came out of a discussion, capture the
   spec / plan / ADR / decision (kind: `spec`/`plan`/`adr`/`decision`/`note`) BEFORE
   delegating. Record a significant decision immediately — otherwise it dies with
   the session.
3. **`draft_task`** — the full assignment text in `task_text`. A good assignment
   consists, in order, of:
   - **context**: repository, branch, "read the project instructions", stack rules;
   - **a finished root cause**: exact `file:line`, evidence, a reference behaviour —
     "do not re-investigate";
   - **the order of work**: TDD — failing test (capture the output) → minimal fix →
     targeted run → full run against the stated baseline ("baseline is N/N, any new
     failure is yours");
   - **prohibitions**: do not commit, do not push, do not touch the listed files and
     directories, do not widen scope, do not add dependencies;
   - **the report format**: per item "was failing → now passes" with command output,
     the list of changed files, and findings.

   If the worker must judge something on its own, give it a stopping criterion
   ("found a consumer → do not delete, report back").
4. **`delegate`** (plus starting the worker — another CLI, a subagent, or, in the solo configuration, simply switching hats). The worker's prompt carries: the
   `task_id`, the assignment text, and three journal duties — `search_precedents`
   before starting; progress along the way via `record_artifact` (kind `note`, bound
   to `task_id`, with a stable `title` such as `progress: <slug>`, which versions
   itself); `submit_report` at the end. It is worth duplicating the report into a
   file: a file survives a crash of the worker's shell, a journal event does not if
   the crash happens before `submit_report`.
5. While the worker runs, the orchestrator does not wait blindly: poll `get_task`
   and `list_artifacts` by `task_id`. If the worker dies before `submit_report`, the
   orchestrator submits the report to the journal itself, from the file.
6. **Acceptance**: read the report → review the **diff line by line** against the
   assignment's scope → your own build and **full** test run → compare with the
   baseline number. New failures → suspect interference first (orphaned background
   builds, races over build artifacts, infrastructure flakes), only then the code.
7. Exactly one of: **`accept`** (only after a green run and a commit, with
   `verify_commit`) / **`request_rework`** (then `delegate` again) / **`mark_failed`**.
8. Hit a trap → **`record_incident`**: the symptom and the lesson for next time.

State overview: `get_task` (task plus its event history), `list_tasks` (filters by
status and project), `list_artifacts`.

## Journal rules

- Status changes only through an MCP event. A direct INSERT past the server bypasses
  transition validation.
- Artifacts are versioned: recording the same `title` again creates a new version;
  the old one stays in history.
- Continuing an already closed task is always a **new** task plus `link_tasks`
  (`kind: continues`) pointing at the old one; the rework cycle only lives inside an
  open task. Work discovered while doing something else becomes a new task plus
  `discovered_from`.
- The journal is information for search and retrospectives. Proof of acceptance is
  the orchestrator's own test run, not a record in the database.

## Known traps

| Symptom / temptation | Reality |
|---|---|
| "The worker wrote: all tests passed" | The worker's run is information, not proof. A sandbox can swallow a failure as a timeout. |
| The worker hangs on a build or a test run | Do not wait it out: stop the task, the code is usually already written, and verification is yours anyway. |
| A sudden burst of failures, the run taking far longer than usual | Almost always interference from orphaned build processes, not the diff. Kill them and re-run. |
| Two or three green runs in a row "proved" the suite is stable | With a flaky suite that streak happens on its own. Tie failures to a change only after A/B (stash → run → pop → run), at least three pairs. |
| "The task is simple, let the worker find the root cause" | Without a finished diagnosis the worker re-investigates, drifts and burns limits. Diagnosis is the orchestrator's job. |
| "I'll keep their commit, they are the author" | A commit is accepted responsibility. Whoever verified it commits it. |
| "I'll register the project from memory" | Near-duplicates appear instantly, and a rename-migration of the database has been known to destroy data. Call `resolve_project` first; move a database only with a backup. |
| Retyping a token by hand, "it's just one line" | One typo means broken credentials. Copy only, then verify what was written. |
| Mixing work in a worktree and in the main repository in one command | Merge answers "Already up to date" and the run does nothing useful. Split them into separate calls. |

## The cloud — an optional layer

The journal works fully offline; cloud sync sits on top and changes nothing about
the discipline.

- **`connect`** — checks the connection and writes the sync config next to the
  database. After that every journal write is pushed upstream fire-and-forget.
- **`sync`** — a forced push (strictly one direction: the journal goes up, the cloud
  never rewrites the journal).
- **`inbox`** / **`take`** — task intents arrive from the cloud: `inbox` shows the
  queue, `take` claims an intent. An intent is not yet an assignment: it still goes
  through `draft_task` with the full text.

An unreachable cloud must never block the work: the local journal is the source of
truth, the push is only transport.
