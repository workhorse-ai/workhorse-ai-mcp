---
name: workhorse-ai-all
description: Use when one single agent both performs and accepts substantive work (feature, bugfix, refactor, tests) with the workhorse journal — the solo discipline where the report and the acceptance stay separate acts with separate evidence.
---

# Workhorse AI — solo: orchestrator and workhorse in one

One agent plays both roles. The invariant does not disappear — it becomes a
guard against self-deception: **the report and the acceptance are separate
acts with separate evidence**. "I just wrote this myself" is never evidence.

The journal is the `workhorse` MCP server (tool names may be prefixed, e.g.
`mcp__workhorse__*`). If it is not connected, add it first:

```json
{ "mcpServers": { "workhorse": { "command": "npx", "args": ["-y", "workhorse-ai-mcp"] } } }
```

## Statuses and the key invariant

```
DRAFT → DELEGATED → REPORTED → ACCEPTED | REWORK (→ DELEGATED …) | FAILED
```

`REPORTED` ("the work seems done") **≠** `ACCEPTED` ("verified against the
final state of the code"). Between them there must be a fresh, complete test
run — even though both acts are yours.

## Bootstrap — a hard precondition

Before any task on a project: `resolve_project` → `register_project`, plus an
artifact `Project baseline: <project>` (kind `spec`) with the stack, the build
and test commands, **the baseline number of a full test run**, and standing
prohibitions. No baseline — no tasks: without a reference number acceptance
cannot be proven. Update the baseline (same title = new version) after
substantial changes.

## The solo loop

1. **`search_precedents`** — before drafting: similar tasks, artifacts,
   incidents. Remembering is cheaper than repeating.
2. **`record_artifact`** — capture the spec / plan / decision born in the
   discussion (kind `spec`/`plan`/`adr`/`decision`/`note`) before the work,
   or it dies with the session.
3. **`draft_task`** — a full assignment even for yourself: context, the
   proven root cause, the order of work against the stated baseline,
   prohibitions, report format. Writing it down is what makes step 7
   verifiable later.
4. **`delegate`** — executor `inline` (or spawn a subagent and hand the
   assignment over; then you are back to two roles and the orchestrator
   skill applies).
5. Do the work. Record progress via `record_artifact`
   (kind `note`, stable title `progress: <slug>`).
6. **`submit_report`** — per item "was failing → now passes" with command
   output, changed files, findings. Set it down honestly *before* switching
   into the accepting mindset.
7. **Acceptance as a separate act**: re-read the assignment → review your
   own diff line by line against its scope → run the **full** test suite
   over the final state → compare with the baseline number. Only then
   exactly one of: **`accept`** (with `verify_commit` after committing) /
   **`request_rework`** (and back to work) / **`mark_failed`**.
8. Hit a trap → **`record_incident`**: the symptom and the lesson.

## Solo traps

| Temptation | Reality |
|---|---|
| "I ran the tests while writing — that counts" | It does not. The accepting run happens after the last edit, over the final state. |
| "The diff is fresh in my memory, skipping the review" | Memory holds intent, not what is actually on disk. Read the diff. |
| "Skipping draft_task — I know what I'm doing" | Without a written assignment, step 7 has nothing to verify against. |
| "One green run is enough on a flaky suite" | Tie failures to your change only via A/B (stash → run → pop → run). |
| "I'll fix this unrelated thing while I'm here" | Out-of-scope work becomes a new task (`discovered_from`), not a silent widening. |

## Journal rules

Status changes only through MCP events. Artifacts version by title.
Continuing a closed task = a new task + `link_tasks` (`kind: continues`).
The journal is for search and retrospectives; proof of acceptance is the
verifying test run, not a database record.
