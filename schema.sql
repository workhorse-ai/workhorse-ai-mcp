-- workhorse.db — журнал делегирования оркестратор ↔ рабочая лошадка
-- Event sourcing в миниатюре: events — источник истины (append-only),
-- tasks/incidents — материализованные проекции, обновляются триггерами.

PRAGMA journal_mode = WAL;

-- ============ источник истины ============

CREATE TABLE IF NOT EXISTS events (
    seq     INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    type    TEXT NOT NULL CHECK (type IN (
                'TaskDrafted',      -- payload: project, title, task_text
                'Delegated',        -- payload: executor
                'ReportSubmitted',  -- payload: report
                'Accepted',         -- payload: outcome, verify_commit
                'ReworkRequested',  -- payload: reason
                'Failed',           -- payload: reason
                'IncidentRecorded', -- payload: description, lesson
                'ArtifactRecorded', -- payload: project, kind, title, body
                'TaskLinked',       -- payload: to_task_id, kind
                'ProjectRegistered' -- payload: name, root_path, cloud_workspace_id
            )),
    payload TEXT NOT NULL DEFAULT '{}',
    at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id);

-- журнал только дописывается, никогда не правится
CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;

CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;

-- ============ инварианты уровня БД ============
-- Дублируют серверные guard'ы, чтобы fallback-вставки мимо MCP не ломали протокол.

CREATE TRIGGER IF NOT EXISTS guard_payload_json BEFORE INSERT ON events
WHEN NOT json_valid(NEW.payload)
BEGIN SELECT RAISE(ABORT, 'payload must be valid JSON'); END;

CREATE TRIGGER IF NOT EXISTS guard_project_registered BEFORE INSERT ON events
WHEN NEW.type IN ('TaskDrafted', 'ArtifactRecorded')
 AND (json_extract(NEW.payload, '$.project') IS NULL
      OR NOT EXISTS (SELECT 1 FROM projects WHERE name = json_extract(NEW.payload, '$.project')))
BEGIN SELECT RAISE(ABORT, 'project missing or not registered'); END;

CREATE TRIGGER IF NOT EXISTS guard_redraft BEFORE INSERT ON events
WHEN NEW.type = 'TaskDrafted'
 AND EXISTS (SELECT 1 FROM tasks WHERE task_id = NEW.task_id AND status NOT IN ('DRAFT', 'REWORK'))
BEGIN SELECT RAISE(ABORT, 'task already past DRAFT/REWORK — start a new task'); END;

CREATE TRIGGER IF NOT EXISTS guard_delegated BEFORE INSERT ON events
WHEN NEW.type = 'Delegated'
 AND coalesce((SELECT status FROM tasks WHERE task_id = NEW.task_id), 'NONE') NOT IN ('DRAFT', 'REWORK')
BEGIN SELECT RAISE(ABORT, 'Delegated allowed only from DRAFT/REWORK'); END;

CREATE TRIGGER IF NOT EXISTS guard_report BEFORE INSERT ON events
WHEN NEW.type = 'ReportSubmitted'
 AND coalesce((SELECT status FROM tasks WHERE task_id = NEW.task_id), 'NONE') <> 'DELEGATED'
BEGIN SELECT RAISE(ABORT, 'ReportSubmitted allowed only from DELEGATED'); END;

CREATE TRIGGER IF NOT EXISTS guard_accept_rework BEFORE INSERT ON events
WHEN NEW.type IN ('Accepted', 'ReworkRequested')
 AND coalesce((SELECT status FROM tasks WHERE task_id = NEW.task_id), 'NONE') <> 'REPORTED'
BEGIN SELECT RAISE(ABORT, 'Accepted/ReworkRequested allowed only from REPORTED'); END;

CREATE TRIGGER IF NOT EXISTS guard_failed BEFORE INSERT ON events
WHEN NEW.type = 'Failed'
 AND coalesce((SELECT status FROM tasks WHERE task_id = NEW.task_id), 'NONE') NOT IN ('DELEGATED', 'REPORTED', 'REWORK')
BEGIN SELECT RAISE(ABORT, 'Failed allowed only from DELEGATED/REPORTED/REWORK'); END;

CREATE TRIGGER IF NOT EXISTS guard_link BEFORE INSERT ON events
WHEN NEW.type = 'TaskLinked'
 AND (NOT EXISTS (SELECT 1 FROM tasks WHERE task_id = NEW.task_id)
      OR NOT EXISTS (SELECT 1 FROM tasks WHERE task_id = json_extract(NEW.payload, '$.to_task_id'))
      OR json_extract(NEW.payload, '$.to_task_id') = NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'link endpoints must be distinct existing tasks'); END;

-- ============ проекции ============

CREATE TABLE IF NOT EXISTS tasks (
    task_id       TEXT PRIMARY KEY,
    project       TEXT,
    title         TEXT,
    task_text     TEXT,
    report_text   TEXT,
    status        TEXT,  -- DRAFT / DELEGATED / REPORTED / ACCEPTED / REWORK / FAILED
    outcome       TEXT,  -- accepted / reworked / failed
    verify_commit TEXT,
    executor      TEXT,
    created_at    TEXT,
    updated_at    TEXT
);

CREATE TABLE IF NOT EXISTS incidents (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT,
    description TEXT,
    lesson      TEXT,
    at          TEXT
);

-- Реестр проектов: имя = неймспейс задач, root_path = привязка к папке на диске,
-- cloud_workspace_id = маппинг на пространство в облаке Workhorse AI (задачи/артефакты/доки
-- наследуют workspace через проект). Перерегистрация = обновление.
CREATE TABLE IF NOT EXISTS projects (
    name                 TEXT PRIMARY KEY,
    root_path            TEXT,
    cloud_workspace_id TEXT,
    at                   TEXT
);

-- Связи задач: направленные, from → to. continues = «from продолжает to».
CREATE TABLE IF NOT EXISTS task_links (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    from_task TEXT NOT NULL,
    to_task   TEXT NOT NULL,
    kind      TEXT CHECK (kind IN ('continues', 'relates', 'blocks', 'discovered_from')),
    at        TEXT,
    UNIQUE (from_task, to_task, kind)
);

-- Артефакты: спеки, планы, ADR, решения, заметки. Append-only версии:
-- повторная запись с тем же (project, kind, title) — новая версия, старая остаётся.
-- Маппинг на облако: spec/plan → Documentation, adr → Adr, note/decision → Thought.
CREATE TABLE IF NOT EXISTS artifacts (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT,   -- опциональная привязка к задаче
    project TEXT,
    kind    TEXT CHECK (kind IN ('spec', 'plan', 'adr', 'decision', 'note', 'doc')),
    title   TEXT,
    body    TEXT,
    at      TEXT
);

-- ============ применение событий ============

CREATE TRIGGER IF NOT EXISTS evt_task_drafted AFTER INSERT ON events
WHEN NEW.type = 'TaskDrafted'
BEGIN
    INSERT INTO tasks(task_id, project, title, task_text, status, created_at, updated_at)
    VALUES (NEW.task_id,
            json_extract(NEW.payload, '$.project'),
            json_extract(NEW.payload, '$.title'),
            json_extract(NEW.payload, '$.task_text'),
            'DRAFT', NEW.at, NEW.at)
    ON CONFLICT(task_id) DO UPDATE SET
        title      = excluded.title,
        task_text  = excluded.task_text,
        updated_at = NEW.at;
END;

CREATE TRIGGER IF NOT EXISTS evt_delegated AFTER INSERT ON events
WHEN NEW.type = 'Delegated'
BEGIN
    UPDATE tasks SET status = 'DELEGATED',
                     executor = json_extract(NEW.payload, '$.executor'),
                     updated_at = NEW.at
    WHERE task_id = NEW.task_id;
END;

CREATE TRIGGER IF NOT EXISTS evt_report_submitted AFTER INSERT ON events
WHEN NEW.type = 'ReportSubmitted'
BEGIN
    UPDATE tasks SET status = 'REPORTED',
                     report_text = json_extract(NEW.payload, '$.report'),
                     updated_at = NEW.at
    WHERE task_id = NEW.task_id;
END;

CREATE TRIGGER IF NOT EXISTS evt_accepted AFTER INSERT ON events
WHEN NEW.type = 'Accepted'
BEGIN
    UPDATE tasks SET status = 'ACCEPTED',
                     outcome = coalesce(json_extract(NEW.payload, '$.outcome'), 'accepted'),
                     verify_commit = json_extract(NEW.payload, '$.verify_commit'),
                     updated_at = NEW.at
    WHERE task_id = NEW.task_id;
END;

CREATE TRIGGER IF NOT EXISTS evt_rework_requested AFTER INSERT ON events
WHEN NEW.type = 'ReworkRequested'
BEGIN
    UPDATE tasks SET status = 'REWORK', updated_at = NEW.at
    WHERE task_id = NEW.task_id;
END;

CREATE TRIGGER IF NOT EXISTS evt_failed AFTER INSERT ON events
WHEN NEW.type = 'Failed'
BEGIN
    UPDATE tasks SET status = 'FAILED',
                     outcome = 'failed',
                     updated_at = NEW.at
    WHERE task_id = NEW.task_id;
END;

CREATE TRIGGER IF NOT EXISTS evt_incident_recorded AFTER INSERT ON events
WHEN NEW.type = 'IncidentRecorded'
BEGIN
    INSERT INTO incidents(task_id, description, lesson, at)
    VALUES (NEW.task_id,
            json_extract(NEW.payload, '$.description'),
            json_extract(NEW.payload, '$.lesson'),
            NEW.at);
END;

CREATE TRIGGER IF NOT EXISTS evt_project_registered AFTER INSERT ON events
WHEN NEW.type = 'ProjectRegistered'
BEGIN
    INSERT INTO projects(name, root_path, cloud_workspace_id, at)
    VALUES (json_extract(NEW.payload, '$.name'),
            json_extract(NEW.payload, '$.root_path'),
            json_extract(NEW.payload, '$.cloud_workspace_id'),
            NEW.at)
    ON CONFLICT(name) DO UPDATE SET
        root_path            = excluded.root_path,
        cloud_workspace_id = coalesce(excluded.cloud_workspace_id, cloud_workspace_id),
        at                   = NEW.at;
END;

CREATE TRIGGER IF NOT EXISTS evt_task_linked AFTER INSERT ON events
WHEN NEW.type = 'TaskLinked'
BEGIN
    INSERT OR IGNORE INTO task_links(from_task, to_task, kind, at)
    VALUES (NEW.task_id,
            json_extract(NEW.payload, '$.to_task_id'),
            json_extract(NEW.payload, '$.kind'),
            NEW.at);
END;

CREATE TRIGGER IF NOT EXISTS evt_artifact_recorded AFTER INSERT ON events
WHEN NEW.type = 'ArtifactRecorded'
BEGIN
    INSERT INTO artifacts(task_id, project, kind, title, body, at)
    VALUES (CASE WHEN NEW.task_id = '_general' THEN NULL ELSE NEW.task_id END,
            json_extract(NEW.payload, '$.project'),
            json_extract(NEW.payload, '$.kind'),
            json_extract(NEW.payload, '$.title'),
            json_extract(NEW.payload, '$.body'),
            NEW.at);
END;

-- ============ полнотекстовый поиск ============

CREATE VIRTUAL TABLE IF NOT EXISTS task_fts USING fts5(
    title, task_text, report_text,
    content = 'tasks', content_rowid = 'rowid'
);

CREATE TRIGGER IF NOT EXISTS tasks_fts_insert AFTER INSERT ON tasks
BEGIN
    INSERT INTO task_fts(rowid, title, task_text, report_text)
    VALUES (NEW.rowid, NEW.title, NEW.task_text, NEW.report_text);
END;

CREATE TRIGGER IF NOT EXISTS tasks_fts_update AFTER UPDATE ON tasks
BEGIN
    INSERT INTO task_fts(task_fts, rowid, title, task_text, report_text)
    VALUES ('delete', OLD.rowid, OLD.title, OLD.task_text, OLD.report_text);
    INSERT INTO task_fts(rowid, title, task_text, report_text)
    VALUES (NEW.rowid, NEW.title, NEW.task_text, NEW.report_text);
END;

CREATE VIRTUAL TABLE IF NOT EXISTS artifact_fts USING fts5(
    title, body,
    content = 'artifacts', content_rowid = 'id'
);

CREATE TRIGGER IF NOT EXISTS artifacts_fts_insert AFTER INSERT ON artifacts
BEGIN
    INSERT INTO artifact_fts(rowid, title, body)
    VALUES (NEW.id, NEW.title, NEW.body);
END;
