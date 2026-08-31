-- Переименование planado_workspace_id → cloud_workspace_id (v0.8.0).
-- Старое имя было привязано к стухшему бренду и уехало бы в публичный API.
-- Колонка везде NULL (маппинг не реализован), так что перенос данных не нужен.
--
-- ВАЖНО: RENAME COLUMN чинит ссылки-колонки в триггерах, но НЕ трогает
-- json-путь '$.planado_workspace_id' внутри строкового литерала — поэтому
-- триггер пересоздаётся целиком.
--
-- Перед запуском: sqlite3 <db> ".backup <db>.bak-before-rename"

ALTER TABLE projects RENAME COLUMN planado_workspace_id TO cloud_workspace_id;

DROP TRIGGER IF EXISTS evt_project_registered;

CREATE TRIGGER evt_project_registered AFTER INSERT ON events
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
