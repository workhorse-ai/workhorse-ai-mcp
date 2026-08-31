#!/usr/bin/env node
// workhorse-mcp — MCP-сервер журнала делегирования оркестратор ↔ рабочая лошадка.
// Хранилище: ~/.workhorse-ai/journal.db (event sourcing: append-only events,
// проекции tasks/incidents через триггеры, FTS5); путь перекрывается
// WORKHORSE_DB — см. resolveDbPath в sync.mjs.
// Zero deps: node:sqlite (Node >= 22.5).

import { DatabaseSync } from "node:sqlite";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import {
	defaultJournalId,
	inboxUrlFromSyncUrl,
	loadSyncConfig,
	pushJournal,
	resolveDbPath,
	writeSyncConfig,
} from "./sync.mjs";

const DB_PATH = resolveDbPath();
const SCHEMA_PATH =
	process.env.WORKHORSE_SCHEMA ?? join(dirname(fileURLToPath(import.meta.url)), "schema.sql");

// Чистый старт на пустой машине: директория данных создаётся сама,
// иначе node:sqlite падает CANTOPEN ещё до применения схемы.
mkdirSync(dirname(DB_PATH), { recursive: true });

const db = new DatabaseSync(DB_PATH);
const hasEvents = db
	.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
	.get();
if (!hasEvents) {
	if (!existsSync(SCHEMA_PATH)) {
		console.error(`workhorse-mcp: база пуста, а схема не найдена: ${SCHEMA_PATH}`);
		process.exit(1);
	}
	db.exec(readFileSync(SCHEMA_PATH, "utf8"));
}

// Допустимые переходы: событие → из каких статусов его можно применить.
const VALID_FROM = {
	Delegated: ["DRAFT", "REWORK"],
	ReportSubmitted: ["DELEGATED"],
	Accepted: ["REPORTED"],
	ReworkRequested: ["REPORTED"],
	Failed: ["DELEGATED", "REPORTED", "REWORK"],
};

const getTaskStmt = db.prepare("SELECT * FROM tasks WHERE task_id = ?");
const insertEventStmt = db.prepare(
	"INSERT INTO events(task_id, type, payload) VALUES (?, ?, ?)",
);

// Авто-пуш синка: fire-and-forget после успешной записи события. НИКОГДА не
// блокирует и не роняет запись: нет конфига — молчаливый no-op, ошибка/офлайн —
// одна строка в stderr (курсор просто отстаёт до следующего пуша).
let syncInFlight = false;
let syncQueued = false;
function scheduleAutoPush() {
	if (syncInFlight) {
		syncQueued = true;
		return;
	}
	syncInFlight = true;
	setImmediate(async () => {
		try {
			const config = loadSyncConfig({ dbPath: DB_PATH });
			if (config) {
				const result = await pushJournal({ dbPath: DB_PATH, config });
				if (result.error) {
					console.error(`workhorse-mcp: авто-пуш синка не прошёл: ${result.error}`);
				}
			}
		} catch (err) {
			console.error(`workhorse-mcp: авто-пуш синка не прошёл: ${err.message}`);
		} finally {
			syncInFlight = false;
			if (syncQueued) {
				syncQueued = false;
				scheduleAutoPush();
			}
		}
	});
}

// Общая точка записи события: вставка + авто-пуш (только после успешной вставки).
function appendEvent(taskId, type, payloadJson) {
	insertEventStmt.run(taskId, type, payloadJson);
	scheduleAutoPush();
}

function emit(taskId, type, payload) {
	const guard = VALID_FROM[type];
	if (guard) {
		const task = getTaskStmt.get(taskId);
		if (!task) throw new Error(`Задача ${taskId} не найдена`);
		if (!guard.includes(task.status)) {
			throw new Error(
				`${type} недопустим из статуса ${task.status} (допустимо из: ${guard.join(", ")})`,
			);
		}
	}
	appendEvent(taskId, type, JSON.stringify(payload ?? {}));
	return getTaskStmt.get(taskId);
}

// Префиксный матч каждого токена: русская морфология без стеммера —
// «стандарт» находит «стандарты/стандартов», «ранг» — «ранги».
function ftsExpr(query) {
	return query
		.split(/\s+/)
		.filter(Boolean)
		.map((t) => `"${t.replaceAll('"', '""')}"*`)
		.join(" OR ");
}

const getProjectStmt = db.prepare("SELECT * FROM projects WHERE name = ?");

// Нормализация имени проекта для поиска почти-дублей: dompro ~ dom-pro ~ DomPro
function normalizeProjectName(name) {
	return name.toLowerCase().replaceAll("-", "");
}

// Путь с гарантированным завершающим слэшем — для честного prefix-сравнения
function withSlash(p) {
	return p.endsWith("/") ? p : `${p}/`;
}

// Пересечение корней: равенство, вложенность в любую сторону
function pathsOverlap(a, b) {
	const A = withSlash(a);
	const B = withSlash(b);
	return A === B || A.startsWith(B) || B.startsWith(A);
}

function findProjectConflicts(name, root_path) {
	const all = db.prepare("SELECT name, root_path FROM projects").all();
	const norm = normalizeProjectName(name);
	return all.filter(
		(p) =>
			p.name !== name &&
			(normalizeProjectName(p.name) === norm || pathsOverlap(p.root_path, root_path)),
	);
}

function requireProject(name) {
	const p = getProjectStmt.get(name);
	if (!p) {
		throw new Error(
			`Проект "${name}" не зарегистрирован — сначала register_project (list_projects покажет существующие)`,
		);
	}
	return p;
}

function today() {
	const d = new Date();
	const mm = String(d.getMonth() + 1).padStart(2, "0");
	const dd = String(d.getDate()).padStart(2, "0");
	return `${d.getFullYear()}-${mm}-${dd}`;
}

// ============ инструменты ============

const TOOLS = [
	{
		name: "search_precedents",
		description:
			"ОБЯЗАТЕЛЬНО перед постановкой новой задачи: полнотекстовый поиск похожих задач " +
			"(задания + отчёты) по всем проектам. Возвращает совпавшие задачи и релевантные инциденты.",
		inputSchema: {
			type: "object",
			properties: {
				query: { type: "string", description: "Ключевые слова (любой язык)" },
				limit: { type: "number", description: "Максимум задач (default 5)" },
			},
			required: ["query"],
		},
		handler: ({ query, limit = 5 }) => {
			const expr = ftsExpr(query);
			const tasks = expr
				? db
						.prepare(
							`SELECT t.task_id, t.project, t.title, t.status, t.outcome, t.verify_commit,
							        snippet(task_fts, 1, '[', ']', '…', 12) AS task_snippet,
							        snippet(task_fts, 2, '[', ']', '…', 12) AS report_snippet
							 FROM task_fts JOIN tasks t ON t.rowid = task_fts.rowid
							 WHERE task_fts MATCH ? ORDER BY rank LIMIT ?`,
						)
						.all(expr, limit)
				: [];
			const words = query.split(/\s+/).filter(Boolean);
			const cond = words.map(() => "(description LIKE ? OR lesson LIKE ?)").join(" OR ");
			const args = words.flatMap((w) => [`%${w}%`, `%${w}%`]);
			const incidents = words.length
				? db
						.prepare(
							`SELECT task_id, description, lesson, at FROM incidents WHERE ${cond} ORDER BY at DESC LIMIT 10`,
						)
						.all(...args)
				: [];
			const artifacts = expr
				? db
						.prepare(
							`SELECT a.id, a.project, a.kind, a.title, a.task_id, a.at,
							        snippet(artifact_fts, 1, '[', ']', '…', 12) AS body_snippet
							 FROM artifact_fts JOIN artifacts a ON a.id = artifact_fts.rowid
							 WHERE artifact_fts MATCH ? ORDER BY rank LIMIT ?`,
						)
						.all(expr, limit)
				: [];
			return { tasks, artifacts, incidents };
		},
	},
	{
		name: "register_project",
		description:
			"Зарегистрировать проект (событие ProjectRegistered): имя = неймспейс задач, root_path = " +
			"привязка к папке на диске, cloud_workspace_id = маппинг на пространство в облаке (опционально). " +
			"Повторная регистрация обновляет путь/маппинг. Задачи и артефакты принимаются только для зарегистрированных проектов.",
		inputSchema: {
			type: "object",
			properties: {
				name: { type: "string", description: "Короткое имя (латиница/цифры/дефисы), напр. dom-pro" },
				root_path: { type: "string", description: "Абсолютный путь к корню проекта" },
				cloud_workspace_id: { type: "string", description: "Id пространства в облаке (опционально)" },
				force: {
					type: "boolean",
					description: "Осознанно зарегистрировать несмотря на похожий проект (guard от почти-дублей)",
				},
			},
			required: ["name", "root_path"],
		},
		handler: ({ name, root_path, cloud_workspace_id, force }) => {
			if (!/^[a-z0-9-]+$/.test(name)) {
				throw new Error("Имя проекта: латиница/цифры/дефисы — оно входит в task_id как неймспейс");
			}
			// Guard от почти-дублей: похожее имя или пересекающийся корень.
			// Точное совпадение имени — легальная перерегистрация (обновление пути).
			if (!force) {
				const conflicts = findProjectConflicts(name, root_path);
				if (conflicts.length > 0) {
					const listing = conflicts
						.map((p) => `"${p.name}" (${p.root_path})`)
						.join(", ");
					throw new Error(
						`Похоже, проект уже зарегистрирован: ${listing}. ` +
							"Используй существующее имя (resolve_project найдёт проект по пути), " +
							"либо повтори с force: true, если это осознанно отдельный проект.",
					);
				}
			}
			appendEvent(
				"_general",
				"ProjectRegistered",
				JSON.stringify({ name, root_path, cloud_workspace_id: cloud_workspace_id ?? null }),
			);
			return getProjectStmt.get(name);
		},
	},
	{
		name: "resolve_project",
		description:
			"Найти уже зарегистрированный проект ПЕРЕД register_project/бутстрапом: по пути (какой проект " +
			"покрывает эту папку) и/или по имени (похожие имена, dompro ~ dom-pro). Пустой ответ = проекта нет.",
		inputSchema: {
			type: "object",
			properties: {
				path: { type: "string", description: "Абсолютный путь (папка проекта или файла внутри него)" },
				name: { type: "string", description: "Предполагаемое имя проекта" },
			},
		},
		handler: ({ path, name }) => {
			const all = db.prepare("SELECT name, root_path, cloud_workspace_id, at FROM projects").all();
			const byPath = path
				? all.filter((p) => withSlash(path).startsWith(withSlash(p.root_path)) || pathsOverlap(p.root_path, path))
				: [];
			const norm = name ? normalizeProjectName(name) : null;
			const byName = norm
				? all.filter(
						(p) =>
							normalizeProjectName(p.name) === norm ||
							normalizeProjectName(p.name).includes(norm) ||
							norm.includes(normalizeProjectName(p.name)),
					)
				: [];
			const seen = new Set();
			const matches = [...byPath, ...byName].filter((p) => !seen.has(p.name) && seen.add(p.name));
			return { matches };
		},
	},
	{
		name: "list_projects",
		description: "Реестр проектов: имя, путь на диске, маппинг на пространство в облаке.",
		inputSchema: { type: "object", properties: {} },
		handler: () =>
			db.prepare("SELECT name, root_path, cloud_workspace_id, at FROM projects ORDER BY name").all(),
	},
	{
		name: "draft_task",
		description:
			"Поставить задачу в журнал (событие TaskDrafted). Полный текст задания — в task_text: " +
			"контекст, готовый root cause, порядок работ (TDD, базовая цифра тестов), запреты, формат отчёта.",
		inputSchema: {
			type: "object",
			properties: {
				project: { type: "string", description: "Имя проекта, напр. dom-pro" },
				slug: { type: "string", description: "Короткий латинский слаг, напр. gantt-ranks" },
				title: { type: "string", description: "Человекочитаемое название" },
				task_text: { type: "string", description: "Полный текст задания для исполнителя" },
				task_id: {
					type: "string",
					description: "Явный id (иначе соберётся как <project>/<дата>-<slug>)",
				},
				intent_task_id: {
					type: "string",
					description:
						"Id облачного Task-намерения (из take): связка journal→intent едет в payload события",
				},
			},
			required: ["project", "title", "task_text"],
		},
		handler: ({ project, slug, title, task_text, task_id, intent_task_id }) => {
			if (!task_id) {
				if (!slug || !/^[a-z0-9-]+$/.test(slug)) {
					throw new Error("Нужен slug (латиница/цифры/дефисы) или явный task_id");
				}
				task_id = `${project}/${today()}-${slug}`;
			} else if (!task_id.startsWith(`${project}/`)) {
				throw new Error(
					`task_id должен начинаться с "${project}/" — неймспейс проекта входит в id`,
				);
			}
			requireProject(project);
			const existing = getTaskStmt.get(task_id);
			if (existing && !["DRAFT", "REWORK"].includes(existing.status)) {
				throw new Error(
					`Задача ${task_id} уже в статусе ${existing.status} — редактировать нельзя, заведи новую`,
				);
			}
			// Связка с намерением — свободный payload события: схема БД не меняется,
			// облачный транслятор прочитает intent_task_id при материализации.
			return emit(task_id, "TaskDrafted", {
				project,
				title,
				task_text,
				...(intent_task_id ? { intent_task_id } : {}),
			});
		},
	},
	{
		name: "delegate",
		description:
			"Отметить делегацию задачи исполнителю (событие Delegated). Допустимо из DRAFT или REWORK. " +
			"Сам запуск исполнителя — отдельно; в его промпт включить текст задания и путь файла для отчёта.",
		inputSchema: {
			type: "object",
			properties: {
				task_id: { type: "string" },
				executor: { type: "string", description: "grok / codex / subagent / inline" },
			},
			required: ["task_id", "executor"],
		},
		handler: ({ task_id, executor }) => emit(task_id, "Delegated", { executor }),
	},
	{
		name: "submit_report",
		description: "Записать отчёт исполнителя (событие ReportSubmitted). Допустимо из DELEGATED.",
		inputSchema: {
			type: "object",
			properties: {
				task_id: { type: "string" },
				report: { type: "string", description: "Полный текст отчёта исполнителя" },
			},
			required: ["task_id", "report"],
		},
		handler: ({ task_id, report }) => emit(task_id, "ReportSubmitted", { report }),
	},
	{
		name: "accept",
		description:
			"Принять задачу (событие Accepted). ТОЛЬКО после собственной верификации: дифф построчно, " +
			"своя сборка, полный прогон тестов, коммит. Допустимо из REPORTED.",
		inputSchema: {
			type: "object",
			properties: {
				task_id: { type: "string" },
				verify_commit: { type: "string", description: "Хэш коммита, которым принята работа" },
				outcome: { type: "string", description: "accepted (default) | accepted-with-fixes" },
			},
			required: ["task_id", "verify_commit"],
		},
		handler: ({ task_id, verify_commit, outcome = "accepted" }) =>
			emit(task_id, "Accepted", { outcome, verify_commit }),
	},
	{
		name: "request_rework",
		description:
			"Вернуть задачу на доработку (событие ReworkRequested), из REPORTED. Дальше — новая delegate.",
		inputSchema: {
			type: "object",
			properties: {
				task_id: { type: "string" },
				reason: { type: "string", description: "Что именно не так (по итогам верификации)" },
			},
			required: ["task_id", "reason"],
		},
		handler: ({ task_id, reason }) => emit(task_id, "ReworkRequested", { reason }),
	},
	{
		name: "mark_failed",
		description: "Закрыть задачу как проваленную (событие Failed), из DELEGATED/REPORTED/REWORK.",
		inputSchema: {
			type: "object",
			properties: {
				task_id: { type: "string" },
				reason: { type: "string" },
			},
			required: ["task_id", "reason"],
		},
		handler: ({ task_id, reason }) => emit(task_id, "Failed", { reason }),
	},
	{
		name: "record_incident",
		description:
			"Записать грабли/урок (событие IncidentRecorded). Для инцидентов вне конкретной задачи " +
			"task_id можно опустить. Повторяющийся урок — продублировать в таблицу граблей скилла.",
		inputSchema: {
			type: "object",
			properties: {
				description: { type: "string", description: "Что случилось" },
				lesson: { type: "string", description: "Как обходить впредь" },
				task_id: { type: "string" },
			},
			required: ["description", "lesson"],
		},
		handler: ({ description, lesson, task_id = "_general" }) => {
			appendEvent(task_id, "IncidentRecorded", JSON.stringify({ description, lesson }));
			return { recorded: true, task_id, description, lesson };
		},
	},
	{
		name: "link_tasks",
		description:
			"Связать задачи (событие TaskLinked, направленно from → to). kind: continues " +
			"(«from — продолжение to»; для продолжения заводи НОВУЮ задачу и линкуй на старую), " +
			"relates, blocks («from блокирует to»), discovered_from («from найдена в ходе to»).",
		inputSchema: {
			type: "object",
			properties: {
				from_task_id: { type: "string" },
				to_task_id: { type: "string" },
				kind: { type: "string", description: "continues | relates | blocks | discovered_from" },
			},
			required: ["from_task_id", "to_task_id", "kind"],
		},
		handler: ({ from_task_id, to_task_id, kind }) => {
			if (!["continues", "relates", "blocks", "discovered_from"].includes(kind)) {
				throw new Error("kind: continues | relates | blocks | discovered_from");
			}
			if (from_task_id === to_task_id) throw new Error("Задача не может ссылаться на себя");
			for (const id of [from_task_id, to_task_id]) {
				if (!getTaskStmt.get(id)) throw new Error(`Задача ${id} не найдена`);
			}
			appendEvent(from_task_id, "TaskLinked", JSON.stringify({ to_task_id, kind }));
			return db
				.prepare("SELECT from_task, to_task, kind, at FROM task_links WHERE from_task = ?")
				.all(from_task_id);
		},
	},
	{
		name: "record_artifact",
		description:
			"Зафиксировать артефакт: спеку, план, ADR, решение или заметку (событие ArtifactRecorded). " +
			"Спека пишется ДО делегации; повторная запись с тем же title = новая версия (старая остаётся в истории).",
		inputSchema: {
			type: "object",
			properties: {
				project: { type: "string" },
				kind: { type: "string", description: "spec | plan | adr | decision | note | doc" },
				title: { type: "string" },
				body: { type: "string", description: "Полный текст артефакта (markdown)" },
				task_id: { type: "string", description: "Опциональная привязка к задаче журнала" },
			},
			required: ["project", "kind", "title", "body"],
		},
		handler: ({ project, kind, title, body, task_id = "_general" }) => {
			if (!["spec", "plan", "adr", "decision", "note", "doc"].includes(kind)) {
				throw new Error("kind: spec | plan | adr | decision | note | doc");
			}
			requireProject(project);
			appendEvent(task_id, "ArtifactRecorded", JSON.stringify({ project, kind, title, body }));
			return db
				.prepare("SELECT id, task_id, project, kind, title, at FROM artifacts ORDER BY id DESC LIMIT 1")
				.get();
		},
	},
	{
		name: "get_artifact",
		description: "Полный текст артефакта по id (id даёт search_precedents / list_artifacts).",
		inputSchema: {
			type: "object",
			properties: { id: { type: "number" } },
			required: ["id"],
		},
		handler: ({ id }) => {
			const a = db.prepare("SELECT * FROM artifacts WHERE id = ?").get(id);
			if (!a) throw new Error(`Артефакт ${id} не найден`);
			return a;
		},
	},
	{
		name: "list_artifacts",
		description: "Список артефактов с фильтрами по проекту/типу/задаче (без тела — только заголовки).",
		inputSchema: {
			type: "object",
			properties: {
				project: { type: "string" },
				kind: { type: "string", description: "spec | plan | adr | decision | note | doc" },
				task_id: { type: "string" },
				limit: { type: "number", description: "default 20" },
			},
		},
		handler: ({ project, kind, task_id, limit = 20 }) => {
			const cond = [];
			const args = [];
			if (project) { cond.push("project = ?"); args.push(project); }
			if (kind) { cond.push("kind = ?"); args.push(kind); }
			if (task_id) { cond.push("task_id = ?"); args.push(task_id); }
			const where = cond.length ? `WHERE ${cond.join(" AND ")}` : "";
			return db
				.prepare(`SELECT id, task_id, project, kind, title, at FROM artifacts ${where} ORDER BY id DESC LIMIT ?`)
				.all(...args, limit);
		},
	},
	{
		name: "connect",
		description:
			"Подключить журнал к облаку Workhorse AI: проверяет связь (GET курсора с токеном) и при успехе " +
			"сам пишет sync.json рядом с базой — ручная настройка не нужна. journal_id по умолчанию " +
			"собирается из имени пользователя и машины. Повторный connect перезаписывает конфиг " +
			"(осознанная смена воркспейса/токена). При ошибке связи конфиг не трогается.",
		inputSchema: {
			type: "object",
			properties: {
				url: {
					type: "string",
					description: "URL синка воркспейса, напр. https://<ваш-хост>/api/mcp/journal-sync",
				},
				token: { type: "string", description: "MCP-токен воркспейса (Bearer)" },
				journal_id: {
					type: "string",
					description: "Имя журнала (default: <username>-<hostname>, нормализованное)",
				},
			},
			required: ["url", "token"],
		},
		handler: async ({ url, token, journal_id }) => {
			const journalId = journal_id ?? defaultJournalId();
			if (!/^[a-z0-9-]+$/.test(journalId)) {
				return `не подключено: journal_id «${journalId}» — допустимы только строчные латинские буквы, цифры и дефисы`;
			}
			let cursorUrl;
			try {
				cursorUrl = new URL(url);
			} catch {
				return `не подключено: некорректный url «${url}»`;
			}
			cursorUrl.searchParams.set("journalId", journalId);
			let res;
			try {
				res = await fetch(cursorUrl, {
					headers: { authorization: `Bearer ${token}` },
				});
			} catch (err) {
				return `не подключено: облако недоступно (${err.cause?.code ?? err.message})`;
			}
			if (!res.ok) {
				const reason =
					res.status === 401
						? "токен не принят (невалидный или отозванный)"
						: res.status === 403
							? "доступ запрещён"
							: "облако отвергло запрос";
				return `не подключено: HTTP ${res.status} — ${reason}. Конфиг не записан.`;
			}
			let lastSeq;
			try {
				({ lastSeq } = await res.json());
			} catch {
				return "не подключено: облако ответило не-JSON (это точно URL journal-sync?). Конфиг не записан.";
			}
			if (!Number.isInteger(lastSeq) || lastSeq < 0) {
				return `не подключено: облако вернуло некорректный lastSeq (${lastSeq}). Конфиг не записан.`;
			}
			const configPath = writeSyncConfig({
				dbPath: DB_PATH,
				config: { url, token, journalId },
			});
			scheduleAutoPush();
			return `подключено: курсор ${lastSeq}, журнал ${journalId}, конфиг ${configPath}`;
		},
	},
	{
		name: "sync",
		description:
			"Принудительно отправить журнал в облако (push, только вверх): GET курсор → " +
			"POST события с seq > курсора. Конфиг: sync.json рядом с базой (url, token, journalId) " +
			"либо env WORKHORSE_SYNC_URL/TOKEN/JOURNAL_ID. Без конфига синк выключен.",
		inputSchema: { type: "object", properties: {} },
		handler: async () => {
			const config = loadSyncConfig({ dbPath: DB_PATH });
			if (!config) return "синк не настроен (нет sync.json)";
			const result = await pushJournal({ dbPath: DB_PATH, config });
			if (result.error) return `ошибка синка: ${result.error}`;
			return `отправлено ${result.pushed} событий, курсор ${result.lastSeq}`;
		},
	},
	{
		name: "inbox",
		description:
			"Инбокс намерений из облака (pull: облако только отдаёт список, забирает оркестратор). " +
			"Показывает предложенные Task-намерения с контекстом фичи. Конфиг — тот же, что у синка " +
			"(sync.json / env), URL инбокса выводится из url синка. Забрать намерение — инструмент take.",
		inputSchema: { type: "object", properties: {} },
		handler: async () => {
			const config = loadSyncConfig({ dbPath: DB_PATH });
			if (!config) return "инбокс не настроен (нет sync.json)";
			if (!config.url || !config.token) {
				return "инбокс не настроен: в конфиге синка нужны url и token";
			}
			try {
				const res = await fetch(inboxUrlFromSyncUrl(config.url), {
					headers: { authorization: `Bearer ${config.token}` },
				});
				if (!res.ok) return `ошибка инбокса: HTTP ${res.status}`;
				const { items } = await res.json();
				if (!Array.isArray(items) || items.length === 0) {
					return "инбокс пуст: 0 намерений";
				}
				const lines = items.map(
					(item) =>
						`- ${item.id} — ${item.title} (фича: ${item.feature?.title ?? "?"})`,
				);
				return `${items.length} намерений:\n${lines.join("\n")}\n\nЗабрать: take { task_id: <id> }.`;
			} catch (err) {
				return `ошибка инбокса: ${err.message}`;
			}
		},
	},
	{
		name: "take",
		description:
			"Забрать намерение из инбокса облака (идемпотентно: повторный take вернёт тот же контекст). " +
			"Отвечает готовой болванкой для draft_task: контекст фичи, текст намерения, intent_task_id. " +
			"Дальше обычный цикл: search_precedents → draft_task (с intent_task_id) → delegate → …",
		inputSchema: {
			type: "object",
			properties: {
				task_id: { type: "string", description: "Id облачного Task-намерения (из inbox)" },
			},
			required: ["task_id"],
		},
		handler: async ({ task_id }) => {
			const config = loadSyncConfig({ dbPath: DB_PATH });
			if (!config) return "инбокс не настроен (нет sync.json)";
			if (!config.url || !config.token) {
				return "инбокс не настроен: в конфиге синка нужны url и token";
			}
			try {
				const res = await fetch(inboxUrlFromSyncUrl(config.url), {
					method: "POST",
					headers: {
						authorization: `Bearer ${config.token}`,
						"content-type": "application/json",
					},
					body: JSON.stringify({ taskId: task_id }),
				});
				if (res.status === 404) return `намерение ${task_id} не найдено в облаке`;
				if (!res.ok) return `ошибка take: HTTP ${res.status}`;
				const { alreadyTaken, context } = await res.json();
				const feature = context.feature ?? {};
				return [
					alreadyTaken
						? `Намерение уже было забрано ранее — повторная выдача контекста (take идемпотентен).`
						: `Намерение забрано из инбокса.`,
					``,
					`intent_task_id: ${context.id}`,
					``,
					`## Контекст фичи: ${feature.title ?? "?"}`,
					feature.description ?? "(описание фичи отсутствует)",
					...(feature.clarifications ? [``, `Уточнения: ${feature.clarifications}`] : []),
					``,
					`## Намерение: ${context.title}`,
					context.description ?? "(описания нет — уточни постановку сам)",
					``,
					`Рекомендованный slug: ${context.recommendedSlug}`,
					``,
					`Дальше: search_precedents по теме → draft_task с intent_task_id: "${context.id}" ` +
						`(project, slug, title, полный task_text по правилам журнала) → delegate → обычный цикл.`,
				].join("\n");
			} catch (err) {
				return `ошибка take: ${err.message}`;
			}
		},
	},
	{
		name: "get_task",
		description: "Текущее состояние задачи + полная история её событий.",
		inputSchema: {
			type: "object",
			properties: { task_id: { type: "string" } },
			required: ["task_id"],
		},
		handler: ({ task_id }) => {
			const task = getTaskStmt.get(task_id);
			if (!task) throw new Error(`Задача ${task_id} не найдена`);
			const events = db
				.prepare("SELECT seq, type, payload, at FROM events WHERE task_id = ? ORDER BY seq")
				.all(task_id);
			const links = {
				outgoing: db
					.prepare("SELECT to_task, kind FROM task_links WHERE from_task = ?")
					.all(task_id),
				incoming: db
					.prepare("SELECT from_task, kind FROM task_links WHERE to_task = ?")
					.all(task_id),
			};
			const artifacts = db
				.prepare("SELECT id, kind, title, at FROM artifacts WHERE task_id = ?")
				.all(task_id);
			return { task, events, links, artifacts };
		},
	},
	{
		name: "list_tasks",
		description: "Список задач журнала с фильтрами по статусу/проекту.",
		inputSchema: {
			type: "object",
			properties: {
				status: {
					type: "string",
					description: "DRAFT | DELEGATED | REPORTED | ACCEPTED | REWORK | FAILED",
				},
				project: { type: "string" },
				limit: { type: "number", description: "default 20" },
			},
		},
		handler: ({ status, project, limit = 20 }) => {
			const cond = [];
			const args = [];
			if (status) { cond.push("status = ?"); args.push(status); }
			if (project) { cond.push("project = ?"); args.push(project); }
			const where = cond.length ? `WHERE ${cond.join(" AND ")}` : "";
			return db
				.prepare(
					`SELECT task_id, project, title, status, outcome, executor, updated_at
					 FROM tasks ${where} ORDER BY updated_at DESC LIMIT ?`,
				)
				.all(...args, limit);
		},
	},
];

// ============ инструкция сервера и промпты (слеш-команды) ============

const SERVER_INSTRUCTIONS = `Журнал делегирования оркестратор ↔ рабочая лошадка (event sourcing поверх SQLite).
Статусы: DRAFT → DELEGATED → REPORTED → ACCEPTED | REWORK (→ DELEGATED…) | FAILED.
Ключевой инвариант: REPORTED («исполнитель считает, что готово») ≠ ACCEPTED («оркестратор верифицировал»).
Проекты регистрируются в реестре (register_project: имя-неймспейс + root_path + маппинг на пространство в облаке); задачи и артефакты принимаются только для зарегистрированных. Проект без workhorse — просто не регистрируй.
ЖЁСТКОЕ PRECONDITION: без явного bootstrap'а (регистрация + артефакт "Project baseline: <project>" с базовой цифрой полного прогона тестов) на проекте не работать — ни draft_task, ни делегаций. Нет baseline — сначала промпт bootstrap, без исключений для «маленьких задач».
Порядок: search_precedents (ОБЯЗАТЕЛЬНО до постановки) → record_artifact для спеки/плана/решения, если они рождались в обсуждении → draft_task (полный текст задания: контекст, root cause, порядок работ, запреты, формат отчёта) → delegate + запуск исполнителя (отчёт он пишет в файл, не в stdout) → submit_report → собственная верификация (дифф построчно, свой полный прогон тестов) → accept (только после зелёного прогона, с verify_commit) | request_rework | mark_failed → record_incident, если были грабли.
Артефакты (spec/plan/adr/decision/note) — версионируемые: повторная запись с тем же title = новая версия. Значимое проектное решение из обсуждения фиксируется артефактом СРАЗУ — иначе оно умрёт с сессией.
Продолжение закрытой задачи — всегда НОВАЯ задача + link_tasks kind=continues на старую (rework-цикл только внутри незакрытой). Найденная по ходу работа — новая задача + discovered_from.
Запись в журнале — информация; доказательство приёмки — собственный прогон оркестратора.`;

const PROMPTS = [
	{
		name: "tasks",
		description: "Показать журнал делегирования: задачи, поиск, инциденты",
		arguments: [
			{
				name: "filter",
				description:
					"Пусто = последние задачи; статус / проект / task_id / 'incidents' / ключевые слова",
				required: false,
			},
		],
		build: ({ filter = "" }) => `Покажи журнал делегирования workhorse (инструменты mcp__workhorse__*).

Фильтр: "${filter}"

Разбор фильтра: пусто — list_tasks (последние 20); статус (DRAFT/DELEGATED/REPORTED/ACCEPTED/REWORK/FAILED, регистр не важен) — list_tasks по статусу; имя проекта — list_tasks по проекту (комбинируются); "incidents" — search_precedents/база: последние грабли (description + lesson); значение с "/" — get_task: карточка + история событий; иные слова — search_precedents.

Вывод: компактная таблица task_id | статус | исполнитель | outcome | обновлено (для карточки — поля + хронология). Пусто — так и сказать, без лишнего текста.`,
	},
	{
		name: "bootstrap",
		description:
			"Собрать основу (baseline) текущего проекта в журнал: стек, команды, базовая цифра тестов, запреты",
		arguments: [
			{
				name: "project",
				description: "Имя проекта (пусто — определить по текущему каталогу)",
				required: false,
			},
		],
		build: ({ project = "" }) => `Собери основу (baseline) проекта для журнала делегирования workhorse.

Проект: "${project}" (если пусто — определи по текущему каталогу / git remote).

Шаг 0: проект должен быть в реестре — list_projects; нет — register_project (имя-неймспейс, root_path = корень проекта на диске).
Затем проверь, нет ли уже baseline: list_artifacts kind=spec по проекту, title "Project baseline: <project>". Есть и актуален — доложи и обнови только устаревшие пункты (record_artifact с тем же title = новая версия). Нет — собирай, фактами, не предположениями:

1. Стек и структура: языки, фреймворки, ключевые каталоги; правила из CLAUDE.md / AGENTS.md / README (конвенции кода, стек-правила).
2. Команды: сборка, точечный прогон тестов, полный прогон. Выполни полный прогон и зафиксируй базовую цифру (N passed / M total) — это эталон приёмки для всех будущих делегаций на проекте.
3. Запреты для исполнителей: защищённые каталоги и файлы (submodules, генерённый код, чужие подсистемы), что нельзя коммитить/трогать.
4. Особенности среды: известные флейки, осиротевшие build-серверы, специфика тулчейна.

Зафиксируй ОДНИМ артефактом: record_artifact kind=spec, title "Project baseline: <project>", body — пункты выше в markdown.

Дальше при каждой draft_task этого проекта: подтянуть baseline (list_artifacts / get_artifact) и включить в task_text базовую цифру тестов и запреты. После существенных изменений проекта (новый тулчейн, изменилась цифра тестов) — обновить baseline той же командой.`,
	},
];

// ============ JSON-RPC поверх stdio (newline-delimited) ============

function respond(id, result) {
	if (id === undefined || id === null) return;
	process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id, result }) + "\n");
}

function respondError(id, message, code = -32000) {
	if (id === undefined || id === null) return;
	process.stdout.write(
		JSON.stringify({ jsonrpc: "2.0", id, error: { code, message } }) + "\n",
	);
}

function handle(msg) {
	const { id, method, params } = msg;
	try {
		if (method === "initialize") {
			respond(id, {
				protocolVersion: params?.protocolVersion ?? "2024-11-05",
				capabilities: { tools: {}, prompts: {} },
				serverInfo: { name: "workhorse-mcp", version: "0.8.0" },
				instructions: SERVER_INSTRUCTIONS,
			});
		} else if (method === "prompts/list") {
			respond(id, {
				prompts: PROMPTS.map(({ name, description, arguments: args }) => ({
					name,
					description,
					arguments: args,
				})),
			});
		} else if (method === "prompts/get") {
			const prompt = PROMPTS.find((p) => p.name === params?.name);
			if (!prompt) throw new Error(`Неизвестный промпт: ${params?.name}`);
			respond(id, {
				description: prompt.description,
				messages: [
					{
						role: "user",
						content: { type: "text", text: prompt.build(params?.arguments ?? {}) },
					},
				],
			});
		} else if (method === "notifications/initialized") {
			// notification — ответа не требует
		} else if (method === "ping") {
			respond(id, {});
		} else if (method === "tools/list") {
			respond(id, {
				tools: TOOLS.map(({ name, description, inputSchema }) => ({
					name,
					description,
					inputSchema,
				})),
			});
		} else if (method === "tools/call") {
			const tool = TOOLS.find((t) => t.name === params?.name);
			if (!tool) throw new Error(`Неизвестный инструмент: ${params?.name}`);
			// Promise.resolve поддерживает и синхронные, и async-хендлеры (sync).
			// Строковый результат — готовый текст ответа, объект — JSON.
			Promise.resolve()
				.then(() => tool.handler(params?.arguments ?? {}))
				.then((result) => {
					respond(id, {
						content: [
							{
								type: "text",
								text: typeof result === "string" ? result : JSON.stringify(result, null, 2),
							},
						],
					});
				})
				.catch((err) => {
					respond(id, {
						content: [{ type: "text", text: `Ошибка: ${err.message}` }],
						isError: true,
					});
				});
		} else {
			respondError(id, `Unsupported method: ${method}`, -32601);
		}
	} catch (err) {
		respondError(id, err instanceof Error ? err.message : String(err));
	}
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
	buffer += chunk;
	let nl;
	while ((nl = buffer.indexOf("\n")) !== -1) {
		const line = buffer.slice(0, nl).trim();
		buffer = buffer.slice(nl + 1);
		if (!line) continue;
		try {
			handle(JSON.parse(line));
		} catch {
			console.error("workhorse-mcp: непарсимая строка входа");
		}
	}
});
process.stdin.on("end", () => process.exit(0));
