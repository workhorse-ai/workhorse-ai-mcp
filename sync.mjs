#!/usr/bin/env node
// workhorse-sync — пушер журнала в облако Workhorse AI.
// Направление строго вверх: GET курсор → POST батчи событий с seq > курсора.
// Zero deps: node:sqlite (read-only) + встроенный fetch. Ошибки возвращаются
// значением ({error}), исключения наружу не летят — пушер никогда не роняет журнал.

import { DatabaseSync } from "node:sqlite";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { homedir, hostname, userInfo } from "node:os";
import { join, dirname } from "node:path";
import { pathToFileURL } from "node:url";

export const BATCH_SIZE = 200;

// События уровня проекта (не привязанные к задаче) живут под этим task_id.
export const GENERAL_TASK_ID = "_general";

// Данные под бренд Workhorse AI: директория по бренду, файл по смыслу.
export const DEFAULT_DB_DIR = ".workhorse-ai";
export const DEFAULT_DB_FILE = "journal.db";

// Единственный резолвер пути к базе (сервер и CLI пушера используют его же):
// WORKHORSE_DB перекрывает всё, иначе ~/.workhorse-ai/journal.db.
// Чистая функция — env и homedir подменяемы, тестируется без реальной ФС.
export function resolveDbPath({ env = process.env, homedir: home = homedir() } = {}) {
	return env.WORKHORSE_DB ?? join(home, DEFAULT_DB_DIR, DEFAULT_DB_FILE);
}

// Конфиг синка: JSON-файл рядом с базой (sync.json), путь переопределяется
// WORKHORSE_SYNC_CONFIG. Две формы:
//   1) одна цель (как было): {url, token, journalId};
//   2) несколько целей: {targets: [{alias, url, token, journalId}, ...]}.
// env-переменные WORKHORSE_SYNC_URL / TOKEN / JOURNAL_ID описывают ОДНУ цель:
// при плоском конфиге они перекрывают его поля (как и раньше), а при списке
// targets игнорируются со строкой в лог — перекрыть список одной парой
// url/token нечем, а молча слать журнал ещё и в env-цель нельзя.
// Нет ни файла, ни env → синк выключен (пустой список) — это норма.
export function syncConfigPath({ dbPath, env = process.env } = {}) {
	const resolvedDbPath = dbPath ?? resolveDbPath({ env });
	return env.WORKHORSE_SYNC_CONFIG ?? join(dirname(resolvedDbPath), "sync.json");
}

// Сырой конфиг из файла: объект либо null (файла нет / не парсится / не объект).
export function readSyncConfigFile({ dbPath, env = process.env, log = console.error } = {}) {
	const configPath = syncConfigPath({ dbPath, env });
	if (!existsSync(configPath)) return null;
	try {
		const parsed = JSON.parse(readFileSync(configPath, "utf8"));
		return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed : null;
	} catch {
		log(`workhorse-sync: конфиг не парсится, игнорирую: ${configPath}`);
		return null;
	}
}

// Человекочитаемое имя цели. Именно им человек называет цель (sync_scope
// { target }): id пространства знать не нужно, а адрес длинный. По умолчанию —
// хост адреса: он и узнаваем, и не выдаёт токен.
export function targetAlias(target, index = 0) {
	if (typeof target?.alias === "string" && target.alias.trim()) return target.alias.trim();
	if (typeof target?.url === "string" && target.url) {
		try {
			return new URL(normalizeBaseUrl(target.url)).host;
		} catch {
			// не URL — падать из-за имени нельзя, ниже есть запасное
		}
	}
	return `цель-${index + 1}`;
}

// Список целей в канонический вид: пустые выкинуть, алиасы сделать
// уникальными (по ним человек адресует цель — двусмысленности быть не должно),
// journalId вывести из имени машины, если не задан.
function finalizeTargets(rawTargets) {
	const used = new Set();
	const targets = [];
	rawTargets.forEach((raw, index) => {
		if (!raw?.url && !raw?.token && !raw?.journalId) return;
		let alias = targetAlias(raw, index);
		if (used.has(alias)) {
			let n = 2;
			while (used.has(`${alias}#${n}`)) n += 1;
			alias = `${alias}#${n}`;
		}
		used.add(alias);
		targets.push({
			alias,
			url: raw.url,
			token: raw.token,
			// journalId — единственное поле, которое можно вывести самим: connect
			// делает это автоматически, и настройка через env не должна требовать
			// от человека выдумывать идентификатор своей машины.
			journalId: raw.journalId ?? defaultJournalId(),
		});
	});
	return targets;
}

// Все цели синка: [] = синк выключен.
export function loadSyncTargets({ dbPath, env = process.env, log = console.error } = {}) {
	const fileConfig = readSyncConfigFile({ dbPath, env, log }) ?? {};
	const listed = Array.isArray(fileConfig.targets)
		? fileConfig.targets.filter((t) => t && typeof t === "object" && !Array.isArray(t))
		: null;
	const envUrl = env.WORKHORSE_SYNC_URL;
	const envToken = env.WORKHORSE_SYNC_TOKEN;
	const envJournalId = env.WORKHORSE_SYNC_JOURNAL_ID;

	if (listed && listed.length > 0) {
		if (envUrl || envToken || envJournalId) {
			log(
				"workhorse-sync: в конфиге список targets — WORKHORSE_SYNC_URL/TOKEN/JOURNAL_ID " +
					"игнорируются (env описывает только одну цель)",
			);
		}
		return finalizeTargets(listed);
	}

	const single = {
		alias: fileConfig.alias,
		url: envUrl ?? fileConfig.url,
		token: envToken ?? fileConfig.token,
		journalId: envJournalId ?? fileConfig.journalId,
	};
	if (!single.url && !single.token && !single.journalId) return [];
	return finalizeTargets([single]);
}

// Одна цель как раньше ({url, token, journalId} либо null) — для инбокса
// намерений (pull всегда из одного облака) и для совместимости вызовов.
export function loadSyncConfig({ dbPath, env = process.env, log = console.error } = {}) {
	const [first] = loadSyncTargets({ dbPath, env, log });
	if (!first) return null;
	return { url: first.url, token: first.token, journalId: first.journalId };
}

// Запись конфига синка по тому же пути, откуда его читает loadSyncTargets
// (WORKHORSE_SYNC_CONFIG ?? sync.json рядом с базой). Перезапись существующего
// файла — осознанное действие пользователя (connect).
// Возвращает путь записанного файла.
export function writeSyncConfig({ dbPath, env = process.env, config }) {
	const configPath = syncConfigPath({ dbPath, env });
	writeFileSync(configPath, `${JSON.stringify(config, null, "\t")}\n`);
	return configPath;
}

// Дефолтный id журнала: <username>-<hostname>, нормализованный в [a-z0-9-]
// (строчные буквы, всё прочее — дефис, серии дефисов схлопнуты, края обрезаны).
export function defaultJournalId({ username, host } = {}) {
	const raw = `${username ?? userInfo().username}-${host ?? hostname()}`;
	return (
		raw
			.toLowerCase()
			.replace(/[^a-z0-9-]+/g, "-")
			.replace(/-+/g, "-")
			.replace(/^-|-$/g, "") || "journal"
	);
}

// URL инбокса намерений выводится из url синка заменой последнего сегмента
// пути (journal-sync → journal-inbox): один конфиг — меньше настройки.
// Пути эндпоинтов — деталь реализации сервера, а не пользователя: в конфиге
// живёт БАЗОВЫЙ адрес инстанса (облако или on-premise, в том числе за
// реверс-прокси с префиксом), а конкретные пути выводятся здесь.
// Управляемое облако Workhorse AI — адрес по умолчанию, когда url не задан.
// Перекрывается WORKHORSE_CLOUD_URL (полезно для стейджинга и своих сборок).
export const DEFAULT_CLOUD_URL = "https://app.workhorse-ai.dev";

export function resolveCloudUrl({ url, env = process.env } = {}) {
	return url ?? env.WORKHORSE_CLOUD_URL ?? DEFAULT_CLOUD_URL;
}

export const SYNC_PATH = "/api/mcp/journal-sync";
export const INBOX_PATH = "/api/mcp/journal-inbox";

// Принимает и базу («https://wh.acme.internal», «https://tools.acme.com/workhorse»),
// и полный эндпоинт — последнее нужно для конфигов, написанных до 0.8.1.
export function normalizeBaseUrl(input) {
	const url = new URL(input);
	let path = url.pathname.replace(/\/+$/, "");
	for (const suffix of [SYNC_PATH, INBOX_PATH]) {
		if (path.toLowerCase().endsWith(suffix)) {
			path = path.slice(0, -suffix.length);
			break;
		}
	}
	url.pathname = path;
	url.search = "";
	url.hash = "";
	return url.toString().replace(/\/+$/, "");
}

export function syncUrlFromBase(base) {
	return `${normalizeBaseUrl(base)}${SYNC_PATH}`;
}

export function inboxUrlFromBase(base) {
	return `${normalizeBaseUrl(base)}${INBOX_PATH}`;
}

// Совместимость со старым именем: принимает что угодно из двух форм.
export function inboxUrlFromSyncUrl(input) {
	return inboxUrlFromBase(input);
}

// ============ Область синка (какие проекты уезжают в это пространство) ============
//
// Локальный журнал — один на машину и содержит все проекты сразу, а облачное
// пространство принадлежит команде. Слать всё подряд — утечка: второй участник
// пространства увидит проекты, к которым не имеет отношения.
//
// Токен личный: GET курсора отдаёт ВСЕ пространства пользователя, и область
// вычисляется для каждого. Приоритет источников:
//   1. WORKHORSE_SYNC_PROJECTS — только при единственном пространстве
//      (env называет проекты, но не адресата);
//   2. projects.cloud_workspace_id == id пространства (маппинг из sync_scope);
//   3. маппинга нет ни у одного проекта: одно пространство → шлём всё туда
//      (+ предупреждение), несколько → не шлём ничего и просим sync_scope.

// Файл состояния синка рядом с базой: помнит область прошлого успешного пуша.
// Это НЕ конфиг (тот может целиком жить в env) и НЕ журнал — журнал read-only.
// Файл появляется только у тех, кто реально ограничил область.
//
// Область — своя у каждой цели, поэтому состояние хранится по ключу цели:
//   {"targets": {"<ключ>": {"projects": [...], "at": "..."}}}
// Плоский файл старых версий ({projects, at}) читается как состояние
// единственной цели — апгрейд ничего не теряет и не вызывает пересинк.
export const SYNC_STATE_FILE = "sync-state.json";

export function syncStatePath({ dbPath, env = process.env } = {}) {
	return join(dirname(dbPath ?? resolveDbPath({ env })), SYNC_STATE_FILE);
}

// Ключ состояния цели: id пространства — он переживает переезд инстанса на
// другой адрес, а url нет. Пространство неизвестно (старое облако) — запасной
// ключ из адреса и журнала.
export function syncStateKey({ target, workspaceId = null } = {}) {
	if (typeof workspaceId === "string" && workspaceId) return workspaceId;
	let base = typeof target?.url === "string" ? target.url : "";
	try {
		base = normalizeBaseUrl(base);
	} catch {
		// не URL — ключ всё равно должен получиться
	}
	return `${base}#${target?.journalId ?? ""}`;
}

function parseSyncStateFile(path) {
	if (!existsSync(path)) return null;
	try {
		const parsed = JSON.parse(readFileSync(path, "utf8"));
		return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed : null;
	} catch {
		return null;
	}
}

function stateTargetsMap(parsed) {
	const targets = parsed?.targets;
	return typeof targets === "object" && targets !== null && !Array.isArray(targets)
		? targets
		: null;
}

// Прошлая область цели: null = «слалось всё». Файла нет — значит синк работал
// старой версией (она слала всё) либо не работал вовсе; в обоих случаях «всё» —
// честный ответ. Без key возвращается состояние единственной цели (если она
// одна) — так читают состояние и старые вызовы, и тесты.
export function readSyncState({ dbPath, env = process.env, key = null } = {}) {
	const parsed = parseSyncStateFile(syncStatePath({ dbPath, env }));
	if (!parsed) return { projects: null };
	const flat = Array.isArray(parsed.projects) ? parsed.projects : null;
	const targets = stateTargetsMap(parsed);
	if (!targets) return { projects: flat }; // плоский файл = состояние единственной цели
	if (key !== null) {
		const entry = targets[key];
		return { projects: Array.isArray(entry?.projects) ? entry.projects : null };
	}
	const entries = Object.values(targets);
	if (entries.length === 1) {
		return { projects: Array.isArray(entries[0]?.projects) ? entries[0].projects : null };
	}
	return { projects: flat };
}

// Запись области цели. Плоское состояние старых версий при первой записи
// переезжает в карту целей под ключом записываемой цели.
export function writeSyncState({ dbPath, env = process.env, projects, key = null }) {
	const path = syncStatePath({ dbPath, env });
	const at = new Date().toISOString();
	if (key === null) {
		writeFileSync(path, `${JSON.stringify({ projects: projects ?? null, at }, null, "\t")}\n`);
		return path;
	}
	const parsed = parseSyncStateFile(path);
	const targets = { ...(stateTargetsMap(parsed) ?? {}) };
	targets[key] = { projects: projects ?? null, at };
	writeFileSync(path, `${JSON.stringify({ targets, at }, null, "\t")}\n`);
	return path;
}

// WORKHORSE_SYNC_PROJECTS="acme-web, acme-api" → ["acme-web", "acme-api"].
// Пустая строка/пробелы = переменная не задана (иначе опечатка молча вырубила бы синк).
export function parseSyncProjects(value) {
	if (typeof value !== "string") return null;
	const list = value
		.split(",")
		.map((name) => name.trim())
		.filter(Boolean);
	return list.length > 0 ? list : null;
}

// Проект события. Правила проверены по живому журналу:
//   ProjectRegistered      → payload.name (task_id всегда _general);
//   ArtifactRecorded / TaskDrafted → payload.project;
//   всё остальное с task_id → префикс до «/» (формат <project>/<дата>-<slug>);
//   иначе (task_id = _general) → null, событие общее.
export function eventProject({ taskId, type, payload }) {
	if (type === "ProjectRegistered") {
		return typeof payload?.name === "string" && payload.name ? payload.name : null;
	}
	if (typeof payload?.project === "string" && payload.project) return payload.project;
	if (typeof taskId === "string" && taskId && taskId !== GENERAL_TASK_ID) {
		return taskId.split("/")[0] || null;
	}
	return null;
}

// Область ОДНОГО пространства → {projects: Set|null, source, warning}.
// projects === null означает «ограничения нет, шлём всё»; пустой Set —
// «в это пространство не уедет ничего».
//
// Токен личный, пространств у пользователя может быть несколько (workspaceCount):
//   - WORKHORSE_SYNC_PROJECTS применяется только при ЕДИНСТВЕННОМ пространстве —
//     env называет проекты, но не адресата, при нескольких пространствах он
//     неоднозначен (предупреждение, дальше решает маппинг);
//   - маппинга нет ни у одного проекта: одно пространство → шлём всё туда
//     (+ предупреждение, как раньше); несколько → НИЧЕГО не шлём и просим
//     sync_scope — приватность важнее удобства.
export function resolveSyncScope({
	env = process.env,
	projects = [],
	workspaceId = null,
	workspaceCount = 1,
} = {}) {
	const fromEnv = parseSyncProjects(env.WORKHORSE_SYNC_PROJECTS);
	if (fromEnv && workspaceCount === 1) {
		return { projects: new Set(fromEnv), source: "env", warning: null };
	}
	const envWarning =
		fromEnv && workspaceCount > 1
			? "WORKHORSE_SYNC_PROJECTS игнорируется: пространств несколько, адресат " +
				"неоднозначен — привяжите проекты инструментом sync_scope."
			: null;

	const mapped = projects.filter((p) => p?.cloud_workspace_id);
	if (mapped.length === 0) {
		if (workspaceCount === 1) {
			return {
				projects: null,
				source: "all",
				warning: [
					envWarning,
					"область синка не задана: ни у одного проекта нет cloud_workspace_id — " +
						"в пространство уедут ВСЕ проекты журнала. Ограничить: инструмент sync_scope " +
						"или переменная WORKHORSE_SYNC_PROJECTS.",
				]
					.filter(Boolean)
					.join(" "),
			};
		}
		return {
			projects: new Set(),
			source: "unmapped",
			warning: [
				envWarning,
				"область синка не задана, а пространств несколько — журнал НИКУДА не уедет. " +
					"Привязать проекты: sync_scope { workspace: <slug>, projects: [...] }.",
			]
				.filter(Boolean)
				.join(" "),
		};
	}

	return {
		projects: new Set(
			mapped.filter((p) => p.cloud_workspace_id === workspaceId).map((p) => p.name),
		),
		source: "mapping",
		warning: envWarning,
	};
}

// Уезжает ли событие при данной области.
// Общие события (task_id = _general: инциденты и артефакты уровня журнала) при
// АКТИВНОЙ фильтрации не уезжают: их тексты сплошь и рядом называют конкретные
// проекты, в чужое пространство им нельзя.
// ProjectRegistered тоже не уезжает: в облаке у него нет проекции
// (planJournalEventMutation → kind: "none"), а payload несёт локальный root_path.
export function shouldSyncEvent(event, scope) {
	if (!scope) return true;
	if (event.type === "ProjectRegistered") return false;
	const project = eventProject(event);
	return project !== null && scope.has(project);
}

// Нужна ли пересинхронизация с нуля: область расширилась, значит за курсором
// остались события, которые прошлая область отфильтровала.
// null (всё) шире любого списка; отсутствие состояния = «слалось всё» = не нужна.
export function needsRescan(previousProjects, currentScope) {
	if (previousProjects === null) return false;
	const previous = new Set(previousProjects);
	if (!currentScope) return true; // было сужено, стало «всё»
	for (const name of currentScope) {
		if (!previous.has(name)) return true;
	}
	return false;
}

// Локальный payload хранится TEXT-JSON; облачная схема ждёт объект.
function parsePayload(text) {
	try {
		const parsed = JSON.parse(text);
		return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed : {};
	} catch {
		return {};
	}
}

// Валидация списка пространств из ответа облака. Контракт GET:
//   { workspaces: [{ id, slug, name, lastSeq }] }
// — по элементу на каждое пространство пользователя (токен личный),
// lastSeq — курсор пары (workspace, journal), 0 если курсора нет.
export function parseWorkspaces(body) {
	if (!Array.isArray(body?.workspaces)) return null;
	const workspaces = [];
	for (const ws of body.workspaces) {
		if (typeof ws?.id !== "string" || !ws.id) return null;
		if (!Number.isInteger(ws.lastSeq) || ws.lastSeq < 0) return null;
		workspaces.push({
			id: ws.id,
			slug: typeof ws.slug === "string" ? ws.slug : ws.id,
			name: typeof ws.name === "string" ? ws.name : "",
			lastSeq: ws.lastSeq,
		});
	}
	return workspaces;
}

// GET курсора: {workspaces: [{id, slug, name, lastSeq}]} либо {error}.
// Отдельно от pushJournal, потому что список пространств нужен и инструменту
// sync_scope — локально его знать неоткуда, единственный источник — ответ
// облака на токен.
export async function fetchCursor({ config }) {
	if (!config?.url || !config?.token || !config?.journalId) {
		return { error: "конфиг синка неполный: нужны url, token и journalId" };
	}
	try {
		const cursorUrl = new URL(syncUrlFromBase(config.url));
		cursorUrl.searchParams.set("journalId", config.journalId);
		const res = await fetch(cursorUrl, {
			headers: {
				authorization: `Bearer ${config.token}`,
				"cache-control": "no-store",
				pragma: "no-cache",
			},
		});
		if (!res.ok) return { error: `GET курсора: HTTP ${res.status}` };
		const body = await res.json();
		const workspaces = parseWorkspaces(body);
		if (!workspaces) {
			return { error: "GET курсора: облако не вернуло workspaces (несовместимая версия сервера)" };
		}
		return { workspaces };
	} catch (err) {
		return { error: err instanceof Error ? err.message : String(err) };
	}
}

// Фиксация области после успешного пуша. Пишем только при расхождении: у тех,
// кто область не ограничивал, файл состояния так и не появится. Сбой записи —
// строка в лог, не ошибка синка (в худшем случае лишняя пересинхронизация).
function persistScope({ dbPath, env, projects, key, log }) {
	const previous = readSyncState({ dbPath, env, key }).projects;
	const same =
		previous === null
			? projects === null
			: projects !== null &&
				previous.length === projects.length &&
				previous.every((name) => projects.includes(name));
	if (same) return;
	try {
		writeSyncState({ dbPath, env, projects, key });
	} catch (err) {
		log(`workhorse-sync: не удалось записать состояние области: ${err.message}`);
	}
}

// Пуш журнала в ОДНУ цель (один сервер, один личный токен): облако отдаёт
// список пространств пользователя, и журнал раскладывается по КАЖДОМУ из них
// согласно маппингу cloud_workspace_id. Курсор, область и состояние области —
// свои у каждой пары (пространство, журнал); пространства независимы: ошибка
// одного не отменяет остальные.
// Результат: {pushed: <сумма>, lastSeq: <максимум по пространствам>,
//   workspaces: [{id, slug, pushed, lastSeq, error?}]} либо {error} — без
// исключений наружу. Базу открывает read-only: пушер — транспорт, журнал он
// не меняет никогда.
export async function pushToTarget({ dbPath, target: config, env = process.env, log = () => {} }) {
	if (!config) return { pushed: 0, disabled: true };
	if (!config.url || !config.token || !config.journalId) {
		return { error: "конфиг синка неполный: нужны url, token и journalId" };
	}

	try {
		// Заголовок — как ждёт authenticateMcpRequest: Authorization: Bearer <McpToken>
		const headers = {
			authorization: `Bearer ${config.token}`,
			"content-type": "application/json",
		};

		// GET курсора обязан быть свежим: закешированный прокси-ответ вернул бы
		// устаревшие lastSeq, и мы погнали бы уже отправленные события заново.
		const cursorHeaders = { ...headers, "cache-control": "no-store", pragma: "no-cache" };
		const cursorUrl = new URL(syncUrlFromBase(config.url));
		cursorUrl.searchParams.set("journalId", config.journalId);
		const cursorRes = await fetch(cursorUrl, { headers: cursorHeaders });
		if (!cursorRes.ok) {
			return { error: `GET курсора: HTTP ${cursorRes.status}` };
		}
		const workspaces = parseWorkspaces(await cursorRes.json());
		if (!workspaces) {
			return { error: "GET курсора: облако не вернуло workspaces (несовместимая версия сервера)" };
		}
		if (workspaces.length === 0) {
			log("workhorse-sync: у пользователя нет пространств — журнал слать некуда");
			return { pushed: 0, lastSeq: 0, workspaces: [] };
		}

		// Журнал читаем один раз: реестр проектов (источник маппинга) и все
		// события — дальше каждая пара (пространство, курсор) фильтрует своё.
		let projects = [];
		let allEvents;
		const db = new DatabaseSync(dbPath, { readOnly: true });
		try {
			// Древняя база без таблицы projects не должна ронять пуш.
			try {
				projects = db.prepare("SELECT name, cloud_workspace_id FROM projects").all();
			} catch {
				projects = [];
			}
			allEvents = db
				.prepare("SELECT seq, task_id, type, at, payload FROM events ORDER BY seq")
				.all()
				.map((row) => ({
					seq: row.seq,
					taskId: row.task_id,
					type: row.type,
					at: row.at,
					payload: parsePayload(row.payload),
				}));
		} finally {
			db.close();
		}

		const results = [];
		// Одинаковые предупреждения (например «пространств несколько, области
		// нет») не повторяются на каждое пространство.
		const seenWarnings = new Set();
		for (const ws of workspaces) {
			// Лог помечаем пространством только при нескольких — с одним
			// пространством строки остаются такими же, какими были всегда.
			const wsLog = workspaces.length > 1 ? (line) => log(`{${ws.slug}} ${line}`) : log;

			const scope = resolveSyncScope({
				env,
				projects,
				workspaceId: ws.id,
				workspaceCount: workspaces.length,
			});
			if (scope.warning && !seenWarnings.has(scope.warning)) {
				seenWarnings.add(scope.warning);
				log(`workhorse-sync: ${scope.warning}`);
			}

			// Ключ состояния области — id пространства: расширение области у
			// одного пространства не должно вызывать пересинк у остальных.
			const stateKey = syncStateKey({ target: config, workspaceId: ws.id });

			// Курсор в облаке — на пару (пространство, журнал) и двигается до
			// максимального применённого seq, поэтому отфильтрованные события
			// остаются позади него. Расширили область — идём с нуля: приём батча
			// идемпотентен по seq, облако отбросит уже виденное.
			const previous = readSyncState({ dbPath, env, key: stateKey }).projects;
			const rescan = needsRescan(previous, scope.projects);
			const from = rescan ? 0 : ws.lastSeq;
			if (rescan) {
				wsLog(
					"workhorse-sync: область синка расширилась — пересинхронизация с нуля " +
						"(облако отбросит дубли по seq)",
				);
			}

			const beyond = allEvents.filter((event) => event.seq > from);
			const rows = beyond.filter((event) => shouldSyncEvent(event, scope.projects));
			if (scope.projects) {
				wsLog(
					`workhorse-sync: область — ${scope.source === "env" ? "WORKHORSE_SYNC_PROJECTS" : "маппинг"}: ` +
						`${[...scope.projects].join(", ") || "(пусто)"}; отфильтровано ${beyond.length - rows.length} из ${beyond.length}`,
				);
			}
			// Область текущего пуша — её запишем в состояние после успеха.
			const scopeForState = scope.projects ? [...scope.projects] : null;
			wsLog(`workhorse-sync: событий за курсором ${from}: ${rows.length}`);

			if (rows.length === 0) {
				persistScope({ dbPath, env, projects: scopeForState, key: stateKey, log: wsLog });
				results.push({ id: ws.id, slug: ws.slug, pushed: 0, lastSeq: ws.lastSeq });
				continue;
			}

			let cursor = ws.lastSeq;
			let pushed = 0;
			let wsError = null;
			for (let i = 0; i < rows.length; i += BATCH_SIZE) {
				const batch = rows.slice(i, i + BATCH_SIZE);
				const res = await fetch(syncUrlFromBase(config.url), {
					method: "POST",
					headers,
					body: JSON.stringify({
						workspaceId: ws.id,
						journalId: config.journalId,
						events: batch,
					}),
				});
				if (!res.ok) {
					wsError = `POST батча: HTTP ${res.status}`;
					break;
				}
				const result = await res.json();
				cursor = result.lastSeq ?? batch[batch.length - 1].seq;
				pushed += batch.length;
				wsLog(`workhorse-sync: батч ${batch.length}, курсор облака ${cursor}`);
			}
			if (wsError) {
				wsLog(`workhorse-sync: пространство не синхронизировано: ${wsError}`);
				results.push({ id: ws.id, slug: ws.slug, pushed, lastSeq: cursor, error: wsError });
				continue;
			}
			// Состояние пишем только после полного успеха: оборвался пуш — область
			// не зафиксирована, следующий заход при необходимости повторит пересинк.
			persistScope({ dbPath, env, projects: scopeForState, key: stateKey, log: wsLog });
			results.push({ id: ws.id, slug: ws.slug, pushed, lastSeq: cursor });
		}

		const failed = results.filter((r) => r.error);
		const summary = {
			pushed: results.reduce((sum, r) => sum + r.pushed, 0),
			lastSeq: results.reduce((max, r) => Math.max(max, r.lastSeq ?? 0), 0),
			workspaces: results,
		};
		if (failed.length === results.length && results.length > 0) {
			summary.error =
				results.length === 1
					? failed[0].error
					: failed.map((r) => `${r.slug}: ${r.error}`).join("; ");
		}
		return summary;
	} catch (err) {
		return { error: err instanceof Error ? err.message : String(err) };
	}
}

// Цели пуша из аргумента: одна цель (объект, как до 0.11), список целей,
// null/пусто (синк выключен).
function normalizeTargetList(input) {
	if (!input) return [];
	const list = Array.isArray(input) ? input : [input];
	return list
		.filter((t) => t && typeof t === "object")
		.map((t, index) => ({ ...t, alias: targetAlias(t, index) }));
}

// Пуш журнала во ВСЕ настроенные цели. Цели независимы: свои курсоры, свои
// области, своё состояние — падение одной не отменяет остальные.
// Результат:
//   {pushed: <сумма>, lastSeq: <курсор первой цели>, targets: [{alias, url,
//    journalId, workspaces, pushed, lastSeq, error}]}
// error верхнего уровня появляется, только если НИ ОДНА цель не отработала:
// иначе «ошибка» соврала бы про доехавшие цели (их видно в targets).
export async function pushJournal({ dbPath, config, targets, env = process.env, log = () => {} }) {
	const list = normalizeTargetList(targets ?? config);
	if (list.length === 0) return { pushed: 0, disabled: true, targets: [] };

	const results = [];
	for (const target of list) {
		// Лог помечаем целью только при нескольких — у одной цели строки
		// остаются ровно такими, какими были до многоцелевого синка.
		const targetLog = list.length > 1 ? (line) => log(`[${target.alias}] ${line}`) : log;
		let result;
		try {
			result = await pushToTarget({ dbPath, target, env, log: targetLog });
		} catch (err) {
			result = { error: err instanceof Error ? err.message : String(err) };
		}
		if (result.error) targetLog(`workhorse-sync: цель не синхронизирована: ${result.error}`);
		results.push({
			alias: target.alias,
			url: target.url,
			journalId: target.journalId,
			workspaces: result.workspaces ?? [],
			pushed: result.pushed ?? 0,
			lastSeq: result.lastSeq,
			...(result.error ? { error: result.error } : {}),
		});
	}

	const failed = results.filter((r) => r.error);
	const summary = {
		pushed: results.reduce((sum, r) => sum + r.pushed, 0),
		lastSeq: results[0].lastSeq,
		targets: results,
	};
	if (failed.length === results.length) {
		summary.error =
			results.length === 1
				? failed[0].error
				: failed.map((r) => `${r.alias}: ${r.error}`).join("; ");
	}
	return summary;
}

// ============ CLI: node sync.mjs / workhorse-sync ============

const isCli = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isCli) {
	const dbPath = resolveDbPath();
	const targets = loadSyncTargets({ dbPath });
	if (targets.length === 0) {
		console.error(
			"workhorse-sync: синк не настроен — нет sync.json рядом с базой и нет WORKHORSE_SYNC_URL/TOKEN/JOURNAL_ID",
		);
		process.exit(1);
	}
	const result = await pushJournal({ dbPath, targets, log: console.error });
	if (targets.length === 1) {
		const [only] = result.targets;
		if (only.error) {
			console.error(`workhorse-sync: ${only.error}`);
			process.exit(1);
		}
		console.log(`отправлено ${only.pushed}, курсор ${only.lastSeq}`);
		process.exit(0);
	}
	// Несколько целей: строка на цель, ненулевой код — если упала хоть одна
	// (молчаливо потерянная цель хуже громкого выхода).
	for (const t of result.targets) {
		console.log(
			t.error
				? `${t.alias}: ошибка — ${t.error}`
				: `${t.alias}: отправлено ${t.pushed}, курсор ${t.lastSeq}`,
		);
	}
	process.exit(result.targets.some((t) => t.error) ? 1 : 0);
}
