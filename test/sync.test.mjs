// Тесты пушера синка workhorse → облако Workhorse AI.
// Облако — in-process мок на node:http (контракт journal-sync: GET курсор,
// POST батч, идемпотентность по seq, auth Authorization: Bearer <token>).
// Никакой сети наружу: только 127.0.0.1 c портом от ОС.

import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

import {
	BATCH_SIZE,
	defaultJournalId,
	DEFAULT_CLOUD_URL,
	eventProject,
	inboxUrlFromBase,
	inboxUrlFromSyncUrl,
	loadSyncConfig,
	loadSyncTargets,
	needsRescan,
	normalizeBaseUrl,
	parseSyncProjects,
	pushJournal,
	readSyncState,
	resolveCloudUrl,
	resolveSyncScope,
	shouldSyncEvent,
	syncStateKey,
	syncStatePath,
	syncUrlFromBase,
	writeSyncConfig,
} from "../sync.mjs";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const SCHEMA = join(ROOT, "schema.sql");
const SERVER = join(ROOT, "server.mjs");
const SYNC_CLI = join(ROOT, "sync.mjs");

const TOKEN = "test-mcp-token";
const JOURNAL = "journal-test";
const WORKSPACE = "ws-alpha";
const OTHER_WORKSPACE = "ws-other";

// ============ фикстуры ============

function tmpDir() {
	return mkdtempSync(join(tmpdir(), "workhorse-sync-test-"));
}

function openDb(dbPath) {
	const db = new DatabaseSync(dbPath);
	return {
		db,
		raw: (task, type, payload) =>
			db
				.prepare("INSERT INTO events(task_id, type, payload) VALUES (?, ?, ?)")
				.run(task, type, JSON.stringify(payload)),
	};
}

// Временная база со схемой и десятком валидных событий разных типов (все с task_id).
function makeJournalDb(dir) {
	const dbPath = join(dir, "journal.db");
	const { db, raw } = openDb(dbPath);
	db.exec(readFileSync(SCHEMA, "utf8"));
	raw("_general", "ProjectRegistered", { name: "test", root_path: "/tmp/test" });
	raw("test/a", "TaskDrafted", { project: "test", title: "Задача А", task_text: "сделать" });
	raw("test/a", "Delegated", { executor: "grok" });
	raw("test/a", "ReportSubmitted", { report: "готово" });
	raw("test/a", "Accepted", { outcome: "accepted", verify_commit: "abc1234" });
	raw("test/b", "TaskDrafted", { project: "test", title: "Задача Б", task_text: "ещё" });
	raw("test/b", "Delegated", { executor: "subagent" });
	raw("_general", "IncidentRecorded", { description: "грабли", lesson: "урок" });
	raw("_general", "ArtifactRecorded", {
		project: "test",
		kind: "note",
		title: "заметка",
		body: "текст",
	});
	raw("test/b", "TaskLinked", { to_task_id: "test/a", kind: "relates" });
	db.close();
	return dbPath; // 10 событий, seq 1..10
}

// Журнал с двумя проектами и общими событиями — как на живой машине, где один
// журнал на всё сразу. Раскладка seq:
//   1,2 ProjectRegistered alpha/beta   5,6 beta/one
//   3,4 alpha/one                      7 общий инцидент, 8 артефакт alpha
function makeMultiProjectDb(dir, { mappings = {} } = {}) {
	const dbPath = join(dir, "journal.db");
	const { db, raw } = openDb(dbPath);
	db.exec(readFileSync(SCHEMA, "utf8"));
	for (const name of ["alpha", "beta"]) {
		raw("_general", "ProjectRegistered", {
			name,
			root_path: `/tmp/${name}`,
			cloud_workspace_id: mappings[name] ?? null,
		});
	}
	raw("alpha/one", "TaskDrafted", { project: "alpha", title: "А", task_text: "x" });
	raw("alpha/one", "Delegated", { executor: "grok" });
	raw("beta/one", "TaskDrafted", { project: "beta", title: "Б", task_text: "y" });
	raw("beta/one", "Delegated", { executor: "grok" });
	// Общий инцидент: текста «в beta сломался билд» в чужом пространстве быть не должно.
	raw("_general", "IncidentRecorded", { description: "в beta сломался билд", lesson: "урок" });
	raw("_general", "ArtifactRecorded", { project: "alpha", kind: "note", title: "з", body: "т" });
	db.close();
	return dbPath;
}

// Какие seq реально приняло облако (идемпотентность: повторы не считаются).
function appliedSeqs(cloud, journalId = JOURNAL) {
	return [...(cloud.state.seen.get(journalId) ?? new Set())].sort((a, b) => a - b);
}

function countEvents(dbPath) {
	const db = new DatabaseSync(dbPath, { readOnly: true });
	try {
		return db.prepare("SELECT count(*) AS n, coalesce(max(seq), 0) AS maxSeq FROM events").get();
	} finally {
		db.close();
	}
}

// ============ мок облака ============

function startMockCloud(t, { token = TOKEN, workspaceId = WORKSPACE } = {}) {
	const state = {
		cursors: new Map(),
		seen: new Map(),
		posts: 0,
		gets: 0,
		lastAuth: null,
		cursorPath: null,
		cursorHeaders: null,
		postPath: null,
		workspaceId,
	};
	const server = createServer((req, res) => {
		const url = new URL(req.url, "http://127.0.0.1");
		state.lastAuth = req.headers.authorization ?? null;
		const send = (code, obj) => {
			res.writeHead(code, { "content-type": "application/json" });
			res.end(JSON.stringify(obj));
		};
		if (state.lastAuth !== `Bearer ${token}`)
			return send(401, { error: "Invalid or revoked MCP token" });
		if (req.method === "GET") {
			state.gets += 1;
			state.cursorPath = url.pathname;
			state.cursorHeaders = req.headers;
			const journalId = url.searchParams.get("journalId");
			if (!journalId) return send(400, { error: "Missing journalId query parameter" });
			// Контракт роута: курсор + пространство токена (локально неизвестно).
			return send(200, {
				lastSeq: state.cursors.get(journalId) ?? 0,
				...(state.workspaceId === null ? {} : { workspaceId: state.workspaceId }),
			});
		}
		if (req.method === "POST") {
			let body = "";
			req.on("data", (c) => (body += c));
			req.on("end", () => {
				state.posts += 1;
				state.postPath = url.pathname;
				const { journalId, events } = JSON.parse(body);
				if (!journalId || !Array.isArray(events) || events.length === 0)
					return send(400, { error: "Invalid input" });
				const seen = state.seen.get(journalId) ?? new Set();
				state.seen.set(journalId, seen);
				let cursor = state.cursors.get(journalId) ?? 0;
				let applied = 0;
				let skipped = 0;
				for (const e of events) {
					if (!Number.isInteger(e.seq) || typeof e.taskId !== "string" || !e.taskId)
						return send(400, { error: "Invalid input" });
					if (typeof e.payload !== "object" || e.payload === null || Array.isArray(e.payload))
						return send(400, { error: "Invalid input" });
					if (seen.has(e.seq)) {
						skipped += 1;
						continue;
					}
					seen.add(e.seq);
					applied += 1;
					if (e.seq > cursor) cursor = e.seq;
				}
				state.cursors.set(journalId, cursor);
				send(200, { applied, skipped, lastSeq: cursor });
			});
			return;
		}
		send(405, { error: "Method not allowed" });
	});
	return new Promise((resolve) => {
		server.listen(0, "127.0.0.1", () => {
			const port = server.address().port;
			t.after(() => new Promise((r) => server.close(r)));
			resolve({
				url: `http://127.0.0.1:${port}/api/mcp/journal-sync`,
				baseUrl: `http://127.0.0.1:${port}`,
				port,
				state,
				close: () => new Promise((r) => server.close(r)),
			});
		});
	});
}

// Порт, на котором гарантированно никто не слушает (открыли и закрыли).
async function deadEndpoint() {
	const srv = createServer(() => {});
	await new Promise((r) => srv.listen(0, "127.0.0.1", r));
	const port = srv.address().port;
	await new Promise((r) => srv.close(r));
	return `http://127.0.0.1:${port}/api/mcp/journal-sync`;
}

async function waitFor(fn, { timeout = 4000, step = 25 } = {}) {
	const deadline = Date.now() + timeout;
	while (Date.now() < deadline) {
		if (fn()) return true;
		await new Promise((r) => setTimeout(r, step));
	}
	return false;
}

// env без унаследованных WORKHORSE_SYNC_* (чтобы окружение машины не влияло)
function cleanEnv(extra = {}) {
	const env = { ...process.env };
	for (const k of [
		"WORKHORSE_SYNC_URL",
		"WORKHORSE_SYNC_TOKEN",
		"WORKHORSE_SYNC_JOURNAL_ID",
		"WORKHORSE_SYNC_CONFIG",
		"WORKHORSE_DB",
	])
		delete env[k];
	return { ...env, ...extra };
}

function runCli(env) {
	return new Promise((resolve) => {
		const child = spawn(process.execPath, [SYNC_CLI], { env, stdio: ["ignore", "pipe", "pipe"] });
		let stdout = "";
		let stderr = "";
		child.stdout.on("data", (c) => (stdout += c));
		child.stderr.on("data", (c) => (stderr += c));
		child.on("close", (code) => resolve({ code, stdout, stderr }));
	});
}

// Минимальный MCP-клиент поверх спавна server.mjs (как в server.test.mjs).
function startMcp(t, env) {
	const child = spawn(process.execPath, [SERVER], {
		env,
		stdio: ["pipe", "pipe", "pipe"],
	});
	t.after(() => child.kill());
	let stderr = "";
	child.stderr.setEncoding("utf8");
	child.stderr.on("data", (c) => (stderr += c));

	let buf = "";
	const pending = new Map();
	child.stdout.setEncoding("utf8");
	child.stdout.on("data", (chunk) => {
		buf += chunk;
		while (true) {
			const nl = buf.indexOf("\n");
			if (nl === -1) break;
			const line = buf.slice(0, nl).trim();
			buf = buf.slice(nl + 1);
			if (!line) continue;
			const msg = JSON.parse(line);
			pending.get(msg.id)?.(msg);
			pending.delete(msg.id);
		}
	});

	let seq = 0;
	const call = (method, params) =>
		new Promise((resolve) => {
			const id = ++seq;
			pending.set(id, resolve);
			child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
		});
	const tool = async (name, args) => {
		const msg = await call("tools/call", { name, arguments: args });
		const text = msg.result.content[0].text;
		if (msg.result.isError) return { ok: false, error: text };
		try {
			return { ok: true, data: JSON.parse(text), text };
		} catch {
			return { ok: true, text };
		}
	};
	return { call, tool, getStderr: () => stderr };
}

// ============ loadSyncConfig ============

test("loadSyncConfig: нет файла и env → синк выключен (null)", () => {
	const dir = tmpDir();
	const config = loadSyncConfig({ dbPath: join(dir, "x.db"), env: {}, log: () => {} });
	assert.equal(config, null);
});

test("loadSyncConfig: файл sync.json рядом с базой + env-перекрытия", () => {
	const dir = tmpDir();
	const dbPath = join(dir, "x.db");
	writeFileSync(
		join(dir, "sync.json"),
		JSON.stringify({ url: "http://file", token: "t-file", journalId: "j-file" }),
	);

	let config = loadSyncConfig({ dbPath, env: {}, log: () => {} });
	assert.deepEqual(config, { url: "http://file", token: "t-file", journalId: "j-file" });

	config = loadSyncConfig({
		dbPath,
		env: { WORKHORSE_SYNC_URL: "http://env", WORKHORSE_SYNC_JOURNAL_ID: "j-env" },
		log: () => {},
	});
	assert.deepEqual(config, { url: "http://env", token: "t-file", journalId: "j-env" });

	// явный путь конфига через WORKHORSE_SYNC_CONFIG
	const altPath = join(dir, "alt.json");
	writeFileSync(altPath, JSON.stringify({ url: "http://alt", token: "t-alt", journalId: "j-alt" }));
	config = loadSyncConfig({ dbPath, env: { WORKHORSE_SYNC_CONFIG: altPath }, log: () => {} });
	assert.equal(config.url, "http://alt");
});

test("loadSyncConfig: url и token из env — journalId выводится сам", () => {
	const dir = tmpDir();
	const config = loadSyncConfig({
		dbPath: join(dir, "x.db"),
		env: {
			WORKHORSE_SYNC_URL: "https://app.workhorse-ai.dev",
			WORKHORSE_SYNC_TOKEN: "t-env",
		},
		log: () => {},
	});

	// Настройка через env в .mcp.json не должна требовать выдумывать
	// идентификатор журнала: иначе синк падал бы на «конфиг неполный».
	assert.equal(config.url, "https://app.workhorse-ai.dev");
	assert.equal(config.token, "t-env");
	assert.equal(config.journalId, defaultJournalId());
});

test("loadSyncConfig: битый JSON игнорируется (строка в лог), без env → выключен", () => {
	const dir = tmpDir();
	const dbPath = join(dir, "x.db");
	writeFileSync(join(dir, "sync.json"), "не json {");
	const lines = [];
	const config = loadSyncConfig({ dbPath, env: {}, log: (l) => lines.push(l) });
	assert.equal(config, null);
	assert.equal(lines.length, 1);
	assert.match(lines[0], /не парсится/);
});

// ============ writeSyncConfig / defaultJournalId ============

test("writeSyncConfig: пишет sync.json рядом с базой, loadSyncConfig его читает", () => {
	const dir = tmpDir();
	const dbPath = join(dir, "x.db");
	const config = { url: "http://cloud", token: "t-1", journalId: "j-1" };

	const path = writeSyncConfig({ dbPath, env: {}, config });
	assert.equal(path, join(dir, "sync.json"));
	assert.deepEqual(JSON.parse(readFileSync(path, "utf8")), config);
	assert.deepEqual(loadSyncConfig({ dbPath, env: {}, log: () => {} }), config);

	// перезапись — новый конфиг целиком заменяет старый
	writeSyncConfig({
		dbPath,
		env: {},
		config: { url: "http://other", token: "t-2", journalId: "j-2" },
	});
	assert.equal(JSON.parse(readFileSync(path, "utf8")).journalId, "j-2");

	// WORKHORSE_SYNC_CONFIG переопределяет путь записи так же, как путь чтения
	const altPath = join(dir, "alt.json");
	const written = writeSyncConfig({ dbPath, env: { WORKHORSE_SYNC_CONFIG: altPath }, config });
	assert.equal(written, altPath);
	assert.equal(JSON.parse(readFileSync(altPath, "utf8")).token, "t-1");
});

test("defaultJournalId: <username>-<hostname>, нормализация в [a-z0-9-]", () => {
	assert.equal(
		defaultJournalId({ username: "KVandake", host: "MacBook-Pro.local" }),
		"kvandake-macbook-pro-local",
	);
	assert.equal(defaultJournalId({ username: "иван__2", host: "x..y" }), "2-x-y");
	assert.equal(
		defaultJournalId({ username: "--", host: "--" }),
		"journal",
		"пустой остаток → заглушка",
	);
	assert.match(defaultJournalId(), /^[a-z0-9-]+$/, "реальные username/hostname нормализуются");
});

// Результат pushJournal — агрегат по целям: {pushed, lastSeq, targets: [...]}.
// Для одной цели сумма совпадает с её собственными числами — это и проверяем
// (многоцелевые ожидания живут в тестах про несколько целей ниже).
function assertOneTarget(result, { pushed, lastSeq }) {
	assert.equal(result.pushed, pushed, "суммарно отправлено");
	assert.equal(result.lastSeq, lastSeq, "курсор");
	assert.equal(result.targets.length, 1, "цель одна");
	assert.equal(result.targets[0].pushed, pushed);
	assert.equal(result.targets[0].lastSeq, lastSeq);
	assert.equal(result.targets[0].error, undefined);
	assert.equal(result.error, undefined);
}

// ============ pushJournal ============

test("pushJournal: полный пуш с нуля, повтор, докат", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const cloud = await startMockCloud(t);
	const config = { url: cloud.url, token: TOKEN, journalId: JOURNAL };

	// полный пуш с нуля
	let r = await pushJournal({ dbPath, config });
	assertOneTarget(r, { pushed: 10, lastSeq: 10 });
	assert.equal(cloud.state.cursors.get(JOURNAL), 10);
	assert.equal(cloud.state.lastAuth, `Bearer ${TOKEN}`, "заголовок Authorization: Bearer <token>");

	// повторный пуш — нечего отправлять
	r = await pushJournal({ dbPath, config });
	assertOneTarget(r, { pushed: 0, lastSeq: 10 });

	// докат после новых событий
	const { db, raw } = openDb(dbPath);
	raw("test/b", "ReportSubmitted", { report: "готово б" });
	raw("test/c", "TaskDrafted", { project: "test", title: "Задача В", task_text: "три" });
	raw("test/c", "Delegated", { executor: "grok" });
	db.close();

	r = await pushJournal({ dbPath, config });
	assertOneTarget(r, { pushed: 3, lastSeq: 13 });
	assert.equal(cloud.state.cursors.get(JOURNAL), 13);
});

test("pushJournal: батчи по 200 (несколько POST)", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir); // 10 событий
	const { db, raw } = openDb(dbPath);
	for (let i = 0; i < 245; i++)
		raw("_general", "IncidentRecorded", { description: `d${i}`, lesson: `l${i}` });
	db.close(); // всего 255

	const cloud = await startMockCloud(t);
	const r = await pushJournal({
		dbPath,
		config: { url: cloud.url, token: TOKEN, journalId: JOURNAL },
	});
	assertOneTarget(r, { pushed: 255, lastSeq: 255 });
	assert.equal(cloud.state.posts, Math.ceil(255 / BATCH_SIZE), "255 событий = 2 батча");
});

test("pushJournal: недоступное облако → {error}, база не тронута", async () => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const before = countEvents(dbPath);

	const r = await pushJournal({
		dbPath,
		config: { url: await deadEndpoint(), token: TOKEN, journalId: JOURNAL },
	});
	assert.ok(r.error, "ошибка возвращается значением, не исключением");
	assert.deepEqual(countEvents(dbPath), before, "журнал не изменён");
});

test("pushJournal: невалидный токен → {error} с HTTP-статусом", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const cloud = await startMockCloud(t);
	const r = await pushJournal({
		dbPath,
		config: { url: cloud.url, token: "wrong", journalId: JOURNAL },
	});
	assert.match(r.error, /401/);
});

test("pushJournal: без конфига выключен, неполный конфиг → {error}", async () => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	let r = await pushJournal({ dbPath, config: null });
	assert.deepEqual(r, { pushed: 0, disabled: true, targets: [] });
	r = await pushJournal({ dbPath, config: { url: "http://127.0.0.1:1" } });
	assert.match(r.error, /неполный/);
});

// ============ CLI ============

test("CLI: успешный пуш → stdout и exit 0", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const cloud = await startMockCloud(t);

	const { code, stdout } = await runCli(
		cleanEnv({
			WORKHORSE_DB: dbPath,
			WORKHORSE_SYNC_URL: cloud.url,
			WORKHORSE_SYNC_TOKEN: TOKEN,
			WORKHORSE_SYNC_JOURNAL_ID: JOURNAL,
		}),
	);
	assert.equal(code, 0);
	assert.match(stdout, /отправлено 10, курсор 10/);
});

test("CLI: синк не настроен → exit 1 с понятной ошибкой", async () => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir); // sync.json рядом нет
	const { code, stderr } = await runCli(cleanEnv({ WORKHORSE_DB: dbPath }));
	assert.equal(code, 1);
	assert.match(stderr, /синк не настроен/);
});

test("CLI: недоступное облако → exit 1", async () => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const { code, stderr } = await runCli(
		cleanEnv({
			WORKHORSE_DB: dbPath,
			WORKHORSE_SYNC_URL: await deadEndpoint(),
			WORKHORSE_SYNC_TOKEN: TOKEN,
			WORKHORSE_SYNC_JOURNAL_ID: JOURNAL,
		}),
	);
	assert.equal(code, 1);
	assert.match(stderr, /workhorse-sync:/);
});

// ============ MCP-инструмент sync и авто-пуш ============

test("MCP sync: инструмент отвечает текстом; без конфига — «не настроен»", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const cloud = await startMockCloud(t);

	const configured = startMcp(
		t,
		cleanEnv({
			WORKHORSE_DB: dbPath,
			WORKHORSE_SYNC_URL: cloud.url,
			WORKHORSE_SYNC_TOKEN: TOKEN,
			WORKHORSE_SYNC_JOURNAL_ID: JOURNAL,
		}),
	);
	const tools = (await configured.call("tools/list")).result.tools.map((x) => x.name);
	assert.ok(tools.includes("sync"), "инструмент sync объявлен");

	let r = await configured.tool("sync", {});
	assert.equal(r.text, "отправлено 10 событий, курсор 10");
	r = await configured.tool("sync", {});
	assert.equal(r.text, "отправлено 0 событий, курсор 10");

	const dir2 = tmpDir();
	const unconfigured = startMcp(t, cleanEnv({ WORKHORSE_DB: makeJournalDb(dir2) }));
	r = await unconfigured.tool("sync", {});
	assert.equal(r.text, "синк не настроен (нет sync.json)");
});

test("MCP sync: ошибка облака → текст ошибки, сервер жив", async (t) => {
	const dir = tmpDir();
	const c = startMcp(
		t,
		cleanEnv({
			WORKHORSE_DB: makeJournalDb(dir),
			WORKHORSE_SYNC_URL: await deadEndpoint(),
			WORKHORSE_SYNC_TOKEN: TOKEN,
			WORKHORSE_SYNC_JOURNAL_ID: JOURNAL,
		}),
	);
	const r = await c.tool("sync", {});
	assert.match(r.text, /^ошибка синка:/);
	const alive = await c.tool("list_tasks", {});
	assert.equal(alive.ok, true, "сервер продолжает отвечать");
});

test("авто-пуш: запись события уезжает в облако fire-and-forget", async (t) => {
	const dir = tmpDir();
	const dbPath = join(dir, "auto.db"); // пустая база — схему создаст сервер
	const cloud = await startMockCloud(t);
	const c = startMcp(
		t,
		cleanEnv({
			WORKHORSE_DB: dbPath,
			WORKHORSE_SCHEMA: SCHEMA,
			WORKHORSE_SYNC_URL: cloud.url,
			WORKHORSE_SYNC_TOKEN: TOKEN,
			WORKHORSE_SYNC_JOURNAL_ID: JOURNAL,
		}),
	);

	let r = await c.tool("register_project", { name: "auto", root_path: "/tmp/auto" });
	assert.equal(r.ok, true);
	assert.ok(
		await waitFor(() => (cloud.state.cursors.get(JOURNAL) ?? 0) >= 1),
		"ProjectRegistered доехал без вызова sync",
	);

	r = await c.tool("draft_task", { project: "auto", slug: "one", title: "т", task_text: "x" });
	assert.equal(r.ok, true);
	assert.ok(
		await waitFor(() => (cloud.state.cursors.get(JOURNAL) ?? 0) >= 2),
		"TaskDrafted доехал авто-пушем",
	);
});

test("авто-пуш: недоступное облако не блокирует и не роняет запись", async (t) => {
	const dir = tmpDir();
	const c = startMcp(
		t,
		cleanEnv({
			WORKHORSE_DB: join(dir, "offline.db"),
			WORKHORSE_SCHEMA: SCHEMA,
			WORKHORSE_SYNC_URL: await deadEndpoint(),
			WORKHORSE_SYNC_TOKEN: TOKEN,
			WORKHORSE_SYNC_JOURNAL_ID: JOURNAL,
		}),
	);

	let r = await c.tool("register_project", { name: "off", root_path: "/tmp/off" });
	assert.equal(r.ok, true, "запись проходит при мёртвом облаке");
	r = await c.tool("draft_task", { project: "off", slug: "one", title: "т", task_text: "x" });
	assert.equal(r.ok, true);

	assert.ok(
		await waitFor(() => c.getStderr().includes("авто-пуш синка не прошёл")),
		"одна строка в stderr вместо исключения",
	);
	r = await c.tool("list_tasks", {});
	assert.equal(r.ok, true, "сервер жив");
	assert.equal(r.data.length, 1);
});

// ============ MCP-инструмент connect ============

test("MCP connect: успех → sync.json написан рядом с базой, ответ с курсором", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir); // 10 событий
	const configPath = join(dir, "sync.json");
	const cloud = await startMockCloud(t);
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath }));

	const r = await c.tool("connect", {
		url: cloud.url,
		token: TOKEN,
		journal_id: "my-journal",
	});
	assert.equal(r.text, `подключено: курсор 0, журнал my-journal, конфиг ${configPath}`);
	assert.deepEqual(JSON.parse(readFileSync(configPath, "utf8")), {
		url: cloud.baseUrl,
		token: TOKEN,
		journalId: "my-journal",
	});

	// connect сразу пинает авто-пуш: накопленный журнал уезжает без ручного sync
	assert.ok(
		await waitFor(() => (cloud.state.cursors.get("my-journal") ?? 0) >= 10),
		"события доехали после connect",
	);

	// повторный connect — осознанная перезапись конфига
	const r2 = await c.tool("connect", { url: cloud.url, token: TOKEN, journal_id: "other-journal" });
	assert.match(r2.text, /^подключено: курсор 0, журнал other-journal/);
	assert.equal(JSON.parse(readFileSync(configPath, "utf8")).journalId, "other-journal");
});

test("MCP connect: плохой токен → 401, конфиг НЕ записан", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const configPath = join(dir, "sync.json");
	const cloud = await startMockCloud(t);
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath }));

	const r = await c.tool("connect", { url: cloud.url, token: "wrong", journal_id: "j" });
	assert.match(r.text, /^не подключено: HTTP 401 — токен не принят/);
	assert.equal(existsSync(configPath), false, "sync.json не появился");

	// недоступное облако — тоже без конфига, с понятной причиной
	const r2 = await c.tool("connect", { url: await deadEndpoint(), token: TOKEN });
	// Сообщение называет конкретный хост — при on-premise это половина диагностики.
	assert.match(r2.text, /^не подключено: http:\/\/127\.0\.0\.1:\d+ недоступен \(/);
	assert.equal(existsSync(configPath), false);
});

test("MCP connect: дефолтный journal_id — нормализованный <username>-<hostname>", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const configPath = join(dir, "sync.json");
	const cloud = await startMockCloud(t);
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath }));

	const expected = defaultJournalId();
	const r = await c.tool("connect", { url: cloud.url, token: TOKEN });
	assert.match(r.text, new RegExp(`^подключено: курсор 0, журнал ${expected},`));
	const written = JSON.parse(readFileSync(configPath, "utf8"));
	assert.equal(written.journalId, expected);
	assert.match(written.journalId, /^[a-z0-9-]+$/, "id нормализован");
});

test("авто-пуш: без конфига — тихий no-op (ни строки в stderr)", async (t) => {
	const dir = tmpDir();
	const c = startMcp(
		t,
		cleanEnv({ WORKHORSE_DB: join(dir, "plain.db"), WORKHORSE_SCHEMA: SCHEMA }),
	);
	await c.tool("register_project", { name: "plain", root_path: "/tmp/plain" });
	await c.tool("draft_task", { project: "plain", slug: "one", title: "т", task_text: "x" });
	// даём setImmediate-хвостам отработать
	await new Promise((r) => setTimeout(r, 100));
	assert.equal(c.getStderr(), "", "никакого шума без конфига");
});

// ============ Базовый URL инстанса (on-premise, 0.8.1) ============

test("normalizeBaseUrl: голая база, префикс прокси, полный эндпоинт, слеши", () => {
	assert.equal(normalizeBaseUrl("https://wh.acme.internal"), "https://wh.acme.internal");
	assert.equal(normalizeBaseUrl("https://wh.acme.internal/"), "https://wh.acme.internal");
	// On-premise за реверс-прокси: префикс пути обязан сохраниться.
	assert.equal(
		normalizeBaseUrl("https://tools.acme.com/workhorse/"),
		"https://tools.acme.com/workhorse",
	);
	// Совместимость: конфиги до 0.8.1 хранили полный адрес эндпоинта.
	assert.equal(
		normalizeBaseUrl("https://wh.acme.internal/api/mcp/journal-sync"),
		"https://wh.acme.internal",
	);
	assert.equal(
		normalizeBaseUrl("https://tools.acme.com/workhorse/api/mcp/journal-sync"),
		"https://tools.acme.com/workhorse",
	);
	// Внутренняя сеть: http и нестандартный порт — валидная база.
	assert.equal(normalizeBaseUrl("http://10.0.0.5:3300"), "http://10.0.0.5:3300");
});

test("syncUrlFromBase / inboxUrlFromBase: пути выводятся, префикс не теряется", () => {
	assert.equal(
		syncUrlFromBase("https://tools.acme.com/workhorse"),
		"https://tools.acme.com/workhorse/api/mcp/journal-sync",
	);
	assert.equal(
		inboxUrlFromBase("https://tools.acme.com/workhorse"),
		"https://tools.acme.com/workhorse/api/mcp/journal-inbox",
	);
	// Старое имя принимает полный эндпоинт и отдаёт соседний.
	assert.equal(
		inboxUrlFromSyncUrl("https://wh.acme.internal/api/mcp/journal-sync"),
		"https://wh.acme.internal/api/mcp/journal-inbox",
	);
});

test("pushJournal: работает от базы и просит не кешировать курсор", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const cloud = await startMockCloud(t);

	// Передаём БАЗУ — пути должен вывести сам пушер.
	const r = await pushJournal({
		dbPath,
		config: { url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL },
	});

	assertOneTarget(r, { pushed: 10, lastSeq: 10 });
	assert.equal(
		cloud.state.cursorPath,
		"/api/mcp/journal-sync",
		"GET курсора ушёл на выведенный путь",
	);
	assert.equal(cloud.state.postPath, "/api/mcp/journal-sync", "POST батча тоже");
	// Кеширующий прокси перед on-premise вернул бы устаревший lastSeq —
	// и пушер погнал бы уже отправленные события заново.
	assert.equal(cloud.state.cursorHeaders["cache-control"], "no-store");
});

test("resolveCloudUrl: без url — управляемое облако, env перекрывает, явный url главнее", () => {
	assert.equal(resolveCloudUrl({ env: {} }), DEFAULT_CLOUD_URL);
	assert.equal(
		resolveCloudUrl({ env: { WORKHORSE_CLOUD_URL: "https://staging.workhorse-ai.dev" } }),
		"https://staging.workhorse-ai.dev",
	);
	// On-premise: явный адрес перебивает и дефолт, и env.
	assert.equal(
		resolveCloudUrl({ url: "https://wh.acme.internal", env: { WORKHORSE_CLOUD_URL: "https://x" } }),
		"https://wh.acme.internal",
	);
});

// ============ Область синка: какие проекты уезжают в пространство ============

test("eventProject: проект события выводится для каждого типа", () => {
	// payload.project — источник для артефактов и постановок
	assert.equal(
		eventProject({ taskId: "_general", type: "ArtifactRecorded", payload: { project: "alpha" } }),
		"alpha",
	);
	assert.equal(
		eventProject({ taskId: "alpha/one", type: "TaskDrafted", payload: { project: "alpha" } }),
		"alpha",
	);
	// task_id — источник для всего жизненного цикла задачи (префикс до «/»)
	for (const type of [
		"Delegated",
		"ReportSubmitted",
		"Accepted",
		"ReworkRequested",
		"Failed",
		"TaskLinked",
	]) {
		assert.equal(
			eventProject({ taskId: "alpha/2026-09-01-slug", type, payload: {} }),
			"alpha",
			type,
		);
	}
	// ProjectRegistered — имя в payload, task_id всегда _general
	assert.equal(
		eventProject({
			taskId: "_general",
			type: "ProjectRegistered",
			payload: { name: "alpha", root_path: "/tmp/alpha" },
		}),
		"alpha",
	);
	// инцидент бывает и привязанным к задаче, и общим
	assert.equal(
		eventProject({ taskId: "alpha/one", type: "IncidentRecorded", payload: {} }),
		"alpha",
	);
	assert.equal(eventProject({ taskId: "_general", type: "IncidentRecorded", payload: {} }), null);
});

test("parseSyncProjects: список через запятую, пустая строка = не задано", () => {
	assert.deepEqual(parseSyncProjects("planado, dom-pro"), ["planado", "dom-pro"]);
	assert.deepEqual(parseSyncProjects("one"), ["one"]);
	assert.equal(parseSyncProjects(""), null);
	assert.equal(parseSyncProjects("  ,  "), null);
	assert.equal(parseSyncProjects(undefined), null);
});

test("resolveSyncScope: env > маппинг > всё-с-предупреждением", () => {
	const projects = [
		{ name: "alpha", cloud_workspace_id: WORKSPACE },
		{ name: "beta", cloud_workspace_id: OTHER_WORKSPACE },
	];

	// 1. env — явное намерение человека, перекрывает маппинг
	const byEnv = resolveSyncScope({
		env: { WORKHORSE_SYNC_PROJECTS: "beta" },
		projects,
		workspaceId: WORKSPACE,
	});
	assert.deepEqual([...byEnv.projects], ["beta"]);
	assert.equal(byEnv.source, "env");

	// 2. маппинг на пространство токена
	const byMapping = resolveSyncScope({ env: {}, projects, workspaceId: WORKSPACE });
	assert.deepEqual([...byMapping.projects], ["alpha"]);
	assert.equal(byMapping.source, "mapping");
	assert.equal(byMapping.warning, null);

	// 3. маппинга нет ни у кого → всё, но громко
	const none = resolveSyncScope({
		env: {},
		projects: [{ name: "alpha", cloud_workspace_id: null }],
		workspaceId: WORKSPACE,
	});
	assert.equal(none.projects, null);
	assert.equal(none.source, "all");
	assert.match(none.warning, /ни у одного проекта нет cloud_workspace_id/);

	// 4. маппинг есть, но облако не назвало пространство (старый сервер) → всё
	const blind = resolveSyncScope({ env: {}, projects, workspaceId: null });
	assert.equal(blind.projects, null);
	assert.match(blind.warning, /не сообщило id пространства/);
});

test("shouldSyncEvent: общее и ProjectRegistered при активной области не уезжают", () => {
	const scope = new Set(["alpha"]);
	assert.equal(
		shouldSyncEvent({ taskId: "alpha/one", type: "Delegated", payload: {} }, scope),
		true,
	);
	assert.equal(
		shouldSyncEvent({ taskId: "beta/one", type: "Delegated", payload: {} }, scope),
		false,
	);
	// общий инцидент — текст называет чужие проекты, в чужое пространство нельзя
	assert.equal(
		shouldSyncEvent({ taskId: "_general", type: "IncidentRecorded", payload: {} }, scope),
		false,
	);
	// реестр проектов: в облаке проекции нет, а payload несёт локальный root_path
	assert.equal(
		shouldSyncEvent(
			{ taskId: "_general", type: "ProjectRegistered", payload: { name: "alpha" } },
			scope,
		),
		false,
	);
	// без области (обратная совместимость) уезжает всё
	assert.equal(
		shouldSyncEvent({ taskId: "_general", type: "IncidentRecorded", payload: {} }, null),
		true,
	);
});

test("pushJournal: WORKHORSE_SYNC_PROJECTS ограничивает область", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir);
	const cloud = await startMockCloud(t);

	const r = await pushJournal({
		dbPath,
		env: { WORKHORSE_SYNC_PROJECTS: "alpha" },
		config: { url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL },
	});

	// 3,4 — задача alpha; 8 — артефакт alpha (у него есть payload.project).
	// Не уехали: 1,2 (ProjectRegistered), 5,6 (beta), 7 (общий инцидент).
	assert.equal(r.pushed, 3);
	assert.deepEqual(appliedSeqs(cloud), [3, 4, 8]);
});

test("pushJournal: фильтр по cloud_workspace_id пространства токена", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir, {
		mappings: { alpha: WORKSPACE, beta: OTHER_WORKSPACE },
	});
	const cloud = await startMockCloud(t);

	const r = await pushJournal({
		dbPath,
		env: {},
		config: { url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL },
	});

	assert.equal(r.pushed, 3);
	assert.deepEqual(appliedSeqs(cloud), [3, 4, 8], "beta привязан к другому пространству");
});

test("pushJournal: маппинга нет ни у одного проекта → едет всё + предупреждение", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir);
	const cloud = await startMockCloud(t);
	const lines = [];

	const r = await pushJournal({
		dbPath,
		env: {},
		config: { url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL },
		log: (line) => lines.push(line),
	});

	assert.equal(r.pushed, 8, "обратная совместимость: поведение до 0.9");
	assert.deepEqual(appliedSeqs(cloud), [1, 2, 3, 4, 5, 6, 7, 8]);
	assert.ok(
		lines.some((line) => /ни у одного проекта нет cloud_workspace_id/.test(line)),
		"молча всё слать нельзя — в логе предупреждение",
	);
	assert.equal(existsSync(syncStatePath({ dbPath })), false, "без области файл состояния не нужен");
});

test("pushJournal: маппинг есть, но ни один не на это пространство → не уезжает ничего", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir, {
		mappings: { alpha: OTHER_WORKSPACE, beta: OTHER_WORKSPACE },
	});
	const cloud = await startMockCloud(t);

	const r = await pushJournal({
		dbPath,
		env: {},
		config: { url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL },
	});

	assert.equal(r.pushed, 0);
	assert.equal(cloud.state.posts, 0, "пустой батч не отправляется");
});

test("pushJournal: облако без workspaceId (старый сервер) не ломает синк", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir, { mappings: { alpha: WORKSPACE } });
	const cloud = await startMockCloud(t, { workspaceId: null });
	const lines = [];

	const r = await pushJournal({
		dbPath,
		env: {},
		config: { url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL },
		log: (line) => lines.push(line),
	});

	assert.equal(r.pushed, 8, "сузить не по чему — поведение не меняем");
	assert.ok(lines.some((line) => /не сообщило id пространства/.test(line)));
});

// ============ Курсор при смене области ============

test("курсор: расширение области → пересинхронизация с нуля, отфильтрованное догоняет", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir);
	const cloud = await startMockCloud(t);
	const config = { url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL };

	// Область = alpha: курсор облака уезжает на 8, события beta (5,6) остаются
	// ПОЗАДИ курсора — общий курсор один на журнал.
	let r = await pushJournal({ dbPath, env: { WORKHORSE_SYNC_PROJECTS: "alpha" }, config });
	assert.deepEqual(appliedSeqs(cloud), [3, 4, 8]);
	assert.equal(r.lastSeq, 8);
	assert.deepEqual(readSyncState({ dbPath }).projects, ["alpha"], "область зафиксирована");

	// Расширяем область — beta обязан догнать, несмотря на курсор 8.
	const lines = [];
	r = await pushJournal({
		dbPath,
		env: { WORKHORSE_SYNC_PROJECTS: "alpha,beta" },
		config,
		log: (line) => lines.push(line),
	});
	assert.ok(lines.some((line) => /пересинхронизация с нуля/.test(line)));
	assert.deepEqual(appliedSeqs(cloud), [3, 4, 5, 6, 8], "beta доехал из-за курсора");
	assert.deepEqual(readSyncState({ dbPath }).projects, ["alpha", "beta"]);
});

test("курсор: сужение области пересинка не требует", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir);
	const cloud = await startMockCloud(t);
	const config = { url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL };

	await pushJournal({ dbPath, env: { WORKHORSE_SYNC_PROJECTS: "alpha,beta" }, config });
	const postsAfterFirst = cloud.state.posts;

	const lines = [];
	const r = await pushJournal({
		dbPath,
		env: { WORKHORSE_SYNC_PROJECTS: "alpha" },
		config,
		log: (line) => lines.push(line),
	});
	assert.equal(r.pushed, 0);
	assert.equal(cloud.state.posts, postsAfterFirst, "новых POST нет");
	assert.ok(!lines.some((line) => /пересинхронизация/.test(line)));
});

test("курсор: апгрейд со старой версии (состояния нет) пересинк не запускает", () => {
	// Файла состояния нет → прошлая область «всё» → любая новая ⊆ прошлой.
	assert.equal(needsRescan(null, new Set(["alpha"])), false);
	assert.equal(needsRescan(null, null), false);
	// Было сужено, стало «всё» — за курсором остались чужие события.
	assert.equal(needsRescan(["alpha"], null), true);
	assert.equal(needsRescan(["alpha"], new Set(["alpha", "beta"])), true);
	assert.equal(needsRescan(["alpha", "beta"], new Set(["alpha"])), false);
});

// ============ MCP-инструмент sync_scope ============

test("MCP sync_scope: показывает область и привязывает проекты к пространству", async (t) => {
	const dir = tmpDir();
	const dbPath = join(dir, "journal.db");
	const cloud = await startMockCloud(t);
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath, WORKHORSE_SCHEMA: SCHEMA }));

	await c.tool("register_project", { name: "alpha", root_path: "/tmp/alpha" });
	await c.tool("register_project", { name: "beta", root_path: "/tmp/beta", force: true });
	await c.tool("connect", { url: cloud.baseUrl, token: TOKEN, journal_id: JOURNAL });

	// без аргументов — честный отчёт: область не задана, уедет всё
	const before = await c.tool("sync_scope", {});
	assert.match(before.text, /ВСЕ проекты/);

	// с projects — маппинг проставляется сам, id пространства спрашивается у облака
	const set = await c.tool("sync_scope", { projects: ["alpha"] });
	assert.match(set.text, /область синка: alpha/);
	assert.match(set.text, new RegExp(WORKSPACE));

	const list = await c.tool("list_projects", {});
	const byName = Object.fromEntries(list.data.map((p) => [p.name, p.cloud_workspace_id]));
	assert.equal(byName.alpha, WORKSPACE);
	assert.ok(!byName.beta, "beta к этому пространству не привязан");

	// снятие привязки: снова уедет всё
	const cleared = await c.tool("sync_scope", { projects: [] });
	assert.match(cleared.text, /ВСЕ проекты/);
});

test("MCP sync_scope: незарегистрированный проект — ошибка, журнал не трогаем", async (t) => {
	const dir = tmpDir();
	const dbPath = join(dir, "journal.db");
	const cloud = await startMockCloud(t);
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath, WORKHORSE_SCHEMA: SCHEMA }));

	await c.tool("register_project", { name: "alpha", root_path: "/tmp/alpha" });
	await c.tool("connect", { url: cloud.baseUrl, token: TOKEN, journal_id: JOURNAL });

	const r = await c.tool("sync_scope", { projects: ["ghost"] });
	assert.equal(r.ok, false);
	assert.match(r.error, /Не зарегистрированы: ghost/);
});

test("MCP connect: несколько проектов без области — предупреждение в ответе", async (t) => {
	const dir = tmpDir();
	const dbPath = join(dir, "journal.db");
	const cloud = await startMockCloud(t);
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath, WORKHORSE_SCHEMA: SCHEMA }));

	await c.tool("register_project", { name: "alpha", root_path: "/tmp/alpha" });
	await c.tool("register_project", { name: "beta", root_path: "/tmp/beta", force: true });

	const r = await c.tool("connect", { url: cloud.baseUrl, token: TOKEN, journal_id: JOURNAL });
	assert.match(r.text, /уедут ВСЕ проекты журнала \(alpha, beta\)/);
	assert.match(r.text, /sync_scope/);
});

test("MCP connect: без url подключается к облаку по умолчанию", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const configPath = join(dir, "sync.json");
	const cloud = await startMockCloud(t);
	// Подменяем адрес управляемого облака на мок: проверяется сам факт, что
	// url не обязателен, без похода в сеть.
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath, WORKHORSE_CLOUD_URL: cloud.baseUrl }));

	const r = await c.tool("connect", { token: TOKEN, journal_id: "default-cloud" });

	assert.equal(r.text, `подключено: курсор 0, журнал default-cloud, конфиг ${configPath}`);
	assert.equal(cloud.state.cursorPath, "/api/mcp/journal-sync");
	const cfg = JSON.parse(readFileSync(configPath, "utf8"));
	assert.equal(cfg.url, cloud.baseUrl, "в конфиг записан фактический адрес, а не пустота");
});

// ============ Несколько целей синка ============
//
// Маппинг проект → пространство лежит у каждого проекта, курсор в облаке — на
// пару (пространство, журнал). Транспорт обязан ходить по всем целям, не
// смешивая ни курсоры, ни области.

test("loadSyncTargets: плоский конфиг старого формата = одна цель, env перекрывает поля", () => {
	const dir = tmpDir();
	const dbPath = join(dir, "journal.db");
	writeFileSync(
		join(dir, "sync.json"),
		JSON.stringify({ url: "http://file", token: "t-file", journalId: "j-file" }),
	);

	const targets = loadSyncTargets({ dbPath, env: {}, log: () => {} });
	assert.equal(targets.length, 1);
	assert.equal(targets[0].url, "http://file");
	assert.equal(targets[0].journalId, "j-file");
	assert.equal(targets[0].alias, "file", "алиас по умолчанию — хост адреса");

	// Поведение env-перекрытий не изменилось ни на йоту.
	const overridden = loadSyncTargets({
		dbPath,
		env: { WORKHORSE_SYNC_URL: "http://env", WORKHORSE_SYNC_JOURNAL_ID: "j-env" },
		log: () => {},
	});
	assert.equal(overridden.length, 1);
	assert.equal(overridden[0].url, "http://env");
	assert.equal(overridden[0].token, "t-file");
	assert.equal(overridden[0].journalId, "j-env");
});

test("loadSyncTargets: список targets, уникальные алиасы, env со списком не смешивается", () => {
	const dir = tmpDir();
	const dbPath = join(dir, "journal.db");
	writeFileSync(
		join(dir, "sync.json"),
		JSON.stringify({
			targets: [
				{ alias: "acme", url: "https://wh.acme.internal", token: "t1", journalId: "j" },
				{ url: "https://app.workhorse-ai.dev", token: "t2", journalId: "j" },
				{ url: "https://app.workhorse-ai.dev", token: "t3", journalId: "j2" },
			],
		}),
	);

	const lines = [];
	const targets = loadSyncTargets({
		dbPath,
		env: { WORKHORSE_SYNC_URL: "http://env", WORKHORSE_SYNC_TOKEN: "t-env" },
		log: (line) => lines.push(line),
	});

	assert.deepEqual(
		targets.map((t) => t.alias),
		["acme", "app.workhorse-ai.dev", "app.workhorse-ai.dev#2"],
		"алиасы уникальны — ими человек адресует цель",
	);
	assert.equal(
		targets.some((t) => t.url === "http://env"),
		false,
		"env не превращается в тайную третью цель",
	);
	assert.ok(
		lines.some((line) => /WORKHORSE_SYNC_URL\/TOKEN\/JOURNAL_ID/.test(line)),
		"игнорирование env — со строкой в лог, не молча",
	);
});

test("две цели: каждый проект уезжает только в своё пространство", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir, {
		mappings: { alpha: WORKSPACE, beta: OTHER_WORKSPACE },
	});
	const first = await startMockCloud(t);
	const second = await startMockCloud(t, { workspaceId: OTHER_WORKSPACE });

	const r = await pushJournal({
		dbPath,
		env: {},
		targets: [
			{ alias: "first", url: first.baseUrl, token: TOKEN, journalId: JOURNAL },
			{ alias: "second", url: second.baseUrl, token: TOKEN, journalId: JOURNAL },
		],
	});

	assert.equal(r.pushed, 5, "3 события alpha + 2 события beta");
	assert.deepEqual(
		r.targets.map((x) => [x.alias, x.pushed, x.lastSeq]),
		[
			["first", 3, 8],
			["second", 2, 6],
		],
	);
	assert.deepEqual(appliedSeqs(first), [3, 4, 8], "в первое пространство только alpha");
	assert.deepEqual(appliedSeqs(second), [5, 6], "во второе только beta");
	// Курсоры независимы: у каждой цели свой.
	assert.equal(first.state.cursors.get(JOURNAL), 8);
	assert.equal(second.state.cursors.get(JOURNAL), 6);
	// Состояние области — по целям, ключ = пространство.
	const state = JSON.parse(readFileSync(syncStatePath({ dbPath }), "utf8"));
	assert.deepEqual(state.targets[WORKSPACE].projects, ["alpha"]);
	assert.deepEqual(state.targets[OTHER_WORKSPACE].projects, ["beta"]);
});

test("две цели: недоступность одной не мешает пушу в остальные", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir, {
		mappings: { alpha: WORKSPACE, beta: OTHER_WORKSPACE },
	});
	const alive = await startMockCloud(t, { workspaceId: OTHER_WORKSPACE });
	const lines = [];

	const r = await pushJournal({
		dbPath,
		env: {},
		targets: [
			{ alias: "dead", url: await deadEndpoint(), token: TOKEN, journalId: JOURNAL },
			{ alias: "alive", url: alive.baseUrl, token: TOKEN, journalId: JOURNAL },
		],
		log: (line) => lines.push(line),
	});

	assert.ok(r.targets[0].error, "первая цель с ошибкой");
	assert.equal(r.targets[1].error, undefined, "вторая отработала");
	assert.equal(r.pushed, 2);
	assert.deepEqual(appliedSeqs(alive), [5, 6]);
	assert.equal(r.error, undefined, "часть целей доехала — общей ошибки нет");
	assert.ok(
		lines.some((line) => /^\[dead\]/.test(line)),
		"в логе видно, какая цель упала",
	);
});

test("две цели: расширение области у одной не вызывает пересинк у другой", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir, {
		mappings: { alpha: WORKSPACE, beta: OTHER_WORKSPACE },
	});
	const first = await startMockCloud(t);
	const second = await startMockCloud(t, { workspaceId: OTHER_WORKSPACE });
	const targets = [
		{ alias: "first", url: first.baseUrl, token: TOKEN, journalId: JOURNAL },
		{ alias: "second", url: second.baseUrl, token: TOKEN, journalId: JOURNAL },
	];

	await pushJournal({ dbPath, env: {}, targets });
	const secondPosts = second.state.posts;

	// beta переезжает в первое пространство: у первой цели область расширилась,
	// у второй — сузилась.
	const { db, raw } = openDb(dbPath);
	raw("_general", "ProjectRegistered", {
		name: "beta",
		root_path: "/tmp/beta",
		cloud_workspace_id: WORKSPACE,
	});
	db.close();

	const lines = [];
	await pushJournal({ dbPath, env: {}, targets, log: (line) => lines.push(line) });

	assert.ok(
		lines.some((line) => /^\[first\].*пересинхронизация с нуля/.test(line)),
		"расширенная цель идёт с нуля",
	);
	assert.ok(
		!lines.some((line) => /^\[second\].*пересинхронизация/.test(line)),
		"вторую цель чужое расширение не касается",
	);
	assert.deepEqual(appliedSeqs(first), [3, 4, 5, 6, 8], "beta догнал в первом пространстве");
	assert.equal(second.state.posts, secondPosts, "во второе новых POST нет");
});

test("состояние: плоский sync-state.json старых версий = состояние единственной цели", async (t) => {
	const dir = tmpDir();
	const dbPath = makeMultiProjectDb(dir, { mappings: { alpha: WORKSPACE, beta: WORKSPACE } });
	const cloud = await startMockCloud(t);
	// Файл, оставшийся от версии до многоцелевого синка: область была уже alpha.
	writeFileSync(
		syncStatePath({ dbPath }),
		JSON.stringify({ projects: ["alpha"], at: "2026-01-01T00:00:00.000Z" }),
	);

	const lines = [];
	await pushJournal({
		dbPath,
		env: {},
		targets: [{ url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL }],
		log: (line) => lines.push(line),
	});

	assert.ok(
		lines.some((line) => /пересинхронизация с нуля/.test(line)),
		"старое состояние прочитано: область расширилась с alpha до alpha+beta",
	);
	const state = JSON.parse(readFileSync(syncStatePath({ dbPath }), "utf8"));
	assert.deepEqual(state.targets[WORKSPACE].projects, ["alpha", "beta"], "переехало в карту целей");
});

test("syncStateKey: пространство надёжнее адреса, адрес — запасной ключ", () => {
	const target = { url: "https://wh.acme.internal/", journalId: "j" };
	assert.equal(syncStateKey({ target, workspaceId: "ws-1" }), "ws-1");
	assert.equal(syncStateKey({ target }), "https://wh.acme.internal#j");
});

test("совместимость: плоский sync.json без env — пуш работает как раньше (CLI)", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const cloud = await startMockCloud(t);
	writeFileSync(
		join(dir, "sync.json"),
		JSON.stringify({ url: cloud.baseUrl, token: TOKEN, journalId: JOURNAL }),
	);

	const { code, stdout } = await runCli(cleanEnv({ WORKHORSE_DB: dbPath }));
	assert.equal(code, 0);
	assert.match(stdout, /отправлено 10, курсор 10/);
	assert.equal(cloud.state.cursors.get(JOURNAL), 10);
});

test("MCP: две цели — sync_scope по каждой, привязка по алиасу, sync со сводкой", async (t) => {
	const dir = tmpDir();
	const dbPath = join(dir, "journal.db");
	const acme = await startMockCloud(t);
	const lab = await startMockCloud(t, { workspaceId: OTHER_WORKSPACE });
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath, WORKHORSE_SCHEMA: SCHEMA }));

	// Проекты и задачи заводим ДО настройки синка: seq 1,2 — ProjectRegistered,
	// 3 — задача alpha, 4 — задача beta.
	await c.tool("register_project", { name: "alpha", root_path: "/tmp/alpha" });
	await c.tool("register_project", { name: "beta", root_path: "/tmp/beta", force: true });
	await c.tool("draft_task", { project: "alpha", slug: "one", title: "А", task_text: "x" });
	await c.tool("draft_task", { project: "beta", slug: "one", title: "Б", task_text: "y" });

	writeFileSync(
		join(dir, "sync.json"),
		JSON.stringify({
			targets: [
				{ alias: "acme", url: acme.baseUrl, token: TOKEN, journalId: JOURNAL },
				{ alias: "lab", url: lab.baseUrl, token: TOKEN, journalId: JOURNAL },
			],
		}),
	);

	// Показ — по всем целям сразу.
	const before = await c.tool("sync_scope", {});
	assert.match(before.text, /область синка по целям \(2\)/);
	assert.match(before.text, /- acme .*ВСЕ проекты/);
	assert.match(before.text, /- lab .*ВСЕ проекты/);

	// Привязка без указания цели при нескольких целях — отказ с перечнем целей.
	const ambiguous = await c.tool("sync_scope", { projects: ["alpha"] });
	assert.equal(ambiguous.ok, false);
	assert.match(ambiguous.error, /укажи target: acme, lab/);

	// Человек называет цель алиасом, id пространства не знает.
	const bound = await c.tool("sync_scope", { target: "acme", projects: ["alpha"] });
	assert.match(bound.text, /- acme .*: alpha/);
	const bound2 = await c.tool("sync_scope", { target: "lab", projects: ["beta"] });
	assert.match(bound2.text, /- lab .*: beta/);
	assert.match(bound2.text, /- acme .*: alpha/, "привязка второй цели не сняла первую");

	const list = await c.tool("list_projects", {});
	const byName = Object.fromEntries(list.data.map((p) => [p.name, p.cloud_workspace_id]));
	assert.equal(byName.alpha, WORKSPACE);
	assert.equal(byName.beta, OTHER_WORKSPACE);

	const synced = await c.tool("sync", {});
	assert.match(synced.text, /целей 2, все успешно:/);
	assert.match(synced.text, /- acme: отправлено/);
	assert.match(synced.text, /- lab: отправлено/);

	assert.deepEqual(appliedSeqs(acme), [3], "в acme только задача alpha");
	assert.deepEqual(appliedSeqs(lab), [4], "в lab только задача beta");
});

test("MCP connect: alias добавляет вторую цель, без alias — перезапись как раньше", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const configPath = join(dir, "sync.json");
	const first = await startMockCloud(t);
	const second = await startMockCloud(t, { workspaceId: OTHER_WORKSPACE });
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath }));

	await c.tool("connect", { url: first.baseUrl, token: TOKEN, journal_id: "j1" });
	assert.deepEqual(JSON.parse(readFileSync(configPath, "utf8")), {
		url: first.baseUrl,
		token: TOKEN,
		journalId: "j1",
	});

	const added = await c.tool("connect", {
		url: second.baseUrl,
		token: TOKEN,
		journal_id: "j2",
		alias: "lab",
	});
	assert.match(added.text, /^подключено/);
	assert.match(added.text, /целей синка: 2 \(.*lab.*\) — журнал поедет во все/);
	const config = JSON.parse(readFileSync(configPath, "utf8"));
	assert.equal(config.targets.length, 2, "первая цель осталась");
	assert.equal(config.targets[0].url, first.baseUrl);
	assert.deepEqual(config.targets[1], {
		alias: "lab",
		url: second.baseUrl,
		token: TOKEN,
		journalId: "j2",
	});

	// Обе цели получают журнал.
	assert.ok(
		await waitFor(
			() =>
				(first.state.cursors.get("j1") ?? 0) >= 10 && (second.state.cursors.get("j2") ?? 0) >= 10,
		),
		"журнал уехал в обе цели",
	);
});

test("авто-пуш: мёртвая цель — строка в stderr, живая цель получает событие", async (t) => {
	const dir = tmpDir();
	const dbPath = join(dir, "journal.db");
	const alive = await startMockCloud(t);
	writeFileSync(
		join(dir, "sync.json"),
		JSON.stringify({
			targets: [
				{ alias: "dead", url: await deadEndpoint(), token: TOKEN, journalId: JOURNAL },
				{ alias: "alive", url: alive.baseUrl, token: TOKEN, journalId: JOURNAL },
			],
		}),
	);
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: dbPath, WORKHORSE_SCHEMA: SCHEMA }));

	const r = await c.tool("register_project", { name: "auto", root_path: "/tmp/auto" });
	assert.equal(r.ok, true, "запись проходит, несмотря на мёртвую цель");
	assert.ok(
		await waitFor(() => (alive.state.cursors.get(JOURNAL) ?? 0) >= 1),
		"живая цель получила событие",
	);
	assert.ok(
		await waitFor(() => c.getStderr().includes("авто-пуш синка не прошёл (dead)")),
		"про мёртвую цель — одна строка в stderr, с её именем",
	);
	assert.equal((await c.tool("list_projects", {})).ok, true, "сервер жив");
});
