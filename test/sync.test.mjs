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
	inboxUrlFromBase,
	inboxUrlFromSyncUrl,
	loadSyncConfig,
	normalizeBaseUrl,
	pushJournal,
	resolveCloudUrl,
	syncUrlFromBase,
	writeSyncConfig,
} from "../sync.mjs";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const SCHEMA = join(ROOT, "schema.sql");
const SERVER = join(ROOT, "server.mjs");
const SYNC_CLI = join(ROOT, "sync.mjs");

const TOKEN = "test-mcp-token";
const JOURNAL = "journal-test";

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
	raw("_general", "ArtifactRecorded", { project: "test", kind: "note", title: "заметка", body: "текст" });
	raw("test/b", "TaskLinked", { to_task_id: "test/a", kind: "relates" });
	db.close();
	return dbPath; // 10 событий, seq 1..10
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

function startMockCloud(t, { token = TOKEN } = {}) {
	const state = { cursors: new Map(), seen: new Map(), posts: 0, gets: 0, lastAuth: null, cursorPath: null, cursorHeaders: null, postPath: null };
	const server = createServer((req, res) => {
		const url = new URL(req.url, "http://127.0.0.1");
		state.lastAuth = req.headers.authorization ?? null;
		const send = (code, obj) => {
			res.writeHead(code, { "content-type": "application/json" });
			res.end(JSON.stringify(obj));
		};
		if (state.lastAuth !== `Bearer ${token}`) return send(401, { error: "Invalid or revoked MCP token" });
		if (req.method === "GET") {
			state.gets += 1;
			state.cursorPath = url.pathname;
			state.cursorHeaders = req.headers;
			const journalId = url.searchParams.get("journalId");
			if (!journalId) return send(400, { error: "Missing journalId query parameter" });
			return send(200, { lastSeq: state.cursors.get(journalId) ?? 0 });
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
	for (const k of ["WORKHORSE_SYNC_URL", "WORKHORSE_SYNC_TOKEN", "WORKHORSE_SYNC_JOURNAL_ID", "WORKHORSE_SYNC_CONFIG", "WORKHORSE_DB"])
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
		let nl;
		while ((nl = buf.indexOf("\n")) !== -1) {
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
			child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
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
	writeFileSync(join(dir, "sync.json"), JSON.stringify({ url: "http://file", token: "t-file", journalId: "j-file" }));

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
	writeSyncConfig({ dbPath, env: {}, config: { url: "http://other", token: "t-2", journalId: "j-2" } });
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
	assert.equal(defaultJournalId({ username: "--", host: "--" }), "journal", "пустой остаток → заглушка");
	assert.match(defaultJournalId(), /^[a-z0-9-]+$/, "реальные username/hostname нормализуются");
});

// ============ pushJournal ============

test("pushJournal: полный пуш с нуля, повтор, докат", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	const cloud = await startMockCloud(t);
	const config = { url: cloud.url, token: TOKEN, journalId: JOURNAL };

	// полный пуш с нуля
	let r = await pushJournal({ dbPath, config });
	assert.deepEqual(r, { pushed: 10, lastSeq: 10 });
	assert.equal(cloud.state.cursors.get(JOURNAL), 10);
	assert.equal(cloud.state.lastAuth, `Bearer ${TOKEN}`, "заголовок Authorization: Bearer <token>");

	// повторный пуш — нечего отправлять
	r = await pushJournal({ dbPath, config });
	assert.deepEqual(r, { pushed: 0, lastSeq: 10 });

	// докат после новых событий
	const { db, raw } = openDb(dbPath);
	raw("test/b", "ReportSubmitted", { report: "готово б" });
	raw("test/c", "TaskDrafted", { project: "test", title: "Задача В", task_text: "три" });
	raw("test/c", "Delegated", { executor: "grok" });
	db.close();

	r = await pushJournal({ dbPath, config });
	assert.deepEqual(r, { pushed: 3, lastSeq: 13 });
	assert.equal(cloud.state.cursors.get(JOURNAL), 13);
});

test("pushJournal: батчи по 200 (несколько POST)", async (t) => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir); // 10 событий
	const { db, raw } = openDb(dbPath);
	for (let i = 0; i < 245; i++) raw("_general", "IncidentRecorded", { description: `d${i}`, lesson: `l${i}` });
	db.close(); // всего 255

	const cloud = await startMockCloud(t);
	const r = await pushJournal({ dbPath, config: { url: cloud.url, token: TOKEN, journalId: JOURNAL } });
	assert.deepEqual(r, { pushed: 255, lastSeq: 255 });
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
	const r = await pushJournal({ dbPath, config: { url: cloud.url, token: "wrong", journalId: JOURNAL } });
	assert.match(r.error, /401/);
});

test("pushJournal: без конфига выключен, неполный конфиг → {error}", async () => {
	const dir = tmpDir();
	const dbPath = makeJournalDb(dir);
	let r = await pushJournal({ dbPath, config: null });
	assert.deepEqual(r, { pushed: 0, disabled: true });
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
	const c = startMcp(t, cleanEnv({ WORKHORSE_DB: join(dir, "plain.db"), WORKHORSE_SCHEMA: SCHEMA }));
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

	assert.deepEqual(r, { pushed: 10, lastSeq: 10 });
	assert.equal(cloud.state.cursorPath, "/api/mcp/journal-sync", "GET курсора ушёл на выведенный путь");
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
