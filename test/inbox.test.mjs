// Тесты pull-инбокса намерений (6b): инструменты inbox/take и связка
// draft_task → payload.intent_task_id. Облако — in-process мок на node:http
// (контракт journal-inbox: GET список, POST идемпотентный take, auth Bearer).
// Никакой сети наружу: только 127.0.0.1 с портом от ОС.

import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { inboxUrlFromSyncUrl } from "../sync.mjs";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const SCHEMA = join(ROOT, "schema.sql");
const SERVER = join(ROOT, "server.mjs");

const TOKEN = "test-mcp-token";
const JOURNAL = "journal-test";

// ============ фикстуры ============

function tmpDir() {
	return mkdtempSync(join(tmpdir(), "workhorse-inbox-test-"));
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

// ============ мок облака: journal-sync (для авто-пуша) + journal-inbox ============

function makeIntent(id, title, featureTitle, extra = {}) {
	return {
		id,
		title,
		description: extra.description ?? null,
		feature: {
			title: featureTitle,
			description: extra.featureDescription ?? null,
			clarifications: extra.clarifications ?? null,
		},
		createdAt: "2026-08-30T10:00:00.000Z",
		...("recommendedSlug" in extra ? { recommendedSlug: extra.recommendedSlug } : {}),
	};
}

function startMockCloud(t, { token = TOKEN, items = [] } = {}) {
	const state = {
		items: new Map(items.map((item) => [item.id, { ...item, takenAt: null }])),
		cursor: 0,
		seen: new Set(),
		takes: [],
	};
	const server = createServer((req, res) => {
		const url = new URL(req.url, "http://127.0.0.1");
		const send = (code, obj) => {
			res.writeHead(code, { "content-type": "application/json" });
			res.end(JSON.stringify(obj));
		};
		if (req.headers.authorization !== `Bearer ${token}`)
			return send(401, { error: "Invalid or revoked MCP token" });

		// Контракт journal-sync — чтобы авто-пуш сервера не сыпал ошибками
		if (url.pathname.endsWith("/journal-sync")) {
			if (req.method === "GET") return send(200, { lastSeq: state.cursor });
			let body = "";
			req.on("data", (c) => (body += c));
			req.on("end", () => {
				const { events } = JSON.parse(body);
				for (const e of events) {
					if (!state.seen.has(e.seq)) {
						state.seen.add(e.seq);
						if (e.seq > state.cursor) state.cursor = e.seq;
					}
				}
				send(200, { applied: events.length, skipped: 0, lastSeq: state.cursor });
			});
			return;
		}

		// Контракт journal-inbox: GET список, POST идемпотентный take
		if (url.pathname.endsWith("/journal-inbox")) {
			if (req.method === "GET") {
				const pending = [...state.items.values()].filter((i) => !i.takenAt);
				return send(200, {
					items: pending.map(({ takenAt, ...item }) => item),
				});
			}
			let body = "";
			req.on("data", (c) => (body += c));
			req.on("end", () => {
				const { taskId } = JSON.parse(body);
				state.takes.push(taskId);
				const item = state.items.get(taskId);
				if (!item) return send(404, { error: "Task not found" });
				const alreadyTaken = item.takenAt !== null;
				if (!alreadyTaken) item.takenAt = new Date().toISOString();
				const { takenAt, ...context } = item;
				send(200, {
					taken: true,
					alreadyTaken,
					context: { ...context, recommendedSlug: context.recommendedSlug ?? "intent" },
				});
			});
			return;
		}
		send(404, { error: "Unknown endpoint" });
	});
	return new Promise((resolve) => {
		server.listen(0, "127.0.0.1", () => {
			const port = server.address().port;
			t.after(() => new Promise((r) => server.close(r)));
			resolve({
				syncUrl: `http://127.0.0.1:${port}/api/mcp/journal-sync`,
				state,
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

// Минимальный MCP-клиент поверх спавна server.mjs (как в sync.test.mjs).
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

function mcpEnv(dir, syncUrl) {
	return cleanEnv({
		WORKHORSE_DB: join(dir, "inbox.db"),
		WORKHORSE_SCHEMA: SCHEMA,
		WORKHORSE_SYNC_URL: syncUrl,
		WORKHORSE_SYNC_TOKEN: TOKEN,
		WORKHORSE_SYNC_JOURNAL_ID: JOURNAL,
	});
}

// ============ inboxUrlFromSyncUrl ============

test("inboxUrlFromSyncUrl: последний сегмент пути → journal-inbox", () => {
	assert.equal(
		inboxUrlFromSyncUrl("https://cloud.example/api/mcp/journal-sync"),
		"https://cloud.example/api/mcp/journal-inbox",
	);
	// хвостовой слэш и порт не ломают вывод
	assert.equal(
		inboxUrlFromSyncUrl("http://127.0.0.1:3000/api/mcp/journal-sync/"),
		"http://127.0.0.1:3000/api/mcp/journal-inbox",
	);
});

// ============ инструмент inbox ============

test("inbox: список намерений — счётчик и краткие строки; пустой инбокс", async (t) => {
	const dir = tmpDir();
	const cloud = await startMockCloud(t, {
		items: [
			makeIntent("intent-1", "Тёмная тема", "UI-полировка"),
			makeIntent("intent-2", "Инбокс намерений", "Обратная петля"),
		],
	});
	const c = startMcp(t, mcpEnv(dir, cloud.syncUrl));

	const tools = (await c.call("tools/list")).result.tools.map((x) => x.name);
	assert.ok(tools.includes("inbox"), "инструмент inbox объявлен");
	assert.ok(tools.includes("take"), "инструмент take объявлен");

	let r = await c.tool("inbox", {});
	assert.match(r.text, /^2 намерений:/);
	assert.match(r.text, /- intent-1 — Тёмная тема \(фича: UI-полировка\)/);
	assert.match(r.text, /- intent-2 — Инбокс намерений \(фича: Обратная петля\)/);
	assert.match(r.text, /take/);

	// после забора обоих инбокс пуст
	await c.tool("take", { task_id: "intent-1" });
	await c.tool("take", { task_id: "intent-2" });
	r = await c.tool("inbox", {});
	assert.equal(r.text, "инбокс пуст: 0 намерений");
});

// ============ инструмент take ============

test("take: болванка для draft_task — intent_task_id, контекст фичи, slug; идемпотентность", async (t) => {
	const dir = tmpDir();
	const cloud = await startMockCloud(t, {
		items: [
			makeIntent("intent-42", "Тёмная тема", "UI-полировка", {
				description: "Переключатель в хедере",
				featureDescription: "Довести визуал до ума",
				clarifications: "Уважать prefers-color-scheme",
				recommendedSlug: "temnaya-tema",
			}),
		],
	});
	const c = startMcp(t, mcpEnv(dir, cloud.syncUrl));

	let r = await c.tool("take", { task_id: "intent-42" });
	assert.match(r.text, /Намерение забрано из инбокса/);
	assert.match(r.text, /intent_task_id: intent-42/);
	assert.match(r.text, /## Контекст фичи: UI-полировка/);
	assert.match(r.text, /Довести визуал до ума/);
	assert.match(r.text, /Уточнения: Уважать prefers-color-scheme/);
	assert.match(r.text, /## Намерение: Тёмная тема/);
	assert.match(r.text, /Переключатель в хедере/);
	assert.match(r.text, /Рекомендованный slug: temnaya-tema/);
	assert.match(r.text, /search_precedents .*draft_task с intent_task_id: "intent-42"/);

	// повторный take — не ошибка, тот же контекст с пометкой
	r = await c.tool("take", { task_id: "intent-42" });
	assert.match(r.text, /уже было забрано ранее/);
	assert.match(r.text, /intent_task_id: intent-42/);
	assert.deepEqual(cloud.state.takes, ["intent-42", "intent-42"]);

	// несуществующее намерение — понятный текст, не исключение
	r = await c.tool("take", { task_id: "no-such" });
	assert.equal(r.text, "намерение no-such не найдено в облаке");
});

// ============ draft_task → payload.intent_task_id ============

test("draft_task: intent_task_id уезжает в payload события TaskDrafted (сырое событие)", async (t) => {
	const dir = tmpDir();
	// Без конфига синка: авто-пуш — тихий no-op, журнал живёт локально
	const c = startMcp(
		t,
		cleanEnv({ WORKHORSE_DB: join(dir, "local.db"), WORKHORSE_SCHEMA: SCHEMA }),
	);
	await c.tool("register_project", { name: "demo", root_path: "/tmp/demo" });

	let r = await c.tool("draft_task", {
		project: "demo",
		slug: "with-intent",
		title: "Со ссылкой",
		task_text: "текст",
		intent_task_id: "intent-42",
	});
	assert.equal(r.ok, true);

	r = await c.tool("get_task", {
		task_id: `demo/${new Date().toISOString().slice(0, 10)}-with-intent`,
	});
	assert.equal(r.ok, true);
	const drafted = r.data.events.find((e) => e.type === "TaskDrafted");
	assert.equal(JSON.parse(drafted.payload).intent_task_id, "intent-42");

	// без аргумента — ключа в payload нет (не null-мусор)
	r = await c.tool("draft_task", {
		project: "demo",
		slug: "plain",
		title: "Без ссылки",
		task_text: "текст",
	});
	assert.equal(r.ok, true);
	r = await c.tool("get_task", { task_id: `demo/${new Date().toISOString().slice(0, 10)}-plain` });
	const plain = r.data.events.find((e) => e.type === "TaskDrafted");
	assert.equal("intent_task_id" in JSON.parse(plain.payload), false);
});

// ============ без конфига / мёртвое облако ============

test("inbox/take: без конфига синка — «инбокс не настроен», сервер жив", async (t) => {
	const dir = tmpDir();
	const c = startMcp(
		t,
		cleanEnv({ WORKHORSE_DB: join(dir, "plain.db"), WORKHORSE_SCHEMA: SCHEMA }),
	);

	let r = await c.tool("inbox", {});
	assert.equal(r.text, "инбокс не настроен (нет sync.json)");
	r = await c.tool("take", { task_id: "intent-1" });
	assert.equal(r.text, "инбокс не настроен (нет sync.json)");
	r = await c.tool("list_tasks", {});
	assert.equal(r.ok, true, "сервер продолжает отвечать");
});

test("inbox/take: недоступное облако → текст ошибки, не исключение", async (t) => {
	const dir = tmpDir();
	const c = startMcp(t, mcpEnv(dir, await deadEndpoint()));

	let r = await c.tool("inbox", {});
	assert.match(r.text, /^ошибка инбокса:/);
	r = await c.tool("take", { task_id: "intent-1" });
	assert.match(r.text, /^ошибка take:/);
	r = await c.tool("list_tasks", {});
	assert.equal(r.ok, true, "сервер жив");
});
