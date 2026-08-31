// Тесты workhorse-mcp: спавним настоящий сервер с временной базой,
// говорим с ним по JSON-RPC через stdio — как реальный MCP-клиент.
// Запуск: pnpm --filter workhorse-ai-mcp test (или node --test test/ из apps/mcp)

import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url))); // ~/.workhorse
const SERVER = join(ROOT, "server.mjs");

function startClient(t, dbPathOverride) {
	const dir = mkdtempSync(join(tmpdir(), "workhorse-test-"));
	const dbPath = dbPathOverride ?? join(dir, "test.db");
	const child = spawn(process.execPath, [SERVER], {
		env: { ...process.env, WORKHORSE_DB: dbPath },
		stdio: ["pipe", "pipe", "inherit"],
	});
	t.after(() => child.kill());

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

	// tools/call → {ok:true, data} либо {ok:false, error}
	const tool = async (name, args) => {
		const msg = await call("tools/call", { name, arguments: args });
		const text = msg.result.content[0].text;
		return msg.result.isError
			? { ok: false, error: text }
			: { ok: true, data: JSON.parse(text) };
	};

	return { call, tool, dbPath };
}

async function reg(c, name) {
	await c.tool("register_project", { name, root_path: `/tmp/${name}` });
}

// Полный путь задачи до REPORTED — общая заготовка нескольких тестов.
// Проект выводится из префикса id: неймспейс проекта входит в task_id.
async function bringToReported(c, taskId) {
	const project = taskId.split("/")[0];
	await reg(c, project);
	await c.tool("draft_task", { task_id: taskId, project, title: "т", task_text: "текст задания" });
	await c.tool("delegate", { task_id: taskId, executor: "grok" });
	await c.tool("submit_report", { task_id: taskId, report: "падал -> прошёл" });
}

test("handshake: initialize, tools, prompts, instructions", async (t) => {
	const c = startClient(t);
	const init = (await c.call("initialize", { protocolVersion: "2024-11-05" })).result;
	assert.equal(init.serverInfo.name, "workhorse-mcp");
	assert.ok(init.instructions.includes("REPORTED"), "initialize отдаёт протокол");
	assert.deepEqual(Object.keys(init.capabilities).sort(), ["prompts", "tools"]);

	const tools = (await c.call("tools/list")).result.tools.map((x) => x.name);
	assert.equal(tools.length, 21);
	for (const name of ["search_precedents", "draft_task", "delegate", "submit_report", "accept", "record_artifact", "link_tasks", "register_project", "connect", "sync", "inbox", "take"])
		assert.ok(tools.includes(name), name);

	const prompts = (await c.call("prompts/list")).result.prompts;
	assert.deepEqual(prompts.map((p) => p.name), ["tasks", "bootstrap"]);
	const got = (await c.call("prompts/get", { name: "tasks", arguments: { filter: "dom-pro" } })).result;
	assert.ok(got.messages[0].content.text.includes('"dom-pro"'), "фильтр подставлен в промпт");
	const boot = (await c.call("prompts/get", { name: "bootstrap", arguments: { project: "dom-pro" } })).result;
	assert.ok(boot.messages[0].content.text.includes("Project baseline:"), "bootstrap ведёт к артефакту baseline");
});

test("чистый старт: WORKHORSE_DB в несуществующей вложенной директории", async (t) => {
	const dir = mkdtempSync(join(tmpdir(), "workhorse-clean-"));
	const dbPath = join(dir, "no", "such", "dir", "workhorse.db");
	assert.equal(existsSync(dirname(dbPath)), false, "директории заведомо нет");

	const c = startClient(t, dbPath);
	const tools = (await c.call("tools/list")).result.tools;
	assert.ok(tools.length > 0, "сервер стартовал, не упав на открытии базы");
	assert.ok(existsSync(dbPath), "директория и база созданы сервером");

	const r = await c.tool("list_projects", {});
	assert.deepEqual(r.data, [], "схема применена — реестр проектов отвечает");
});

test("жизненный цикл: draft → delegate → report → accept", async (t) => {
	const c = startClient(t);
	await reg(c, "test");
	const id = "test/lifecycle";

	let r = await c.tool("draft_task", {
		task_id: id, project: "test", title: "Цикл", task_text: "сделать дело, база 385/385",
	});
	assert.equal(r.data.status, "DRAFT");

	r = await c.tool("delegate", { task_id: id, executor: "grok" });
	assert.equal(r.data.status, "DELEGATED");
	assert.equal(r.data.executor, "grok");

	r = await c.tool("submit_report", { task_id: id, report: "тест падал -> прошёл" });
	assert.equal(r.data.status, "REPORTED");
	assert.equal(r.data.report_text, "тест падал -> прошёл");

	r = await c.tool("accept", { task_id: id, verify_commit: "abc1234" });
	assert.equal(r.data.status, "ACCEPTED");
	assert.equal(r.data.outcome, "accepted");
	assert.equal(r.data.verify_commit, "abc1234");

	const hist = await c.tool("get_task", { task_id: id });
	assert.deepEqual(
		hist.data.events.map((e) => e.type),
		["TaskDrafted", "Delegated", "ReportSubmitted", "Accepted"],
	);
});

test("недопустимые переходы отбиваются", async (t) => {
	const c = startClient(t);
	await reg(c, "test");
	const id = "test/transitions";

	await c.tool("draft_task", { task_id: id, project: "test", title: "т", task_text: "x" });

	let r = await c.tool("accept", { task_id: id, verify_commit: "x" });
	assert.equal(r.ok, false);
	assert.match(r.error, /недопустим из статуса DRAFT/);

	r = await c.tool("submit_report", { task_id: id, report: "x" });
	assert.equal(r.ok, false, "отчёт без делегации");

	await c.tool("delegate", { task_id: id, executor: "grok" });
	r = await c.tool("delegate", { task_id: id, executor: "grok" });
	assert.equal(r.ok, false, "повторная делегация из DELEGATED");

	r = await c.tool("accept", { task_id: "test/ghost", verify_commit: "x" });
	assert.equal(r.ok, false);
	assert.match(r.error, /не найдена/);
});

test("rework: возврат на доработку и повторный цикл", async (t) => {
	const c = startClient(t);
	const id = "test/rework";
	await bringToReported(c, id);

	let r = await c.tool("request_rework", { task_id: id, reason: "сломан полный прогон" });
	assert.equal(r.data.status, "REWORK");

	r = await c.tool("delegate", { task_id: id, executor: "grok" });
	assert.equal(r.data.status, "DELEGATED", "из REWORK можно делегировать снова");

	await c.tool("submit_report", { task_id: id, report: "дофикс, 385/385" });
	r = await c.tool("accept", { task_id: id, verify_commit: "def5678" });
	assert.equal(r.data.status, "ACCEPTED");
});

test("mark_failed из зависшей делегации", async (t) => {
	const c = startClient(t);
	await reg(c, "test");
	const id = "test/failed";
	await c.tool("draft_task", { task_id: id, project: "test", title: "т", task_text: "x" });
	await c.tool("delegate", { task_id: id, executor: "grok" });

	const r = await c.tool("mark_failed", { task_id: id, reason: "shell SIGSEGV" });
	assert.equal(r.data.status, "FAILED");
	assert.equal(r.data.outcome, "failed");
});

test("draft_task: guards id и slug", async (t) => {
	const c = startClient(t);

	await reg(c, "test");
	let r = await c.tool("draft_task", { project: "test", title: "без слага", task_text: "x" });
	assert.equal(r.ok, false);
	assert.match(r.error, /slug/);

	r = await c.tool("draft_task", { project: "test", slug: "auto-id", title: "т", task_text: "x" });
	assert.match(r.data.task_id, /^test\/\d{4}-\d{2}-\d{2}-auto-id$/, "id собран из project/даты/slug");

	r = await c.tool("draft_task", {
		task_id: "other-project/чужой-неймспейс", project: "demo-app", title: "т", task_text: "x",
	});
	assert.equal(r.ok, false, "id из чужого неймспейса отбивается");
	assert.match(r.error, /должен начинаться с "demo-app\/"/);

	const id = "test/redraft";
	await bringToReported(c, id);
	await c.tool("accept", { task_id: id, verify_commit: "x" });
	r = await c.tool("draft_task", { task_id: id, project: "test", title: "т2", task_text: "y" });
	assert.equal(r.ok, false, "принятую задачу нельзя перечертить");
	assert.match(r.error, /уже в статусе ACCEPTED/);
});

test("поиск прецедентов: FTS по задачам и отчётам + инциденты", async (t) => {
	const c = startClient(t);
	await bringToReported(c, "test/search-me"); // report: «падал -> прошёл»
	await c.tool("draft_task", {
		task_id: "test/other", project: "test", title: "иное",
		task_text: "перенормировать ранги ганта в расписании",
	});
	await c.tool("record_incident", {
		description: "гонка за obj/ при параллельной сборке",
		lesson: "dotnet build-server shutdown перед прогоном",
	});

	let r = await c.tool("search_precedents", { query: "ранги ганта" });
	assert.deepEqual(r.data.tasks.map((x) => x.task_id), ["test/other"], "поиск по тексту задания");

	r = await c.tool("search_precedents", { query: "прошёл" });
	assert.ok(r.data.tasks.some((x) => x.task_id === "test/search-me"), "поиск по тексту отчёта");

	r = await c.tool("search_precedents", { query: "сборка гонка" });
	assert.equal(r.data.incidents.length, 1);
	assert.match(r.data.incidents[0].lesson, /build-server shutdown/);

	// русская морфология через префиксный матч: запрос в другой форме, чем в тексте
	r = await c.tool("search_precedents", { query: "ранг расписание" });
	assert.ok(r.data.tasks.some((x) => x.task_id === "test/other"), "«ранг» находит «ранги», «расписание» — «расписании»");
});

test("list_tasks: фильтры по статусу и проекту", async (t) => {
	const c = startClient(t);
	await bringToReported(c, "alpha/one");
	await reg(c, "beta");
	await c.tool("draft_task", { task_id: "beta/two", project: "beta", title: "т", task_text: "x" });

	let r = await c.tool("list_tasks", { status: "REPORTED" });
	assert.deepEqual(r.data.map((x) => x.task_id), ["alpha/one"]);

	r = await c.tool("list_tasks", { project: "beta" });
	assert.deepEqual(r.data.map((x) => x.task_id), ["beta/two"]);

	r = await c.tool("list_tasks", {});
	assert.equal(r.data.length, 2);
});

test("артефакты: запись, версии, выборка, поиск", async (t) => {
	const c = startClient(t);

	await reg(c, "workhorse");
	let r = await c.tool("record_artifact", {
		project: "workhorse", kind: "spec", title: "Формат журнала",
		body: "event sourcing поверх sqlite, проекции триггерами",
	});
	assert.equal(r.data.kind, "spec");
	const v1 = r.data.id;

	r = await c.tool("record_artifact", {
		project: "workhorse", kind: "spec", title: "Формат журнала",
		body: "версия 2: добавлены артефакты и маппинг на облако",
	});
	assert.ok(r.data.id > v1, "повторная запись = новая версия, старая остаётся");

	r = await c.tool("record_artifact", {
		project: "test", kind: "bogus", title: "x", body: "y",
	});
	assert.equal(r.ok, false, "неизвестный kind отбивается");

	await bringToReported(c, "test/linked");
	r = await c.tool("record_artifact", {
		project: "test", kind: "decision", title: "Решение по задаче",
		body: "принято в обсуждении", task_id: "test/linked",
	});
	assert.equal(r.data.task_id, "test/linked");

	r = await c.tool("list_artifacts", { project: "workhorse", kind: "spec" });
	assert.equal(r.data.length, 2);
	assert.ok(!("body" in r.data[0]), "список без тел");

	r = await c.tool("list_artifacts", { task_id: "test/linked" });
	assert.deepEqual(r.data.map((a) => a.kind), ["decision"]);

	const full = await c.tool("get_artifact", { id: v1 });
	assert.match(full.data.body, /event sourcing/);

	r = await c.tool("search_precedents", { query: "маппинг облако" });
	assert.ok(r.data.artifacts.some((a) => a.body_snippet.includes("[облако]")), "FTS по телу артефакта");

	const hist = await c.tool("get_task", { task_id: "test/linked" });
	assert.ok(hist.data.events.some((e) => e.type === "ArtifactRecorded"), "событие в истории задачи");
});

test("связи задач: continues/discovered_from, guards", async (t) => {
	const c = startClient(t);
	const oldId = "test/finished-work";
	await bringToReported(c, oldId);
	await c.tool("accept", { task_id: oldId, verify_commit: "abc" });

	const newId = "test/follow-up";
	await c.tool("draft_task", { task_id: newId, project: "test", title: "Продолжение", task_text: "x" });

	let r = await c.tool("link_tasks", { from_task_id: newId, to_task_id: oldId, kind: "continues" });
	assert.deepEqual(r.data, [{ from_task: newId, to_task: oldId, kind: "continues", at: r.data[0].at }]);

	r = await c.tool("link_tasks", { from_task_id: newId, to_task_id: oldId, kind: "bogus" });
	assert.equal(r.ok, false, "неизвестный kind отбивается");

	r = await c.tool("link_tasks", { from_task_id: newId, to_task_id: newId, kind: "relates" });
	assert.equal(r.ok, false, "самоссылка отбивается");

	r = await c.tool("link_tasks", { from_task_id: newId, to_task_id: "test/ghost", kind: "relates" });
	assert.equal(r.ok, false, "линк на несуществующую задачу отбивается");

	// повторный тот же линк — идемпотентно (UNIQUE + OR IGNORE)
	await c.tool("link_tasks", { from_task_id: newId, to_task_id: oldId, kind: "continues" });
	r = await c.tool("get_task", { task_id: newId });
	assert.equal(r.data.links.outgoing.length, 1);

	// обратная сторона видит входящий линк
	r = await c.tool("get_task", { task_id: oldId });
	assert.deepEqual(r.data.links.incoming, [{ from_task: newId, kind: "continues" }]);
});

test("реестр проектов: регистрация, guard, маппинг на облако", async (t) => {
	const c = startClient(t);

	let r = await c.tool("draft_task", { project: "ghost", slug: "x", title: "т", task_text: "x" });
	assert.equal(r.ok, false, "незарегистрированный проект отбивается");
	assert.match(r.error, /не зарегистрирован/);

	r = await c.tool("record_artifact", { project: "ghost", kind: "note", title: "т", body: "x" });
	assert.equal(r.ok, false, "артефакт для незарегистрированного проекта отбивается");

	r = await c.tool("register_project", { name: "Плохое Имя", root_path: "/tmp/x" });
	assert.equal(r.ok, false, "имя-неймспейс только латиница/цифры/дефисы");

	r = await c.tool("register_project", { name: "demo", root_path: "/tmp/demo-v1" });
	assert.equal(r.data.root_path, "/tmp/demo-v1");
	assert.equal(r.data.cloud_workspace_id, null);

	r = await c.tool("register_project", {
		name: "demo", root_path: "/tmp/demo-v2", cloud_workspace_id: "ws-123",
	});
	assert.equal(r.data.root_path, "/tmp/demo-v2", "перерегистрация обновляет путь");
	assert.equal(r.data.cloud_workspace_id, "ws-123");

	r = await c.tool("register_project", { name: "demo", root_path: "/tmp/demo-v3" });
	assert.equal(r.data.cloud_workspace_id, "ws-123", "маппинг не затирается при обновлении пути");

	r = await c.tool("draft_task", { project: "demo", slug: "works", title: "т", task_text: "x" });
	assert.equal(r.data.status, "DRAFT", "после регистрации задачи принимаются");

	r = await c.tool("list_projects", {});
	assert.deepEqual(r.data.map((p) => p.name), ["demo"]);
});

test("схема: guard-триггеры отбивают вставки мимо сервера", async (t) => {
	const c = startClient(t);
	await bringToReported(c, "test/db-guards");

	const db = new DatabaseSync(c.dbPath);
	t.after(() => db.close());
	const raw = (task, type, payload) =>
		db.prepare("INSERT INTO events(task_id,type,payload) VALUES (?,?,?)").run(task, type, JSON.stringify(payload));

	assert.throws(() => raw("_general", "ArtifactRecorded", { project: "ghost", kind: "doc", title: "x", body: "y" }),
		/not registered/, "артефакт в незарегистрированный проект");
	assert.throws(() => raw("ghost/task", "TaskDrafted", { project: "ghost", title: "x", task_text: "y" }),
		/not registered/, "задача в незарегистрированный проект");

	// из REPORTED rework разрешён (guard молчит), а отчёт после него — уже нет
	raw("test/db-guards", "ReworkRequested", { reason: "проверка" });
	assert.throws(() => raw("test/db-guards", "ReportSubmitted", { report: "x" }),
		/only from DELEGATED/, "отчёт из REWORK мимо делегации");
	assert.throws(() => raw("test/db-guards", "TaskLinked", { to_task_id: "test/db-guards", kind: "relates" }),
		/distinct existing/, "самоссылка на уровне БД");
	assert.throws(() => db.prepare("INSERT INTO events(task_id,type,payload) VALUES ('_general','IncidentRecorded','не json')").run(),
		/valid JSON/, "битый payload");
});

test("схема: журнал событий append-only", async (t) => {
	const c = startClient(t);
	await reg(c, "test");
	await c.tool("draft_task", { task_id: "test/ro", project: "test", title: "т", task_text: "x" });

	const db = new DatabaseSync(c.dbPath);
	t.after(() => db.close());
	assert.throws(() => db.exec("UPDATE events SET payload='{}'"), /append-only/);
	assert.throws(() => db.exec("DELETE FROM events"), /append-only/);
});

test("register_project: guard от почти-дублей и force", async (t) => {
	const c = startClient(t);
	await c.tool("register_project", { name: "dom-pro", root_path: "/tmp/dom-pro" });

	// похожее имя (нормализация убирает дефисы)
	let r = await c.tool("register_project", { name: "dompro", root_path: "/tmp/elsewhere" });
	assert.equal(r.ok, false);
	assert.ok(r.error.includes('"dom-pro"'), "ошибка называет существующий проект");

	// вложенный путь под чужим корнем
	r = await c.tool("register_project", { name: "sub", root_path: "/tmp/dom-pro/app" });
	assert.equal(r.ok, false);
	assert.ok(r.error.includes("dom-pro"), "конфликт по пути называет владельца корня");

	// родительский путь над чужим корнем — тоже конфликт
	r = await c.tool("register_project", { name: "umbrella", root_path: "/tmp" });
	assert.equal(r.ok, false);

	// точное имя — легальная перерегистрация (обновление пути)
	r = await c.tool("register_project", { name: "dom-pro", root_path: "/tmp/dom-pro-moved" });
	assert.equal(r.ok, true);
	assert.equal(r.data.root_path, "/tmp/dom-pro-moved");

	// force — осознанный обход
	r = await c.tool("register_project", { name: "dompro", root_path: "/tmp/elsewhere", force: true });
	assert.equal(r.ok, true);
});

test("resolve_project: поиск по пути и похожему имени", async (t) => {
	const c = startClient(t);
	await c.tool("register_project", { name: "dom-pro", root_path: "/tmp/proj/dom-pro" });
	await c.tool("register_project", { name: "other", root_path: "/tmp/proj/other" });

	// файл внутри корня → находит владельца
	let r = await c.tool("resolve_project", { path: "/tmp/proj/dom-pro/src/x.cs" });
	assert.equal(r.data.matches.length, 1);
	assert.equal(r.data.matches[0].name, "dom-pro");

	// похожее имя без дефисов
	r = await c.tool("resolve_project", { name: "dompro" });
	assert.equal(r.data.matches.length, 1);
	assert.equal(r.data.matches[0].name, "dom-pro");

	// подстрока имени
	r = await c.tool("resolve_project", { name: "dom-pro-app-net" });
	assert.equal(r.data.matches.some((m) => m.name === "dom-pro"), true);

	// ничего не найдено — пустой ответ
	r = await c.tool("resolve_project", { path: "/nowhere", name: "zzz" });
	assert.equal(r.data.matches.length, 0);
});
