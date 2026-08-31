#!/usr/bin/env node
// workhorse-sync — пушер журнала в облако planado (шаг 2 спеки «Sync design»).
// Направление строго вверх: GET курсор → POST батчи событий с seq > курсора.
// Zero deps: node:sqlite (read-only) + встроенный fetch. Ошибки возвращаются
// значением ({error}), исключения наружу не летят — пушер никогда не роняет журнал.

import { DatabaseSync } from "node:sqlite";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { homedir, hostname, userInfo } from "node:os";
import { join, dirname } from "node:path";
import { pathToFileURL } from "node:url";

export const BATCH_SIZE = 200;

// Данные под бренд Workhorse AI: директория по бренду, файл по смыслу.
export const DEFAULT_DB_DIR = ".workhorse-ai";
export const DEFAULT_DB_FILE = "journal.db";

// Единственный резолвер пути к базе (сервер и CLI пушера используют его же):
// WORKHORSE_DB перекрывает всё, иначе ~/.workhorse-ai/journal.db.
// Чистая функция — env и homedir подменяемы, тестируется без реальной ФС.
export function resolveDbPath({ env = process.env, homedir: home = homedir() } = {}) {
	return env.WORKHORSE_DB ?? join(home, DEFAULT_DB_DIR, DEFAULT_DB_FILE);
}

// Конфиг синка: JSON-файл {url, token, journalId} рядом с базой (sync.json),
// путь переопределяется WORKHORSE_SYNC_CONFIG; env-переменные
// WORKHORSE_SYNC_URL / WORKHORSE_SYNC_TOKEN / WORKHORSE_SYNC_JOURNAL_ID
// перекрывают значения файла. Нет ни файла, ни env → синк выключен (null) — это норма.
export function loadSyncConfig({ dbPath, env = process.env, log = console.error } = {}) {
	const resolvedDbPath = dbPath ?? resolveDbPath({ env });
	const configPath = env.WORKHORSE_SYNC_CONFIG ?? join(dirname(resolvedDbPath), "sync.json");

	let fileConfig = {};
	if (existsSync(configPath)) {
		try {
			fileConfig = JSON.parse(readFileSync(configPath, "utf8"));
		} catch {
			log(`workhorse-sync: конфиг не парсится, игнорирую: ${configPath}`);
			fileConfig = {};
		}
	}

	const config = {
		url: env.WORKHORSE_SYNC_URL ?? fileConfig.url,
		token: env.WORKHORSE_SYNC_TOKEN ?? fileConfig.token,
		journalId: env.WORKHORSE_SYNC_JOURNAL_ID ?? fileConfig.journalId,
	};
	if (!config.url && !config.token && !config.journalId) return null;
	return config;
}

// Запись конфига синка: JSON {url, token, journalId} по тому же пути, откуда
// его читает loadSyncConfig (WORKHORSE_SYNC_CONFIG ?? sync.json рядом с базой).
// Перезапись существующего файла — осознанное действие пользователя (connect).
// Возвращает путь записанного файла.
export function writeSyncConfig({ dbPath, env = process.env, config }) {
	const resolvedDbPath = dbPath ?? resolveDbPath({ env });
	const configPath = env.WORKHORSE_SYNC_CONFIG ?? join(dirname(resolvedDbPath), "sync.json");
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
export function inboxUrlFromSyncUrl(syncUrl) {
	const url = new URL(syncUrl);
	url.pathname = url.pathname
		.replace(/\/+$/, "")
		.replace(/[^/]*$/, "journal-inbox");
	return url.toString();
}

// Локальный payload хранится TEXT-JSON; облачная схема ждёт объект.
function parsePayload(text) {
	try {
		const parsed = JSON.parse(text);
		return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
			? parsed
			: {};
	} catch {
		return {};
	}
}

// Пуш журнала: {pushed, lastSeq} либо {error} — без исключений наружу.
// Базу открывает read-only: пушер — транспорт, журнал он не меняет никогда.
export async function pushJournal({ dbPath, config, log = () => {} }) {
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

		const cursorUrl = new URL(config.url);
		cursorUrl.searchParams.set("journalId", config.journalId);
		const cursorRes = await fetch(cursorUrl, { headers });
		if (!cursorRes.ok) {
			return { error: `GET курсора: HTTP ${cursorRes.status}` };
		}
		const { lastSeq } = await cursorRes.json();
		if (!Number.isInteger(lastSeq) || lastSeq < 0) {
			return { error: `GET курсора: облако вернуло некорректный lastSeq (${lastSeq})` };
		}

		let rows;
		const db = new DatabaseSync(dbPath, { readOnly: true });
		try {
			rows = db
				.prepare(
					"SELECT seq, task_id, type, at, payload FROM events WHERE seq > ? ORDER BY seq",
				)
				.all(lastSeq);
		} finally {
			db.close();
		}
		log(`workhorse-sync: событий за курсором ${lastSeq}: ${rows.length}`);
		if (rows.length === 0) return { pushed: 0, lastSeq };

		let cursor = lastSeq;
		let pushed = 0;
		for (let i = 0; i < rows.length; i += BATCH_SIZE) {
			const batch = rows.slice(i, i + BATCH_SIZE).map((row) => ({
				seq: row.seq,
				taskId: row.task_id,
				type: row.type,
				at: row.at,
				payload: parsePayload(row.payload),
			}));
			const res = await fetch(config.url, {
				method: "POST",
				headers,
				body: JSON.stringify({ journalId: config.journalId, events: batch }),
			});
			if (!res.ok) {
				return { error: `POST батча: HTTP ${res.status}`, pushed, lastSeq: cursor };
			}
			const result = await res.json();
			cursor = result.lastSeq ?? batch[batch.length - 1].seq;
			pushed += batch.length;
			log(`workhorse-sync: батч ${batch.length}, курсор облака ${cursor}`);
		}
		return { pushed, lastSeq: cursor };
	} catch (err) {
		return { error: err instanceof Error ? err.message : String(err) };
	}
}

// ============ CLI: node sync.mjs / workhorse-sync ============

const isCli =
	process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isCli) {
	const dbPath = resolveDbPath();
	const config = loadSyncConfig({ dbPath });
	if (!config) {
		console.error(
			"workhorse-sync: синк не настроен — нет sync.json рядом с базой и нет WORKHORSE_SYNC_URL/TOKEN/JOURNAL_ID",
		);
		process.exit(1);
	}
	const result = await pushJournal({ dbPath, config, log: console.error });
	if (result.error) {
		console.error(`workhorse-sync: ${result.error}`);
		process.exit(1);
	}
	console.log(`отправлено ${result.pushed}, курсор ${result.lastSeq}`);
	process.exit(0);
}
