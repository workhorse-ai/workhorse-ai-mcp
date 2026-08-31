// Скиллы едут внутри пакета: все три на месте, frontmatter корректен,
// каталог skills/ входит в files-whitelist (иначе npm его не упакует).

import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

// Линейка: оркестратор / исполнитель / соло. Имена симметричны ролям.
const SKILLS = ["workhorse-ai-orchestrator", "workhorse-ai-worker", "workhorse-ai-all"];

test("все три скилла лежат в пакете с корректным frontmatter", () => {
	for (const name of SKILLS) {
		const path = join(ROOT, "skills", name, "SKILL.md");
		assert.equal(existsSync(path), true, `skills/${name}/SKILL.md на месте`);

		const text = readFileSync(path, "utf8");
		const frontmatter = text.match(/^---\n([\s\S]*?)\n---\n/);
		assert.ok(frontmatter, `${name}: есть frontmatter`);
		assert.match(frontmatter[1], new RegExp(`^name: ${name}$`, "m"));
		assert.match(frontmatter[1], /^description: .+/m);
		assert.ok(text.includes("REPORTED"), `${name}: инвариант REPORTED описан`);
	}

	// Исполнителю приёмка запрещена явно — это стержень инварианта.
	const worker = readFileSync(join(ROOT, "skills", "workhorse-ai-worker", "SKILL.md"), "utf8");
	assert.match(worker, /not.*call.*`accept`/i);

	const pkg = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8"));
	assert.ok(
		pkg.files.some((entry) => entry.replace(/\/$/, "") === "skills"),
		"skills/ есть в files-whitelist",
	);
});
