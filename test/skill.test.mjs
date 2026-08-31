// Скилл оркестрации едет внутри пакета: файл на месте, frontmatter корректен,
// каталог skills/ входит в files-whitelist (иначе npm его не упакует).

import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const SKILL = join(ROOT, "skills", "workhorse-ai", "SKILL.md");

test("скилл workhorse-ai лежит в пакете и попадает в npm-пакет", () => {
	assert.equal(existsSync(SKILL), true, "skills/workhorse-ai/SKILL.md на месте");

	const text = readFileSync(SKILL, "utf8");
	const frontmatter = text.match(/^---\n([\s\S]*?)\n---\n/);
	assert.ok(frontmatter, "есть frontmatter");
	assert.match(frontmatter[1], /^name: workhorse-ai$/m);
	assert.match(frontmatter[1], /^description: .+/m);
	assert.ok(text.includes("REPORTED"), "инвариант REPORTED != ACCEPTED описан");

	const pkg = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8"));
	assert.ok(
		pkg.files.some((entry) => entry.replace(/\/$/, "") === "skills"),
		"skills/ есть в files-whitelist",
	);
});
