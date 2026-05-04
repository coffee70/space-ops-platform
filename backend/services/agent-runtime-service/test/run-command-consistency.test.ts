import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const serviceRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const EXPECTED_START = "node dist/server.js";
const EXPECTED_CMD = ["node", "dist/server.js"];

test("package.json start and Dockerfile CMD match node dist/server.js", () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(serviceRoot, "package.json"), "utf8")) as {
    scripts?: { start?: string };
  };
  assert.equal(pkg.scripts?.start, EXPECTED_START);

  const dockerfile = fs.readFileSync(path.join(serviceRoot, "Dockerfile"), "utf8");
  const lines = dockerfile.split(/\r?\n/).map((l) => l.trim());
  const cmdLines = lines.filter((l) => l.startsWith("CMD "));
  assert.ok(cmdLines.length >= 1, "Dockerfile must contain CMD");
  const lastCmd = cmdLines[cmdLines.length - 1];
  const jsonPart = lastCmd.slice(3).trim();
  const parsed = JSON.parse(jsonPart) as string[];
  assert.deepStrictEqual(parsed, EXPECTED_CMD);
});
