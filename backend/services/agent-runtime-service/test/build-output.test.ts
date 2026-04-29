import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const serviceRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

function walkFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...walkFiles(full));
    } else {
      out.push(full);
    }
  }
  return out;
}

test("production build emits dist/server.js only, no tests or dist/src", () => {
  const distDir = path.join(serviceRoot, "dist");
  fs.rmSync(distDir, { recursive: true, force: true });

  const result = spawnSync("npx", ["tsc", "-p", "tsconfig.json"], {
    cwd: serviceRoot,
    stdio: "inherit",
    shell: false,
  });
  assert.equal(result.status, 0, "tsc must exit 0");

  assert.ok(fs.existsSync(path.join(distDir, "server.js")), "dist/server.js must exist");
  assert.equal(fs.existsSync(path.join(distDir, "test")), false, "dist/test must not exist");
  assert.equal(fs.existsSync(path.join(distDir, "src")), false, "dist/src must not exist");

  const allFiles = walkFiles(distDir);
  const testJs = allFiles.filter((f) => f.endsWith(".test.js"));
  assert.equal(testJs.length, 0, `no .test.js under dist; found: ${testJs.join(", ")}`);
});
