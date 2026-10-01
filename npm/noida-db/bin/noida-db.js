#!/usr/bin/env node
const { spawnSync } = require("node:child_process");
const path = require("node:path");

const platformPkg = {
  "linux-x64": "@noida-db/linux-x64",
  "linux-arm64": "@noida-db/linux-arm64",
  "darwin-x64": "@noida-db/darwin-x64",
  "darwin-arm64": "@noida-db/darwin-arm64",
  "win32-x64": "@noida-db/win32-x64",
}[`${process.platform}-${process.arch}`];

if (!platformPkg) {
  console.error(`noida-db: unsupported platform ${process.platform}-${process.arch}`);
  process.exit(1);
}

let binPath;
try {
  binPath = require.resolve(`${platformPkg}/noida-db${process.platform === "win32" ? ".exe" : ""}`);
} catch {
  console.error(`noida-db: optional dependency ${platformPkg} failed to install`);
  process.exit(1);
}

const result = spawnSync(binPath, process.argv.slice(2), { stdio: "inherit" });
process.exit(result.status ?? 1);
