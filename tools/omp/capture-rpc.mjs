#!/usr/bin/env node
// Captures the omp CLI --mode rpc JSONL contract to
// tools/omp/rpc-capture-reference.jsonl. Fixture is the authority on frame
// shapes; rerun after any omp update (exit 0 + a prompt_result frame).
// Usage: node tools/omp/capture-rpc.mjs
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const outPath = join(here, 'rpc-capture-reference.jsonl');
const binary = process.env.CINDY_OMP_BINARY || 'C:\\Users\\olvchk\\AppData\\Local\\omp\\omp.exe';

const PROMPTS = [
  'Reply with the single word ok',
  'List the files in the current directory using your file tools, do not modify anything',
];

// --approval-mode yolo: headless capture must not wait on approval frames.
const child = spawn(binary, ['--mode', 'rpc', '--no-session', '--approval-mode', 'yolo'], {
  stdio: ['pipe', 'pipe', 'pipe'],
});
const frames = [];
const pending = new Map(); // id -> resolve
let nextId = 1;
let stderrTail = '';

const rl = createInterface({ input: child.stdout });
rl.on('line', (line) => {
  if (!line.trim()) return;
  frames.push(line);
  try {
    const f = JSON.parse(line);
    if (f.id && pending.has(f.id)) {
      pending.get(f.id)(f);
      pending.delete(f.id);
    }
  } catch { /* non-JSON line: keep raw */ }
});
child.stderr.on('data', (d) => { stderrTail = (stderrTail + d).slice(-2000); });
child.on('exit', (code) => {
  for (const resolve of pending.values()) resolve(null);
  pending.clear();
  child.exitCode = code;
});

function request(command, params = {}) {
  const id = String(nextId++);
  const frame = { id, type: command, ...params };
  return new Promise((resolve) => {
    pending.set(id, resolve);
    child.stdin.write(JSON.stringify(frame) + '\n');
    // Hard cap so a stalled binary cannot hang the capture.
    setTimeout(() => {
      if (pending.has(id)) { pending.delete(id); resolve(null); }
    }, 120_000);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function run() {
  // Wait for ready announcing protocolVersion.
  let ready = null;
  for (let i = 0; i < 300; i++) {
    await sleep(100);
    ready = frames.map((l) => { try { return JSON.parse(l); } catch { return null; } })
      .find((f) => f && f.type === 'ready');
    if (ready) break;
  }
  if (!ready) {
    console.error('no ready frame; stderr tail:\n' + stderrTail);
    process.exit(2);
  }
  console.error('ready: protocolVersion=' + ready.protocolVersion);

  for (const text of PROMPTS) {
    const id = String(nextId++);
    const marker = frames.length; // events belonging to this prompt start here
    const respPromise = new Promise((resolve) => {
      pending.set(id, resolve);
      setTimeout(() => {
        if (pending.has(id)) { pending.delete(id); resolve(null); }
      }, 120_000);
    });
    child.stdin.write(JSON.stringify({ id, type: 'prompt', message: text }) + '\n');
    const resp = await respPromise;
    if (!resp) {
      console.error('no response frame for prompt ' + id + '; stderr tail:\n' + stderrTail);
      process.exit(3);
    }
    console.error('response[' + id + ']: success=' + resp.success);
    // A second prompt sent before the first turn settles is rejected with
    // "Agent is already processing"; gate the next send on turn_end +
    // prompt_result + agent_end, not on the response frame.
    const from = marker;
    const done = () =>
      frames.slice(from).some((l) => l.includes('"type":"prompt_result"')) &&
      frames.slice(from).some((l) => l.includes('"type":"agent_end"'));
    const deadline = Date.now() + 300_000;
    while (Date.now() < deadline && !done()) await sleep(200);
    if (!done()) {
      console.error('prompt ' + id + ' never settled (no agent_end); stderr tail:\n' + stderrTail);
      process.exit(5);
    }
    frames.push(`__capture__ prompt=${id} marker=${marker}`);
  }

  child.stdin.end();
  await sleep(500);
  child.kill();
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, frames.join('\n') + '\n');
  const types = frames
    .filter((l) => !l.startsWith('__capture__'))
    .map((l) => { try { return JSON.parse(l).type; } catch { return '(non-json)'; } });
  const counts = {};
  for (const t of types) counts[t] = (counts[t] || 0) + 1;
  console.error('frame types: ' + JSON.stringify(counts));
  if (!counts.prompt_result) {
    console.error('MISSING prompt_result frame — capture does not match expected contract');
    process.exit(4);
  }
  console.error('captured ' + frames.length + ' lines -> ' + outPath);
}

run().catch((e) => { console.error(e); child.kill(); process.exit(1); });
