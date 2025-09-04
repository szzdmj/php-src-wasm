// V66k + KV(目录前缀自适配) + 运行时内存自救：
// - 保留原有 KV 读取与多前缀/多文件名回退、/__kv/__/ls/__/put 诊断与上传。
// - 工厂初始化更稳，去掉误导性的 "WORKER" 回退形态。
// - 新增运行时内存调节：?mem=256(MB) 或 ?memBytes=268435456，?allowmg=1。
//   遇到 “memory access out of bounds/oom/Cannot enlarge memory arrays” 自动放大 INITIAL_MEMORY 再试。
// - 新增（本次合并）：/seed/clean 与 /seed/unpack，清理非 tar 键并把 tar(.tar/.tar.gz) 正确解包到 public/。

import wasmAsset from "../scripts/php_8_4.wasm";

// Minimal globals for Emscripten glue in Workers
(function ensureGlobals() {
  const g = globalThis as any;
  if (typeof g.location === "undefined" || typeof g.location?.href === "undefined") {
    try { Object.defineProperty(g, "location", { value: { href: "file:///" }, configurable: true }); } catch {}
  }
  if (typeof g.self === "undefined") {
    try { Object.defineProperty(g, "self", { value: g, configurable: true }); } catch {}
  } else if (typeof g.self.location === "undefined" && typeof g.location !== "undefined") {
    try { g.self.location = g.location; } catch {}
  }
})();

const DEFAULT_INIS = [
  "pcre.jit=0",
  "opcache.enable=0",
  "opcache.enable_cli=0",
  "opcache.jit=0",
  "opcache.jit_buffer_size=0",
  "opcache.file_cache=",
  "output_buffering=0",
  "implicit_flush=1",
  "zlib.output_compression=0",
  "display_errors=1",
  "display_startup_errors=1",
];

type RunResult = {
  ok: boolean;
  stdout: string[];
  stderr: string[];
  debug: string[];
  exitStatus?: number;
  error?: string;
};

function isWasmModule(x: any): x is WebAssembly.Module {
  return Object.prototype.toString.call(x) === "[object WebAssembly.Module]";
}
function makeInstantiateWithModule(wasmModule: WebAssembly.Module) {
  return (imports: WebAssembly.Imports, successCallback: (i: WebAssembly.Instance) => void) => {
    const instance = new WebAssembly.Instance(wasmModule, imports);
    try { successCallback(instance); } catch {}
    return instance.exports as any;
  };
}

function textResponse(body: string, status = 200, headers: Record<string, string> = {}) {
  return new Response(body, { status, headers: { "content-type": "text/plain; charset=utf-8", ...headers } });
}

function parseIniParams(url: URL): string[] {
  const out: string[] = [];
  const ds = url.searchParams.getAll("d").concat(url.searchParams.getAll("ini"));
  for (const s of ds) {
    const t = s.trim();
    if (t && t.includes("=")) out.push(t);
  }
  return out;
}

function buildArgvForCode(code: string, iniList: string[] = []): string[] {
  const argv: string[] = [];
  for (const d of DEFAULT_INIS) argv.push("-d", d);
  for (const d of iniList) if (d) argv.push("-d", d);
  argv.push("-r", code);
  return argv;
}

// 从 URL/ENV 读取运行时内存参数
function readRuntimeMemoryToggles(url?: URL, env?: any): { memBytes?: number; allowMG?: boolean } {
  let memBytes: number | undefined;
  let allowMG = false;

  if (url) {
    const memMB = Number(url.searchParams.get("mem") || "0");
    const memB = Number(url.searchParams.get("memBytes") || "0");
    if (Number.isFinite(memMB) && memMB > 0) memBytes = Math.floor(memMB * 1024 * 1024);
    if (!memBytes && Number.isFinite(memB) && memB > 0) memBytes = Math.floor(memB);
    const amg = url.searchParams.get("allowmg");
    if (amg === "1" || amg === "true") allowMG = true;
  }
  if (!memBytes && env?.INITIAL_MEMORY_BYTES && Number(env.INITIAL_MEMORY_BYTES) > 0) {
    memBytes = Number(env.INITIAL_MEMORY_BYTES);
  }
  if (!allowMG && (env?.ALLOW_MEMORY_GROWTH === "1" || env?.ALLOW_MEMORY_GROWTH === "true")) {
    allowMG = true;
  }
  return { memBytes, allowMG };
}

async function buildInitOptions(base: Partial<any>) {
  const opts: any = {
    noInitialRun: false, // auto-run
    print: () => {},
    printErr: () => {},
    onRuntimeInitialized: () => {},
    quit: (status: number, toThrow?: any) => {
      (opts as any).__exitStatus = status;
      (opts as any).__exitThrown = toThrow ? String(toThrow) : "";
    },
    ...base,
  };

  // 强制提供 wasmBinary，避免 locateFile/URL 环节的差异
  if (isWasmModule(wasmAsset)) {
    opts.instantiateWasm = makeInstantiateWithModule(wasmAsset as WebAssembly.Module);
    return opts;
  }
  if (typeof wasmAsset === "string") {
    const res = await fetch(wasmAsset);
    if (!res.ok) throw new Error(`Failed to fetch wasm from URL: ${res.status}`);
    const buf = await res.arrayBuffer();
    opts.wasmBinary = new Uint8Array(buf);
    return opts;
  }
  if (wasmAsset instanceof ArrayBuffer) {
    opts.wasmBinary = new Uint8Array(wasmAsset);
    return opts;
  }
  if (ArrayBuffer.isView(wasmAsset)) {
    const view = wasmAsset as ArrayBufferView;
    opts.wasmBinary = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    return opts;
  }
  throw new Error("Unsupported wasm asset type at runtime");
}

// 判断是否为“内存相关”错误
function isMemoryError(e: any): boolean {
  const m = String(e?.message || e || "").toLowerCase();
  return m.includes("out of memory")
      || m.includes("memory access out of bounds")
      || m.includes("cannot enlarge memory arrays")
      || m.includes("abort(\"oom") || m.includes("abort('oom")
      || m.includes("oom");
}

// 单次初始化尝试
async function initOnce(initFactory: Function, moduleOptions: any, debug: string[]): Promise<void> {
  debug.push("auto:init-factory");
  await initFactory(moduleOptions);
  // Emscripten 会在 runtime ready 时调用 onRuntimeInitialized
}

// Auto-run with retries on memory error
async function runAuto(argv: string[], waitMs = 8000, toggles?: { memBytes?: number; allowMG?: boolean; retryBoost?: number[] }): Promise<RunResult> {
  const debug: string[] = [];
  const stdout: string[] = [];
  const stderr: string[] = [];
  let exitStatus: number | undefined;

  const retryBoost = toggles?.retryBoost && toggles.retryBoost.length ? toggles.retryBoost : [256, 512]; // MB

  try {
    debug.push("auto:import-glue");
    const phpModule = await import("../scripts/php_8_4.js");
    const initCandidate = (phpModule as any).init || (phpModule as any).default || (phpModule as any);
    if (typeof initCandidate !== "function") {
      return { ok: false, stdout, stderr, debug, error: "PHP init factory not found" };
    }

    // 构造基础 options
    debug.push("auto:build-options");
    const baseOptions: any = await buildInitOptions({ arguments: argv.slice() });
    baseOptions.print = (txt: string) => { try { stdout.push(String(txt)); } catch {} };
    baseOptions.printErr = (txt: string) => { try { stderr.push(String(txt)); } catch {} };
    baseOptions.onRuntimeInitialized = () => { debug.push("auto:event:onRuntimeInitialized"); };

    // 应用第一次内存配置
    if (toggles?.memBytes && Number.isFinite(toggles.memBytes)) {
      baseOptions.INITIAL_MEMORY = Math.floor(toggles.memBytes);
      baseOptions.initialMemory = Math.floor(toggles.memBytes); // 兼容字段
    }
    if (toggles?.allowMG) {
      baseOptions.ALLOW_MEMORY_GROWTH = 1;
    }

    // 第一次尝试
    try {
      await initOnce(initCandidate, baseOptions, debug);
    } catch (e1: any) {
      if (!isMemoryError(e1)) {
        return { ok: false, stdout, stderr, debug, error: String(e1?.message || e1) };
      }
      // 内存相关，按更大的 INITIAL_MEMORY 逐级重试
      for (const mb of retryBoost) {
        try {
          const opt2: any = await buildInitOptions({ arguments: argv.slice() });
          opt2.print = baseOptions.print;
          opt2.printErr = baseOptions.printErr;
          opt2.onRuntimeInitialized = baseOptions.onRuntimeInitialized;
          opt2.wasmBinary = baseOptions.wasmBinary; // 复用二进制
          opt2.INITIAL_MEMORY = mb * 1024 * 1024;
          opt2.initialMemory = opt2.INITIAL_MEMORY;
          if (toggles?.allowMG) opt2.ALLOW_MEMORY_GROWTH = 1;
          debug.push(`auto:retry:mem=${mb}MB`);
          await initOnce(initCandidate, opt2, debug);
          // 成功则跳出重试
          break;
        } catch (e2: any) {
          if (isMemoryError(e2)) {
            // 继续下一档
            if (mb === retryBoost[retryBoost.length - 1]) {
              // 最后一档仍失败
              return { ok: false, stdout, stderr, debug, error: `memory error after retries: ${e2?.message || e2}` };
            }
          } else {
            return { ok: false, stdout, stderr, debug, error: e2?.message || String(e2) };
          }
        }
      }
    }

    // 等待输出或退出
    const start = Date.now();
    while (Date.now() - start < waitMs) {
      if (typeof (baseOptions as any).__exitStatus === "number") { exitStatus = (baseOptions as any).__exitStatus; debug.push("auto:exit:" + String(exitStatus)); break; }
      if (stdout.length > 0) break;
      await new Promise((r) => setTimeout(r, 20));
    }
    return { ok: true, stdout, stderr, debug, exitStatus };
  } catch (e: any) {
    return { ok: false, stdout, stderr, debug: ["auto:catch-top", e?.message || String(e)], error: e?.stack || String(e) };
  }
}

// stderr noise filtering
function filterKnownNoise(lines: string[], keepAll: boolean) {
  if (keepAll) return { kept: lines.slice(), ignored: 0 };
  const patterns: RegExp[] = [/^munmap\(\)\s+failed:\s+\[\d+\]\s+Invalid argument/i];
  let ignored = 0;
  const kept = lines.filter((l) => {
    const t = String(l || "").trim();
    if (!t) return false;
    for (const re of patterns) {
      if (re.test(t)) { ignored++; return false; }
    }
    return true;
  });
  return { kept, ignored };
}
function inferContentTypeFromOutput(stdout: string, fallback = "text/plain; charset=utf-8") {
  if (stdout.includes("<html") || stdout.includes("<!DOCTYPE html")) return "text/html; charset=utf-8";
  return fallback;
}
function finalizeOk(url: URL, res: RunResult, defaultCT: string) {
  const debugMode = url.searchParams.get("debug") === "1" || url.searchParams.get("showstderr") === "1";
  const stdout = res.stdout.join("\n");
  const filtered = filterKnownNoise(res.stderr, debugMode);
  const err = filtered.kept.join("\n");
  const ct = inferContentTypeFromOutput(stdout, defaultCT);
  const trace = res.debug.length ? `\n<!-- trace:${res.debug.join("->")} -->` : "";

  let body = "";
  let status = 200;

  if (stdout) {
    body = stdout + trace;
  } else if (err) {
    body = err + trace;
    status = 500;
  } else {
    const hint = [
      "[wasmphp] No output from PHP script.",
      `exitStatus=${typeof res.exitStatus === "number" ? res.exitStatus : "unknown"}`,
      `stderr_filtered_lines=${filtered.ignored}`,
      res.debug.length ? `trace=${res.debug.join("->")}` : "",
      "Tip: append ?debug=1 to see unfiltered stderr.",
    ].filter(Boolean).join("\n");
    body = hint;
    status = 200;
  }
  return new Response(body, { status, headers: { "content-type": ct } });
}

// —— Sources: KV first (with prefixes), then GitHub fallback ——
function normalizePhpCodeForEval(src: string): string {
  let s = src.trimStart();
  if (s.charCodeAt(0) === 0xfeff) s = s.slice(1);
  if (s.startsWith("<?php")) s = s.slice(5);
  else if (s.startsWith("<?")) s = s.slice(2);
  s = s.replace(/\?>\s*$/s, "");
  return s;
}

async function fetchKVPhpSource(env: any, key: string): Promise<{ ok: boolean; code?: string; err?: string }> {
  try {
    if (!env || !env.SRC || typeof env.SRC.get !== "function") {
      return { ok: false, err: "KV binding SRC is not configured" };
    }
    const txt = await env.SRC.get(key, "text");
    if (txt == null) return { ok: false, err: `KV object not found: ${key}` };
    return { ok: true, code: normalizePhpCodeForEval(txt) };
  } catch (e: any) {
    return { ok: false, err: "KV get failed: " + (e?.message || String(e)) };
  }
}

async function fetchKVPhpSourceWithPrefixes(env: any, url: URL, baseKey = "public/index.php"): Promise<{ ok: boolean; key?: string; code?: string; tried: string[]; err?: string }> {
  const tried: string[] = [];
  if (!env || !env.SRC || typeof env.SRC.get !== "function") {
    return { ok: false, tried, err: "KV binding SRC is not configured" };
  }

  const qpPrefix = (url.searchParams.get("kp") || url.searchParams.get("prefix") || "").trim();
  const envPrefix = (env.KV_PREFIX || "").trim();

  function normPrefix(p: string): string {
    if (!p) return "";
    return p.endsWith("/") ? p : p + "/";
  }

  const candidatesPrefixes = Array.from(new Set([
    normPrefix(qpPrefix),
    normPrefix(envPrefix),
    "", "public/", "site/", "dist/", "app/", "www/", "out/", "build/",
  ].filter((s) => typeof s === "string")));

  const fileNames = Array.from(new Set([
    baseKey,
    "public/index.php",
    "index.php",
  ]));

  const keyOverride = (url.searchParams.get("key") || "").trim();
  const finalNames = keyOverride ? [keyOverride] : fileNames;

  const keysToTry: string[] = [];
  for (const pf of candidatesPrefixes) {
    for (const fn of finalNames) {
      const cleanFn = fn.replace(/^\/+/, "");
      const candidate = pf ? (pf + cleanFn) : cleanFn;
      if (!keysToTry.includes(candidate)) keysToTry.push(candidate);
    }
  }

  for (const key of keysToTry) {
    tried.push(key);
    try {
      const txt = await env.SRC.get(key, "text");
      if (txt != null) {
        return { ok: true, key, code: normalizePhpCodeForEval(txt), tried };
      }
    } catch {}
  }

  return { ok: false, tried, err: "KV object not found for any candidate keys" };
}

async function fetchGithubPhpSource(owner: string, repo: string, path: string, ref?: string): Promise<{ ok: boolean; code?: string; err?: string; status?: number }> {
  try {
    const branch = (ref && ref.trim()) || "main";
    const u = `https://raw.githubusercontent.com/${owner}/${repo}/${encodeURIComponent(branch)}/${path.replace(/^\/+/, "")}`;
    const headers: Record<string, string> = {
      "cache-control": "no-cache",
      "user-agent": "wasmphp-worker",
      "accept": "text/plain, */*",
    };
    let res = await fetch(u, { method: "GET", headers });
    if (res.ok) {
      const txt = await res.text();
      return { ok: true, code: normalizePhpCodeForEval(txt), status: res.status };
    }
    if (branch !== "master") {
      const u2 = `https://raw.githubusercontent.com/${owner}/${repo}/master/${path.replace(/^\/+/, "")}`;
      res = await fetch(u2, { method: "GET", headers });
      if (res.ok) {
        const txt2 = await res.text();
        return { ok: true, code: normalizePhpCodeForEval(txt2), status: res.status };
      }
    }
    return { ok: false, err: `Raw fetch failed: ${owner}/${repo}:${branch}/${path} (${res.status})`, status: res.status };
  } catch (e: any) {
    return { ok: false, err: "Raw fetch threw: " + (e?.message || String(e)) };
  }
}

// Base64 (UTF-8) helper safe for large inputs
function toBase64Utf8(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

// ===== 追加：KV 清理 + tar 解包到 public/ =====

type AnyKV = {
  get: (key: string, type?: "text" | "arrayBuffer" | "json" | "stream") => Promise<any>;
  put: (key: string, value: any, opts?: any) => Promise<void>;
  delete: (key: string) => Promise<void>;
  list: (opts?: { prefix?: string; cursor?: string; limit?: number }) => Promise<{ keys: Array<{ name: string; metadata?: any }>; list_complete: boolean; cursor?: string }>;
};

function getKV(env: any): AnyKV {
  const kv = (env as any).SRC || (env as any).FILES || (env as any).SRC_KV || (env as any).KV;
  if (!kv || typeof kv.get !== "function") throw new Error("KV not configured (expected one of: SRC, FILES, SRC_KV, KV)");
  return kv;
}

function requireAdmin(req: Request, env: any): Response | null {
  const token = req.headers.get("x-admin-token") || req.headers.get("authorization")?.replace(/^Bearer\s+/i, "");
  if (!env?.ADMIN_TOKEN) return textResponse("ADMIN_TOKEN not set", 401);
  if (token !== env.ADMIN_TOKEN) return textResponse("Unauthorized", 401);
  return null;
}

function collapseSlashes(s: string) { return s.replace(/\/{2,}/g, "/"); }
function normalizePosixPath(p: string): string | null {
  let path = String(p || "").trim().replace(/^(\.\/)+/, "").replace(/^\/+/, "");
  path = collapseSlashes(path);
  const parts = path.split("/");
  const out: string[] = [];
  for (const seg of parts) {
    if (!seg || seg === ".") continue;
    if (seg === "..") { if (out.length === 0) return null; out.pop(); continue; }
    out.push(seg);
  }
  return out.join("/");
}
// 归档 entry => 最终 KV 键：强制 targetPrefix（默认 public/），折叠 public/public/
function normalizeTarEntryKey(path: string, targetPrefix = "public/"): string | null {
  const norm = normalizePosixPath(path);
  if (!norm) return null;
  let key = norm;
  const pref = targetPrefix.endsWith("/") ? targetPrefix : targetPrefix + "/";
  if (!key.startsWith(pref)) key = pref + key;
  key = key.replace(/^public\/public\//, "public/");
  if (!key || key.endsWith("/") || key.includes("..")) return null;
  return key;
}

function isGzip(u8: Uint8Array) { return u8.length >= 2 && u8[0] === 0x1f && u8[1] === 0x8b; }
async function gunzipIfNeeded(ab: ArrayBuffer): Promise<Uint8Array> {
  const u8 = new Uint8Array(ab);
  if (!isGzip(u8)) return u8;
  const ds = new DecompressionStream("gzip");
  const decompressed = new Response(new Response(ab).body!.pipeThrough(ds));
  const buf = await decompressed.arrayBuffer();
  return new Uint8Array(buf);
}

function readCString(bytes: Uint8Array): string {
  let end = bytes.length;
  for (let i = 0; i < bytes.length; i++) { if (bytes[i] === 0) { end = i; break; } }
  return new TextDecoder().decode(bytes.subarray(0, end));
}
function parseOctal(bytes: Uint8Array): number {
  const str = new TextDecoder().decode(bytes).replace(/\0.*$/, "").trim();
  if (!str) return 0;
  const s = str.replace(/[^0-7]/g, "");
  return s ? parseInt(s, 8) : 0;
}
function hexOf(buf: ArrayBuffer | Uint8Array): string {
  const u = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  let s = ""; for (let i = 0; i < u.length; i++) s += u[i].toString(16).padStart(2, "0"); return s;
}
async function sha1Of(buf: ArrayBuffer | Uint8Array): Promise<string> {
  const ab = buf instanceof Uint8Array ? buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) : buf;
  const h = await crypto.subtle.digest("SHA-1", ab);
  return hexOf(h);
}
function pLimit(concurrency: number) {
  let active = 0;
  const queue: Array<() => void> = [];
  return function <T>(fn: () => Promise<T>): Promise<T> {
    return new Promise((resolve, reject) => {
      const run = () => {
        active++;
        fn().then(resolve).catch(reject).finally(() => {
          active--;
          const next = queue.shift();
          if (next) next();
        });
      };
      if (active < concurrency) run(); else queue.push(run);
    });
  };
}

async function kvListAll(kv: AnyKV, opts: { prefix?: string; keep?: (name: string) => boolean }): Promise<{ total: number; deleted: number; kept: number }> {
  let cursor: string | undefined = undefined;
  let total = 0, deleted = 0, kept = 0;

  do {
    // @ts-ignore list typing
    const page = await kv.list({ cursor, limit: 1000, prefix: opts.prefix });
    cursor = page.list_complete ? undefined : page.cursor;
    for (const k of page.keys as Array<{ name: string }>) {
      total++;
      if (opts.keep && opts.keep(k.name)) { kept++; continue; }
      await kv.delete(k.name);
      deleted++;
    }
  } while (cursor);

  return { total, deleted, kept };
}

async function parseTarEntries(tarBytes: Uint8Array, targetPrefix = "public/"): Promise<Array<{ key: string; data: Uint8Array }>> {
  const out: Array<{ key: string; data: Uint8Array }> = [];
  let off = 0;
  let pendingLongName: string | null = null;

  while (off + 512 <= tarBytes.length) {
    const header = tarBytes.subarray(off, off + 512);
    off += 512;
    const isZero = header.every((b) => b === 0);
    if (isZero) break;

    const name = readCString(header.subarray(0, 100));
    const size = parseOctal(header.subarray(124, 136));
    const typeflag = header[156];

    let filename = pendingLongName || name;
    pendingLongName = null;

    if (typeflag === 76 /* 'L' GNU LongName */) {
      const longBytes = tarBytes.subarray(off, off + Math.ceil(size / 512) * 512).subarray(0, size);
      pendingLongName = new TextDecoder().decode(longBytes).replace(/\0+$/, "");
      off += Math.ceil(size / 512) * 512;
      continue;
    }

    const fileData = tarBytes.subarray(off, off + Math.ceil(size / 512) * 512).subarray(0, size);
    off += Math.ceil(size / 512) * 512;

    // 目录条目跳过
    if (typeflag === 53 /* '5' */) continue;

    const key = normalizeTarEntryKey(filename, targetPrefix);
    if (!key) continue;

    out.push({ key, data: fileData });
  }

  return out;
}

async function unpackTarToKV(kv: AnyKV, tarBuf: ArrayBuffer, opts?: { targetPrefix?: string; concurrency?: number; dry?: boolean }) {
  const u8 = await gunzipIfNeeded(tarBuf); // 支持 .tar 与 .tar.gz
  const entries = await parseTarEntries(u8, opts?.targetPrefix || "public/");
  const limit = pLimit(Math.max(1, Math.min(32, opts?.concurrency || 8)));

  let files = 0, bytes = 0, skipped = 0, errors = 0;
  const tasks = entries.map((e) => limit(async () => {
    try {
      if (opts?.dry) { files++; bytes += e.data.byteLength; return; }
      const etag = await sha1Of(e.data);
      await kv.put(e.key, e.data as any, { metadata: { updatedAt: Date.now(), src: "seed-tar", etag } });
      files++; bytes += e.data.byteLength;
    } catch {
      errors++; skipped++;
    }
  }));

  await Promise.allSettled(tasks);
  return { files, bytes, skipped, errors, totalEntries: entries.length };
}

async function routeSeedClean(request: Request, url: URL, env: any): Promise<Response> {
  const guard = requireAdmin(request, env);
  if (guard) return guard;

  try {
    const kv = getKV(env);
    const tarKey = url.searchParams.get("tarKey") || "public/./public.tar";
    const keepMeta = url.searchParams.get("keepMeta") === "1" || url.searchParams.get("keepMeta") === "true";
    const dry = url.searchParams.get("dry") === "1" || url.searchParams.get("dry") === "true";

    const keepFn = (name: string) => {
      if (name === tarKey) return true;
      if (keepMeta && name.startsWith("meta/")) return true;
      return false;
    };

    if (dry) {
      // 仅统计
      let cursor: string | undefined = undefined;
      let total = 0, toDelete = 0, kept = 0;
      do {
        // @ts-ignore KV.list
        const page = await kv.list({ cursor, limit: 1000 });
        cursor = page.list_complete ? undefined : page.cursor;
        for (const k of page.keys as Array<{ name: string }>) {
          total++;
          if (keepFn(k.name)) kept++; else toDelete++;
        }
      } while (cursor);
      return new Response(JSON.stringify({ ok: true, mode: "dry", total, toDelete, kept, tarKey, keepMeta }, null, 2), {
        status: 200, headers: { "content-type": "application/json; charset=utf-8" },
      });
    }

    const res = await kvListAll(kv, { keep: keepFn });
    return new Response(JSON.stringify({ ok: true, ...res, tarKey, keepMeta }, null, 2), {
      status: 200, headers: { "content-type": "application/json; charset=utf-8" },
    });
  } catch (e: any) {
    return textResponse("clean error: " + (e?.stack || String(e)), 500);
  }
}

async function routeSeedUnpack(request: Request, url: URL, env: any): Promise<Response> {
  const guard = requireAdmin(request, env);
  if (guard) return guard;

  try {
    const kv = getKV(env);
    const tarKey = url.searchParams.get("key") || "public/./public.tar";
    const dry = url.searchParams.get("dry") === "1" || url.searchParams.get("dry") === "true";
    const targetPrefix = url.searchParams.get("prefix") || "public/";
    const concurrency = Number(url.searchParams.get("concurrency") || "8");

    const arr = await kv.get(tarKey, "arrayBuffer");
    if (!arr) return textResponse("tar not found: " + tarKey, 404);

    const result = await unpackTarToKV(kv, arr as ArrayBuffer, { targetPrefix, concurrency, dry });

    // HEAD 请求仅做预览
    if (request.method === "HEAD") {
      return new Response(JSON.stringify({ ok: true, tarKey, dry: true, targetPrefix, concurrency, ...result }, null, 2), {
        status: 200, headers: { "content-type": "application/json; charset=utf-8" },
      });
    }
    return new Response(JSON.stringify({ ok: true, tarKey, dry, targetPrefix, concurrency, ...result }, null, 2), {
      status: 200, headers: { "content-type": "application/json; charset=utf-8" },
    });
  } catch (e: any) {
    return textResponse("unpack error: " + (e?.stack || String(e)), 500);
  }
}

// ——— Routes ———
async function routeInfo(url: URL, env?: any): Promise<Response> {
  try {
    const argv = buildArgvForCode("phpinfo();");
    const toggles = readRuntimeMemoryToggles(url, env);
    const res = await runAuto(argv, 10000, { ...toggles });
    if (!res.ok) {
      const body = (res.stderr.join("\n") ? res.stderr.join("\n") + "\n" : "") + (res.error || "Unknown error") + "\ntrace: " + res.debug.join("->");
      return textResponse(body, 500);
    }
    return finalizeOk(url, res, "text/html; charset=utf-8");
  } catch (e: any) {
    return textResponse("routeInfo error: " + (e?.stack || String(e)), 500);
  }
}

function wrapCodeWithShutdownNewline(code: string): string {
  return `register_shutdown_function(function(){echo "\\n";}); ${code}`;
}

async function routeRunGET(url: URL, env?: any): Promise<Response> {
  try {
    const codeRaw = url.searchParams.get("code") ?? "";
    if (!codeRaw) return textResponse("Bad Request: missing code", 400);
    if (codeRaw.length > 64 * 1024) return textResponse("Payload Too Large", 413);
    const ini = parseIniParams(url);
    const argv = buildArgvForCode(wrapCodeWithShutdownNewline(codeRaw), ini);
    const toggles = readRuntimeMemoryToggles(url, env);
    const res = await runAuto(argv, 10000, { ...toggles });
    if (!res.ok) {
      const body = (res.stderr.join("\n") ? res.stderr.join("\n") + "\n" : "") + (res.error || "Unknown error") + "\ntrace: " + res.debug.join("->");
      return textResponse(body, 500);
    }
    return finalizeOk(url, res, "text/plain; charset=utf-8");
  } catch (e: any) {
    return textResponse("routeRunGET error: " + (e?.stack || String(e)), 500);
  }
}

async function routeRunPOST(request: Request, url: URL, env?: any): Promise<Response> {
  try {
    const code = await request.text();
    if (!code) return textResponse("Bad Request: empty body", 400);
    if (code.length > 256 * 1024) return textResponse("Payload Too Large", 413);
    const ini = parseIniParams(url);
    const argv = buildArgvForCode(wrapCodeWithShutdownNewline(code), ini);
    const toggles = readRuntimeMemoryToggles(url, env);
    const res = await runAuto(argv, 12000, { ...toggles });
    if (!res.ok) {
      const body = (res.stderr.join("\n") ? res.stderr.join("\n") + "\n" : "") + (res.error || "Unknown error") + "\ntrace: " + res.debug.join("->");
      return textResponse(body, 500);
    }
    return finalizeOk(url, res, "text/plain; charset=utf-8");
  } catch (e: any) {
    return textResponse("routeRunPOST error: " + (e?.stack || String(e)), 500);
  }
}

// 列表诊断：列出 KV 键（prefix + limit + cursor）
async function routeKvList(url: URL, env: any): Promise<Response> {
  try {
    if (!env?.SRC || typeof env.SRC.list !== "function") {
      return textResponse("KV not configured", 500);
    }
    const prefix = url.searchParams.get("prefix") || "";
    const limit = Math.max(1, Math.min(1000, Number(url.searchParams.get("limit") || "100")));
    const cursor = url.searchParams.get("cursor") || undefined;
    // @ts-ignore KVNamespace.list
    const r = await env.SRC.list({ prefix, limit, cursor });
    return new Response(JSON.stringify(r, null, 2), { status: 200, headers: { "content-type": "application/json; charset=utf-8" } });
  } catch (e: any) {
    return textResponse("kv list error: " + (e?.stack || String(e)), 500);
  }
}

// 读取诊断：支持 ?key=（单 key）或走“多前缀回退”，返回 tried 列表便于定位
async function routeKvDiag(url: URL, env: any): Promise<Response> {
  try {
    const explicitKey = url.searchParams.get("key");
    if (explicitKey) {
      const r = await fetchKVPhpSource(env, explicitKey);
      const payload = { explicitKey, ...r };
      return new Response(JSON.stringify(payload, null, 2), { status: r.ok ? 200 : 404, headers: { "content-type": "application/json; charset=utf-8" } });
    }
    const r2 = await fetchKVPhpSourceWithPrefixes(env, url, "public/index.php");
    return new Response(JSON.stringify(r2, null, 2), { status: r2.ok ? 200 : 404, headers: { "content-type": "application/json; charset=utf-8" } });
  } catch (e: any) {
    return textResponse("kv diag error: " + (e?.stack || String(e)), 500);
  }
}

function badKey(key: string) {
  return key.length > 512 || key.includes("..") || key.startsWith("/") || key === "";
}
async function routeKvPut(request: Request, url: URL, env: any): Promise<Response> {
  try {
    if (!env?.SRC || typeof env.SRC.put !== "function") return textResponse("KV not configured", 500);
    const admin = (env as any).ADMIN_TOKEN;
    const token = request.headers.get("x-admin-token") || request.headers.get("authorization")?.replace(/^Bearer\s+/i, "");
    if (!admin || token !== admin) return textResponse("Unauthorized", 401);

    const key = url.searchParams.get("key") || "";
    if (badKey(key)) return textResponse("Bad key", 400);

    const body = await request.text();
    if (!body) return textResponse("Empty body", 400);

    await env.SRC.put(key, body, { expirationTtl: 0, metadata: { updatedAt: Date.now() } });
    return textResponse("OK", 200);
  } catch (e: any) {
    return textResponse("kv put error: " + (e?.stack || String(e)), 500);
  }
}

async function routeRepoIndex(url: URL, env: any): Promise<Response> {
  try {
    const ref = url.searchParams.get("ref") || undefined;
    const mode = url.searchParams.get("mode") || "";
    const baseKey = url.searchParams.get("key") || "public/index.php";

    let codeNormalized: string | undefined;
    let hitKey: string | undefined;
    if (env && env.SRC) {
      const kv = await fetchKVPhpSourceWithPrefixes(env, url, baseKey);
      if (kv.ok) {
        codeNormalized = kv.code!;
        hitKey = kv.key!;
      }
    }

    if (!codeNormalized) {
      const pull = await fetchGithubPhpSource("szzdmj", "wasmphp", baseKey, ref);
      if (!pull.ok) return textResponse(pull.err || "Fetch error", 502);
      codeNormalized = pull.code || "";
      if (mode === "raw") {
        return new Response(codeNormalized, { status: 200, headers: { "content-type": "text/plain; charset=utf-8", "x-source": "github" } });
      }
    } else {
      if (mode === "raw") {
        return new Response(codeNormalized, { status: 200, headers: { "content-type": "text/plain; charset=utf-8", "x-source": "kv", ...(hitKey ? { "x-kv-key": hitKey } : {}) } });
      }
    }

    const b64 = toBase64Utf8(codeNormalized || "");
    const oneLiner = wrapCodeWithShutdownNewline(`eval(base64_decode('${b64}'));`);
    const ini = parseIniParams(url);
    const argv = buildArgvForCode(oneLiner, ini);
    const toggles = readRuntimeMemoryToggles(url, env);
    const res = await runAuto(argv, 12000, { ...toggles });
    if (!res.ok) {
      const body = (res.stderr.join("\n") ? res.stderr.join("\n") + "\n" : "") + (res.error || "Unknown error") + "\ntrace: " + res.debug.join("->");
      const extra = hitKey ? { "x-kv-key": hitKey } : {};
      return textResponse(body, 500, extra);
    }
    return finalizeOk(url, res, "text/html; charset=utf-8");
  } catch (e: any) {
    return textResponse("routeRepoIndex error: " + (e?.stack || String(e)), 500);
  }
}

async function routeVersion(url?: URL, env?: any): Promise<Response> {
  try {
    const argv: string[] = [];
    for (const d of DEFAULT_INIS) { argv.push("-d", d); }
    argv.push("-v");
    const toggles = readRuntimeMemoryToggles(url, env);
    const res = await runAuto(argv, 8000, { ...toggles });
    const body = (res.stdout.join("\n") || res.stderr.join("\n") || "") + (res.debug.length ? `\ntrace:${res.debug.join("->")}` : "");
    return textResponse(body || "", res.ok ? 200 : 500);
  } catch (e: any) {
    return textResponse("routeVersion error: " + (e?.stack || String(e)), 500);
  }
}

async function routeHelp(url?: URL, env?: any): Promise<Response> {
  try {
    const argv: string[] = [];
    for (const d of DEFAULT_INIS) { argv.push("-d", d); }
    argv.push("-h");
    const toggles = url ? readRuntimeMemoryToggles(url, env) : {};
    const res = await runAuto(argv, 8000, { ...(toggles as any) });
    const body = (res.stdout.join("\n") || res.stderr.join("\n") || "") + (res.debug.length ? `\ntrace:${res.debug.join("->")}` : "");
    return textResponse(body || "", res.ok ? 200 : 500);
  } catch (e: any) {
    return textResponse("routeHelp error: " + (e?.stack || String(e)), 500);
  }
}

export default {
  async fetch(request: Request, env: any): Promise<Response> {
    try {
      const url = new URL(request.url);
      const { pathname } = url;

      if (pathname === "/health") return textResponse("ok");

      if (pathname === "/__probe") {
        try {
          let importMetaUrl: string | null = null;
          try { importMetaUrl = (import.meta as any)?.url ?? null; } catch { importMetaUrl = null; }
          const body = {
            env: {
              esm: true,
              hasWorkerCtor: typeof (globalThis as any).Worker !== "undefined",
              hasSharedArrayBuffer: typeof (globalThis as any).SharedArrayBuffer !== "undefined",
              userAgent: (globalThis as any).navigator?.userAgent ?? null,
              hasKV: !!env?.SRC,
            },
            importMeta: { available: typeof import.meta !== "undefined", url: importMetaUrl },
          };
          return new Response(JSON.stringify(body, null, 2), { status: 200, headers: { "content-type": "application/json; charset=utf-8" } });
        } catch (e: any) {
          return textResponse("probe error: " + (e?.stack || String(e)), 500);
        }
      }

      if (pathname === "/__jsplus") {
        try {
          const mod = await import("../scripts/php_8_4.js");
          const def = (mod as any)?.default ?? null;
          const hasFactory = typeof def === "function";
          const payload = { imported: true, hasDefaultFactory: hasFactory, exportKeys: Object.keys(mod || {}), note: "import-only, no init" };
          return new Response(JSON.stringify(payload, null, 2), { status: 200, headers: { "content-type": "application/json; charset=utf-8" } });
        } catch (e: any) {
          return textResponse("Import glue failed:\n" + (e?.stack || String(e)), 500);
        }
      }

      if (pathname === "/__kv") {
        return routeKvDiag(url, env);
      }
      if (pathname === "/__ls") {
        return routeKvList(url, env);
      }
      if (pathname === "/__put" && (request.method === "POST" || request.method === "PUT")) {
        return routeKvPut(request, url, env);
      }

      // 新增：KV 清理与解包
      if (pathname === "/seed/clean" && request.method === "POST") {
        return routeSeedClean(request, url, env);
      }
      if (pathname === "/seed/unpack" && (request.method === "POST" || request.method === "HEAD")) {
        return routeSeedUnpack(request, url, env);
      }

      if (pathname === "/" || pathname === "/index.php" || pathname === "/public/index.php") {
        return routeRepoIndex(url, env);
      }

      if (pathname === "/info") return routeInfo(url, env);
      if (pathname === "/run" && request.method === "GET") return routeRunGET(url, env);
      if (pathname === "/run" && request.method === "POST") return routeRunPOST(request, url, env);
      if (pathname === "/version") return routeVersion(url, env);
      if (pathname === "/help") return routeHelp(url, env);

      return textResponse("Not Found", 404);
    } catch (e: any) {
      return textResponse("Top-level handler error: " + (e?.stack || String(e)), 500);
    }
  },
};
