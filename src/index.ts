// Files Worker: KV 清理 + tar(.tar/.tar.gz) 解包到 public/ + 静态文件读取
// 提供：
// - GET  /__ls?prefix=&limit=&cursor=         列出 KV 键
// - POST/PUT /__put?key=...                   管理上传（支持二进制/文本），需 ADMIN_TOKEN
// - POST /seed/clean?tarKey=...&keepMeta=0    删除除 tarKey 以外的键；keepMeta=1 可保留 meta/；dry=1 仅统计
// - POST/HEAD /seed/unpack?key=...            从 KV 读取 tar 或 tar.gz，解包到 public/；dry=1 或 HEAD 仅预览
// - GET/HEAD /public/...                      从 KV 读取并返回静态资源（自动 content-type）
// 说明：所有写操作需携带 x-admin-token 或 Authorization: Bearer 与 ADMIN_TOKEN 匹配

type AnyKV = {
  get: (key: string, type?: "text" | "arrayBuffer" | "json" | "stream") => Promise<any>;
  put: (key: string, value: any, opts?: any) => Promise<void>;
  delete: (key: string) => Promise<void>;
  list: (opts?: { prefix?: string; cursor?: string; limit?: number }) => Promise<{ keys: Array<{ name: string; metadata?: any }>; list_complete: boolean; cursor?: string }>;
};

type Env = {
  ADMIN_TOKEN?: string;
  // 任意一个绑定存在即可
  SRC?: AnyKV;
  FILES?: AnyKV;
  SRC_KV?: AnyKV;
  KV?: AnyKV;
};

function getKV(env: Env): AnyKV {
  const kv = (env as any).SRC || (env as any).FILES || (env as any).SRC_KV || (env as any).KV;
  if (!kv || typeof kv.get !== "function") throw new Error("KV binding not configured (expected one of: SRC, FILES, SRC_KV, KV)");
  return kv;
}

function text(body: string, status = 200, hdr: Record<string, string> = {}) {
  return new Response(body, { status, headers: { "content-type": "text/plain; charset=utf-8", ...hdr } });
}
function json(obj: any, status = 200, hdr: Record<string, string> = {}) {
  return new Response(JSON.stringify(obj, null, 2), { status, headers: { "content-type": "application/json; charset=utf-8", ...hdr } });
}
function requireAdmin(req: Request, env: Env): Response | null {
  const token = req.headers.get("x-admin-token") || req.headers.get("authorization")?.replace(/^Bearer\s+/i, "");
  if (!env.ADMIN_TOKEN) return text("ADMIN_TOKEN not set", 401);
  if (token !== env.ADMIN_TOKEN) return text("Unauthorized", 401);
  return null;
}

// ---------- MIME ----------
function guessContentType(pathname: string): string {
  const p = pathname.toLowerCase();
  if (p.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (p.endsWith(".css")) return "text/css; charset=utf-8";
  if (p.endsWith(".m3u8")) return "application/vnd.apple.mpegurl";
  if (p.endsWith(".m4s")) return "video/iso.segment";
  if (p.endsWith(".ts")) return "video/mp2t";
  if (p.endsWith(".json")) return "application/json; charset=utf-8";
  if (p.endsWith(".svg")) return "image/svg+xml";
  if (p.endsWith(".png")) return "image/png";
  if (p.endsWith(".jpg") || p.endsWith(".jpeg")) return "image/jpeg";
  if (p.endsWith(".gif")) return "image/gif";
  if (p.endsWith(".webp")) return "image/webp";
  if (p.endsWith(".ico")) return "image/x-icon";
  if (p.endsWith(".woff2")) return "font/woff2";
  if (p.endsWith(".woff")) return "font/woff";
  return "application/octet-stream";
}

// ---------- 路径规范/并发/散列 ----------
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
// 解包时：归档 entry => KV 键：强制 public/ 前缀，折叠 public/public/
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
// 读取静态时：URL path => KV 键：强制 public/ 前缀，折叠 public/public/
function normalizeServeKey(pathname: string): string | null {
  const norm = normalizePosixPath(pathname);
  if (!norm) return null;
  let key = norm;
  if (!key.startsWith("public/")) key = "public/" + key;
  key = key.replace(/^public\/public\//, "public/");
  if (!key || key.endsWith("/") || key.includes("..")) return null;
  return key;
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

// ---------- 核心：列出/上传/清理/解包 ----------
async function routeList(url: URL, env: Env): Promise<Response> {
  try {
    const kv = getKV(env);
    const prefix = url.searchParams.get("prefix") || "";
    const limit = Math.max(1, Math.min(1000, Number(url.searchParams.get("limit") || "100")));
    const cursor = url.searchParams.get("cursor") || undefined;
    // @ts-ignore KV.list
    const r = await kv.list({ prefix, limit, cursor });
    return json(r, 200);
  } catch (e: any) {
    return text("ls error: " + (e?.stack || String(e)), 500);
  }
}

function badKey(key: string) {
  return key.length > 512 || key.includes("..") || key.startsWith("/") || key === "";
}
async function routePut(request: Request, url: URL, env: Env): Promise<Response> {
  const guard = requireAdmin(request, env);
  if (guard) return guard;
  try {
    const kv = getKV(env);
    const key = url.searchParams.get("key") || "";
    if (badKey(key)) return text("Bad key", 400);

    // 尝试按内容类型读取
    const ctype = request.headers.get("content-type") || "";
    if (ctype.startsWith("text/") || ctype.includes("json") || ctype.includes("xml") || ctype.includes("urlencoded") || ctype.includes("javascript")) {
      const bodyTxt = await request.text();
      if (!bodyTxt) return text("Empty body", 400);
      await kv.put(key, bodyTxt, { metadata: { updatedAt: Date.now() } });
    } else {
      const ab = await request.arrayBuffer();
      if (!ab || ab.byteLength === 0) return text("Empty body", 400);
      await kv.put(key, ab, { metadata: { updatedAt: Date.now() } });
    }
    return text("OK", 200);
  } catch (e: any) {
    return text("put error: " + (e?.stack || String(e)), 500);
  }
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

async function routeSeedClean(request: Request, url: URL, env: Env): Promise<Response> {
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
      return json({ ok: true, mode: "dry", total, toDelete, kept, tarKey, keepMeta }, 200);
    }

    const res = await kvListAll(kv, { keep: keepFn });
    return json({ ok: true, ...res, tarKey, keepMeta }, 200);
  } catch (e: any) {
    return text("clean error: " + (e?.stack || String(e)), 500);
  }
}

async function routeSeedUnpack(request: Request, url: URL, env: Env): Promise<Response> {
  const guard = requireAdmin(request, env);
  if (guard) return guard;

  try {
    const kv = getKV(env);
    const tarKey = url.searchParams.get("key") || "public/./public.tar";
    const dry = url.searchParams.get("dry") === "1" || url.searchParams.get("dry") === "true";
    const targetPrefix = url.searchParams.get("prefix") || "public/";
    const concurrency = Number(url.searchParams.get("concurrency") || "8");

    const arr = await kv.get(tarKey, "arrayBuffer");
    if (!arr) return text("tar not found: " + tarKey, 404);

    const result = await unpackTarToKV(kv, arr as ArrayBuffer, { targetPrefix, concurrency, dry });

    // HEAD 请求仅做预览
    if (request.method === "HEAD") {
      return json({ ok: true, tarKey, dry: true, targetPrefix, concurrency, ...result }, 200);
    }
    return json({ ok: true, tarKey, dry, targetPrefix, concurrency, ...result }, 200);
  } catch (e: any) {
    return text("unpack error: " + (e?.stack || String(e)), 500);
  }
}

// ---------- 静态文件读取 ----------
async function routeStaticGet(request: Request, env: Env): Promise<Response> {
  try {
    const url = new URL(request.url);
    const key = normalizeServeKey(url.pathname);
    if (!key) return text("Bad path", 400);

    const kv = getKV(env);
    const ab = await kv.get(key, "arrayBuffer");
    if (!ab) return text("Not Found", 404, { "cache-control": "no-store" });

    const ct = guessContentType(key);
    const h = new Headers({
      "content-type": ct,
      "cache-control": "public, max-age=86400, stale-while-revalidate=1800",
      "x-kv-key": key,
      "x-source": "kv",
    });
    if (request.method === "HEAD") {
      return new Response(null, { status: 200, headers: h });
    }
    return new Response(ab as ArrayBuffer, { status: 200, headers: h });
  } catch (e: any) {
    return text("static get error: " + (e?.stack || String(e)), 500);
  }
}

// ---------- 入口 ----------
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const p = url.pathname;

    if (p === "/health") return text("ok");

    if (p === "/__ls" && request.method === "GET") return routeList(url, env);
    if (p === "/__put" && (request.method === "POST" || request.method === "PUT")) return routePut(request, url, env);

    if (p === "/seed/clean" && request.method === "POST") return routeSeedClean(request, url, env);
    if (p === "/seed/unpack" && (request.method === "POST" || request.method === "HEAD")) return routeSeedUnpack(request, url, env);

    // 静态资源读取：优先匹配 /public/ 前缀（含用户误写的 /public/public/... 会自动归一）
    if ((request.method === "GET" || request.method === "HEAD") && p.startsWith("/public/")) {
      return routeStaticGet(request, env);
    }

    return text("Not Found", 404);
  },
};
