#!/usr/bin/env node
// Independent canonical encoder for runwick fingerprints (docs/spec/02 §8). Written from the
// spec, not from the C# code. Generates tests/vectors/wire/*.json and verifies them (--verify).
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const wireDir = join(here, "..", "wire");

// ---- canonical value model: {t:'null'|'bool'|'str'|'num'|'arr'|'obj', v} ----
const NUM = /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/;
export const C = {
  null: { t: "null" },
  bool: (b) => ({ t: "bool", v: !!b }),
  str: (s) => ({ t: "str", v: s }),
  num: (lexeme) => ({ t: "num", v: String(lexeme) }),
  int: (n) => ({ t: "num", v: BigInt(n).toString(10) }),
  arr: (items) => ({ t: "arr", v: items }),
  obj: (entries) => ({ t: "obj", v: Object.entries(entries) }),
  orNull: (s) => (s === null || s === undefined ? C.null : C.str(s)),
};

function strictUtf8(s) {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const d = s.charCodeAt(i + 1);
      if (!(d >= 0xdc00 && d <= 0xdfff)) throw new CanonError("payload_invalid_utf8");
      i++;
    } else if (c >= 0xdc00 && c <= 0xdfff) throw new CanonError("payload_invalid_utf8");
  }
  return Buffer.from(s, "utf8");
}

export class CanonError extends Error {
  constructor(code) { super(code); this.code = code; }
}

function u32(n) { const b = Buffer.alloc(4); b.writeUInt32BE(n); return b; }

export function encode(value) {
  const parts = [];
  const w = (x) => {
    switch (x.t) {
      case "null": parts.push(Buffer.from([0])); break;
      case "bool": parts.push(Buffer.from([x.v ? 2 : 1])); break;
      case "str": { const b = strictUtf8(x.v); parts.push(Buffer.from([3]), u32(b.length), b); break; }
      case "num": {
        if (!NUM.test(x.v)) throw new CanonError("payload_invalid_number");
        if (x.v.length > 128) throw new CanonError("payload_number_too_long");
        const b = Buffer.from(x.v, "ascii"); parts.push(Buffer.from([4]), u32(b.length), b); break;
      }
      case "arr": parts.push(Buffer.from([5]), u32(x.v.length)); x.v.forEach(w); break;
      case "obj": {
        const items = x.v.map(([k, v]) => [strictUtf8(k), v]).sort((a, b) => Buffer.compare(a[0], b[0]));
        for (let i = 1; i < items.length; i++)
          if (Buffer.compare(items[i - 1][0], items[i][0]) === 0) throw new CanonError("payload_duplicate_property");
        parts.push(Buffer.from([6]), u32(items.length));
        for (const [k, v] of items) { parts.push(Buffer.from([3]), u32(k.length), k); w(v); }
        break;
      }
      default: throw new Error("unknown canonical value " + x.t);
    }
  };
  w(value);
  return Buffer.concat(parts);
}

export function digest(domain, value) {
  return createHash("sha256").update(Buffer.from(domain, "ascii")).update(Buffer.from([0])).update(encode(value)).digest("hex");
}

// ---- strict JSON parser over raw bytes (spec 02 §4.2) keeping number lexemes ----
export function parseJson(bytes) {
  if (bytes.length >= 3 && bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) throw new CanonError("payload_invalid_json");
  let text;
  try { text = new TextDecoder("utf-8", { fatal: true }).decode(bytes); } catch { throw new CanonError("payload_invalid_utf8"); }
  let i = 0;
  const ws = () => { while (i < text.length && " \t\n\r".includes(text[i])) i++; };
  const fail = (code = "payload_invalid_json") => { throw new CanonError(code); };
  const value = (depth) => {
    ws();
    const ch = text[i];
    if (ch === "{" || ch === "[") {
      if (depth + 1 > 32) fail("payload_depth_exceeded");
    }
    if (ch === "{") {
      i++; const entries = []; const names = new Set(); ws();
      if (text[i] === "}") { i++; return { t: "obj", v: entries }; }
      for (;;) {
        ws(); if (text[i] !== '"') fail();
        const name = string(); ws(); if (text[i] !== ":") fail(); i++;
        if (Buffer.byteLength(name, "utf8") > 256) fail("payload_property_name_too_long");
        if (names.has(name)) fail("payload_duplicate_property");
        names.add(name);
        entries.push([name, value(depth + 1)]);
        if (entries.length > 256) fail("payload_too_many_properties");
        ws(); if (text[i] === ",") { i++; continue; } if (text[i] === "}") { i++; break; } fail();
      }
      return { t: "obj", v: entries };
    }
    if (ch === "[") {
      i++; const items = []; ws();
      if (text[i] === "]") { i++; return { t: "arr", v: items }; }
      for (;;) {
        items.push(value(depth + 1));
        if (items.length > 16384) fail("payload_array_too_long");
        ws(); if (text[i] === ",") { i++; continue; } if (text[i] === "]") { i++; break; } fail();
      }
      return { t: "arr", v: items };
    }
    if (ch === '"') {
      const s = string();
      if (Buffer.byteLength(s, "utf8") > 262144) fail("payload_string_too_long");
      return { t: "str", v: s };
    }
    if (text.startsWith("true", i)) { i += 4; return C.bool(true); }
    if (text.startsWith("false", i)) { i += 5; return C.bool(false); }
    if (text.startsWith("null", i)) { i += 4; return C.null; }
    const m = /^-?[0-9][0-9.eE+-]*/.exec(text.slice(i, i + 200));
    if (!m) fail();
    const lex = m[0];
    if (!NUM.test(lex)) fail("payload_invalid_number");
    if (lex.length > 128) fail("payload_number_too_long");
    i += lex.length;
    return { t: "num", v: lex };
  };
  const string = () => {
    i++; let out = "";
    for (;;) {
      if (i >= text.length) fail();
      const c = text[i];
      if (c === '"') { i++; break; }
      if (c.charCodeAt(0) < 0x20) fail();
      if (c !== "\\") { out += c; i++; continue; }
      const e = text[i + 1]; i += 2;
      const simple = { '"': '"', "\\": "\\", "/": "/", b: "\b", f: "\f", n: "\n", r: "\r", t: "\t" };
      if (e in simple) { out += simple[e]; continue; }
      if (e !== "u") fail();
      const hex = (s) => { if (!/^[0-9a-fA-F]{4}$/.test(s)) fail(); return parseInt(s, 16); };
      const cu = hex(text.slice(i, i + 4)); i += 4;
      if (cu >= 0xdc00 && cu <= 0xdfff) fail("payload_invalid_utf8");
      if (cu >= 0xd800 && cu <= 0xdbff) {
        if (text[i] !== "\\" || text[i + 1] !== "u") fail("payload_invalid_utf8");
        const lo = hex(text.slice(i + 2, i + 6));
        if (!(lo >= 0xdc00 && lo <= 0xdfff)) fail("payload_invalid_utf8");
        i += 6; out += String.fromCharCode(cu, lo); continue;
      }
      out += String.fromCharCode(cu);
    }
    return out;
  };
  const root = value(0);
  ws();
  if (i !== text.length) fail();
  return root;
}

export function parsePayload(bytes) {
  if (bytes.length > 1048576) throw new CanonError("payload_too_large");
  const v = parseJson(bytes);
  if (v.t !== "obj") throw new CanonError("payload_root_not_object");
  return v;
}

// ---- envelopes (spec 02 §7, §8.3) ----
const retry = (r) => C.obj({ backoff: C.str(r.backoff), evaluatorVersion: C.int(r.evaluatorVersion), initialDelay: C.int(r.initialDelay), jitterKind: C.str(r.jitterKind), maximumDelay: C.int(r.maximumDelay) });
const scopes = (list) => C.arr([...list].sort((a, b) => Buffer.compare(Buffer.from(a.kind), Buffer.from(b.kind)) || Buffer.compare(Buffer.from(a.key), Buffer.from(b.key))).map((s) => C.obj({ key: C.str(s.key), kind: C.str(s.kind) })));
const strMap = (m) => C.obj(Object.fromEntries(Object.entries(m).map(([k, v]) => [k, C.str(v)])));

export function businessEnvelope(r) {
  return C.obj({
    application: C.str(r.application),
    businessMetadata: strMap(r.businessMetadata),
    concurrencyScopes: scopes(r.concurrencyScopes),
    eligibility: r.notBeforeMs === null ? C.obj({ mode: C.str("immediate") }) : C.obj({ mode: C.str("instant"), utcEpochMilliseconds: C.int(r.notBeforeMs) }),
    fingerprintVersion: C.int(1),
    jobName: C.str(r.jobName),
    payload: parsePayload(Buffer.from(r.payload, "utf8")),
    payloadVersion: C.int(r.payloadVersion),
    priority: C.str(r.priority),
    queue: C.str(r.queue),
    rateScopes: scopes(r.rateScopes),
    serializerProfileId: C.str(r.serializerProfileId),
    serializerProfileVersion: C.int(r.serializerProfileVersion),
    uniqueness: C.obj({ activeKey: C.orNull(r.activeKey), window: r.windowMs === null ? C.null : C.int(r.windowMs), windowKey: C.orNull(r.windowKey) }),
  });
}

export function policyEnvelope(p) {
  return C.obj({
    executionTimeout: p.executionTimeout === null ? C.null : C.int(p.executionTimeout),
    fingerprintVersion: C.int(1),
    maxConsecutiveUnstartedAbandonments: C.int(p.maxConsecutiveUnstartedAbandonments),
    maxExecutions: C.int(p.maxExecutions),
    retry: retry(p.retry),
    unstartedRetry: retry(p.unstartedRetry),
  });
}

export function dispatchCommandEnvelope(r) {
  return C.obj({
    application: C.str(r.application),
    body: C.obj({ business: businessEnvelope(r), diagnosticMetadata: strMap(r.diagnosticMetadata), policy: policyEnvelope(r.policy), proposedJobId: C.str(r.proposedJobId) }),
    commandId: C.str(r.operationId),
    fingerprintVersion: C.int(1),
    notAfter: C.int(r.notAfter),
    operation: C.str("Dispatch"),
    operationVersion: C.int(1),
    storeId: C.str(r.storeId),
  });
}

// ---- vector definitions (spec 02 §12) ----
const defaultPolicy = () => ({
  maxExecutions: 3, executionTimeout: null, maxConsecutiveUnstartedAbandonments: 5,
  retry: { backoff: "exponential", evaluatorVersion: 1, jitterKind: "equal", initialDelay: 5000, maximumDelay: 60000 },
  unstartedRetry: { backoff: "exponential", evaluatorVersion: 1, jitterKind: "equal", initialDelay: 1000, maximumDelay: 30000 },
});
const base = () => ({
  storeId: "5d0c6f8e-2b7a-4c1d-9e3f-0a1b2c3d4e5f", application: "billing", jobName: "send-welcome-email",
  payloadVersion: 2, serializerProfileId: "runwick.json", serializerProfileVersion: 1,
  payload: '{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d","locale":"en"}',
  queue: "emails", priority: "high", notBeforeMs: null, concurrencyScopes: [{ kind: "queue", key: "emails" }], rateScopes: [],
  activeKey: "user:42", windowKey: "req-7f3a", windowMs: 86400000, businessMetadata: { tenant: "acme" },
  diagnosticMetadata: { traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" },
  policy: defaultPolicy(), operationId: "0192f3a1-8000-7000-8000-000000000001",
  proposedJobId: "0192f3a1-8000-7000-8000-000000000002", notAfter: 1793354700000,
});

const primitives = [
  ["null", "00"], ["false", "01"], ["true", "02"], ['"a"', "030000000161"], ['"\\u0061"', "030000000161"],
  ['"é"', "0300000002c3a9"], ['"é"', "030000000365cc81"], ["1", "040000000131"], ["1.0", "0400000003312e30"],
  ["1e0", "0400000003316530"], ["-0", "04000000022d30"], ["[1,2]", "0500000002040000000131040000000132"],
  ["[2,1]", "0500000002040000000132040000000131"],
  ['{"b":2,"a":1}', "0600000002030000000161040000000131030000000162040000000132"],
  ['{ "a" : 1 ,\n "b" : 2 }', "0600000002030000000161040000000131030000000162040000000132"], ["{}", "0600000000"],
];

const rejections = [
  ["duplicate", Buffer.from('{"a":1,"a":2}'), "payload_duplicate_property"],
  ["duplicate-escaped", Buffer.from('{"a":1,"\\u0061":2}'), "payload_duplicate_property"],
  ["bad-utf8-c328", Buffer.from([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xc3, 0x28, 0x22, 0x7d]), "payload_invalid_utf8"],
  ["bad-utf8-surrogate", Buffer.from([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xed, 0xa0, 0x80, 0x22, 0x7d]), "payload_invalid_utf8"],
  ["bad-utf8-overlong", Buffer.from([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xc0, 0xaf, 0x22, 0x7d]), "payload_invalid_utf8"],
  ["lone-surrogate", Buffer.from('{"a":"\\ud800"}'), "payload_invalid_utf8"],
  ["reversed-surrogates", Buffer.from('{"a":"\\udc00\\ud800"}'), "payload_invalid_utf8"],
  ["bom", Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), Buffer.from('{"a":1}')]), "payload_invalid_json"],
  ["nan", Buffer.from('{"a":NaN}'), "payload_invalid_json"],
  ["plus", Buffer.from('{"a":+1}'), "payload_invalid_json"],
  ["leading-zero", Buffer.from('{"a":01}'), "payload_invalid_number"],
  ["dot-five", Buffer.from('{"a":.5}'), "payload_invalid_json"],
  ["one-dot", Buffer.from('{"a":1.}'), "payload_invalid_number"],
  ["number-129", Buffer.from('{"a":' + "1".repeat(129) + "}"), "payload_number_too_long"],
  ["root-array", Buffer.from("[1]"), "payload_root_not_object"],
  ["trailing-comma", Buffer.from('{"a":1,}'), "payload_invalid_json"],
  ["comment", Buffer.from('{"a":1}//x'), "payload_invalid_json"],
  ["depth-33", Buffer.from('{"a":' + "[".repeat(32) + "]".repeat(32) + "}"), "payload_depth_exceeded"],
];
const accepts = [
  ["number-128", Buffer.from('{"a":' + "1".repeat(128) + "}")],
  ["depth-32", Buffer.from('{"a":' + "[".repeat(31) + "]".repeat(31) + "}")],
  ["big-integer", Buffer.from('{"a":12345678901234567890123}')],
  ["escaped-control", Buffer.from('{"a":"\\u0000","b":"\\/"}')],
];

function dispatchVectors() {
  const out = [];
  const add = (id, change, mutate, expect) => {
    const r = base(); mutate(r);
    out.push({
      id, change, input: r,
      businessCanonicalHex: encode(businessEnvelope(r)).toString("hex"),
      policyCanonicalHex: encode(policyEnvelope(r.policy)).toString("hex"),
      businessDigest: digest("runwick.dispatch-request.v1", businessEnvelope(r)),
      policyDigest: digest("runwick.dispatch-policy.v1", policyEnvelope(r.policy)),
      commandDigest: digest("runwick.command.v1", dispatchCommandEnvelope(r)),
      expect,
    });
  };
  add("GV-B0", "base", () => {}, { businessDigest: "e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277", policyDigest: "b6af7a208ee69df3802b637b02e52582dd2525eaf10b99720be012e0c69b91b3", commandDigest: "6103715514c292481704a7901e98ccccaa7ead786c69b7c742aee6746806a362" });
  add("GV-B1", "payload whitespace/order", (r) => { r.payload = '{ "locale" : "en",\n  "userId" : "0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d" }'; }, { businessDigest: "e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277" });
  add("GV-B2", "escaped payload string", (r) => { r.payload = '{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d","locale":"\\u0065n"}'; }, { businessDigest: "e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277" });
  add("GV-B3", "queue bulk", (r) => { r.queue = "bulk"; r.concurrencyScopes = [{ kind: "queue", key: "emails" }]; }, { businessDigest: "20559a776a6a5c0b9677c0262b4bd09596699bf5e5cc1fdba7fd01b8f7d19f56" });
  add("GV-B4", "tenant globex", (r) => { r.businessMetadata = { tenant: "globex" }; }, { businessDigest: "227b7a85093aecaca215a40ad29cbe60c9112794fa4f37298a2f934d7279a407" });
  add("GV-B5", "NotBefore 10:00Z", (r) => { r.notBeforeMs = 1793354400000; }, { businessDigest: "b78eef3cb5297df3040045bc877ff479612b2571fcdd425e24f4e3b211a74c67" });
  add("GV-B7", "NotBefore 10:00:00.0001Z ceil", (r) => { r.notBeforeMs = 1793354400001; }, { businessDigest: "e74d140895560330d2f42041db9198244bb451f1579ad88a4b8c024c4f59210a" });
  add("GV-P1", "retry initialDelay 10000", (r) => { r.policy.retry.initialDelay = 10000; }, { businessDigest: "e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277", policyDigest: "553184ed28dccf4da5a06ea3d19315ac90461c56fe65197e9085bf2f58e3bb65" });
  add("GV-C1", "traceparent parent-id changed", (r) => { r.diagnosticMetadata = { traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01" }; }, { businessDigest: "e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277", commandDigest: "87f00f288c36013da7e3b20b83cdeba13acf13a9f6506795bff7fbc018cd141d" });
  add("WV-07-priority", "priority low", (r) => { r.priority = "low"; }, {});
  add("WV-07-max-executions", "maxExecutions 5", (r) => { r.policy.maxExecutions = 5; }, { businessDigest: "e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277" });
  add("WV-07-resource-scope", "added resource scope", (r) => { r.concurrencyScopes = [{ kind: "resource", key: "tenant:acme" }, { kind: "queue", key: "emails" }]; }, {});
  add("WV-04-timeout", "executionTimeout 30000", (r) => { r.policy.executionTimeout = 30000; }, { businessDigest: "e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277" });
  add("WV-04-null-member", "payload member null", (r) => { r.payload = '{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d","locale":null}'; }, {});
  add("WV-04-absent-member", "payload member absent", (r) => { r.payload = '{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d"}'; }, {});
  return out;
}

// ---- schedule definition identity (spec 12 §5) ----
// The recurrence is given as normalized sets: normalization is the grammar's job (SCHED-01); this is the fingerprint.
export function scheduleEnvelope(recurrence, settings = {}) {
  const rec = recurrence.dialect === "runwick.instant.v1"
    ? C.obj({ dialect: C.str(recurrence.dialect), dueAt: C.int(recurrence.dueAt) })
    : C.obj({
        dayOfMonth: C.arr(recurrence.dayOfMonth.map(C.int)), dayOfWeek: C.arr(recurrence.dayOfWeek.map(C.int)),
        dialect: C.str(recurrence.dialect), evaluatorProfile: C.str(recurrence.evaluatorProfile),
        foldPolicy: C.str(recurrence.foldPolicy), gapPolicy: C.str(recurrence.gapPolicy),
        hour: C.arr(recurrence.hour.map(C.int)), minute: C.arr(recurrence.minute.map(C.int)),
        month: C.arr(recurrence.month.map(C.int)), second: C.arr(recurrence.second.map(C.int)),
        zone: C.obj({ canonicalId: C.str(recurrence.zone.canonicalId), tzdbSha256: C.str(recurrence.zone.tzdbSha256), tzdbVersion: C.str(recurrence.zone.tzdbVersion) }),
      });
  return C.obj({ fingerprintVersion: C.int(1), recurrence: rec, settings: strMap(settings) });
}

const range = (a, b, step = 1) => { const out = []; for (let i = a; i <= b; i += step) out.push(i); return out; };
const tzdb = { tzdbVersion: "2026f", tzdbSha256: "b17c31650e9163761b50645b4ab3806649deb24be55c790857fdd004b5611be3" };
const cron5 = (sets, zone, gap = "skip", fold = "earlier") => ({
  dialect: "runwick.cron5.v1", evaluatorProfile: "runwick.cron.v1/cronos-0.13.0/nodatime-3.3.7", second: [0],
  minute: sets.minute, hour: sets.hour, dayOfMonth: sets.dayOfMonth ?? range(1, 31), month: sets.month ?? range(1, 12),
  dayOfWeek: sets.dayOfWeek ?? range(0, 6), gapPolicy: gap, foldPolicy: fold, zone: { canonicalId: zone, ...tzdb },
});

function scheduleVectors() {
  const out = [];
  const add = (id, text, zoneEntered, recurrence, expect, settings = {}) => {
    const env = scheduleEnvelope(recurrence, settings);
    out.push({ id, text, zoneEntered, settings, recurrence, canonicalHex: encode(env).toString("hex"), fingerprint: digest("runwick.schedule-definition.v1", env), expect });
  };
  const weekdays9 = { minute: [0], hour: [9], dayOfWeek: [1, 2, 3, 4, 5] };
  add("SD-1", "0 9 * * MON-FRI", "Europe/Berlin", cron5(weekdays9, "Europe/Berlin"), { fingerprint: "76883d95cd2413ed1e4f9438a8d786fff8bdbc4a0f48e22391998ee0945a9fc1" });
  add("SD-2", "0 9 * * MON-FRI", "Europe/Berlin", cron5(weekdays9, "Europe/Berlin", "shift", "both"), { fingerprint: "63ee9ef060168a8c58d656396dcf5db4c35c8ea1f8b133bb2c30502ec4d7ec10" });
  add("SD-3", null, null, { dialect: "runwick.instant.v1", dueAt: 1782896400000 }, {
    fingerprint: "c66d3c054d3d4a4713d4b45143931c8f1b80a774c51b6d9415d2d6e0181fffaf",
    canonicalHex: "0600000003030000001266696e6765727072696e7456657273696f6e040000000131030000000a726563757272656e6365060000000203000000076469616c656374030000001272756e7769636b2e696e7374616e742e763103000000056475654174040000000d31373832383936343030303030030000000873657474696e67730600000000",
  });
  add("SD-4", "0\t09 * * 5,4,3,2,1", "Europe/Berlin", cron5(weekdays9, "Europe/Berlin"), { fingerprint: "76883d95cd2413ed1e4f9438a8d786fff8bdbc4a0f48e22391998ee0945a9fc1" });
  add("SD-5", "30 2 * * *", "Asia/Calcutta", cron5({ minute: [30], hour: [2] }, "Asia/Kolkata", "shift", "later"), {});
  add("SD-6", "0 9 1 * MON", null, cron5({ minute: [0], hour: [9], dayOfMonth: [1], dayOfWeek: [1] }, "Etc/UTC"), {});
  add("SD-7", "*/15 30 2 * * SUN", "Europe/Berlin", { ...cron5({ minute: [30], hour: [2], dayOfWeek: [0] }, "Europe/Berlin", "skip", "both"), dialect: "runwick.cron6.v1", second: [0, 15, 30, 45] }, {});
  add("SD-8", "0 0 29 2 *", "America/Sao_Paulo", cron5({ minute: [0], hour: [0], dayOfMonth: [29], month: [2] }, "America/Sao_Paulo"), {});
  return out;
}

// ---- outbox hash domains (spec 14 §7) ----
export function outboxDeliveryObject(d) {
  return C.obj({
    acceptUntil: C.int(d.acceptUntil), application: C.str(d.application), businessDigest: C.str(d.businessDigest),
    destinationStoreId: C.str(d.destinationStoreId), diagnosticMetadata: strMap(d.diagnosticMetadata), fingerprintVersion: C.int(1),
    messageId: C.str(d.messageId), policyDigest: C.str(d.policyDigest), proposedJobId: C.str(d.proposedJobId), sourceStoreId: C.str(d.sourceStoreId),
  });
}

export function outboxCommandObject(c, deliveryDigest) {
  return C.obj({
    application: C.str(c.application), body: C.obj({ deliveryDigest: C.str(deliveryDigest) }), commandId: C.str(c.commandId),
    fingerprintVersion: C.int(1), notAfter: C.int(c.notAfter), operation: C.str("ReceiveOutbox"), operationVersion: C.int(1), storeId: C.str(c.storeId),
  });
}

const transportOf = (d, t) => ({ ...t, application: d.application, storeId: d.destinationStoreId });

export function outboxDigests(d, transport) {
  const delivery = digest("runwick.outbox.delivery/1", outboxDeliveryObject(d));
  const command = transport ? digest("runwick.outbox.command/1", outboxCommandObject(transportOf(d, transport), delivery)) : null;
  return { delivery, command };
}

function outboxVectors() {
  const b0 = base();
  const baseDelivery = () => ({
    sourceStoreId: "8b6e2c1a-4f3d-4e5a-9b7c-1d2e3f405162", messageId: "0192f3a1-8000-7000-8000-0000000000aa",
    destinationStoreId: b0.storeId, application: b0.application,
    businessDigest: digest("runwick.dispatch-request.v1", businessEnvelope(b0)), policyDigest: digest("runwick.dispatch-policy.v1", policyEnvelope(b0.policy)),
    diagnosticMetadata: b0.diagnosticMetadata, proposedJobId: b0.proposedJobId, acceptUntil: 1793354400000 + 604800000,
  });
  const baseTransport = () => ({ commandId: "0192f3a1-8000-7000-8000-0000000000c1", notAfter: 1793354430000 });
  const out = [];
  const add = (id, change, mutate, mutateTransport = () => {}) => {
    const d = baseDelivery(); mutate(d);
    const t = baseTransport(); mutateTransport(t);
    const { delivery, command } = outboxDigests(d, t);
    out.push({
      id, change, delivery: d, transport: t,
      deliveryCanonicalHex: encode(outboxDeliveryObject(d)).toString("hex"), deliveryDigest: delivery,
      commandCanonicalHex: encode(outboxCommandObject(transportOf(d, t), delivery)).toString("hex"), commandDigest: command,
    });
  };
  add("OB-0", "base (GV-B0 intent, staged 2026-10-30T10:00Z, 7-day horizon)", () => {});
  add("OB-1", "acceptUntil + 1 ms", (d) => { d.acceptUntil += 1; });
  add("OB-2", "another source store", (d) => { d.sourceStoreId = "8b6e2c1a-4f3d-4e5a-9b7c-1d2e3f405163"; });
  add("OB-3", "another message ID", (d) => { d.messageId = "0192f3a1-8000-7000-8000-0000000000ab"; });
  add("OB-4", "another destination store", (d) => { d.destinationStoreId = "5d0c6f8e-2b7a-4c1d-9e3f-0a1b2c3d4e60"; });
  add("OB-5", "traceparent parent-id changed (diagnostic metadata is frozen intent)", (d) => { d.diagnosticMetadata = { traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01" }; });
  add("OB-6", "retry initialDelay 10000 (policy digest)", (d) => { const p = defaultPolicy(); p.retry.initialDelay = 10000; d.policyDigest = digest("runwick.dispatch-policy.v1", policyEnvelope(p)); });
  add("OB-7", "new transport command ID: delivery unchanged", () => {}, (t) => { t.commandId = "0192f3a1-8000-7000-8000-0000000000c2"; });
  add("OB-8", "transport NotAfter + 1 ms: delivery unchanged", () => {}, (t) => { t.notAfter += 1; });
  return out;
}

function outboxSelfCheck(errors) {
  const v = Object.fromEntries(outboxVectors().map((c) => [c.id, c]));
  for (const id of ["OB-1", "OB-2", "OB-3", "OB-4", "OB-5", "OB-6"]) if (v[id].deliveryDigest === v["OB-0"].deliveryDigest) errors.push(`${id}: delivery digest unchanged`);
  for (const id of ["OB-7", "OB-8"]) {
    if (v[id].deliveryDigest !== v["OB-0"].deliveryDigest) errors.push(`${id}: transport changed the delivery digest`);
    if (v[id].commandDigest === v["OB-0"].commandDigest) errors.push(`${id}: command digest unchanged`);
  }
  for (const c of scheduleVectors()) for (const [k, x] of Object.entries(c.expect)) if (c[k] !== x) errors.push(`${c.id}.${k}: ${c[k]} != ${x}`);
}

function build() {
  return {
    "primitives.json": { spec: "docs/spec/02 §12.1", cases: primitives.map(([json, hex]) => ({ inputBase64: Buffer.from(json, "utf8").toString("base64"), canonicalHex: hex })) },
    "payload-scanner.json": {
      spec: "docs/spec/02 §4, §13 WV-02/WV-03",
      rejected: rejections.map(([id, bytes, code]) => ({ id, inputBase64: bytes.toString("base64"), code })),
      accepted: accepts.map(([id, bytes]) => ({ id, inputBase64: bytes.toString("base64"), canonicalHex: encode(parsePayload(bytes)).toString("hex") })),
    },
    "dispatch.json": { spec: "docs/spec/02 §12.2", cases: dispatchVectors() },
    "schedule-definition.json": { spec: "docs/spec/12 §5.4", domain: "runwick.schedule-definition.v1", cases: scheduleVectors() },
    "outbox.json": { spec: "docs/spec/14 §7, §10", domains: ["runwick.outbox.delivery/1", "runwick.outbox.command/1"], cases: outboxVectors() },
  };
}

function selfCheck() {
  const errors = [];
  for (const [json, hex] of primitives) {
    const got = encode(parseJson(Buffer.from(json, "utf8"))).toString("hex");
    if (got !== hex) errors.push(`primitive ${json}: ${got} != ${hex}`);
  }
  for (const [id, bytes, code] of rejections) {
    try { parsePayload(bytes); errors.push(`${id}: accepted, expected ${code}`); }
    catch (e) { if (e.code !== code) errors.push(`${id}: ${e.code ?? e.message} != ${code}`); }
  }
  for (const c of dispatchVectors()) {
    for (const [k, v] of Object.entries(c.expect)) if (c[k] !== v) errors.push(`${c.id}.${k}: ${c[k]} != ${v}`);
  }
  const b0 = dispatchVectors()[0];
  const b0Bytes = Buffer.from(b0.businessCanonicalHex, "hex").length;
  if (b0Bytes !== 605) errors.push(`GV-B0 canonical length ${b0Bytes} != 605`);
  outboxSelfCheck(errors);
  return errors;
}

const args = process.argv.slice(2);
if (args[0] === "--outbox-digests") {
  // Independent digests for one stored source row (tests/Runwick.Outbox.Tests): argv[1] = {delivery, transport?} JSON.
  const input = JSON.parse(args[1]);
  console.log(JSON.stringify(outboxDigests(input.delivery, input.transport)));
  process.exit(0);
}
const errors = selfCheck();
if (errors.length) { console.error("canonical self-check FAILED\n  " + errors.join("\n  ")); process.exit(1); }
const files = build();
if (args.includes("--write")) {
  for (const [name, content] of Object.entries(files)) writeFileSync(join(wireDir, name), JSON.stringify(content, null, 2) + "\n");
  console.log(`wrote ${Object.keys(files).length} vector files to ${wireDir}`);
} else if (args.includes("--verify")) {
  const bad = [];
  for (const [name, content] of Object.entries(files)) {
    let current;
    try { current = readFileSync(join(wireDir, name), "utf8"); } catch { bad.push(`${name}: missing`); continue; }
    if (current !== JSON.stringify(content, null, 2) + "\n") bad.push(`${name}: differs from generator`);
  }
  const extra = readdirSync(wireDir).filter((f) => f.endsWith(".json") && !(f in files));
  if (extra.length) bad.push(`unexpected files: ${extra.join(", ")}`);
  if (bad.length) { console.error("vector verify FAILED\n  " + bad.join("\n  ")); process.exit(1); }
  console.log(`vectors OK (${Object.keys(files).length} files, golden digests match spec 02 §12 and 12 §5.4)`);
} else {
  console.log("self-check OK; use --write or --verify");
}
