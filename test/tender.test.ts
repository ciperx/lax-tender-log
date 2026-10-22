import assert from "node:assert/strict";
import test from "node:test";

import {
  EventLog,
  KINDS,
  TenderError,
  emit,
  emitLine,
  findAnomalies,
  parseJsonl,
  reportSection,
  validateEvent,
  type TenderEvent,
} from "../src/tender.ts";

const T0 = "2026-10-24T08:00:00Z";

function at(minute: number): string {
  return `2026-10-24T08:${String(minute).padStart(2, "0")}:00Z`;
}

function base(seq: number, kind: string, id: string, extra: Record<string, unknown> = {}) {
  return { seq, ts: at(seq), kind, tender_id: "tender-1", actor: "agent-a", id, ...extra };
}

/** A complete, healthy tender lifecycle. */
function healthyStream(): TenderEvent[] {
  let stream: TenderEvent[] = [];
  const steps: unknown[] = [
    base(0, "listing", "e0", { title: "KSL tender 1" }),
    base(1, "airlock_decision", "e1", { decision: "allow" }),
    base(2, "bid", "e2", { price_usdc: 26, delivery_by: "2026-10-27T00:00:00Z" }),
    base(3, "selection", "e3", { bid_id: "e2" }),
    base(4, "award", "e4", { bid_id: "e2" }),
    base(5, "delivery", "e5", { artifact: "https://github.com/ciperx/lax-tender-log" }),
    base(6, "review", "e6", { decision: "accepted" }),
    base(7, "payment", "e7", { amount_usdc: 26, tx: "0xabc" }),
  ];
  for (const step of steps) stream = emit(stream, step);
  return stream;
}

test("a complete tender lifecycle is accepted and keeps eight events", () => {
  const stream = healthyStream();
  assert.equal(stream.length, 8);
  assert.deepEqual(stream.map((event) => event.kind), [...KINDS]);
  assert.equal(findAnomalies(stream).length, 0);
});

test("validateEvent accepts every declared kind", () => {
  for (const kind of KINDS) {
    const extra: Record<string, unknown> = {
      listing: { title: "t" },
      airlock_decision: { decision: "allow" },
      bid: { price_usdc: 1, delivery_by: T0 },
      selection: { bid_id: "b" },
      award: { bid_id: "b" },
      delivery: { artifact: "a" },
      review: { decision: "accepted" },
      payment: { amount_usdc: 1, tx: "0x1" },
    }[kind] as Record<string, unknown>;
    const event = base(0, kind, `id-${kind}`, extra);
    assert.equal(validateEvent(event).kind, kind);
  }
});

test("validateEvent rejects a non object event", () => {
  assert.throws(() => validateEvent("nope"), (error: TenderError) => error.code === "not_object");
});

test("validateEvent rejects a missing base field", () => {
  const event = base(0, "listing", "e0", { title: "t" });
  delete (event as Record<string, unknown>).actor;
  assert.throws(() => validateEvent(event), (error: TenderError) => error.code === "missing_field");
});

test("validateEvent rejects a negative or fractional seq", () => {
  assert.throws(() => validateEvent(base(-1, "listing", "e0", { title: "t" })), (error: TenderError) => error.code === "bad_seq");
  assert.throws(() => validateEvent(base(1.5, "listing", "e0", { title: "t" })), (error: TenderError) => error.code === "bad_seq");
});

test("validateEvent rejects a timestamp without a timezone", () => {
  const event = { ...base(0, "listing", "e0", { title: "t" }), ts: "2026-10-24T08:00:00" };
  assert.throws(() => validateEvent(event), (error: TenderError) => error.code === "bad_ts");
});

test("validateEvent rejects an unknown kind", () => {
  assert.throws(() => validateEvent(base(0, "refund", "e0")), (error: TenderError) => error.code === "bad_kind");
});

test("validateEvent rejects an empty tender_id", () => {
  const event = { ...base(0, "listing", "e0", { title: "t" }), tender_id: "  " };
  assert.throws(() => validateEvent(event), (error: TenderError) => error.code === "bad_tender_id");
});

test("validateEvent requires an airlock decision value it knows", () => {
  assert.throws(
    () => validateEvent(base(0, "airlock_decision", "e0", { decision: "maybe" })),
    (error: TenderError) => error.code === "bad_decision",
  );
});

test("validateEvent requires a review decision value it knows", () => {
  assert.throws(
    () => validateEvent(base(0, "review", "e0", { decision: "ok" })),
    (error: TenderError) => error.code === "bad_decision",
  );
});

test("validateEvent rejects a bid without a delivery date", () => {
  assert.throws(
    () => validateEvent(base(0, "bid", "e0", { price_usdc: 5 })),
    (error: TenderError) => error.code === "missing_field",
  );
});

test("validateEvent rejects a negative bid price", () => {
  assert.throws(
    () => validateEvent(base(0, "bid", "e0", { price_usdc: -1, delivery_by: T0 })),
    (error: TenderError) => error.code === "bad_price",
  );
});

test("validateEvent rejects a zero payment amount", () => {
  assert.throws(
    () => validateEvent(base(0, "payment", "e0", { amount_usdc: 0, tx: "0x1" })),
    (error: TenderError) => error.code === "bad_amount",
  );
});

test("validateEvent rejects a payment without a transaction hash", () => {
  assert.throws(
    () => validateEvent(base(0, "payment", "e0", { amount_usdc: 5, tx: " " })),
    (error: TenderError) => error.code === "bad_tx",
  );
});

test("validateEvent rejects a delivery without an artifact", () => {
  assert.throws(
    () => validateEvent(base(0, "delivery", "e0", {})),
    (error: TenderError) => error.code === "missing_field",
  );
});

test("emit does not mutate the stream it was given", () => {
  const stream = healthyStream();
  const before = stream.length;
  const next = emit(stream, base(8, "payment", "e9", { amount_usdc: 1, tx: "0x2" }));
  assert.equal(stream.length, before);
  assert.equal(next.length, before + 1);
  assert.notEqual(stream, next);
});

test("emit refuses a seq gap", () => {
  const stream = healthyStream();
  assert.throws(
    () => emit(stream, base(9, "review", "e9", { decision: "accepted" })),
    (error: TenderError) => error.code === "seq_gap",
  );
});

test("emit refuses an event older than the tail", () => {
  const stream = healthyStream();
  const event = { ...base(8, "review", "e8", { decision: "accepted" }), ts: "2026-10-24T07:00:00Z" };
  assert.throws(() => emit(stream, event), (error: TenderError) => error.code === "time_travel");
});

test("emit refuses a duplicate event id", () => {
  const stream = healthyStream();
  assert.throws(
    () => emit(stream, base(8, "review", "e7", { decision: "accepted" })),
    (error: TenderError) => error.code === "duplicate_id",
  );
});

test("emitLine returns exactly one line and parseJsonl reads it back", () => {
  const stream = healthyStream();
  const line = emitLine(stream, base(8, "review", "e8", { decision: "accepted" }));
  assert.equal(line.includes("\n"), false);
  const parsed = parseJsonl(`${stream.map((event) => JSON.stringify(event)).join("\n")}\n${line}\n`);
  assert.equal(parsed.length, 9);
  assert.deepEqual(parsed, JSON.parse(JSON.stringify(parsed)));
});

test("parseJsonl skips blank lines and tolerates CRLF", () => {
  const stream = healthyStream();
  const text = stream.map((event) => `${JSON.stringify(event)}\r\n`).join("") + "\r\n";
  assert.equal(parseJsonl(text).length, 8);
});

test("parseJsonl refuses a tampered line", () => {
  const stream = healthyStream();
  const lines = stream.map((event) => JSON.stringify(event));
  lines[3] = lines[3].replace("\"selection\"", "\"selction\"");
  assert.throws(() => parseJsonl(lines.join("\n")), (error: TenderError) => error.code === "bad_kind");
});

test("parseJsonl refuses broken JSON", () => {
  const stream = healthyStream();
  const lines = stream.map((event) => JSON.stringify(event));
  lines[2] = lines[2].slice(0, -3);
  assert.throws(() => parseJsonl(lines.join("\n")), (error: TenderError) => error.code === "bad_json");
});

test("parseJsonl refuses a reordered file", () => {
  const stream = healthyStream();
  const lines = stream.map((event) => JSON.stringify(event));
  [lines[2], lines[3]] = [lines[3], lines[2]];
  assert.throws(() => parseJsonl(lines.join("\n")), (error: TenderError) => ["seq_gap", "time_travel"].includes(error.code));
});

test("parseJsonl refuses a duplicated line", () => {
  const stream = healthyStream();
  const lines = stream.map((event) => JSON.stringify(event));
  lines.splice(3, 0, lines[2]);
  assert.throws(() => parseJsonl(lines.join("\n")), (error: TenderError) => ["duplicate_id", "seq_gap"].includes(error.code));
});

test("EventLog appends, validates and serialises a JSONL stream", () => {
  const log = new EventLog();
  for (const event of healthyStream()) log.append(event);
  assert.equal(log.events.length, 8);
  assert.throws(() => log.append(base(8, "payment", "e8", { amount_usdc: 0, tx: "0x1" })), TenderError);
  assert.equal(log.events.length, 8);
  assert.equal(log.toJsonl().trim().split("\n").length, 8);
});

test("reportSection counts each step for the day and marks a clean stream", () => {
  const section = reportSection(healthyStream(), "2026-10-24");
  assert.match(section, /^### 2026-10-24/);
  assert.match(section, /Events: 8 on 1 tender\./);
  for (const kind of KINDS) assert.match(section, new RegExp(`\\| ${kind} \\| 1 \\| 1 \\|`));
  assert.match(section, /Anomalies: none\./);
});

test("reportSection on a day without events says so and counts nothing", () => {
  const section = reportSection(healthyStream(), "2026-10-25");
  assert.match(section, /No tender events were recorded on this day\./);
  assert.equal(section.includes("| listing |"), false);
});

test("reportSection rejects a malformed day", () => {
  assert.throws(() => reportSection(healthyStream(), "24-10-2026"), (error: TenderError) => error.code === "bad_day");
});

test("reportSection is byte identical for equal input", () => {
  assert.equal(reportSection(healthyStream(), "2026-10-24"), reportSection(healthyStream(), "2026-10-24"));
});

test("a bid without an airlock decision is an anomaly", () => {
  let stream: TenderEvent[] = [];
  stream = emit(stream, base(0, "listing", "e0", { title: "t" }));
  stream = emit(stream, base(1, "bid", "e1", { price_usdc: 5, delivery_by: T0 }));
  const codes = findAnomalies(stream).map((anomaly) => anomaly.code);
  assert.deepEqual(codes, ["bid_without_airlock"]);
  assert.match(reportSection(stream, "2026-10-24"), /bid_without_airlock/);
});

test("a bid after a denied airlock decision is an anomaly", () => {
  let stream: TenderEvent[] = [];
  stream = emit(stream, base(0, "listing", "e0", { title: "t" }));
  stream = emit(stream, base(1, "airlock_decision", "e1", { decision: "deny" }));
  stream = emit(stream, base(2, "bid", "e2", { price_usdc: 5, delivery_by: T0 }));
  assert.deepEqual(findAnomalies(stream).map((anomaly) => anomaly.code), ["bid_after_denial"]);
});

test("a payment without acceptance is an anomaly", () => {
  let stream: TenderEvent[] = [];
  stream = emit(stream, base(0, "listing", "e0", { title: "t" }));
  stream = emit(stream, base(1, "airlock_decision", "e1", { decision: "allow" }));
  stream = emit(stream, base(2, "bid", "e2", { price_usdc: 5, delivery_by: T0 }));
  stream = emit(stream, base(3, "award", "e3", { bid_id: "e2" }));
  stream = emit(stream, base(4, "payment", "e4", { amount_usdc: 5, tx: "0x1" }));
  assert.deepEqual(findAnomalies(stream).map((anomaly) => anomaly.code), ["payment_without_acceptance"]);
});

test("a payment after a rejected review is still an anomaly", () => {
  let stream: TenderEvent[] = [];
  stream = emit(stream, base(0, "airlock_decision", "e0", { decision: "allow" }));
  stream = emit(stream, base(1, "bid", "e1", { price_usdc: 5, delivery_by: T0 }));
  stream = emit(stream, base(2, "award", "e2", { bid_id: "e1" }));
  stream = emit(stream, base(3, "delivery", "e3", { artifact: "a" }));
  stream = emit(stream, base(4, "review", "e4", { decision: "rejected" }));
  stream = emit(stream, base(5, "payment", "e5", { amount_usdc: 5, tx: "0x1" }));
  assert.deepEqual(findAnomalies(stream).map((anomaly) => anomaly.code), ["payment_without_acceptance"]);
});

test("a delivery without award and a review without delivery are anomalies", () => {
  let stream: TenderEvent[] = [];
  stream = emit(stream, base(0, "delivery", "e0", { artifact: "a" }));
  assert.deepEqual(findAnomalies(stream).map((anomaly) => anomaly.code), ["delivery_without_award"]);
  let other: TenderEvent[] = [];
  other = emit(other, base(0, "review", "r0", { decision: "accepted" }));
  assert.deepEqual(findAnomalies(other).map((anomaly) => anomaly.code), ["review_without_delivery"]);
});

test("a selection that names an unknown bid is an anomaly", () => {
  let stream: TenderEvent[] = [];
  stream = emit(stream, base(0, "selection", "e0", { bid_id: "ghost" }));
  assert.deepEqual(findAnomalies(stream).map((anomaly) => anomaly.code), ["unknown_bid"]);
});

test("reportSection only looks at events up to the end of its day", () => {
  let stream = healthyStream();
  stream = emit(stream, {
    ...base(8, "payment", "e8", { amount_usdc: 1, tx: "0x2" }),
    ts: "2026-10-25T09:00:00Z",
  });
  const day24 = reportSection(stream, "2026-10-24");
  const day25 = reportSection(stream, "2026-10-25");
  assert.match(day24, /Events: 8 on 1 tender\./);
  assert.match(day25, /Events: 1 on 1 tender\./);
  assert.match(day25, /anomaly|Anomalies/i);
});

test("the daily counts are cumulative, not only the day", () => {
  let stream = healthyStream();
  stream = emit(stream, {
    ...base(8, "review", "e9", { decision: "accepted" }),
    ts: "2026-10-25T09:00:00Z",
  });
  const day25 = reportSection(stream, "2026-10-25");
  assert.match(day25, /\| review \| 1 \| 2 \|/);
  assert.match(day25, /\| payment \| 0 \| 1 \|/);
});
