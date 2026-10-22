/**
 * Tender event log: an append only JSONL stream of tender steps plus a daily
 * Markdown report section. Zero dependencies, no clock, no randomness.
 *
 * Stream model: events are plain objects with a contiguous `seq` starting at 0.
 * `emit` never mutates the stream it is given: it validates the new event
 * against the tail of the stream and returns a new array with the event
 * appended. `emitLine` returns the JSONL line that a caller appends to disk.
 * `parseJsonl` reads a stream back and refuses tampered input.
 */

export const KINDS = [
  "listing",
  "airlock_decision",
  "bid",
  "selection",
  "award",
  "delivery",
  "review",
  "payment",
] as const;

export type Kind = (typeof KINDS)[number];

export interface TenderEvent {
  seq: number;
  ts: string;
  kind: Kind;
  tender_id: string;
  actor: string;
  id: string;
  [field: string]: unknown;
}

export class TenderError extends Error {
  code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "TenderError";
    this.code = code;
  }
}

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;
const DAY = /^\d{4}-\d{2}-\d{2}$/;

const BASE_FIELDS = ["seq", "ts", "kind", "tender_id", "actor", "id"];

const REQUIRED: Record<Kind, string[]> = {
  listing: ["title"],
  airlock_decision: ["decision"],
  bid: ["price_usdc", "delivery_by"],
  selection: ["bid_id"],
  award: ["bid_id"],
  delivery: ["artifact"],
  review: ["decision"],
  payment: ["amount_usdc", "tx"],
};

const DECISIONS: Record<string, string[]> = {
  airlock_decision: ["allow", "deny"],
  review: ["accepted", "rejected"],
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isIso(value: unknown): value is string {
  return typeof value === "string" && ISO.test(value) && !Number.isNaN(Date.parse(value));
}

function isNonNegativeNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

/** Validate a single event shape. Field level only, no stream context. */
export function validateEvent(event: unknown): TenderEvent {
  if (!isPlainObject(event)) {
    throw new TenderError("not_object", "event must be a plain object");
  }
  for (const field of BASE_FIELDS) {
    if (!(field in event)) {
      throw new TenderError("missing_field", `event is missing required field: ${field}`);
    }
  }
  if (!Number.isInteger(event.seq) || (event.seq as number) < 0) {
    throw new TenderError("bad_seq", "seq must be a non negative integer");
  }
  if (!isIso(event.ts)) {
    throw new TenderError("bad_ts", "ts must be an ISO 8601 timestamp with a timezone");
  }
  if (!KINDS.includes(event.kind as Kind)) {
    throw new TenderError("bad_kind", `unknown event kind: ${String(event.kind)}`);
  }
  const kind = event.kind as Kind;
  if (!isNonEmptyString(event.tender_id)) {
    throw new TenderError("bad_tender_id", "tender_id must be a non empty string");
  }
  if (!isNonEmptyString(event.actor)) {
    throw new TenderError("bad_actor", "actor must be a non empty string");
  }
  if (!isNonEmptyString(event.id)) {
    throw new TenderError("bad_id", "id must be a non empty string");
  }
  for (const field of REQUIRED[kind]) {
    if (!(field in event)) {
      throw new TenderError("missing_field", `${kind} event is missing required field: ${field}`);
    }
  }
  if (kind === "bid") {
    if (!isNonNegativeNumber(event.price_usdc)) {
      throw new TenderError("bad_price", "bid price_usdc must be a non negative number");
    }
    if (!isIso(event.delivery_by)) {
      throw new TenderError("bad_delivery_by", "bid delivery_by must be an ISO 8601 timestamp");
    }
  }
  if (kind === "payment") {
    if (!isNonNegativeNumber(event.amount_usdc) || event.amount_usdc === 0) {
      throw new TenderError("bad_amount", "payment amount_usdc must be a positive number");
    }
    if (!isNonEmptyString(event.tx)) {
      throw new TenderError("bad_tx", "payment tx must be a non empty string");
    }
  }
  if (kind === "selection" || kind === "award") {
    if (!isNonEmptyString(event.bid_id)) {
      throw new TenderError("bad_bid_id", `${kind} bid_id must be a non empty string`);
    }
  }
  if (kind === "delivery" && !isNonEmptyString(event.artifact)) {
    throw new TenderError("bad_artifact", "delivery artifact must be a non empty string");
  }
  const allowed = DECISIONS[kind];
  if (allowed) {
    if (typeof event.decision !== "string" || !allowed.includes(event.decision)) {
      throw new TenderError(
        "bad_decision",
        `${kind} decision must be one of: ${allowed.join(", ")}`,
      );
    }
  }
  return event as TenderEvent;
}

/** Position of the event relative to the tail of the stream. */
function checkAgainstTail(stream: TenderEvent[], event: TenderEvent): void {
  const expected = stream.length;
  if (event.seq !== expected) {
    throw new TenderError(
      "seq_gap",
      `event seq ${event.seq} does not follow the stream tail, expected ${expected}`,
    );
  }
  if (stream.length > 0) {
    const previous = stream[stream.length - 1];
    if (Date.parse(event.ts) < Date.parse(previous.ts)) {
      throw new TenderError(
        "time_travel",
        `event ts ${event.ts} is older than the previous event ts ${previous.ts}`,
      );
    }
  }
  const seen = new Set(stream.map((item) => item.id));
  if (seen.has(event.id)) {
    throw new TenderError("duplicate_id", `event id already in the stream: ${event.id}`);
  }
}

/**
 * Append an event to a stream. The input stream is not mutated: a new array is
 * returned. Invalid events and events that break append only order are refused
 * with a TenderError, so a bad event can never enter the stream.
 */
export function emit(stream: readonly TenderEvent[], event: unknown): TenderEvent[] {
  const next = validateEvent(event);
  checkAgainstTail(stream as TenderEvent[], next);
  return [...stream, next];
}

/** Convenience: emit and return only the JSONL line to append to the file. */
export function emitLine(stream: readonly TenderEvent[], event: unknown): string {
  const after = emit(stream, event);
  return JSON.stringify(after[after.length - 1]);
}

/**
 * Parse a JSONL file back into a stream, refusing anything that is not a clean
 * append only log: bad JSON, bad shapes, seq gaps, duplicate ids, decreasing ts.
 */
export function parseJsonl(text: string): TenderEvent[] {
  const stream: TenderEvent[] = [];
  const lines = text.split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index].trim();
    if (line === "") continue;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      throw new TenderError("bad_json", `line ${index + 1} is not valid JSON`);
    }
    stream.push(validateEvent(value));
    if (stream.length > 1) {
      const current = stream[stream.length - 1];
      const previous = stream[stream.length - 2];
      if (current.seq !== previous.seq + 1) {
        throw new TenderError("seq_gap", `line ${index + 1} breaks seq order`);
      }
      if (Date.parse(current.ts) < Date.parse(previous.ts)) {
        throw new TenderError("time_travel", `line ${index + 1} is older than its predecessor`);
      }
    }
    if (stream.slice(0, -1).some((item) => item.id === stream[stream.length - 1].id)) {
      throw new TenderError("duplicate_id", `line ${index + 1} repeats an event id`);
    }
  }
  return stream;
}

/** In memory sink: append a line and get a validated stream back. */
export class EventLog {
  #stream: TenderEvent[] = [];
  #lines: string[] = [];

  append(event: unknown): TenderEvent {
    const next = validateEvent(event);
    this.#stream = emit(this.#stream, next);
    this.#lines.push(JSON.stringify(next));
    return next;
  }

  get events(): readonly TenderEvent[] {
    return this.#stream;
  }

  toJsonl(): string {
    return this.#lines.length === 0 ? "" : `${this.#lines.join("\n")}\n`;
  }
}

export interface Anomaly {
  code: string;
  event_id: string;
  tender_id: string;
  message: string;
}

/** Find the last event of a kind for a tender before a given event. */
function lastBefore(
  events: readonly TenderEvent[],
  index: number,
  tenderId: string,
  predicate: (event: TenderEvent) => boolean,
): TenderEvent | undefined {
  for (let cursor = index - 1; cursor >= 0; cursor -= 1) {
    const candidate = events[cursor];
    if (candidate.tender_id === tenderId && predicate(candidate)) return candidate;
  }
  return undefined;
}

/**
 * Integrity and process anomalies over a stream. `limit` restricts the view to
 * the events up to and including that index, which is how the daily report
 * looks at a day without seeing the future.
 */
export function findAnomalies(events: readonly TenderEvent[], limit?: number): Anomaly[] {
  const end = limit === undefined ? events.length : Math.min(limit, events.length);
  const seen = new Map<string, number>();
  const bidIds = new Set<string>();
  const anomalies: Anomaly[] = [];

  for (let index = 0; index < end; index += 1) {
    const event = events[index];
    const push = (code: string, message: string): void => {
      anomalies.push({ code, event_id: event.id, tender_id: event.tender_id, message });
    };

    if (event.seq !== index) {
      push("seq_gap", `event ${event.id} carries seq ${event.seq} at position ${index}`);
    }
    const previousIndex = seen.get(event.id);
    if (previousIndex !== undefined) {
      push("duplicate_id", `event id ${event.id} first appeared at position ${previousIndex}`);
    } else {
      seen.set(event.id, index);
    }
    if (index > 0 && Date.parse(event.ts) < Date.parse(events[index - 1].ts)) {
      push("time_travel", `event ${event.id} is older than the event before it`);
    }

    if (event.kind === "bid") {
      bidIds.add(event.id);
      const airlock = lastBefore(events, index, event.tender_id, (item) => item.kind === "airlock_decision");
      if (!airlock) {
        push("bid_without_airlock", `bid ${event.id} on tender ${event.tender_id} has no airlock decision`);
      } else if (airlock.decision !== "allow") {
        push(
          "bid_after_denial",
          `bid ${event.id} on tender ${event.tender_id} follows a denied airlock decision (${String(airlock.id)})`,
        );
      }
    }

    if (event.kind === "selection" || event.kind === "award") {
      const target = String(event.bid_id);
      const known = events.slice(0, index).some((item) => item.kind === "bid" && item.id === target);
      if (!known) {
        push("unknown_bid", `${event.kind} ${event.id} names bid ${target} which is not in the stream`);
      }
    }

    if (event.kind === "delivery") {
      const award = lastBefore(events, index, event.tender_id, (item) => item.kind === "award");
      if (!award) {
        push("delivery_without_award", `delivery ${event.id} on tender ${event.tender_id} has no award`);
      }
    }

    if (event.kind === "review") {
      const delivery = lastBefore(events, index, event.tender_id, (item) => item.kind === "delivery");
      if (!delivery) {
        push("review_without_delivery", `review ${event.id} on tender ${event.tender_id} has no delivery`);
      }
    }

    if (event.kind === "payment") {
      const accepted = lastBefore(
        events,
        index,
        event.tender_id,
        (item) => item.kind === "review" && item.decision === "accepted",
      );
      if (!accepted) {
        push("payment_without_acceptance", `payment ${event.id} on tender ${event.tender_id} has no accepted review`);
      }
      const award = lastBefore(events, index, event.tender_id, (item) => item.kind === "award");
      if (!award) {
        push("payment_without_award", `payment ${event.id} on tender ${event.tender_id} has no award`);
      }
    }
  }

  return anomalies;
}

function countByKind(events: readonly TenderEvent[]): Map<Kind, number> {
  const counts = new Map<Kind, number>();
  for (const kind of KINDS) counts.set(kind, 0);
  for (const event of events) counts.set(event.kind, (counts.get(event.kind) ?? 0) + 1);
  return counts;
}

/**
 * One Markdown section for a single day. Counts each step and lists anomalies
 * carried by the stream up to the end of that day. Deterministic: same events
 * and same day always produce the same bytes.
 */
export function reportSection(events: readonly TenderEvent[], day: string): string {
  if (typeof day !== "string" || !DAY.test(day)) {
    throw new TenderError("bad_day", "day must look like YYYY-MM-DD");
  }

  let lastIndex = -1;
  const sameDay: TenderEvent[] = [];
  for (let index = 0; index < events.length; index += 1) {
    const event = events[index];
    if (event.ts.startsWith(day)) {
      sameDay.push(event);
      lastIndex = index;
    } else if (lastIndex >= 0) {
      break;
    }
  }

  const counts = countByKind(sameDay);
  const totals = countByKind(events.slice(0, lastIndex + 1));
  const anomalies = lastIndex >= 0 ? findAnomalies(events, lastIndex + 1) : [];
  const tenderIds = [...new Set(sameDay.map((event) => event.tender_id))].sort();

  const lines: string[] = [];
  lines.push(`### ${day}`);
  lines.push("");
  if (sameDay.length === 0) {
    lines.push("No tender events were recorded on this day.");
    lines.push("");
  } else {
    lines.push(`Events: ${sameDay.length} on ${tenderIds.length} tender${tenderIds.length === 1 ? "" : "s"}.`);
    lines.push("");
    lines.push("| Step | Today | Cumulative |");
    lines.push("| --- | ---: | ---: |");
    for (const kind of KINDS) {
      lines.push(`| ${kind} | ${counts.get(kind) ?? 0} | ${totals.get(kind) ?? 0} |`);
    }
    lines.push("");
  }

  if (anomalies.length === 0) {
    lines.push("Anomalies: none.");
  } else {
    lines.push(`Anomalies: ${anomalies.length}.`);
    lines.push("");
    for (const anomaly of anomalies) {
      lines.push(`- ${anomaly.code}: ${anomaly.message}`);
    }
  }
  lines.push("");
  return lines.join("\n");
}
