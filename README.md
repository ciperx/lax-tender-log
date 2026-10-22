# lax-tender-log

Append only tender event stream in JSONL plus a deterministic daily report section in
Markdown. Zero dependencies, no clock, no randomness, no network calls.

A tender walks through eight steps, and every step is recorded as one event:

`listing`, `airlock_decision`, `bid`, `selection`, `award`, `delivery`, `review`, `payment`.

## Why append only

Racing agents and citizens write to the same tender. If any step can be edited in place, a
later reader cannot tell a real airlock decision from one that was added after a bid. This
module therefore never updates an event: `emit` validates the new event against the tail of
the stream and returns a new array, and events that break the order (a seq gap, a timestamp
older than its predecessor, a repeated event id) are refused before they reach the file.

## Use

```ts
import { EventLog, emit, emitLine, parseJsonl, reportSection } from "lax-tender-log";

let stream = [];
stream = emit(stream, {
  seq: 0, ts: "2026-10-24T08:00:00Z", kind: "listing",
  tender_id: "tender-1", actor: "agent-a", id: "ev-1", title: "Bridge deck repair",
});
stream = emit(stream, {
  seq: 1, ts: "2026-10-24T08:05:00Z", kind: "airlock_decision",
  tender_id: "tender-1", actor: "citizen-7", id: "ev-2", decision: "allow",
});

// one line, ready to append to the JSONL file
const line = emitLine(stream, {
  seq: 2, ts: "2026-10-24T08:06:00Z", kind: "bid",
  tender_id: "tender-1", actor: "agent-a", id: "ev-3",
  price_usdc: 26, delivery_by: "2026-10-27T00:00:00Z",
});

// read it back, refusing tampering
const reread = parseJsonl(lines.join("\n") + "\n" + line + "\n");

// the report section for one day
const section = reportSection(reread, "2026-10-24");
```

`EventLog` is a small in memory wrapper when the caller has no file of its own: it keeps the
validated stream and the exact JSONL lines, and `append` refuses the same bad events as
`emit`.

## Validation

Base fields on every event: `seq` (non negative integer, contiguous from 0), `ts` (ISO 8601
with a timezone), `kind`, `tender_id`, `actor`, `id`.

Per kind:

| Kind | Additional required fields |
| --- | --- |
| listing | `title` |
| airlock_decision | `decision` = `allow` or `deny` |
| bid | `price_usdc` >= 0, `delivery_by` ISO 8601 |
| selection | `bid_id` |
| award | `bid_id` |
| delivery | `artifact` |
| review | `decision` = `accepted` or `rejected` |
| payment | `amount_usdc` > 0, `tx` |

Anything else raises `TenderError` with a stable `code`: `not_object`, `missing_field`,
`bad_seq`, `bad_ts`, `bad_kind`, `bad_tender_id`, `bad_actor`, `bad_id`, `bad_price`,
`bad_delivery_by`, `bad_amount`, `bad_tx`, `bad_bid_id`, `bad_artifact`, `bad_decision`,
`seq_gap`, `time_travel`, `duplicate_id`, `bad_json`, `bad_day`.

## Daily report section

`reportSection(events, "YYYY-MM-DD")` returns:

- a `###` heading for the day;
- the number of events and distinct tenders on that day;
- a table with one row per step, `Today` and `Cumulative` counts. Cumulative is measured over
  the stream up to the end of that day, so a late reading still matches what was known then;
- every anomaly carried by the stream up to that point.

Anomalies:

| Code | Meaning |
| --- | --- |
| `bid_without_airlock` | a bid with no airlock decision before it for that tender |
| `bid_after_denial` | a bid that follows a `deny` airlock decision |
| `payment_without_acceptance` | a payment with no `accepted` review before it |
| `payment_without_award` | a payment with no award before it |
| `delivery_without_award` | a delivery with no award before it |
| `review_without_delivery` | a review with no delivery before it |
| `unknown_bid` | a selection or award naming a bid that is not in the stream |
| `seq_gap`, `duplicate_id`, `time_travel` | stream integrity, on top of the parser refusals |

A day with no events returns a section that says so, instead of an empty table.

## Tests

```sh
node --test test/*.test.ts
```

38 tests: every kind accepted, every field level refusal, append only order (seq gap, time
travel, duplicate id), no mutation of the input stream, JSONL round trip, tampered and
reordered files refused, each anomaly code, day filtering, cumulative counts, and byte
identical output for equal input.

Node 22.6 or newer, type stripping, no build step.

## License

MIT
