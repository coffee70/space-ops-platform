# SatNOGS Adapter

The SatNOGS adapter ingests AX.25 telemetry from SatNOGS observations for one configured satellite/transmitter pair. It resolves the canonical backend vehicle source at startup, publishes expected observation windows, polls live observations, and drains historical backfill through a shared SatNOGS request coordinator.

Each emitted telemetry sample carries an increasing sequence within its observation stream. Backend history uses that sequence with the stream, channel, and timestamp so repeated LASARSAT packets in one observation are preserved.

The example configuration targets LASARSAT:

- NORAD: `62391`
- Transmitter UUID: `C3RnLSSuaKzWhHrtJCqUgu`
- Observation status: `good`
- Decoder strategy: `kaitai`
- Decoder ID: `lasarsat`

## Request Shape

The live telemetry poll calls SatNOGS with only the configured vehicle/transmitter/status identity on its initial request:

```text
satellite__norad_cat_id=<vehicle norad>
transmitter_uuid=<vehicle transmitter uuid>
status=<adapter status>
```

It does not include `start` or `end` on the initial live poll. Bounded historical requests may add `start` and `end`.

## Live And Backfill Boundary

Each adapter process captures one `startup_cutoff_time` at startup. Live polling and backfill continue to run in parallel: backfill reconciles observations from the platform `monitoring_start_time` or `last_reconciled_at` up to the cutoff, while live handles observations whose `end` is after the cutoff.

Live polling keeps the unbounded request shape above and follows SatNOGS pagination backward until it reaches an observation whose `start` is before the startup cutoff. Observations with `end` at or before the cutoff are skipped by live; observations that span the cutoff are live responsibility.

Backfill requests use platform-sized bounded chunks and locally validate every returned observation on both first and next pages. Observations with missing, unparsable, or out-of-chunk timestamps are logged and skipped. A chunk checkpoint is reported only after the whole chunk page walk succeeds.

If the adapter restarts while a previous backfill target is still marked running, the new process supersedes that stale target, keeps the platform checkpoint, and starts a new run up to its new startup cutoff.

## Observations API Ordering

Manual verification against SatNOGS on 2026-04-11 used the live LASARSAT request shape:

```bash
curl -sS -i "https://network.satnogs.org/api/observations/?satellite__norad_cat_id=62391&transmitter_uuid=C3RnLSSuaKzWhHrtJCqUgu&status=good"
```

SatNOGS returned `HTTP/2 200` with 25 observations on page 1 and a cursor-based `Link` header:

```http
link: <https://network.satnogs.org/api/observations/?cursor=cD0yMDI2LTA0LTEwKzEwJTNBNTglM0E0NyUyQjAwJTNBMDA%3D&satellite__norad_cat_id=62391&status=good&transmitter_uuid=C3RnLSSuaKzWhHrtJCqUgu>; rel="next"
```

The first page was ordered newest-to-oldest by observation `start`:

| id | start | end |
| --- | --- | --- |
| 13790944 | 2026-04-11T11:56:29Z | 2026-04-11T12:05:13Z |
| 13790951 | 2026-04-11T11:53:33Z | 2026-04-11T12:04:42Z |
| 13790735 | 2026-04-11T11:52:53Z | 2026-04-11T12:00:46Z |
| 13790731 | 2026-04-11T11:52:53Z | 2026-04-11T12:00:46Z |
| 13789874 | 2026-04-11T11:51:27Z | 2026-04-11T12:02:47Z |
| 13790272 | 2026-04-11T11:50:58Z | 2026-04-11T11:59:39Z |

The last page-1 item was:

| id | start | end |
| --- | --- | --- |
| 13773367 | 2026-04-10T10:58:47Z | 2026-04-10T11:06:46Z |

Following the exact `rel="next"` URL returned another `HTTP/2 200` with 25 observations:

```bash
curl -sS -i "https://network.satnogs.org/api/observations/?cursor=cD0yMDI2LTA0LTEwKzEwJTNBNTglM0E0NyUyQjAwJTNBMDA%3D&satellite__norad_cat_id=62391&status=good&transmitter_uuid=C3RnLSSuaKzWhHrtJCqUgu"
```

Page 2 continued farther into the past:

| id | start | end |
| --- | --- | --- |
| 13774704 | 2026-04-10T10:58:34Z | 2026-04-10T11:04:50Z |
| 13775297 | 2026-04-10T10:55:29Z | 2026-04-10T11:06:49Z |
| 13779833 | 2026-04-10T09:26:36Z | 2026-04-10T09:30:16Z |
| 13779826 | 2026-04-10T09:24:08Z | 2026-04-10T09:32:31Z |
| 13773279 | 2026-04-10T09:21:49Z | 2026-04-10T09:29:27Z |
| 13774702 | 2026-04-10T09:21:49Z | 2026-04-10T09:27:09Z |

The last page-2 item was:

| id | start | end |
| --- | --- | --- |
| 13768013 | 2026-04-09T11:40:32Z | 2026-04-09T11:48:27Z |

A third cursor hop continued the same pattern, from `2026-04-09T11:40:18Z` down to `2026-04-08T12:22:15Z`.

Observed behavior:

- Results are descending by observation `start`.
- Pagination continues backward into older history.
- Cursor `next` links preserve the satellite, transmitter, and status filters.
- Ordering is not strictly descending by `end`, because observation durations overlap.

## Start And End Parameters

Manual verification on 2026-04-11 also checked whether SatNOGS honors `start` and `end`.

Bounded request:

```bash
curl -sS -D /tmp/satnogs_window.headers -o /tmp/satnogs_window.json "https://network.satnogs.org/api/observations/?satellite__norad_cat_id=62391&transmitter_uuid=C3RnLSSuaKzWhHrtJCqUgu&status=good&start=2026-04-10T09:00:00Z&end=2026-04-10T12:00:00Z"
```

SatNOGS returned `HTTP/2 200` with 11 observations, no pagination link, and all returned observation times inside the requested bounds:

| id | start | end |
| --- | --- | --- |
| 13780001 | 2026-04-10T11:01:43Z | 2026-04-10T11:09:42Z |
| 13773367 | 2026-04-10T10:58:47Z | 2026-04-10T11:06:46Z |
| 13774704 | 2026-04-10T10:58:34Z | 2026-04-10T11:04:50Z |
| 13775297 | 2026-04-10T10:55:29Z | 2026-04-10T11:06:49Z |
| 13779833 | 2026-04-10T09:26:36Z | 2026-04-10T09:30:16Z |
| 13779826 | 2026-04-10T09:24:08Z | 2026-04-10T09:32:31Z |
| 13773279 | 2026-04-10T09:21:49Z | 2026-04-10T09:29:27Z |
| 13774702 | 2026-04-10T09:21:49Z | 2026-04-10T09:27:09Z |
| 13779736 | 2026-04-10T09:21:29Z | 2026-04-10T09:27:19Z |
| 13779387 | 2026-04-10T09:20:34Z | 2026-04-10T09:27:56Z |
| 13779842 | 2026-04-10T09:20:08Z | 2026-04-10T09:31:38Z |

Boundary checks suggest SatNOGS applies `start` to observation start time and `end` to observation end time, returning observations fully contained in the interval rather than observations that merely overlap the interval.

This request returned no observations because the matching observation started inside the interval but ended after the requested `end`:

```bash
curl -sS -D /tmp/satnogs_tight.headers -o /tmp/satnogs_tight.json "https://network.satnogs.org/api/observations/?satellite__norad_cat_id=62391&transmitter_uuid=C3RnLSSuaKzWhHrtJCqUgu&status=good&start=2026-04-10T10:58:40Z&end=2026-04-10T10:59:00Z"
```

Extending `end` returned the observation:

```bash
curl -sS -D /tmp/satnogs_tight2.headers -o /tmp/satnogs_tight2.json "https://network.satnogs.org/api/observations/?satellite__norad_cat_id=62391&transmitter_uuid=C3RnLSSuaKzWhHrtJCqUgu&status=good&start=2026-04-10T10:58:40Z&end=2026-04-10T11:07:00Z"
```

| id | start | end |
| --- | --- | --- |
| 13773367 | 2026-04-10T10:58:47Z | 2026-04-10T11:06:46Z |

One-sided bounds also behaved coherently:

- `start=2026-04-10T10:58:40Z` returned latest observations down to `13773367`, with `start=2026-04-10T10:58:47Z`.
- `end=2026-04-10T11:07:00Z` returned observations ending at or before that bound and exposed a cursor `next` link.
- Following the exact `end`-only `next` link preserved `end=2026-04-10T11%3A07%3A00Z`; page 2 also contained only observations ending at or before the bound.

Operational implication: live polling can rely on the unbounded initial request naturally walking newest-to-oldest, while bounded backfill can use `start` and `end` to fetch fully contained historical windows.

## Tests

From the **`space-ops-platform` repository root**:

```bash
./scripts/run-backend-tests.sh backend/tests/adapters/satnogs -q
```

Full workspace testing matrix (Node containers, Playwright Docker runner, sibling repos): [`../README.md`](../README.md), [`../../space-ops-kernel/README.md`](../../space-ops-kernel/README.md), and [`../AGENTS.md`](../AGENTS.md).
