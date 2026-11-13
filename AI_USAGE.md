# AI Usage Disclosure

## Tools used
- Antigravity IDE

## What I used them for
- Scaffolding the initial dispatcher service layout (`db.py`, `clock.py`, `engine.py`, `exporter.py`, `main.py`).
- Drafting helper functions for simulated clock projection math between IST and Unix timestamps.
- Generating unit test boilerplate in `solution/tests/test_scheduler.py`.

## What I wrote or decided myself
- Chose an embedded SQLite database in WAL mode with `BEGIN IMMEDIATE` transactions over Temporal or external task queues, keeping the architecture zero-dependency, single-process, and crash-resilient under `kill -9`.
- Architected the two-phase `INTENT` ledger pattern keyed by pre-committed `request_id`: committing the intent to disk before any network HTTP request is initiated to eliminate double-dialing on process crashes or ambiguous `504` responses.
- Decided on active crash recovery via read-only `GET /v1/calls?request_id=...` rather than blind `POST /v1/calls` retries, ensuring no calls are placed if the calling window closed while the process was down.
- Enforced the `completed > exhausted > dnd_blocked` precedence in SQL transactions so global DND never clobbers already completed leads.

## Where the AI was wrong or unhelpful
1. **Window Close Safety Buffer:** AI generated a fixed 30-second safety cushion before window close. However, under high simulated clock speeds (600x–1200x), 30 simulated seconds corresponds to only 25–50 milliseconds of real wall-clock time. If carrier event-loop latency or loopback TCP connection delays exceed 50ms, calls land outside the window and trigger a 422 violation. I rejected this and implemented an adaptive buffer scaling with clock speed (`max(60s, min(1800s, speed * 1.5))`).
2. **Webhook / 201 Race Condition:** AI initially drafted logic assuming `POST /v1/calls` always returns a `201` response containing `call_sid` before any completion webhook arrives. At 600x clock speed, fast calls (such as failed/busy or quick hang-ups) often deliver the webhook before the HTTP response is read. The AI's code failed because it keyed active calls by `call_sid` instead of `request_id`. I corrected this by indexing everything by `request_id` and making attempt increments idempotent via `counted_attempt`.

## How I verified AI-generated code
- Architectural edge-case testing: designed specific test cases around the 4 core failure traps identified upfront (process kill mid-dial, out-of-order webhook delivery, dropped webhooks, and clock skew).
- Unit tests: ran 7 deterministic unit tests in `solution/tests/test_scheduler.py` covering window bounds, concurrency limits, per-phone mutual exclusion, retry/callback math, and HMAC signatures using a fake clock.
- End-to-end chaos testing: ran the dispatcher against `mock_telephony` with failure injection enabled (`--failure-scale 1.0`), sent hard process kills (`kill -9` / `SIGKILL`) mid-run, and verified `violation_count == 0` on `/admin/report` with a 100% exact match against `/admin/leads`.
