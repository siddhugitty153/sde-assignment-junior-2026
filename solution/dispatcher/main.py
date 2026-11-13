import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request

from dispatcher.clock import SimClock, is_within_calling_window, next_window_open_ts
from dispatcher.db import Database, load_fixtures
from dispatcher.engine import (
    all_leads_terminal,
    claim_next_eligible_lead,
    get_earliest_waiting_ts,
    get_pending_calls,
    get_status_summary,
    on_call_aborted_not_placed,
    on_call_accepted_201,
    process_call_completion,
)
from dispatcher.exporter import export_final_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("dispatcher")

DEFAULT_FIXTURES = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "assignment-b-workflow",
        "fixtures",
        "campaigns.json",
    )
)


def compute_safety_buffer(speed: float, grace_seconds: float) -> float:
    # Ensure ~1.5 real seconds of simulated cushion before window close (capped at 30 sim min)
    # so carrier event-loop stalls never cause outside_calling_window violations.
    return max(60.0, min(1800.0, speed * 1.5))


def build_app(
    db: Database,
    cfg: dict,
    provider_url: str,
    callback_url: str,
    output_path: str,
    server_ref: dict,
    enable_background: bool = True,
) -> FastAPI:
    clock = SimClock()
    webhook_secret = cfg.get("webhook_secret", "")
    retry_policy = cfg.get("retry_policy")
    reg_win = cfg.get("regulatory_window", {"start": "09:00", "end": "21:00"})
    reg_start = reg_win.get("start", "09:00")
    reg_end = reg_win.get("end", "21:00")
    grace_seconds = float(cfg.get("window_grace_seconds", 120))
    campaigns = cfg.get("campaigns", [])

    in_flight_request_ids: set[str] = set()
    in_flight_tasks: set[asyncio.Task] = set()
    wake_event = asyncio.Event()
    reconcile_event = asyncio.Event()
    stop_event = asyncio.Event()

    async def sync_clock(client: httpx.AsyncClient, retries: int = 1) -> bool:
        for attempt in range(retries):
            try:
                resp = await client.get(f"{provider_url}/v1/clock", timeout=5.0)
                if resp.status_code == 200:
                    clock.update_from_provider(resp.json())
                    return True
            except httpx.HTTPError as exc:
                if attempt == retries - 1:
                    log.warning("Failed to sync clock: %s", exc)
            if attempt < retries - 1:
                await asyncio.sleep(0.2)
        return False

    async def reconcile_one(client: httpx.AsyncClient, row: dict, startup: bool):
        rid = row["request_id"]
        if rid in in_flight_request_ids:
            return

        age = time.time() - float(row["created_real_ts"])
        if row["state"] == "INTENT" and age < 0.2:
            await asyncio.sleep(0.2 - age)
        elif row["state"] == "ACTIVE" and not startup and age < 1.0:
            return

        try:
            resp = await client.get(
                f"{provider_url}/v1/calls",
                params={"request_id": rid},
                timeout=5.0,
            )
        except httpx.HTTPError as exc:
            log.warning("Reconciler GET error for %s: %s", rid, exc)
            return

        if resp.status_code == 404:
            if row["state"] == "INTENT":
                log.info("Reconciler: request_id=%s not found (404); reverting INTENT", rid)
                on_call_aborted_not_placed(db, rid)
                wake_event.set()
            else:
                log.warning("Reconciler: ACTIVE call request_id=%s returned 404", rid)
        elif resp.status_code == 200:
            data = resp.json()
            if data.get("state") == "ended" and data.get("result"):
                log.info(
                    "Reconciler recovered ended call %s (%s/%s)",
                    rid,
                    row["campaign_id"],
                    row["lead_id"],
                )
                process_call_completion(db, data["result"], retry_policy)
                wake_event.set()
            elif data.get("state") == "active" and row["state"] == "INTENT":
                log.info("Reconciler confirmed active call %s", rid)
                on_call_accepted_201(db, rid, data["call_sid"])
        else:
            log.warning(
                "Reconciler unexpected status %s for request_id=%s",
                resp.status_code,
                rid,
            )

    async def reconcile_pending(client: httpx.AsyncClient, startup: bool = False):
        pending = get_pending_calls(db)
        if not pending:
            return

        if startup:
            log.info("Startup reconciliation checking %d INTENT/ACTIVE call(s)", len(pending))

        await asyncio.gather(
            *(reconcile_one(client, row, startup) for row in pending),
            return_exceptions=True,
        )

    async def place_call_task(client: httpx.AsyncClient, lead: dict):
        rid = lead["request_id"]
        payload = {
            "request_id": rid,
            "campaign_id": lead["campaign_id"],
            "lead_id": lead["lead_id"],
            "phone": lead["phone"],
            "callback_url": callback_url,
        }
        try:
            resp = await client.post(f"{provider_url}/v1/calls", json=payload, timeout=10.0)
            if resp.status_code in (200, 201):
                data = resp.json()
                if data.get("state") == "ended" and data.get("result"):
                    process_call_completion(db, data["result"], retry_policy)
                else:
                    on_call_accepted_201(db, rid, data["call_sid"])
            elif resp.status_code == 503:
                log.info("Provider 503 for %s/%s (%s); reverting", lead["campaign_id"], lead["lead_id"], rid)
                on_call_aborted_not_placed(db, rid)
                await asyncio.sleep(0.05)
            elif resp.status_code == 504:
                log.warning("Provider 504 (ambiguous) for %s/%s (%s); deferring to reconciler", lead["campaign_id"], lead["lead_id"], rid)
                reconcile_event.set()
            elif resp.status_code in (409, 422, 429):
                log.error(
                    "Provider rule rejection %s for %s/%s (%s): %s",
                    resp.status_code,
                    lead["campaign_id"],
                    lead["lead_id"],
                    rid,
                    resp.text,
                )
                on_call_aborted_not_placed(db, rid)
                await asyncio.sleep(0.1)
            else:
                log.warning("Unexpected POST /v1/calls status %s for %s", resp.status_code, rid)
                reconcile_event.set()
        except httpx.HTTPError as exc:
            log.warning("HTTP error during POST /v1/calls (%s): %s; deferring to reconciler", rid, exc)
            reconcile_event.set()
        finally:
            in_flight_request_ids.discard(rid)
            wake_event.set()

    async def reconciler_loop(client: httpx.AsyncClient):
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(reconcile_event.wait(), timeout=0.3)
                reconcile_event.clear()
            except asyncio.TimeoutError:
                pass
            if stop_event.is_set():
                break
            await reconcile_pending(client, startup=False)

    async def dispatcher_loop(client: httpx.AsyncClient):
        # Allow uvicorn to finish binding the webhook port after lifespan yield
        await asyncio.sleep(0.1)
        last_clock_sync = time.time()
        while not stop_event.is_set():
            if time.time() - last_clock_sync >= 5.0:
                if await sync_clock(client, retries=1):
                    last_clock_sync = time.time()

            wake_event.clear()

            if all_leads_terminal(db):
                count = export_final_state(db, output_path)
                log.info("All %d leads reached terminal states. Wrote %s.", count, output_path)
                stop_event.set()
                reconcile_event.set()
                srv = server_ref.get("server")
                if srv is not None:
                    srv.should_exit = True
                break

            safety_buf = compute_safety_buffer(clock.speed, grace_seconds)

            while not stop_event.is_set():
                sim_dt = clock.now_dt()
                sim_ts = clock.now_ts()
                open_cids = [
                    c["campaign_id"]
                    for c in campaigns
                    if is_within_calling_window(
                        sim_dt,
                        c["window"]["start"],
                        c["window"]["end"],
                        c["window"]["days"],
                        safety_buffer_sec=safety_buf,
                        regulatory_start_str=reg_start,
                        regulatory_end_str=reg_end,
                    )
                ]
                lead = claim_next_eligible_lead(db, sim_ts, open_cids)
                if not lead:
                    break
                in_flight_request_ids.add(lead["request_id"])
                task = asyncio.create_task(place_call_task(client, lead))
                in_flight_tasks.add(task)
                task.add_done_callback(in_flight_tasks.discard)

            # Compute adaptive sleep until next due retry/callback or window opening
            sim_dt = clock.now_dt()
            sim_ts = clock.now_ts()
            open_cids = [
                c["campaign_id"]
                for c in campaigns
                if is_within_calling_window(
                    sim_dt,
                    c["window"]["start"],
                    c["window"]["end"],
                    c["window"]["days"],
                    safety_buffer_sec=safety_buf,
                    regulatory_start_str=reg_start,
                    regulatory_end_str=reg_end,
                )
            ]
            next_targets: list[float] = []
            earliest_wait = get_earliest_waiting_ts(db, open_cids)
            if earliest_wait is not None:
                next_targets.append(earliest_wait)

            if not open_cids:
                for c in campaigns:
                    nxt = next_window_open_ts(
                        sim_dt,
                        c["window"]["start"],
                        c["window"]["end"],
                        c["window"]["days"],
                        regulatory_start_str=reg_start,
                        regulatory_end_str=reg_end,
                    )
                    if nxt is not None and nxt > sim_ts:
                        next_targets.append(nxt)

            if next_targets:
                sim_delta = max(0.0, min(next_targets) - sim_ts)
                real_sleep = max(0.02, min(0.25, sim_delta / max(clock.speed, 1.0)))
            else:
                real_sleep = 0.15

            try:
                await asyncio.wait_for(wake_event.wait(), timeout=real_sleep)
            except asyncio.TimeoutError:
                pass

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
        async with httpx.AsyncClient(limits=limits) as client:
            synced = await sync_clock(client, retries=30)
            if not synced:
                raise RuntimeError(f"Could not connect to mock provider at {provider_url}/v1/clock")
            log.info("Synced clock: speed=%.1fx, sim_now=%s", clock.speed, clock.now_dt().isoformat())

            await reconcile_pending(client, startup=True)

            rec_task = asyncio.create_task(reconciler_loop(client))
            disp_task = asyncio.create_task(dispatcher_loop(client))
            try:
                yield
            finally:
                stop_event.set()
                wake_event.set()
                reconcile_event.set()
                for t in list(in_flight_tasks):
                    t.cancel()
                rec_task.cancel()
                disp_task.cancel()
                await asyncio.gather(rec_task, disp_task, *in_flight_tasks, return_exceptions=True)

    app = FastAPI(
        title="Durable Campaign Dispatcher",
        version="1.0",
        lifespan=lifespan if enable_background else None,
    )

    @app.post("/webhooks/telephony")
    async def receive_webhook(request: Request):
        raw_body = await request.body()
        if webhook_secret:
            sig = request.headers.get("X-AU-Signature", "")
            expected = hmac.new(
                webhook_secret.encode("utf-8"), raw_body, hashlib.sha256
            ).hexdigest()
            if not sig or not hmac.compare_digest(expected, sig):
                raise HTTPException(status_code=401, detail="Invalid X-AU-Signature")

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON") from exc

        process_call_completion(db, payload, retry_policy)
        wake_event.set()
        return {"ok": True}

    @app.get("/status")
    async def status():
        summary = get_status_summary(db)
        summary["sim_now"] = clock.now_dt().isoformat() if clock.real_epoch else None
        summary["speed"] = clock.speed
        return summary

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


def parse_args(argv: list[str] | None = None):
    ap = argparse.ArgumentParser(description="Assignment B: Durable Campaign Dispatcher")
    ap.add_argument("--fixtures", default=DEFAULT_FIXTURES, help="Path to campaigns.json")
    ap.add_argument("--db", default="dispatcher.db", help="Path to SQLite database file")
    ap.add_argument("--output", default="final_state.csv", help="Path to write final_state.csv")
    ap.add_argument("--provider-url", default="http://127.0.0.1:8099", help="Mock telephony base URL")
    ap.add_argument("--host", default="127.0.0.1", help="Webhook server bind host")
    ap.add_argument("--port", type=int, default=8200, help="Webhook server bind port")
    ap.add_argument(
        "--callback-url",
        default=None,
        help="Override callback URL sent to provider (defaults to http://127.0.0.1:<port>/webhooks/telephony)",
    )
    return ap.parse_args(argv)


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    cb_host = "127.0.0.1" if args.host == "0.0.0.0" else args.host
    callback_url = args.callback_url or f"http://{cb_host}:{args.port}/webhooks/telephony"

    db = Database(args.db)
    try:
        cfg = load_fixtures(db, args.fixtures)
        server_ref: dict = {}
        app = build_app(
            db=db,
            cfg=cfg,
            provider_url=args.provider_url.rstrip("/"),
            callback_url=callback_url,
            output_path=args.output,
            server_ref=server_ref,
        )
        config = uvicorn.Config(
            app,
            host=args.host,
            port=args.port,
            log_level="warning",
        )
        server = uvicorn.Server(config)
        server_ref["server"] = server
        server.run()
    finally:
        db.close()


if __name__ == "__main__":
    main()

