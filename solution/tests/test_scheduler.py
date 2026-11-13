import csv
import hashlib
import hmac
import json
from datetime import datetime, timedelta

from dispatcher.clock import IST, SimClock, is_within_calling_window, next_window_open_ts
from dispatcher.db import Database, load_fixtures
from dispatcher.engine import (
    all_leads_terminal,
    claim_next_eligible_lead,
    on_call_aborted_not_placed,
    on_call_accepted_201,
    process_call_completion,
)
from dispatcher.exporter import export_final_state
from dispatcher.main import build_app


def _seed_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls, webhook_secret) VALUES (?, ?, ?)",
            ("t1", 2, "secret"),
        )
        conn.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls, webhook_secret) VALUES (?, ?, ?)",
            ("t2", 2, "secret"),
        )
        conn.execute(
            """INSERT INTO campaigns (campaign_id, tenant_id, max_attempts, window_start, window_end, allowed_days, webhook_secret)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("c1", "t1", 3, "10:00", "19:00", '["mon","tue","wed","thu","fri","sat"]', "secret"),
        )
        conn.execute(
            """INSERT INTO campaigns (campaign_id, tenant_id, max_attempts, window_start, window_end, allowed_days, webhook_secret)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("c2", "t1", 3, "11:00", "20:30", '["mon","tue","wed","thu","fri","sat","sun"]', "secret"),
        )
        conn.execute(
            """INSERT INTO campaigns (campaign_id, tenant_id, max_attempts, window_start, window_end, allowed_days, webhook_secret)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("c3", "t2", 3, "09:30", "18:00", '["mon","tue","wed","thu","fri"]', "secret"),
        )
    return db


def test_window_math_and_safety_buffer():
    # Thu 2026-11-12
    dt_open = datetime(2026, 11, 12, 17, 30, 0, tzinfo=IST)
    assert is_within_calling_window(dt_open, "10:00", "19:00", ["thu", "fri"], safety_buffer_sec=45)

    # Just before close: 18:59:20 + 45s = 19:00:05 -> rejected
    dt_near_close = datetime(2026, 11, 12, 18, 59, 20, tzinfo=IST)
    assert not is_within_calling_window(dt_near_close, "10:00", "19:00", ["thu"], safety_buffer_sec=45)

    # 18:59:10 + 45s = 18:59:55 -> allowed
    dt_ok_close = datetime(2026, 11, 12, 18, 59, 10, tzinfo=IST)
    assert is_within_calling_window(dt_ok_close, "10:00", "19:00", ["thu"], safety_buffer_sec=45)

    # Regulatory window cap [09:00, 21:00) overrides wider campaign window
    dt_early = datetime(2026, 11, 12, 8, 45, 0, tzinfo=IST)
    dt_late = datetime(2026, 11, 12, 20, 59, 30, tzinfo=IST)
    assert not is_within_calling_window(dt_early, "08:00", "22:00", ["thu"], safety_buffer_sec=45)
    assert not is_within_calling_window(dt_late, "08:00", "22:00", ["thu"], safety_buffer_sec=45)

    # Disallowed day (Sunday 2026-11-15)
    dt_sun = datetime(2026, 11, 15, 12, 0, 0, tzinfo=IST)
    assert not is_within_calling_window(dt_sun, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"])


def test_next_window_open_across_weekend_and_sim_clock():
    # Friday 18:05 IST for mon-fri 09:30-18:00 -> Monday 09:30 IST
    fri_after = datetime(2026, 11, 13, 18, 5, 0, tzinfo=IST)
    nxt = next_window_open_ts(fri_after, "09:30", "18:00", ["mon", "tue", "wed", "thu", "fri"])
    assert nxt == datetime(2026, 11, 16, 9, 30, 0, tzinfo=IST).timestamp()

    clock = SimClock()
    clock.update_from_provider({
        "sim_epoch": "2026-11-12T17:30:00+05:30",
        "real_epoch": 1000.0,
        "speed": 600.0,
    })
    assert clock.sim_epoch == datetime(2026, 11, 12, 17, 30, 0, tzinfo=IST).timestamp()
    assert clock.speed == 600.0


def test_tenant_concurrency_and_one_call_per_phone(tmp_path):
    db = _seed_db(tmp_path)
    try:
        with db.transaction() as conn:
            conn.executemany(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone) VALUES (?, ?, ?, ?)",
                [
                    ("c1", "l1", "t1", "+919000000001"),
                    ("c2", "l2", "t1", "+919000000002"),
                    ("c1", "l3", "t1", "+919000000003"),
                    ("c3", "l4", "t2", "+919000000001"),  # same phone as c1/l1!
                ],
            )

        now_ts = datetime(2026, 11, 12, 17, 30, 0, tzinfo=IST).timestamp()

        # Claim 1: c1/l1 (+919000000001)
        a1 = claim_next_eligible_lead(db, now_ts, ["c1", "c2", "c3"])
        assert a1["lead_id"] == "l1"

        # Same phone in c3/l4 must NOT be claimed while c1/l1 is INTENT
        a_c3_only = claim_next_eligible_lead(db, now_ts, ["c3"])
        assert a_c3_only is None

        # Claim 2: c2/l2 (t1 now at 2/2 concurrency across c1+c2)
        a2 = claim_next_eligible_lead(db, now_ts, ["c2"])
        assert a2["lead_id"] == "l2"

        # Claim 3 for t1 (c1/l3) must be blocked by max_concurrent_calls=2
        assert claim_next_eligible_lead(db, now_ts, ["c1", "c2"]) is None

        # Abort a2 (503): frees slot without incrementing attempts
        on_call_aborted_not_placed(db, a2["request_id"])
        a2_retry = claim_next_eligible_lead(db, now_ts, ["c2"])
        assert a2_retry["lead_id"] == "l2"
        with db.transaction() as conn:
            row = conn.execute("SELECT attempts FROM leads WHERE lead_id='l2'").fetchone()
            assert row["attempts"] == 0
    finally:
        db.close()


def test_priority_ordering_retry_and_callback_math(tmp_path):
    db = _seed_db(tmp_path)
    try:
        t0 = datetime(2026, 11, 12, 10, 0, 0, tzinfo=IST)
        t0_ts = t0.timestamp()
        with db.transaction() as conn:
            conn.executemany(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone) VALUES (?, ?, ?, ?)",
                [
                    ("c1", "l_fresh", "t1", "+919000000010"),
                    ("c1", "l_retry", "t1", "+919000000011"),
                    ("c1", "l_cb_late", "t1", "+919000000012"),
                    ("c1", "l_cb_early", "t1", "+919000000013"),
                ],
            )
            # Mark l_retry as failed at 10:00 -> eligible at 10:15:02
            conn.execute(
                """INSERT INTO call_attempts (request_id, campaign_id, lead_id, tenant_id, phone, state, counted_attempt, created_real_ts)
                   VALUES ('r_retry', 'c1', 'l_retry', 't1', '+919000000011', 'ACTIVE', 1, 1.0)"""
            )
            conn.execute("UPDATE leads SET state='ACTIVE', attempts=1 WHERE lead_id='l_retry'")
            # Mark callbacks
            conn.execute(
                """INSERT INTO call_attempts (request_id, campaign_id, lead_id, tenant_id, phone, state, counted_attempt, created_real_ts)
                   VALUES ('r_cb1', 'c1', 'l_cb_late', 't1', '+919000000012', 'ACTIVE', 1, 1.0),
                          ('r_cb2', 'c1', 'l_cb_early', 't1', '+919000000013', 'ACTIVE', 1, 1.0)"""
            )
            conn.execute("UPDATE leads SET state='ACTIVE', attempts=1 WHERE lead_id IN ('l_cb_late', 'l_cb_early')")

        process_call_completion(db, {
            "request_id": "r_retry",
            "call_sid": "CA1",
            "status": "failed",
            "disposition": None,
            "ended_at": t0.isoformat(),
        })
        process_call_completion(db, {
            "request_id": "r_cb1",
            "call_sid": "CA2",
            "status": "completed",
            "disposition": "callback_requested",
            "callback_at": (t0 + timedelta(minutes=30)).isoformat(),
            "ended_at": t0.isoformat(),
        })
        process_call_completion(db, {
            "request_id": "r_cb2",
            "call_sid": "CA3",
            "status": "completed",
            "disposition": "callback_requested",
            "callback_at": (t0 + timedelta(minutes=20)).isoformat(),
            "ended_at": t0.isoformat(),
        })

        # At 10:15:00 -> retry is NOT due yet (+2s safety buffer), so fresh is picked
        # First verify retry ineligible at +15m exact
        # At 10:35:00 -> both callbacks and retry and fresh are eligible; order must be cb_early -> cb_late -> retry -> fresh
        t_due = (t0 + timedelta(minutes=35)).timestamp()
        p1 = claim_next_eligible_lead(db, t_due, ["c1"])
        assert p1["lead_id"] == "l_cb_early"
        p2 = claim_next_eligible_lead(db, t_due, ["c1"])
        assert p2["lead_id"] == "l_cb_late"
        # Free slots
        on_call_aborted_not_placed(db, p1["request_id"])
        on_call_aborted_not_placed(db, p2["request_id"])
        # Mark callbacks completed so we can observe retry > fresh
        with db.transaction() as conn:
            conn.execute("UPDATE leads SET state='COMPLETED' WHERE lead_id IN ('l_cb_early', 'l_cb_late')")

        p3 = claim_next_eligible_lead(db, t_due, ["c1"])
        assert p3["lead_id"] == "l_retry"
        p4 = claim_next_eligible_lead(db, t_due, ["c1"])
        assert p4["lead_id"] == "l_fresh"

        # Check 60m retry math for no_answer
        on_call_accepted_201(db, p4["request_id"], "CA4")
        process_call_completion(db, {
            "request_id": p4["request_id"],
            "call_sid": "CA4",
            "status": "no_answer",
            "disposition": None,
            "ended_at": t0.isoformat(),
        })
        with db.transaction() as conn:
            elig = conn.execute("SELECT next_eligible_sim FROM leads WHERE lead_id='l_fresh'").fetchone()["next_eligible_sim"]
        assert elig == t0_ts + 3600.0 + 2.0
    finally:
        db.close()


def test_webhook_before_201_duplicate_and_global_dnd_precedence(tmp_path):
    db = _seed_db(tmp_path)
    try:
        t0 = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)
        with db.transaction() as conn:
            conn.executemany(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("c1", "l_dnd", "t1", "+919000000099", "PENDING", 0),
                    ("c2", "l_other_pending", "t1", "+919000000099", "PENDING", 0),
                    ("c3", "l_other_exhausted", "t2", "+919000000099", "EXHAUSTED", 3),
                ],
            )

        claim = claim_next_eligible_lead(db, t0.timestamp(), ["c1"])
        rid = claim["request_id"]

        # Trap B: Webhook arrives BEFORE 201!
        payload = {
            "request_id": rid,
            "call_sid": "CA_FAST",
            "status": "completed",
            "disposition": "dnd",
            "ended_at": t0.isoformat(),
        }
        process_call_completion(db, payload)
        # Duplicate webhook arrives
        process_call_completion(db, payload)
        # Delayed 201 returns afterwards
        on_call_accepted_201(db, rid, "CA_FAST")

        with db.transaction() as conn:
            l_dnd = conn.execute("SELECT * FROM leads WHERE campaign_id='c1' AND lead_id='l_dnd'").fetchone()
            l_pend = conn.execute("SELECT * FROM leads WHERE campaign_id='c2' AND lead_id='l_other_pending'").fetchone()
            l_exh = conn.execute("SELECT * FROM leads WHERE campaign_id='c3' AND lead_id='l_other_exhausted'").fetchone()

        assert l_dnd["state"] == "COMPLETED"
        assert l_dnd["attempts"] == 1
        assert l_pend["state"] == "DND_BLOCKED"
        assert l_pend["attempts"] == 0
        # R9 precedence: EXHAUSTED must not be overwritten by DND_BLOCKED
        assert l_exh["state"] == "EXHAUSTED"
        assert all_leads_terminal(db)

        out_csv = str(tmp_path / "final_state.csv")
        export_final_state(db, out_csv)
        with open(out_csv, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert [r["campaign_id"] for r in rows] == ["c1", "c2", "c3"]
        assert rows[0]["final_state"] == "completed"
        assert rows[0]["last_disposition"] == "dnd"
        assert rows[1]["final_state"] == "dnd_blocked"
        assert rows[1]["last_status"] == ""
        assert rows[2]["final_state"] == "exhausted"
    finally:
        db.close()


def test_webhook_hmac_verification(tmp_path):
    import asyncio
    import httpx

    db = _seed_db(tmp_path)
    try:
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone) VALUES ('c1', 'l1', 't1', '+919000000055')"
            )
        t0 = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)
        claim = claim_next_eligible_lead(db, t0.timestamp(), ["c1"])
        cfg = {"webhook_secret": "my_secret", "campaigns": [], "regulatory_window": {"start": "09:00", "end": "21:00"}}
        app = build_app(
            db,
            cfg,
            "http://127.0.0.1:8099",
            "http://127.0.0.1:8200/webhooks/telephony",
            str(tmp_path / "out.csv"),
            {},
            enable_background=False,
        )
        body = json.dumps({
            "request_id": claim["request_id"],
            "call_sid": "CA_WH",
            "status": "completed",
            "disposition": "interested",
            "ended_at": t0.isoformat(),
        }).encode()
        good_sig = hmac.new(b"my_secret", body, hashlib.sha256).hexdigest()

        async def _send():
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                bad_resp = await client.post(
                    "/webhooks/telephony", content=body, headers={"X-AU-Signature": "deadbeef"}
                )
                ok_resp = await client.post(
                    "/webhooks/telephony", content=body, headers={"X-AU-Signature": good_sig}
                )
                return bad_resp.status_code, ok_resp.status_code

        bad_code, ok_code = asyncio.run(_send())
        assert bad_code == 401
        assert ok_code == 200
        assert all_leads_terminal(db)
    finally:
        db.close()


def test_idempotent_fixture_loader(tmp_path):
    db_path = str(tmp_path / "fixtures_test.db")
    db = Database(db_path)
    try:
        fixtures = "assignment-b-workflow/fixtures/campaigns.json"
        cfg = load_fixtures(db, fixtures)
        assert len(cfg["campaigns"]) == 3
        with db.transaction() as conn:
            total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            assert total == 200
            conn.execute("UPDATE leads SET state='COMPLETED', attempts=1 WHERE lead_id='fb_0001'")

        # Re-loading fixtures on crash restart must preserve existing lead state
        load_fixtures(db, fixtures)
        with db.transaction() as conn:
            row = conn.execute("SELECT state, attempts FROM leads WHERE lead_id='fb_0001'").fetchone()
            assert row["state"] == "COMPLETED"
            assert row["attempts"] == 1
    finally:
        db.close()

