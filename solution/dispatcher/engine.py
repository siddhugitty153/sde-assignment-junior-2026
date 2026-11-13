import os
import time
import uuid
from datetime import datetime

TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}
RETRY_60M_STATUSES = {"no_answer", "busy"}
RETRY_15M_STATUSES = {"failed"}
DEFAULT_RETRY_POLICY = {
    "no_answer_minutes": 60,
    "busy_minutes": 60,
    "failed_minutes": 15,
}
SAFETY_BUFFER_SIM_SEC = 2.0


def claim_next_eligible_lead(db, sim_ts: float, open_campaign_ids: list[str]) -> dict | None:
    """
    Atomically selects and claims the highest-priority eligible lead across open
    campaigns while enforcing:
      - R2: tenant concurrency (counting both INTENT and ACTIVE calls)
      - R3: at most one INTENT/ACTIVE call per phone across all campaigns
      - R4: attempts < max_attempts
      - R6: CALLBACK_WAIT > RETRY_WAIT > PENDING, then next_eligible_sim ASC
      - R8: phone not in dnd_registry
    """
    if not open_campaign_ids:
        return None

    placeholders = ",".join("?" for _ in open_campaign_ids)
    with db.transaction() as conn:
        tenants = conn.execute("""
            SELECT t.tenant_id, t.max_concurrent_calls,
                   COUNT(ca.request_id) AS active_count
            FROM tenants t
            LEFT JOIN call_attempts ca
              ON ca.tenant_id = t.tenant_id AND ca.state IN ('INTENT', 'ACTIVE')
            GROUP BY t.tenant_id
            HAVING active_count < t.max_concurrent_calls
        """).fetchall()

        available_tenants = [r["tenant_id"] for r in tenants]
        if not available_tenants:
            return None

        t_placeholders = ",".join("?" for _ in available_tenants)
        query = f"""
            SELECT l.*, c.max_attempts
            FROM leads l
            JOIN campaigns c ON c.campaign_id = l.campaign_id
            WHERE l.state IN ('CALLBACK_WAIT', 'RETRY_WAIT', 'PENDING')
              AND l.next_eligible_sim <= ?
              AND l.campaign_id IN ({placeholders})
              AND l.tenant_id IN ({t_placeholders})
              AND l.attempts < c.max_attempts
              AND l.phone NOT IN (SELECT phone FROM dnd_registry)
              AND l.phone NOT IN (
                  SELECT phone FROM call_attempts WHERE state IN ('INTENT', 'ACTIVE')
              )
            ORDER BY
              CASE l.state
                  WHEN 'CALLBACK_WAIT' THEN 1
                  WHEN 'RETRY_WAIT'    THEN 2
                  WHEN 'PENDING'       THEN 3
              END ASC,
              l.next_eligible_sim ASC,
              l.campaign_id ASC,
              l.lead_id ASC
            LIMIT 1
        """
        params = [sim_ts, *open_campaign_ids, *available_tenants]
        lead = conn.execute(query, params).fetchone()
        if not lead:
            return None

        request_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO call_attempts (
                request_id, campaign_id, lead_id, tenant_id, phone,
                state, counted_attempt, in_flight_pid, created_real_ts
            ) VALUES (?, ?, ?, ?, ?, 'INTENT', 0, ?, ?)
            """,
            (
                request_id,
                lead["campaign_id"],
                lead["lead_id"],
                lead["tenant_id"],
                lead["phone"],
                os.getpid(),
                time.time(),
            ),
        )

        conn.execute(
            """
            UPDATE leads
            SET prior_wait_state = state,
                state = 'DIALING_INTENT',
                active_request_id = ?
            WHERE campaign_id = ? AND lead_id = ?
            """,
            (request_id, lead["campaign_id"], lead["lead_id"]),
        )

        return {
            "request_id": request_id,
            "campaign_id": lead["campaign_id"],
            "lead_id": lead["lead_id"],
            "phone": lead["phone"],
            "tenant_id": lead["tenant_id"],
        }


def on_call_accepted_201(db, request_id: str, call_sid: str):
    """
    Transitions an INTENT call to ACTIVE when POST /v1/calls returns 201/200
    or GET /v1/calls reports state='active'. Idempotent and safe if a webhook
    already marked the call ENDED.
    """
    with db.transaction() as conn:
        att = conn.execute(
            "SELECT * FROM call_attempts WHERE request_id = ?", (request_id,)
        ).fetchone()
        if not att or att["state"] in ("ENDED", "ABORTED"):
            return

        if att["counted_attempt"] == 0:
            conn.execute(
                """
                UPDATE leads SET attempts = attempts + 1
                WHERE campaign_id = ? AND lead_id = ?
                """,
                (att["campaign_id"], att["lead_id"]),
            )

        conn.execute(
            """
            UPDATE call_attempts
            SET state = 'ACTIVE',
                call_sid = COALESCE(?, call_sid),
                counted_attempt = 1
            WHERE request_id = ?
            """,
            (call_sid, request_id),
        )

        conn.execute(
            """
            UPDATE leads
            SET state = CASE WHEN state = 'DIALING_INTENT' THEN 'ACTIVE' ELSE state END
            WHERE campaign_id = ? AND lead_id = ? AND active_request_id = ?
            """,
            (att["campaign_id"], att["lead_id"], request_id),
        )


def on_call_aborted_not_placed(db, request_id: str):
    """
    Marks an INTENT call ABORTED (on 503, 4xx rejection, or Reconciler 404)
    without incrementing attempts, reverting the lead to prior_wait_state
    unless it was globally DND_BLOCKED in the meantime.
    """
    with db.transaction() as conn:
        att = conn.execute(
            "SELECT * FROM call_attempts WHERE request_id = ?", (request_id,)
        ).fetchone()
        if not att or att["state"] != "INTENT":
            return

        conn.execute(
            "UPDATE call_attempts SET state = 'ABORTED' WHERE request_id = ?",
            (request_id,),
        )
        conn.execute(
            """
            UPDATE leads
            SET state = CASE WHEN state = 'DND_BLOCKED' THEN 'DND_BLOCKED' ELSE prior_wait_state END,
                active_request_id = NULL
            WHERE campaign_id = ? AND lead_id = ? AND active_request_id = ?
            """,
            (att["campaign_id"], att["lead_id"], request_id),
        )


def process_call_completion(db, payload: dict, retry_policy: dict | None = None):
    """
    Idempotently processes an ended call from either POST /webhooks/telephony
    or GET /v1/calls?request_id=... reconciliation. Enforces R4-R9.
    """
    policy = retry_policy or DEFAULT_RETRY_POLICY
    request_id = payload["request_id"]
    call_sid = payload.get("call_sid")
    status = payload["status"]
    disposition = payload.get("disposition") or ""
    ended_at_str = payload["ended_at"]
    ended_at_ts = datetime.fromisoformat(ended_at_str).timestamp()
    callback_at_str = payload.get("callback_at")

    with db.transaction() as conn:
        att = conn.execute(
            "SELECT * FROM call_attempts WHERE request_id = ?", (request_id,)
        ).fetchone()
        if not att or att["state"] == "ENDED":
            return

        campaign_id = att["campaign_id"]
        lead_id = att["lead_id"]
        phone = att["phone"]

        if att["counted_attempt"] == 0:
            conn.execute(
                """
                UPDATE leads SET attempts = attempts + 1
                WHERE campaign_id = ? AND lead_id = ?
                """,
                (campaign_id, lead_id),
            )

        conn.execute(
            """
            UPDATE call_attempts
            SET state = 'ENDED',
                call_sid = COALESCE(?, call_sid),
                counted_attempt = 1,
                status = ?,
                disposition = ?,
                ended_at_sim = ?
            WHERE request_id = ?
            """,
            (call_sid, status, disposition, ended_at_str, request_id),
        )

        lead = conn.execute(
            """
            SELECT l.*, c.max_attempts
            FROM leads l JOIN campaigns c ON c.campaign_id = l.campaign_id
            WHERE l.campaign_id = ? AND l.lead_id = ?
            """,
            (campaign_id, lead_id),
        ).fetchone()

        attempts_used = lead["attempts"]
        max_attempts = lead["max_attempts"]

        if ended_at_ts >= (lead["last_ended_at_sim"] or 0.0):
            conn.execute(
                """
                UPDATE leads
                SET last_status = ?, last_disposition = ?, last_ended_at_sim = ?
                WHERE campaign_id = ? AND lead_id = ?
                """,
                (status, disposition, ended_at_ts, campaign_id, lead_id),
            )

        if status == "completed" and disposition in TERMINAL_DISPOSITIONS:
            next_state = "COMPLETED"
            next_elig = 0.0
        elif lead["state"] == "DND_BLOCKED" or conn.execute(
            "SELECT 1 FROM dnd_registry WHERE phone = ?", (phone,)
        ).fetchone():
            next_state = "EXHAUSTED" if attempts_used >= max_attempts else "DND_BLOCKED"
            next_elig = 0.0
        elif attempts_used >= max_attempts:
            next_state = "EXHAUSTED"
            next_elig = 0.0
        elif status == "completed" and disposition == "callback_requested" and callback_at_str:
            next_state = "CALLBACK_WAIT"
            next_elig = datetime.fromisoformat(callback_at_str).timestamp() + SAFETY_BUFFER_SIM_SEC
        elif status == "no_answer":
            next_state = "RETRY_WAIT"
            mins = float(policy.get("no_answer_minutes", 60))
            next_elig = ended_at_ts + (mins * 60.0) + SAFETY_BUFFER_SIM_SEC
        elif status == "busy":
            next_state = "RETRY_WAIT"
            mins = float(policy.get("busy_minutes", 60))
            next_elig = ended_at_ts + (mins * 60.0) + SAFETY_BUFFER_SIM_SEC
        elif status == "failed":
            next_state = "RETRY_WAIT"
            mins = float(policy.get("failed_minutes", 15))
            next_elig = ended_at_ts + (mins * 60.0) + SAFETY_BUFFER_SIM_SEC
        else:
            next_state = "RETRY_WAIT" if attempts_used < max_attempts else "EXHAUSTED"
            mins = float(policy.get("failed_minutes", 15))
            next_elig = ended_at_ts + (mins * 60.0) + SAFETY_BUFFER_SIM_SEC

        conn.execute(
            """
            UPDATE leads
            SET state = ?,
                next_eligible_sim = ?,
                active_request_id = CASE WHEN active_request_id = ? THEN NULL ELSE active_request_id END
            WHERE campaign_id = ? AND lead_id = ?
            """,
            (next_state, next_elig, request_id, campaign_id, lead_id),
        )

        if status == "completed" and disposition == "dnd":
            conn.execute(
                "INSERT OR IGNORE INTO dnd_registry (phone, recorded_at_sim) VALUES (?, ?)",
                (phone, ended_at_str),
            )
            conn.execute(
                """
                UPDATE leads
                SET state = 'DND_BLOCKED', active_request_id = NULL
                WHERE phone = ?
                  AND NOT (campaign_id = ? AND lead_id = ?)
                  AND state NOT IN ('COMPLETED', 'EXHAUSTED')
                """,
                (phone, campaign_id, lead_id),
            )


def get_pending_calls(db) -> list[dict]:
    """Returns all call_attempts currently in INTENT or ACTIVE state."""
    with db.transaction() as conn:
        rows = conn.execute(
            """
            SELECT request_id, call_sid, campaign_id, lead_id, state, created_real_ts
            FROM call_attempts
            WHERE state IN ('INTENT', 'ACTIVE')
            ORDER BY created_real_ts ASC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def get_earliest_waiting_ts(db, open_campaign_ids: list[str]) -> float | None:
    """
    Returns the earliest next_eligible_sim among CALLBACK_WAIT / RETRY_WAIT leads
    in currently open campaigns, or None if none exist.
    """
    if not open_campaign_ids:
        return None
    placeholders = ",".join("?" for _ in open_campaign_ids)
    with db.transaction() as conn:
        row = conn.execute(
            f"""
            SELECT MIN(l.next_eligible_sim) AS min_ts
            FROM leads l
            JOIN campaigns c ON c.campaign_id = l.campaign_id
            WHERE l.state IN ('CALLBACK_WAIT', 'RETRY_WAIT')
              AND l.campaign_id IN ({placeholders})
              AND l.attempts < c.max_attempts
              AND l.phone NOT IN (SELECT phone FROM dnd_registry)
            """,
            open_campaign_ids,
        ).fetchone()
        return row["min_ts"] if row and row["min_ts"] is not None else None


def all_leads_terminal(db) -> bool:
    """True when every lead is COMPLETED/EXHAUSTED/DND_BLOCKED and no calls are INTENT/ACTIVE."""
    with db.transaction() as conn:
        non_term = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM leads
            WHERE state NOT IN ('COMPLETED', 'EXHAUSTED', 'DND_BLOCKED')
            """
        ).fetchone()["cnt"]
        in_prog = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM call_attempts
            WHERE state IN ('INTENT', 'ACTIVE')
            """
        ).fetchone()["cnt"]
        return non_term == 0 and in_prog == 0


def get_status_summary(db) -> dict:
    """Returns per-campaign progress counts for observability (/status)."""
    with db.transaction() as conn:
        rows = conn.execute(
            """
            SELECT campaign_id, state, COUNT(*) AS cnt
            FROM leads
            GROUP BY campaign_id, state
            ORDER BY campaign_id, state
            """
        ).fetchall()
        active_calls = conn.execute(
            "SELECT COUNT(*) AS cnt FROM call_attempts WHERE state IN ('INTENT', 'ACTIVE')"
        ).fetchone()["cnt"]
        dnd_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM dnd_registry"
        ).fetchone()["cnt"]

    campaigns: dict[str, dict[str, int]] = {}
    for r in rows:
        cid = r["campaign_id"]
        campaigns.setdefault(cid, {})[r["state"].lower()] = r["cnt"]

    return {
        "active_or_intent_calls": active_calls,
        "dnd_phones": dnd_count,
        "campaigns": campaigns,
    }

