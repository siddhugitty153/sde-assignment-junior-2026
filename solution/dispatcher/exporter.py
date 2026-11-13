import csv
import os


def export_final_state(db, output_path: str) -> int:
    """
    Writes final_state.csv sorted by (campaign_id, lead_id) matching the
    Assignment B output contract. Returns the number of rows written.
    """
    with db.transaction() as conn:
        rows = conn.execute(
            """
            SELECT campaign_id, lead_id, LOWER(state) AS final_state,
                   attempts, last_status, last_disposition
            FROM leads
            ORDER BY campaign_id ASC, lead_id ASC
            """
        ).fetchall()

    out_dir = os.path.dirname(os.path.abspath(output_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    tmp_path = f"{output_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "campaign_id",
                "lead_id",
                "final_state",
                "attempts",
                "last_status",
                "last_disposition",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r["campaign_id"],
                    r["lead_id"],
                    r["final_state"],
                    r["attempts"],
                    r["last_status"] or "",
                    r["last_disposition"] or "",
                ]
            )
    os.replace(tmp_path, output_path)
    return len(rows)

