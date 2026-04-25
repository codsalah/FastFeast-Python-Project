"""
loaders/fact_events_loader.py
─────────────────────────────
Loads micro-batch ticket event records into warehouse.fact_ticket_events.

Pipeline contract (WAP pattern):
  1. Idempotency check   — skip if file already processed
  2. Read + validate     — quarantine schema-invalid records
  3. Deduplicate         — drop duplicate event_ids within the file
  4. Resolve surrogates  — ticket_id → ticket_key, agent_id → agent_key,
                           event_ts  → date_key
  5. Quarantine unresolvables — events whose ticket_key / agent_key / date_key
                                 can't be resolved (referential_integrity)
  6. Insert into warehouse
  7. Mark file done

"""
from collections import defaultdict

import pandas as pd

from alerting.alert_service import send_alert
from handlers.quarantine_handler import send_batch_to_quarantine
from utils.file_tracker import (
    compute_file_hash,
    is_file_processed,
    mark_file_failed,
    mark_file_success,
    register_file
)
from utils.logger import (
    get_logger_name,
    log_record_rejected,
    log_stage_complete,
    log_stage_start
)
from utils.readers import reader
from utils.retry import db_retry
from utils.timing import StageTimer
from validators.schema_registry import get_contract
from validators.schema_validator import partition_critical_validation_rows, validate_entity
from quality import metrics_tracker as audit_trail
from warehouse.connection import execute_values, get_dict_cursor

logger   = get_logger_name(__name__)
CONTRACT = get_contract("source_ticket_events")


# ═══════════════════════════════════════════════════════════════════════════
#  DIMENSION MAP LOADING
# ═══════════════════════════════════════════════════════════════════════════

@db_retry
def _build_dim_maps() -> tuple[dict, dict, dict]:
    """
    Load surrogate key maps for ticket events:
      - ticket_map : ticket_id (str) → ticket_key (int)
      - agent_map  : agent_id  (int) → agent_key  (int)
      - date_map   : (date, hour)    → date_key   (int)

    """
    with get_dict_cursor() as cur:

        cur.execute("""
            SELECT ticket_id, ticket_key
            FROM warehouse.fact_tickets
        """)
        ticket_map = {str(r["ticket_id"]): r["ticket_key"] for r in cur.fetchall()}

        cur.execute("""
            SELECT agent_id, agent_key
            FROM warehouse.dim_agent
            WHERE agent_id IS NOT NULL
              AND is_current = TRUE
        """)
        agent_map = {int(r["agent_id"]): r["agent_key"] for r in cur.fetchall()}

        cur.execute("""
            SELECT date_key, full_date, hour
            FROM warehouse.dim_date
        """)
        date_map = {(r["full_date"], r["hour"]): r["date_key"] for r in cur.fetchall()}

    return ticket_map, agent_map, date_map

# ═══════════════════════════════════════════════════════════════════════════
#  SURROGATE KEY RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_keys(
    df: pd.DataFrame,
    ticket_map: dict,
    agent_map: dict,
    date_map: dict,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Resolve ticket_key, agent_key, date_key for each event row.

    All three are required (NOT NULL in DDL). Rows that cannot resolve any
    one of them are moved to quarantine_records and excluded from good_df.

    Returns: (good_df, quarantine_records)
    """
    ticket_keys  = []
    agent_keys   = []
    date_keys    = []
    skip_indices = set()
    quarantine_records = []

    for idx, row in df.iterrows():
        ts = pd.to_datetime(row["event_ts"])

        date_key = date_map.get((ts.date(), ts.hour))
        if date_key is None:
            skip_indices.add(idx)
            quarantine_records.append({
                "idx":          idx,
                "error_details": "event_ts has no matching date_key in dim_date",
            })
            continue

        tid = str(row["ticket_id"])
        ticket_key = ticket_map.get(tid)
        if ticket_key is None:
            skip_indices.add(idx)
            quarantine_records.append({
                "idx": idx,
                "error_details": "ticket_id not found in fact_tickets",
            })
            continue

        if pd.isna(row.get("agent_id")):
            skip_indices.add(idx)
            quarantine_records.append({
                "idx": idx,
                "error_details": "agent_id is required for ticket events",
            })
            continue

        agent_key = agent_map.get(int(row["agent_id"]))
        if agent_key is None:
            skip_indices.add(idx)
            quarantine_records.append({
                "idx": idx,
                "error_details": "agent_id not found in dim_agent",
            })
            continue

        ticket_keys.append(ticket_key)
        agent_keys.append(agent_key)
        date_keys.append(date_key)

    good = df.drop(index=list(skip_indices)).copy()

    if not good.empty:
        good["ticket_key"] = ticket_keys
        good["agent_key"]  = agent_keys
        good["date_key"]   = date_keys

    return good, quarantine_records


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN LOADER
# ═══════════════════════════════════════════════════════════════════════════

def load(file_path: str, run_id: int) -> None:
    with StageTimer("fact_events_load") as timer:
        log_stage_start(logger, "fact_events_load", file=file_path)

        # ── 1. Idempotency check ──────────────────────────────────────────
        file_hash = compute_file_hash(file_path)
        if is_file_processed(file_path, file_hash):
            logger.info("file_skipped", file=file_path, reason="already_processed")
            return

        # ── 2. Register file as in-progress ──────────────────────────────
        register_file(run_id, file_path, file_hash, file_type="stream")

        # ── 3. Read file ──────────────────────────────────────────────────
        try:
            df = reader(file_path)
            if df is None or df.empty:
                mark_file_success(file_path, file_hash, 0, 0, 0)
                audit_trail.write_quality_metrics(
                    run_id=run_id,
                    table_name="fact_ticket_events",
                    source_file=file_path,
                    total_records=0,
                    valid_records=0,
                    quarantined_records=0,
                    orphaned_records=0,
                    duplicate_count=0,
                    null_violations=0,
                    business_rule_violations=0,
                    processing_latency_sec=0.0,
                    quality_details={"inserted": 0},
                )
                return
        except Exception as e:
            mark_file_failed(file_path, file_hash, str(e))
            send_alert("read_error", f"Cannot read {file_path}: {e}", run_id)
            return

        total = len(df)

        # ── 4. Schema validation — quarantine critical failures ───────────
        errors = validate_entity(df, "source_ticket_events")
        bad_labels, structural_msg = partition_critical_validation_rows(df, errors)
        if structural_msg is not None:
            mark_file_failed(file_path, file_hash, structural_msg)
            send_alert("schema_validation", f"{file_path}: {structural_msg}", run_id)
            return
        bad  = df.loc[bad_labels] if bad_labels else pd.DataFrame()
        good = df.drop(index=bad_labels) if bad_labels else df.copy()
        schema_quarantined = len(bad)

        if not bad.empty:
            errors_by_row: dict = defaultdict(list)
            for e in errors:
                if e.level == "critical":
                    errors_by_row[e.row_index].append(e.reason)

            send_batch_to_quarantine([
                {
                    "source_file":     file_path,
                    "entity_type":     "source_ticket_events",
                    "raw_record":      df.loc[idx].to_dict(),
                    "error_type":      "schema_validation",
                    "error_details":   " | ".join(reasons),
                    "pipeline_run_id": run_id,
                }
                for idx, reasons in errors_by_row.items()
                if idx in bad.index
            ])
            for _, r in bad.iterrows():
                log_record_rejected(logger, r.to_dict(), reason="schema_validation")

        if good.empty:
            mark_file_success(file_path, file_hash, total, 0, len(bad))
            audit_trail.write_quality_metrics(
                run_id=run_id,
                table_name="fact_ticket_events",
                source_file=file_path,
                total_records=total,
                valid_records=total - schema_quarantined,
                quarantined_records=schema_quarantined,
                orphaned_records=0,
                duplicate_count=0,
                null_violations=0,
                business_rule_violations=0,
                processing_latency_sec=0.0,
                quality_details={"inserted": 0},
            )
            return

        # ── 5. Deduplicate within file ────────────────────────────────────
        pre_dedup_count = len(good)
        good = good.drop_duplicates("event_id").copy()
        duplicate_count = pre_dedup_count - len(good)

        # ── 6. Resolve surrogate keys ─────────────────────────────────────
        ticket_map, agent_map, date_map = _build_dim_maps()
        good, fk_quarantine = _resolve_keys(good, ticket_map, agent_map, date_map)

        # Quarantine rows whose required FKs could not be resolved
        if fk_quarantine:
            send_batch_to_quarantine([
                {
                    "source_file":     file_path,
                    "entity_type":     "source_ticket_events",
                    "raw_record":      df.loc[q["idx"]].to_dict(),
                    "error_type":      "referential_integrity",
                    "error_details":   q["error_details"],
                    "pipeline_run_id": run_id,
                }
                for q in fk_quarantine
            ])
            for q in fk_quarantine:
                log_record_rejected(
                    logger,
                    df.loc[q["idx"]].to_dict(),
                    reason="referential_integrity",
                )

        if good.empty:
            quarantined = len(bad) + len(fk_quarantine)
            mark_file_success(file_path, file_hash, total, 0, quarantined)
            audit_trail.write_quality_metrics(
                run_id=run_id,
                table_name="fact_ticket_events",
                source_file=file_path,
                total_records=total,
                valid_records=0,
                quarantined_records=quarantined,
                orphaned_records=0,
                duplicate_count=duplicate_count,
                null_violations=0,
                business_rule_violations=0,
                processing_latency_sec=0.0,
                quality_details={"inserted": 0},
            )
            return

        # ── 7. Insert into warehouse ──────────────────────────────────────
        columns = [
            "event_id",
            "ticket_key", "agent_key", "date_key",
            "old_status", "new_status",
            "event_ts",
            "notes",
        ]
        rows = good[columns].astype(object).where(pd.notnull(good[columns]), None).to_dict("records")

        try:
            inserted = execute_values(
                f"""
                INSERT INTO warehouse.fact_ticket_events ({','.join(columns)})
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                rows,
            )
        except Exception as e:
            mark_file_failed(file_path, file_hash, str(e))
            send_alert("db_write_error", f"Insert failed for {file_path}: {e}", run_id)
            return

        # ── 8. Mark file as done ──────────────────────────────────────────
        quarantined = len(bad) + len(fk_quarantine)
        mark_file_success(file_path, file_hash, total, inserted, quarantined)
        audit_trail.write_quality_metrics(
            run_id=run_id,
            table_name="fact_ticket_events",
            source_file=file_path,
            total_records=total,
            # Keep metrics aligned with file_tracker.records_loaded and actual DB writes.
            valid_records=inserted,
            quarantined_records=quarantined,
            orphaned_records=0,
            duplicate_count=duplicate_count,
            null_violations=0,
            business_rule_violations=0,
            processing_latency_sec=round(timer.duration_ms / 1000.0, 3),
            quality_details={
                "inserted": inserted,
                "schema_quarantined": schema_quarantined,
                "fk_quarantined": len(fk_quarantine),
            },
        )

        log_stage_complete(
            logger, "fact_events_load",
            records=inserted,
            latency_ms=timer.duration_ms,
        )