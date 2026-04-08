"""
quality/quality_report.py
Generate a daily PDF quality report for a pipeline run.

This report is intentionally aggregate-only (no raw records) to avoid PII exposure.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from quality import metrics_tracker as audit_trail
from utils.logger import get_logger_name
from warehouse.connection import get_dict_cursor

logger = get_logger_name(__name__)


@dataclass(frozen=True)
class QualityReportArtifact:
    filename: str
    pdf_bytes: bytes


def generate_daily_quality_report(run_id: int) -> QualityReportArtifact:
    """
    Build a PDF report for a given pipeline run_id.

    Notes:
    - Uses aggregate tables only (run summary, per-file quality metrics, counts).
    - Safe to email because it does not include raw_record payloads.
    """
    summary = audit_trail.get_run_summary(run_id)
    metrics = audit_trail.get_quality_metrics_for_run(run_id)
    extra = _fetch_extra_counts(run_id)

    created_at = datetime.now()
    filename = f"fastfeast_quality_run_{run_id}_{created_at.strftime('%Y%m%d_%H%M%S')}.pdf"

    pdf = _render_pdf(
        run_id=run_id,
        created_at=created_at,
        summary=summary,
        metrics=metrics,
        extra=extra,
    )
    return QualityReportArtifact(filename=filename, pdf_bytes=pdf)


def _fetch_extra_counts(run_id: int) -> dict[str, Any]:
    """Counts needed for the report that aren't in get_run_summary()."""
    with get_dict_cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE pipeline_run_id = %s) AS quarantine_count,
                COUNT(*) FILTER (WHERE pipeline_run_id = %s AND entity_type IS NOT NULL) AS quarantine_typed_count
            FROM pipeline_audit.quarantine
            """,
            (run_id, run_id),
        )
        q = cur.fetchone() or {}

        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE is_resolved = false) AS orphans_unresolved,
                COUNT(*) FILTER (WHERE is_resolved = true)  AS orphans_resolved
            FROM pipeline_audit.orphan_tracking
            """
        )
        o = cur.fetchone() or {}

        cur.execute(
            """
            SELECT error_type, COUNT(*) AS cnt
            FROM pipeline_audit.quarantine
            WHERE pipeline_run_id = %s
            GROUP BY error_type
            ORDER BY cnt DESC
            """,
            (run_id,),
        )
        quarantine_by_type = [(r["error_type"], int(r["cnt"])) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT ROUND(AVG(processing_latency_sec)::numeric, 4) AS avg_latency_sec
            FROM pipeline_audit.pipeline_quality_metrics
            WHERE run_id = %s
              AND processing_latency_sec IS NOT NULL
              AND processing_latency_sec > 0
            """,
            (run_id,),
        )
        lat = cur.fetchone() or {}

    return {
        "quarantine_count": int(q.get("quarantine_count") or 0),
        "orphans_unresolved": int(o.get("orphans_unresolved") or 0),
        "orphans_resolved": int(o.get("orphans_resolved") or 0),
        "quarantine_by_type": quarantine_by_type,
        "avg_processing_latency_sec": float(lat["avg_latency_sec"])
        if lat.get("avg_latency_sec") is not None
        else None,
    }


def _render_pdf(
    *,
    run_id: int,
    created_at: datetime,
    summary: dict[str, Any],
    metrics: list[dict[str, Any]],
    extra: dict[str, Any],
) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, title=f"FastFeast Quality Report (run {run_id})")
    styles = getSampleStyleSheet()

    story = []
    story.append(Paragraph(f"<b>FastFeast — Daily Quality Report</b>", styles["Title"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(f"Run ID: <b>{run_id}</b>", styles["Normal"]))
    story.append(Paragraph(f"Generated at: {created_at.strftime('%Y-%m-%d %H:%M:%S')}", styles["Normal"]))
    story.append(Spacer(1, 12))

    # 1. Quality metric summary
    story.extend(_section_quality_metrics_summary(styles, summary, extra, metrics))
    story.append(Spacer(1, 12))

    # 2. Validation statistics
    story.extend(_section_validation_statistics(styles, metrics))
    story.append(Spacer(1, 12))

    # 3. Data health indicators
    story.extend(_section_data_health_indicators(styles, summary, extra))
    story.append(Spacer(1, 12))

    # 4. Rejected data summary
    story.extend(_section_rejected_summary(styles, extra))
    story.append(Spacer(1, 12))

    # 5. Processing latency metrics
    story.extend(_section_latency_metrics(styles, metrics, extra))
    story.append(Spacer(1, 12))

    # 6. Orphan reference summary
    story.extend(_section_orphan_summary(styles, summary, extra))

    doc.build(story)
    return buf.getvalue()


def _section_quality_metrics_summary(styles, summary: dict[str, Any], extra: dict[str, Any], metrics: list[dict[str, Any]]) -> list:
    """1. Quality metric summary - Overall pipeline quality metrics."""
    total_records = summary.get("total_records", 0) or 0
    total_quarantined = summary.get("total_quarantined", 0) or 0
    total_orphaned = summary.get("total_orphaned", 0) or 0

    reject_rate = (total_quarantined / total_records * 100) if total_records > 0 else 0
    orphan_rate = (total_orphaned / total_records * 100) if total_records > 0 else 0
    referential_integrity = 100 - orphan_rate if total_records > 0 else 100

    # Aggregate duplicate_count from per-file metrics — pipeline_run_log has no duplicate_rate column
    total_dupes = sum(m.get("duplicate_count", 0) or 0 for m in metrics)
    duplicate_rate = (total_dupes / total_records * 100) if total_records > 0 else 0

    rows = [
        ["Total records processed", f"{total_records:,}"],
        ["Duplicate rate", f"{duplicate_rate:.2f}%"],
        ["Orphan rate", f"{orphan_rate:.2f}%"],
        ["Referential integrity rate", f"{referential_integrity:.2f}%"],
        ["Reject/quarantined record count", f"{total_quarantined:,}"],
        ["Reject/quarantine rate", f"{reject_rate:.2f}%"],
        ["File processing success rate", f"{summary.get('file_success_rate', 0) * 100:.1f}%"],
    ]

    t = Table([["Metric", "Value"]] + rows, colWidths=[250, 270])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))

    return [
        Paragraph("<b>1. Quality Metric Summary</b>", styles["Heading2"]),
        Spacer(1, 6),
        t,
    ]


def _section_validation_statistics(styles, metrics: list[dict[str, Any]]) -> list:
    """2. Validation statistics - Per-file validation errors and counts."""
    if not metrics:
        return [
            Paragraph("<b>2. Validation Statistics</b>", styles["Heading2"]),
            Spacer(1, 6),
            Paragraph("No validation statistics recorded for this run.", styles["Normal"]),
        ]

    rows = []
    for m in metrics:
        src = (m.get("source_file", "") or "")[-35:]
        if len(src) >= 35:
            src = "..." + src[-32:]
        rows.append([
            m.get("table_name", "")[:18],
            src,
            str(m.get("total_records", 0)),
            str(m.get("valid_records", 0)),
            str(m.get("quarantined_records", 0)),
            str(m.get("null_violations", 0)),
            str(m.get("duplicate_count", 0)),
        ])

    header = ["Table", "Source File", "Total", "Valid", "Quar.", "Null", "Dup"]
    t = Table([header] + rows, colWidths=[70, 140, 45, 45, 45, 45, 45], repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#374151")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (2, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))

    return [
        Paragraph("<b>2. Validation Statistics</b>", styles["Heading2"]),
        Spacer(1, 6),
        Paragraph("<i>Per-file validation results:</i>", styles["Normal"]),
        Spacer(1, 3),
        t,
    ]


def _section_data_health_indicators(styles, summary: dict[str, Any], extra: dict[str, Any]) -> list:
    """3. Data health indicators - Key pipeline health metrics."""
    total_files = summary.get("total_files", 0) or 0
    successful_files = summary.get("successful_files", 0) or 0
    failed_files = summary.get("failed_files", 0) or 0
    total_records = summary.get("total_records", 0) or 0
    total_loaded = summary.get("total_loaded", 0) or 0
    
    health_score = (successful_files / total_files * 100) if total_files > 0 else 0
    load_rate = (total_loaded / total_records * 100) if total_records > 0 else 0
    
    rows = [
        ["Pipeline health score", f"{health_score:.1f}%", "Files processed successfully"],
        ["Data load rate", f"{load_rate:.1f}%", "Records loaded vs processed"],
        ["Total files", str(total_files), "Files in batch"],
        ["Successful files", str(successful_files), "Files with no errors"],
        ["Failed files", str(failed_files), "Files with critical errors"],
        ["Run status", summary.get("status", "unknown"), "Pipeline completion status"],
    ]

    t = Table([["Indicator", "Value", "Description"]] + rows, colWidths=[160, 100, 260])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))

    return [
        Paragraph("<b>3. Data Health Indicators</b>", styles["Heading2"]),
        Spacer(1, 6),
        t,
    ]


def _section_latency_metrics(styles, metrics: list[dict[str, Any]], extra: dict[str, Any]) -> list:
    """5. Processing latency metrics - Time from file arrival to load."""
    avg_latency = extra.get("avg_processing_latency_sec")
    avg_latency_str = f"{avg_latency:.2f}s" if avg_latency is not None else "n/a"
    
    # Per-file latency from metrics
    rows = []
    if metrics:
        for m in metrics:
            latency = m.get("processing_latency_sec", 0)
            src = (m.get("source_file", "") or "")[-35:]
            if len(src) >= 35:
                src = "..." + src[-32:]
            rows.append([
                m.get("table_name", "")[:18],
                src,
                f"{latency:.2f}s",
            ])
    
    # Summary table
    summary_rows = [
        ["Average processing latency (all files)", avg_latency_str],
    ]
    
    t_summary = Table([["Metric", "Value"]] + summary_rows, colWidths=[300, 220])
    t_summary.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#374151")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    
    result = [
        Paragraph("<b>5. Processing Latency Metrics</b>", styles["Heading2"]),
        Spacer(1, 6),
        t_summary,
    ]
    
    if rows:
        t_files = Table([["Table", "Source File", "Latency"]] + rows, colWidths=[70, 300, 80])
        t_files.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#374151")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ALIGN", (2, 0), (2, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        result.extend([
            Spacer(1, 12),
            Paragraph("<i>Per-file latency:</i>", styles["Normal"]),
            Spacer(1, 3),
            t_files,
        ])
    
    return result


def _section_orphan_summary(styles, summary: dict[str, Any], extra: dict[str, Any]) -> list:
    """6. Orphan reference summary - Foreign key validation issues."""
    total_orphaned = summary.get("total_orphaned", 0) or 0
    orphans_unresolved = extra.get("orphans_unresolved", 0) or 0
    orphans_resolved = extra.get("orphans_resolved", 0) or 0
    total_records = summary.get("total_records", 0) or 0
    
    orphan_rate = (total_orphaned / total_records * 100) if total_records > 0 else 0
    
    rows = [
        ["Total orphaned records", f"{total_orphaned:,}", "Records with invalid FK references"],
        ["Orphan rate", f"{orphan_rate:.2f}%", "% of records with orphan references"],
        ["Orphans unresolved", f"{orphans_unresolved:,}", "Orphans awaiting resolution"],
        ["Orphans resolved", f"{orphans_resolved:,}", "Orphans successfully resolved"],
        ["Resolution success rate", f"{(orphans_resolved / total_orphaned * 100):.1f}%" if total_orphaned > 0 else "N/A", "% of orphans resolved"],
    ]

    t = Table([["Metric", "Value", "Description"]] + rows, colWidths=[160, 100, 260])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))

    return [
        Paragraph("<b>6. Orphan Reference Summary</b>", styles["Heading2"]),
        Spacer(1, 6),
        t,
    ]


def _section_rejected_summary(styles, extra: dict[str, Any]) -> list:
    """Rejected / quarantined records by error_type (pipeline health only)."""
    rows_bt = extra.get("quarantine_by_type") or []
    if not rows_bt:
        return [
            Paragraph("<b>Rejected data summary</b>", styles["Heading2"]),
            Spacer(1, 6),
            Paragraph("No quarantined rows linked to this run_id.", styles["Normal"]),
        ]

    data = [["error_type", "count"]] + [[str(a), str(b)] for a, b in rows_bt]
    t = Table(data, colWidths=[320, 100])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#374151")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return [
        Paragraph("<b>Rejected data summary</b> <i>(quarantine by error_type)</i>", styles["Heading2"]),
        Spacer(1, 6),
        Paragraph(
            "<i>Counts are for records stored in pipeline_audit.quarantine with this run_id. "
            "Raw payloads are not included in this PDF.</i>",
            styles["Normal"],
        ),
        Spacer(1, 6),
        t,
    ]
