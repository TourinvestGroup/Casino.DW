"""Daily visit-sessions report exporter.

Pulls one row per (visit x overlapping game session) from gold.fn_visit_sessions,
writes an .xlsx matching the legacy Casino Daily Analysis template, and emails it.

Run standalone or invoke from prefect_flow.py.
Defaults to yesterday (UTC).
"""
from __future__ import annotations

import argparse
import os
import smtplib
import sys
from datetime import date, datetime, time as dtime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from openpyxl import Workbook

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent.parent
REPORTS_DIR = PROJECT_DIR / "reports"

load_dotenv(BASE_DIR / ".env")

HEADERS = [
    "GamingDay",
    "Membership",
    "VisitNo",
    "CasinoEntryTime",
    "CasinoExitTime",
    "SessionStart",
    "SessionFinish",
    "averBet",
]


def parse_args() -> argparse.Namespace:
    yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date", default=yesterday, help="YYYY-MM-DD (default: yesterday UTC)")
    parser.add_argument("--to-date", default=yesterday, help="YYYY-MM-DD (default: yesterday UTC)")
    parser.add_argument("--no-email", action="store_true", help="Generate the file only, skip SMTP")
    return parser.parse_args()


def fetch_rows(pg_conn_str: str, from_date: str, to_date: str) -> list[tuple]:
    with psycopg2.connect(pg_conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT gaming_day, membership, visit_no, casino_entry_time, casino_exit_time,
                       session_start, session_finish, averbet
                FROM gold.fn_visit_sessions(%s::date, %s::date)
                """,
                (from_date, to_date),
            )
            return cur.fetchall()


def _trunc_minute(t):
    """Drop seconds + microseconds. Matches the legacy MSSQL `CONVERT(char(5), ..., 108)`
    which output strings like '16:07'. SSMS-exported xlsx stored those as time(h,m,0)."""
    if t is None:
        return None
    return t.replace(second=0, microsecond=0)


def write_xlsx(rows: list[tuple], out_path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.append(HEADERS)

    for gd, mem, vn, in_t, out_t, ss, sf, ab in rows:
        ws.append([
            datetime.combine(gd, dtime.min) if gd is not None else None,
            mem,
            vn,
            _trunc_minute(in_t),
            _trunc_minute(out_t),
            _trunc_minute(ss) if ss is not None else "NULL",
            _trunc_minute(sf) if sf is not None else "NULL",
            float(ab) if ab is not None else "NULL",
        ])

    for r in range(2, ws.max_row + 1):
        ws.cell(row=r, column=1).number_format = "mm-dd-yy"
        ws.cell(row=r, column=4).number_format = "h:mm"
        ws.cell(row=r, column=5).number_format = "h:mm"
        for col in (6, 7):
            cell = ws.cell(row=r, column=col)
            if cell.value != "NULL":
                cell.number_format = "h:mm"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)


def send_report(file_path: Path, from_date: str, to_date: str, row_count: int) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    smtp_use_ssl = os.getenv("SMTP_USE_SSL", "false").lower() == "true"
    smtp_from = os.getenv("SMTP_FROM_EMAIL")
    smtp_to = os.getenv("REPORT_TO_EMAILS")

    if not smtp_host or not smtp_from or not smtp_to:
        raise RuntimeError("SMTP_HOST, SMTP_FROM_EMAIL, and REPORT_TO_EMAILS must be set")

    recipients = [e.strip() for e in smtp_to.split(",") if e.strip()]
    if not recipients:
        raise RuntimeError("REPORT_TO_EMAILS is empty")

    window = from_date if from_date == to_date else f"{from_date}..{to_date}"
    subject = f"Casino Daily Visits Report - {window}"
    body = (
        "Daily visit-sessions report attached.\n\n"
        f"Gaming day window: {window}\n"
        f"Rows: {row_count}\n"
        f"File: {file_path.name}\n"
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with file_path.open("rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=file_path.name,
        )

    if smtp_use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            if smtp_use_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)

    print(f"Emailed {file_path.name} to {', '.join(recipients)}")


def main() -> int:
    args = parse_args()
    pg_conn_str = os.getenv("PG_CONN_STR")
    if not pg_conn_str:
        raise RuntimeError("PG_CONN_STR must be set")

    rows = fetch_rows(pg_conn_str, args.from_date, args.to_date)
    out_path = REPORTS_DIR / f"casino_daily_visits_{args.to_date}.xlsx"
    write_xlsx(rows, out_path)
    print(f"Wrote {len(rows)} rows -> {out_path}")

    if not args.no_email:
        send_report(out_path, args.from_date, args.to_date, len(rows))

    return 0


if __name__ == "__main__":
    sys.exit(main())
