from __future__ import annotations

import os
import smtplib
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

import psycopg2
from prefect import flow, get_run_logger, task
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent.parent
LAST_REFRESH_FILE = PROJECT_DIR / "LAST_REFRESH.md"

load_dotenv(BASE_DIR / ".env")


def get_previous_day(anchor_date: date) -> date:
    return anchor_date - timedelta(days=1)


def _run_python_script(script_name: str, args: list[str], extra_env: dict[str, str] | None = None) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    command = [sys.executable, script_name, *args]
    result = subprocess.run(
        command,
        cwd=BASE_DIR,
        env=env,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"{script_name} failed with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )


def _get_data_snapshot(pg_conn_str: str, from_date: str, to_date: str) -> dict[str, int] | None:
    try:
        with psycopg2.connect(pg_conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM bronze.cashdesk_transactions_raw")
                bronze_cashdesk_cnt = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*) FROM bronze.drgt_sessions_raw")
                bronze_drgt_cnt = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*) FROM bronze.casino_transaction_money_raw")
                bronze_txmoney_cnt = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM silver.fact_membership_day
                    WHERE gamingday BETWEEN %s::date AND %s::date
                    """,
                    (from_date, to_date),
                )
                silver_cnt = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM gold.fact_membership_day
                    WHERE gamingday BETWEEN %s::date AND %s::date
                    """,
                    (from_date, to_date),
                )
                gold_cnt = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*) FROM gold.dim_egd_position")
                egd_dim_cnt = int(cur.fetchone()[0])

                cur.execute(
                    "SELECT COUNT(*) FROM gold.dim_egd_position WHERE is_active = true"
                )
                egd_active_cnt = int(cur.fetchone()[0])

        return {
            "bronze_cashdesk_cnt": bronze_cashdesk_cnt,
            "bronze_drgt_cnt": bronze_drgt_cnt,
            "bronze_txmoney_cnt": bronze_txmoney_cnt,
            "silver_cnt": silver_cnt,
            "gold_cnt": gold_cnt,
            "egd_dim_cnt": egd_dim_cnt,
            "egd_active_cnt": egd_active_cnt,
        }
    except Exception:
        return None


@task(name="bronze-load", retries=2, retry_delay_seconds=60)
def run_bronze_load(bronze_lookback_days: int) -> None:
    logger = get_run_logger()
    logger.info("Starting Bronze load with BRONZE_LOOKBACK_DAYS=%s", bronze_lookback_days)
    _run_python_script(
        "load_bronze_incremental.py",
        [],
        extra_env={"BRONZE_LOOKBACK_DAYS": str(bronze_lookback_days)},
    )
    logger.info("Bronze load completed")


@task(name="bronze-egd-load", retries=2, retry_delay_seconds=60)
def run_bronze_egd_load() -> None:
    logger = get_run_logger()
    logger.info("Starting Bronze EGD load from CIBatumi")
    _run_python_script("load_egd_dimension.py", [])
    logger.info("Bronze EGD load completed (dim_egd_position + bridge refreshed)")


@task(name="gold-refresh", retries=2, retry_delay_seconds=60)
def run_gold_refresh(from_date: str, to_date: str, agent_id: int | None = None) -> None:
    logger = get_run_logger()
    logger.info("Starting Gold refresh for %s..%s", from_date, to_date)

    args = ["--from-date", from_date, "--to-date", to_date, "--skip-egd"]
    if agent_id is not None:
        args.extend(["--agent-id", str(agent_id)])

    _run_python_script("refresh_gold.py", args)
    logger.info("Gold refresh completed")


@task(name="update-refresh-note", retries=1, retry_delay_seconds=15)
def update_refresh_note(
    run_status: str,
    from_date: str,
    to_date: str,
    error_message: str | None = None,
) -> None:
    logger = get_run_logger()
    pg_conn_str = os.getenv("PG_CONN_STR")

    if not pg_conn_str:
        raise RuntimeError("PG_CONN_STR must be set in environment")

    snapshot = _get_data_snapshot(pg_conn_str, from_date, to_date)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    status_label = "SUCCESS" if run_status.lower() == "success" else "FAILED"

    snapshot_section = ""
    if snapshot is not None:
        snapshot_section = f"""
## Last known validation snapshot

- `bronze.cashdesk_transactions_raw`: {snapshot['bronze_cashdesk_cnt']:,} rows
- `bronze.drgt_sessions_raw`: {snapshot['bronze_drgt_cnt']:,} rows
- `bronze.casino_transaction_money_raw`: {snapshot['bronze_txmoney_cnt']:,} rows
- `silver.fact_membership_day` ({from_date}..{to_date}): {snapshot['silver_cnt']:,} rows
- `gold.fact_membership_day` ({from_date}..{to_date}): {snapshot['gold_cnt']:,} rows
- `gold.dim_egd_position`: {snapshot['egd_dim_cnt']} positions ({snapshot['egd_active_cnt']} active)
"""

    content = f"""# Last Refresh Status

- Last run status: **{status_label}**
- Last run timestamp (UTC): **{now_utc}**
- Target PostgreSQL DB: `Casino.DW`
- Bronze strategy: watermark incremental + sliding reread (`BRONZE_LOOKBACK_DAYS={os.getenv('BRONZE_LOOKBACK_DAYS', '3')}`)
- Last Gold refresh window: `{from_date}` .. `{to_date}`
{f'- Last error: `{error_message}`' if error_message else '- Last error: `None`'}

{snapshot_section}

## Update procedure

This file is auto-updated by `prefect_flow.py` after each flow run.
"""

    LAST_REFRESH_FILE.write_text(content, encoding="utf-8")
    logger.info("Updated %s", LAST_REFRESH_FILE)


@task(name="send-status-email", retries=1, retry_delay_seconds=30)
def send_status_email(
    run_status: str,
    from_date: str,
    to_date: str,
    error_message: str | None = None,
) -> None:
    logger = get_run_logger()

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    smtp_use_ssl = os.getenv("SMTP_USE_SSL", "false").lower() == "true"
    smtp_from = os.getenv("SMTP_FROM_EMAIL")
    smtp_to = os.getenv("SMTP_TO_EMAILS")

    if not smtp_host or not smtp_from or not smtp_to:
        logger.warning("Email notification skipped: SMTP configuration is incomplete")
        return

    recipients = [email.strip() for email in smtp_to.split(",") if email.strip()]
    if not recipients:
        logger.warning("Email notification skipped: SMTP_TO_EMAILS is empty")
        return

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subject = f"[Casino.DW] Daily run {run_status.upper()} | {from_date}..{to_date}"
    body = (
        f"Status: {run_status.upper()}\n"
        f"Timestamp (UTC): {now_utc}\n"
        f"Window: {from_date}..{to_date}\n"
        f"Error: {error_message or 'None'}\n"
        f"Status file: {LAST_REFRESH_FILE}\n"
    )

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = smtp_from
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    if smtp_use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(message)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            if smtp_use_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(message)

    logger.info("Status email sent to %s", ", ".join(recipients))


@task(name="export-visits-report", retries=1, retry_delay_seconds=30)
def run_visits_report(from_date: str, to_date: str) -> None:
    """Export the daily visit-sessions xlsx and email it.

    Decoupled from pipeline health: any failure is logged and swallowed so the
    flow stays green. Operators see report state in flow logs, not in the
    pipeline status email.
    """
    logger = get_run_logger()
    try:
        _run_python_script(
            "export_visit_sessions.py",
            ["--from-date", from_date, "--to-date", to_date],
        )
        logger.info("Daily visits report exported and emailed for %s..%s", from_date, to_date)
    except Exception as ex:
        logger.warning("Visits report failed silently (pipeline still healthy): %s", ex)


def _get_last_gold_day(pg_conn_str: str) -> date | None:
    """Return the latest gamingday present in gold.fact_membership_day, or None."""
    try:
        with psycopg2.connect(pg_conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(gamingday) FROM gold.fact_membership_day")
                row = cur.fetchone()
                return row[0] if row and row[0] else None
    except Exception:
        return None


@flow(name="casino-dw-daily")
def daily_casino_dw(
    bronze_lookback_days: int = 3,
    gold_refresh_lookback_days: int = 7,
    use_previous_day_window: bool = True,
    to_date: str | None = None,
    agent_id: int | None = None,
) -> dict[str, str]:
    logger = get_run_logger()

    pg_conn_str = os.getenv("PG_CONN_STR")

    if not os.getenv("MSSQL_CONN_STR") or not pg_conn_str:
        raise RuntimeError("MSSQL_CONN_STR and PG_CONN_STR must be set in environment")

    if not os.getenv("MSSQL_CIBATUMI_CONN_STR"):
        logger.warning("MSSQL_CIBATUMI_CONN_STR not set — EGD dimension refresh will be skipped")

    if to_date is None:
        anchor_date = datetime.now(timezone.utc).date()
    else:
        anchor_date = date.fromisoformat(to_date)

    yesterday = get_previous_day(anchor_date)

    if use_previous_day_window:
        # Auto-catchup: check last refreshed day in gold
        last_gold_day = _get_last_gold_day(pg_conn_str)

        if last_gold_day is not None and last_gold_day < yesterday:
            # Missed days detected — refresh from last_gold_day through yesterday
            from_date_obj = last_gold_day
            to_date_obj = yesterday
            logger.info(
                "Catchup mode: last gold day is %s, refreshing %s..%s (%s missed days)",
                last_gold_day.isoformat(),
                from_date_obj.isoformat(),
                to_date_obj.isoformat(),
                (yesterday - last_gold_day).days,
            )
        else:
            # Normal daily: just yesterday
            from_date_obj = yesterday
            to_date_obj = yesterday
    else:
        to_date_obj = anchor_date
        from_date_obj = to_date_obj - timedelta(days=gold_refresh_lookback_days)

    from_date_str = from_date_obj.isoformat()
    to_date_str = to_date_obj.isoformat()

    logger.info(
        "Flow window: %s..%s | bronze lookback: %s days | previous-day mode: %s | gold lookback: %s days",
        from_date_str,
        to_date_str,
        bronze_lookback_days,
        use_previous_day_window,
        gold_refresh_lookback_days,
    )

    try:
        run_bronze_load(bronze_lookback_days)
        if os.getenv("MSSQL_CIBATUMI_CONN_STR"):
            run_bronze_egd_load()
        run_gold_refresh(from_date_str, to_date_str, agent_id)
        update_refresh_note("success", from_date_str, to_date_str, None)
        send_status_email("success", from_date_str, to_date_str, None)
        # Report task: yesterday only, regardless of catchup window — only the
        # most recent gaming day goes out as the "daily" report.
        report_day = to_date_str
        run_visits_report(report_day, report_day)
    except Exception as ex:
        error_text = str(ex)
        update_refresh_note("failed", from_date_str, to_date_str, error_text)
        send_status_email("failed", from_date_str, to_date_str, error_text)
        raise

    return {
        "status": "success",
        "from_date": from_date_str,
        "to_date": to_date_str,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Run as a scheduled service (cron: daily at 06:00 UTC+4)",
    )
    parser.add_argument("--cron", default="0 6 * * *", help="Cron schedule (default: 06:00 daily)")
    cli_args = parser.parse_args()

    if cli_args.serve:
        daily_casino_dw.serve(
            name="casino-dw-daily-scheduled",
            cron=cli_args.cron,
        )
    else:
        daily_casino_dw()