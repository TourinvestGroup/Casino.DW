from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from prefect import flow, get_run_logger, task

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent.parent
LAST_REFRESH_FILE = PROJECT_DIR / "LAST_REFRESH.md"


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


@task(name="gold-refresh", retries=2, retry_delay_seconds=60)
def run_gold_refresh(from_date: str, to_date: str, agent_id: int | None = None) -> None:
    logger = get_run_logger()
    logger.info("Starting Gold refresh for %s..%s", from_date, to_date)

    args = ["--from-date", from_date, "--to-date", to_date]
    if agent_id is not None:
        args.extend(["--agent-id", str(agent_id)])

    _run_python_script("refresh_gold.py", args)
    logger.info("Gold refresh completed")


@task(name="update-refresh-note", retries=1, retry_delay_seconds=15)
def update_refresh_note(from_date: str, to_date: str) -> None:
    logger = get_run_logger()
    pg_conn_str = os.getenv("PG_CONN_STR")

    if not pg_conn_str:
        raise RuntimeError("PG_CONN_STR must be set in environment")

    with psycopg2.connect(pg_conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.cashdesk_transactions_raw")
            bronze_cashdesk_cnt = int(cur.fetchone()[0])

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

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    content = f"""# Last Refresh Status

- Last successful end-to-end execution (Bronze + Gold): **{now_utc}**
- Target PostgreSQL DB: `Casino.DW`
- Bronze strategy: watermark incremental + sliding reread (`BRONZE_LOOKBACK_DAYS={os.getenv('BRONZE_LOOKBACK_DAYS', '3')}`)
- Last Gold refresh window: `{from_date}` .. `{to_date}`

## Last known validation snapshot

- `bronze.cashdesk_transactions_raw`: {bronze_cashdesk_cnt:,} rows
- `silver.fact_membership_day` ({from_date}..{to_date}): {silver_cnt:,} rows
- `gold.fact_membership_day` ({from_date}..{to_date}): {gold_cnt:,} rows

## Update procedure

This file is auto-updated by `prefect_flow.py` after successful flow runs.
"""

    LAST_REFRESH_FILE.write_text(content, encoding="utf-8")
    logger.info("Updated %s", LAST_REFRESH_FILE)


@flow(name="casino-dw-daily")
def daily_casino_dw(
    bronze_lookback_days: int = 3,
    gold_refresh_lookback_days: int = 7,
    use_previous_day_window: bool = True,
    to_date: str | None = None,
    agent_id: int | None = None,
) -> dict[str, str]:
    logger = get_run_logger()

    if not os.getenv("MSSQL_CONN_STR") or not os.getenv("PG_CONN_STR"):
        raise RuntimeError("MSSQL_CONN_STR and PG_CONN_STR must be set in environment")

    if to_date is None:
        anchor_date = datetime.now(timezone.utc).date()
    else:
        anchor_date = date.fromisoformat(to_date)

    if use_previous_day_window:
        previous_day = get_previous_day(anchor_date)
        from_date_obj = previous_day
        to_date_obj = previous_day
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

    run_bronze_load(bronze_lookback_days)
    run_gold_refresh(from_date_str, to_date_str, agent_id)
    update_refresh_note(from_date_str, to_date_str)

    return {
        "status": "success",
        "from_date": from_date_str,
        "to_date": to_date_str,
    }


if __name__ == "__main__":
    daily_casino_dw()