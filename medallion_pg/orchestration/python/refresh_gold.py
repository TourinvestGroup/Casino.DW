import argparse
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

PG_CONN_STR = os.getenv("PG_CONN_STR")


def parse_args():
    parser = argparse.ArgumentParser(description="Refresh Gold fact from Silver function")
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--agent-id", type=int, default=None, help="Optional agent filter")
    return parser.parse_args()


def main():
    args = parse_args()

    if not PG_CONN_STR:
        raise RuntimeError("Set PG_CONN_STR in environment or .env file")

    with psycopg2.connect(PG_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT gold.sp_load_fact_membership_day(%s, %s, %s)
                """,
                (args.from_date, args.to_date, args.agent_id),
            )
            cur.execute(
                """
                SELECT gold.sp_load_session_marts(%s, %s)
                """,
                (args.from_date, args.to_date),
            )
        conn.commit()

    print(
        f"Gold refreshed for period {args.from_date}..{args.to_date}"
        + ("" if args.agent_id is None else f", agent {args.agent_id}")
    )


if __name__ == "__main__":
    main()
