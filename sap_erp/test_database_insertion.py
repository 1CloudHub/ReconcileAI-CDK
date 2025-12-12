import os
import argparse
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv




load_dotenv()


def db_check(userid: int, po_number: str) -> Optional[str]:
    """
    Connects to Postgres and returns the exception_status for the most recent
    record matching (po_number, userid). Returns None if not found or on error.
    """

    DB_HOST = os.environ.get("db_host")
    DB_PORT = os.environ.get("db_port")
    DB_NAME = os.environ.get("db_database")
    DB_USER = os.environ.get("db_user")
    DB_PASSWORD = os.environ.get("db_password")

    if not (DB_HOST and DB_NAME and DB_USER):
        raise RuntimeError("Missing DB connection environment variables. "
                           "Set DB_HOST, DB_NAME, DB_USER, DB_PASSWORD (DB_PORT optional).")

    if not userid or not po_number:
        return None

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT exception_status
            FROM erp.exception_table
            WHERE po_number = %s AND userid = %s
            ORDER BY created_at DESC
            LIMIT 1
        """

        cur.execute(query, (po_number, userid))
        row = cur.fetchone()

        cur.close()
        conn.close()

        if row and row.get("exception_status") is not None:
            return str(row["exception_status"]).strip()

        return None

    except Exception as e:
        # Print error for debugging in test runs
        print(f"[db_check] Error querying DB: {e}")
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return None




db_check(2, "4500018901")