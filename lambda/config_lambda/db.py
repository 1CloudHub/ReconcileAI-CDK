"""
Database connection module. All connections use credentials from utils (Secrets Manager).
"""
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import re
import utils


def get_connection(connect_timeout=10):
    """Return a new psycopg2 connection using config from utils.get_db_config()."""
    cfg = utils.get_db_config()
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["database"],
        connect_timeout=connect_timeout,
    )


def select_db(query, params=None):
    """Run SELECT query and return all rows."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()
        connection.commit()
        return result
    finally:
        cursor.close()
        connection.close()


def insert_db(query, values):
    """Run INSERT/write query with parameters."""
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()
        return {"status": "insert query successful"}
    except Exception as e:
        print("Exception occurred while insert query : ", e)
        return None
    finally:
        try:
            cursor.close()
            connection.close()
        except Exception:
            pass
            
def execute_db(query, params=None):
    """Run write query and return affected row count."""
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(query, params)
        affected_rows = cursor.rowcount
        connection.commit()
        return {"status": "query successful", "affected_rows": affected_rows}
    except Exception as e:
        print("Exception occurred while executing query : ", e)
        return None
    finally:
        try:
            cursor.close()
            connection.close()
        except Exception:
            pass

def update_db(query):
    """Run UPDATE query."""
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        return {"status": "update query successful"}
    except Exception as e:
        print("Exception occurred while update query : ", e)
        return None
    finally:
        try:
            cursor.close()
            connection.close()
        except Exception:
            pass


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def fetch_all_rows_from_table(schema_name: str, table_name: str):
    """
    Fetch all rows from a given schema.table and return them as a list[dict].

    Uses psycopg2.sql.Identifier to avoid SQL injection via identifiers.
    """
    if not isinstance(schema_name, str) or not isinstance(table_name, str):
        raise ValueError("schema_name and table_name must be strings")
    schema_name = schema_name.strip()
    table_name = table_name.strip()
    if not _IDENTIFIER_RE.match(schema_name) or not _IDENTIFIER_RE.match(table_name):
        raise ValueError("Invalid schema/table identifier")

    connection = get_connection()
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        cursor.execute(query)
        rows = cursor.fetchall()
        connection.commit()
        return rows
    finally:
        cursor.close()
        connection.close()
