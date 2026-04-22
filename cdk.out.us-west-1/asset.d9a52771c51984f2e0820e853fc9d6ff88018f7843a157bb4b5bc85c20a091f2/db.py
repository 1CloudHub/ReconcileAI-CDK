"""
Database connection module. All connections use credentials from utils (Secrets Manager).
"""
import psycopg2
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
