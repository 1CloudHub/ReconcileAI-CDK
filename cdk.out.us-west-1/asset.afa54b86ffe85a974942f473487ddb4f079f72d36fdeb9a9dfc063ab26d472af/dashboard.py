from db import select_db
from datetime import datetime
from dateutil.relativedelta import relativedelta


SOP_SCHEMA = "erp_no_sap"
session_table = "session_table"
job_table = "job_table"
sop_table = "sop_table"
document_table = "document_table"
 
 
def parse_month_filter(mm_yyyy: str):
    """
    Parse mm-yyyy string and return (start_date, end_date) as strings.
    Returns (None, None) if mm_yyyy is None or invalid.
    """
    if not mm_yyyy:
        return None, None
    try:
        dt = datetime.strptime(mm_yyyy, "%m-%Y")
        start_date = dt.replace(day=1).strftime("%Y-%m-%d")
        if dt.month == 12:
            end_date = dt.replace(year=dt.year + 1, month=1, day=1).strftime("%Y-%m-%d")
        else:
            end_date = dt.replace(month=dt.month + 1, day=1).strftime("%Y-%m-%d")
        return start_date, end_date
    except ValueError:
        raise ValueError(f"Invalid mm-yyyy format: '{mm_yyyy}'. Expected format: MM-YYYY (e.g. 03-2025)")

def get_total_reconciliations(mm_yyyy: str = None):
    try:
        start_date, end_date = parse_month_filter(mm_yyyy)
 
        if start_date:
            result = select_db(
                f"""
                SELECT COUNT(*) FROM {SOP_SCHEMA}.{session_table}
                WHERE created_at >= %s AND created_at < %s
                """,
                (start_date, end_date)
            )
        else:
            result = select_db(
                f"SELECT COUNT(*) FROM {SOP_SCHEMA}.{session_table}"
            )
 
        return {
            "statusCode": 200,
            "body": {
                "total_reconciliations": result[0][0]
            }
        }
 
    except ValueError as e:
        return {"statusCode": 400, "message": str(e)}
    except Exception as e:
        print(f"Failed to get total reconciliations: {e}")
        return {"statusCode": 500, "message": "Failed to get total reconciliations"} 

def get_total_jobs(mm_yyyy: str = None):
    try:
        start_date, end_date = parse_month_filter(mm_yyyy)
 
        if start_date:
            result = select_db(
                f"""
                SELECT COUNT(*) FROM {SOP_SCHEMA}.{job_table}
                WHERE COALESCE(delete_status, FALSE) = FALSE
                  AND created_at >= %s AND created_at < %s
                """,
                (start_date, end_date)
            )
        else:
            result = select_db(
                f"""
                SELECT COUNT(*) FROM {SOP_SCHEMA}.{job_table}
                WHERE COALESCE(delete_status, FALSE) = FALSE
                """
            )
 
        return {
            "statusCode": 200,
            "body": {
                "total_jobs": result[0][0]
            }
        }
 
    except ValueError as e:
        return {"statusCode": 400, "message": str(e)}
    except Exception as e:
        print(f"Failed to get total jobs: {e}")
        return {"statusCode": 500, "message": "Failed to get total jobs"}  

def get_total_sops(mm_yyyy: str = None):
    try:
        start_date, end_date = parse_month_filter(mm_yyyy)
 
        if start_date:
            result = select_db(
                f"""
                SELECT COUNT(*) FROM {SOP_SCHEMA}.{sop_table}
                WHERE COALESCE(delete_status, FALSE) = FALSE
                  AND created_at >= %s AND created_at < %s
                """,
                (start_date, end_date)
            )
        else:
            result = select_db(
                f"""
                SELECT COUNT(*) FROM {SOP_SCHEMA}.{sop_table}
                WHERE COALESCE(delete_status, FALSE) = FALSE
                """
            )
 
        return {
            "statusCode": 200,
            "body": {
                "total_sops": result[0][0]
            }
        }
 
    except ValueError as e:
        return {"statusCode": 400, "message": str(e)}
    except Exception as e:
        print(f"Failed to get total SOPs: {e}")
        return {"statusCode": 500, "message": "Failed to get total SOPs"}

def get_total_documents(date: str = None):
    try:
        start_date, end_date = parse_month_filter(date)

        if start_date:
            result = select_db(
                f"""
                SELECT COALESCE(SUM(docs_processed), 0)
                FROM (
                    SELECT
                        COUNT(s.id) * jsonb_array_length(COALESCE(j.document_id, '[]'::jsonb)) AS docs_processed
                    FROM {SOP_SCHEMA}.session_table s
                    LEFT JOIN {SOP_SCHEMA}.job_table j
                        ON s.job_id = j.id
                    WHERE COALESCE(j.delete_status, FALSE) = FALSE
                      AND s.created_at >= %s AND s.created_at < %s
                    GROUP BY j.job_name, j.document_id
                ) sub
                """,
                (start_date, end_date)
            )
        else:
            result = select_db(
                f"""
                SELECT COALESCE(SUM(docs_processed), 0)
                FROM (
                    SELECT
                        COUNT(s.id) * jsonb_array_length(COALESCE(j.document_id, '[]'::jsonb)) AS docs_processed
                    FROM {SOP_SCHEMA}.session_table s
                    LEFT JOIN {SOP_SCHEMA}.job_table j
                        ON s.job_id = j.id
                    WHERE COALESCE(j.delete_status, FALSE) = FALSE
                    GROUP BY j.job_name, j.document_id
                ) sub
                """
            )

        return {
            "statusCode": 200,
            "body": {
                "total_documents_processed": int(result[0][0])
            }
        }

    except ValueError as e:
        return {"statusCode": 400, "message": str(e)}
    except Exception as e:
        print(f"Failed to get total documents: {e}")
        return {"statusCode": 500, "message": "Failed to get total documents"}

def get_day_wise_reconciliation_breakdown(date: str = None):
    try:
        if not date:
            return {"statusCode": 400, "message": "date is required in mm-yyyy format (e.g., 05-2026)"}
        try:
            ref_date = datetime.strptime(date, "%m-%Y")
        except ValueError:
            return {"statusCode": 400, "message": "Invalid date format. Use mm-yyyy (e.g., 05-2026)"}
        month_start = ref_date.replace(day=1)
        month_end   = month_start + relativedelta(months=1)  # exclusive upper bound
        result = select_db(
            f"""
            SELECT
                DATE(s.created_at)                                          AS day,
                COUNT(*)                                                    AS total_reconciliations,
                COUNT(*) FILTER (WHERE s.reconcile_status = 'yes')          AS matched,
                COUNT(*) FILTER (WHERE s.reconcile_status = 'no')           AS mismatched
            FROM {SOP_SCHEMA}.session_table s
            WHERE s.created_at >= %s AND s.created_at < %s
            GROUP BY DATE(s.created_at)
            ORDER BY DATE(s.created_at) ASC
            """,
            (month_start, month_end)
        )
        daily_data = []
        for row in result:
            daily_data.append({
                "day":                   row[0].strftime("%d-%m-%Y"),
                "total_reconciliations": row[1],
                "matched":               row[2],
                "mismatched":            row[3],
            })
        return {
            "statusCode": 200,
            "body": {
                "month": ref_date.strftime("%B %Y"),
                "data":  daily_data
            }
        }
    except Exception as e:
        print(f"Failed to get day wise reconciliation breakdown: {e}")
        return {"statusCode": 500, "message": "Failed to get day wise reconciliation breakdown"}

def get_reconciliation_summary(date: str = None):
    try:
        if date:
            # Parse mm-yyyy input
            try:
                ref_date = datetime.strptime(date, "%m-%Y")
            except ValueError:
                return {"statusCode": 400, "message": "Invalid date format. Use mm-yyyy (e.g., 05-2026)"}

            # Build last 3 months including the given month
            months = []
            for i in range(2, -1, -1):  # i=2,1,0 → 3 months ago to current
                month_start = (ref_date - relativedelta(months=i)).replace(day=1)
                month_end   = month_start + relativedelta(months=1)
                months.append((month_start, month_end))

            monthly_data = []
            for month_start, month_end in months:
                result = select_db(
                    f"""
                    SELECT
                        COUNT(*)                                            AS total_reconciliations,
                        COUNT(*) FILTER (WHERE reconcile_status = 'yes')   AS matched,
                        COUNT(*) FILTER (WHERE reconcile_status = 'no')    AS mismatched
                    FROM {SOP_SCHEMA}.session_table
                    WHERE created_at >= %s AND created_at < %s
                    """,
                    (month_start, month_end)
                )
                row = month_start  # just for label
                r   = result[0]
                monthly_data.append({
                    "month":                month_start.strftime("%B %Y"),   # e.g. "March 2026"
                    "total_reconciliations": r[0],
                    "matched":               r[1],
                    "mismatched":            r[2],
                })

            return {
                "statusCode": 200,
                "body": monthly_data
            }

        else:
            # No date — return overall summary (existing behaviour)
            result = select_db(
                f"""
                SELECT
                    COUNT(*)                                            AS total_reconciliations,
                    COUNT(*) FILTER (WHERE reconcile_status = 'yes')   AS matched,
                    COUNT(*) FILTER (WHERE reconcile_status = 'no')    AS mismatched
                FROM {SOP_SCHEMA}.session_table
                """
            )
            row = result[0]
            return {
                "statusCode": 200,
                "body": {
                    "total_reconciliations": row[0],
                    "matched":               row[1],
                    "mismatched":            row[2],
                }
            }

    except ValueError as e:
        return {"statusCode": 400, "message": str(e)}
    except Exception as e:
        print(f"Failed to get reconciliation summary: {e}")
        return {"statusCode": 500, "message": "Failed to get reconciliation summary"}

def get_top_jobs_by_documents(date: str = None):
    try:
        start_date, end_date = parse_month_filter(date)

        if start_date:
            result = select_db(
                f"""
                SELECT
                    j.job_name,
                    COUNT(s.id) AS total_runs,
                    jsonb_array_length(COALESCE(j.document_id, '[]'::jsonb)) AS docs_configured,
                    COUNT(s.id) * jsonb_array_length(COALESCE(j.document_id, '[]'::jsonb)) AS total_documents_uploaded
                FROM {SOP_SCHEMA}.session_table s
                LEFT JOIN {SOP_SCHEMA}.job_table j
                    ON s.job_id = j.id
                WHERE COALESCE(j.delete_status, FALSE) = FALSE
                  AND s.created_at >= %s AND s.created_at < %s
                GROUP BY
                    j.job_name,
                    j.document_id
                ORDER BY total_documents_uploaded DESC
                LIMIT 5
                """,
                (start_date, end_date)
            )
        else:
            result = select_db(
                f"""
                SELECT
                    j.job_name,
                    COUNT(s.id) AS total_runs,
                    jsonb_array_length(COALESCE(j.document_id, '[]'::jsonb)) AS docs_configured,
                    COUNT(s.id) * jsonb_array_length(COALESCE(j.document_id, '[]'::jsonb)) AS total_documents_uploaded
                FROM {SOP_SCHEMA}.session_table s
                LEFT JOIN {SOP_SCHEMA}.job_table j
                    ON s.job_id = j.id
                WHERE COALESCE(j.delete_status, FALSE) = FALSE
                GROUP BY
                    j.job_name,
                    j.document_id
                ORDER BY total_documents_uploaded DESC
                LIMIT 5
                """
            )

        data = []
        for row in result:
            data.append({
                "job_name":                 row[0],
                "total_documents_uploaded": row[3]
            })

        return {
            "statusCode": 200,
            "body": {
                "data": data
            }
        }

    except ValueError as e:
        return {"statusCode": 400, "message": str(e)}
    except Exception as e:
        print(f"Failed to get top jobs by documents: {e}")
        return {"statusCode": 500, "message": "Failed to get top jobs by documents"}

