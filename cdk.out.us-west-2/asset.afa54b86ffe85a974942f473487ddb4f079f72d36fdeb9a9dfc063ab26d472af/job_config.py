"""
Reusable job configuration data access functions.
"""
import json

from db import insert_db, select_db


def _table_ref(schema, table):
    return f"{schema}.{table}"


def _normalize_pagination(page, page_size):
    try:
        page = int(page)
    except (TypeError, ValueError):
        page = 1

    try:
        page_size = int(page_size)
    except (TypeError, ValueError):
        page_size = 10

    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    return page, page_size


def list_jobs(
    schema,
    job_table,
    doc_table,
    page=None,
    page_size=None,
    job_name=None,
    created_by=None,
    document_name=None,
):
    """
    List jobs with optional document names aggregation.

    Optional filters (ILIKE):
      - job_name, created_by, document_name (via joined document rows)

    Pagination: when `page` or `page_size` is sent, results are limited and `pagination` is set;
    otherwise all matching rows are returned (legacy behavior).
    """
    jref = _table_ref(schema, job_table)
    dref = _table_ref(schema, doc_table)

    filters = ["COALESCE(j.delete_status, FALSE) = FALSE"]
    params = []

    if job_name:
        filters.append("j.job_name ILIKE %s")
        params.append(f"%{job_name}%")

    if created_by:
        filters.append("j.created_by ILIKE %s")
        params.append(f"%{created_by}%")

    if document_name:
        filters.append("d.document_name ILIKE %s")
        params.append(f"%{document_name}%")

    where_sql = " AND ".join(filters)
    base_params = tuple(params) if params else None

    paginate = page is not None or page_size is not None
    pagination_meta = None
    limit_clause = ""
    ps = None
    offset = None

    if paginate:
        p, ps = _normalize_pagination(
            page if page is not None else 1,
            page_size if page_size is not None else 10,
        )
        count_sql = f"""
            SELECT COUNT(DISTINCT j.id)
            FROM {jref} j
            LEFT JOIN {dref} d
                ON d.id IN (
                    SELECT value::bigint
                    FROM jsonb_array_elements_text(COALESCE(j.document_id, '[]'::jsonb)) AS items(value)
                )
            WHERE {where_sql}
        """
        count_rows = select_db(count_sql, base_params)
        total = count_rows[0][0] if count_rows and count_rows[0] else 0
        total_pages = (total + ps - 1) // ps if total else 0
        pagination_meta = {
            "page": p,
            "page_size": ps,
            "total": total,
            "total_pages": total_pages,
            "has_next": p < total_pages,
            "has_prev": p > 1,
        }
        offset = (p - 1) * ps
        limit_clause = " LIMIT %s OFFSET %s"

    query = f"""
        SELECT
            j.id,
            j.job_name,
            j.document_id,
            j.created_at,
            j.created_by,
            j.updated_at,
            j.updated_by,
            j.delete_status,
            COALESCE(
                ARRAY_REMOVE(ARRAY_AGG(d.document_name ORDER BY d.document_name), NULL),
                ARRAY[]::text[]
            ) AS document_names,
            j.reference_document_id,
            rd.document_name AS reference_document_name
        FROM {jref} j
        LEFT JOIN {dref} d
            ON d.id IN (
                SELECT value::bigint
                FROM jsonb_array_elements_text(COALESCE(j.document_id, '[]'::jsonb)) AS items(value)
            )
        LEFT JOIN {dref} rd
            ON rd.id = j.reference_document_id
        WHERE {where_sql}
        GROUP BY
            j.id,
            j.job_name,
            j.document_id,
            j.created_at,
            j.created_by,
            j.updated_at,
            j.updated_by,
            j.delete_status,
            j.reference_document_id,
            rd.document_name
        ORDER BY j.created_at DESC
        {limit_clause}
    """

    if paginate:
        qparams = tuple(params + [ps, offset])
    else:
        qparams = tuple(params) if params else None

    rows = select_db(query, qparams)
    results = []
    for row in rows:
        results.append(
            {
                "id": row[0],
                "job_name": row[1],
                "document_id": row[2] if row[2] is not None else [],
                "created_at": row[3].isoformat() if row[3] else None,
                "created_by": row[4],
                "updated_at": row[5].isoformat() if row[5] else None,
                "updated_by": row[6],
                "delete_status": row[7],
                "document_names": row[8] if row[8] is not None else [],
                "reference_document_id": row[9],
                "reference_document_name": row[10],
            }
        )

    out = {"jobs": results, "pagination": pagination_meta}
    return out

def create_job(schema, job_table, job_name, document_ids, created_by, reference_document_id=None):

    exists_query = f"""
        SELECT id
        FROM {_table_ref(schema, job_table)}
        WHERE COALESCE(delete_status, FALSE) = FALSE
          AND job_name ILIKE %s
        LIMIT 1
    """
    exists_rows = select_db(exists_query, (job_name.strip(),))
    if exists_rows:
        raise ValueError(f"Job name '{job_name.strip()}' already exists")

    query = f"""
        INSERT INTO {_table_ref(schema, job_table)}
            (job_name, document_id, created_at, created_by, updated_at, updated_by, delete_status, reference_document_id)
        VALUES
            (%s, %s::jsonb, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP, %s, FALSE, %s)
        RETURNING id
    """
    values = (job_name.strip(), json.dumps(document_ids), created_by, created_by, reference_document_id)
    response = select_db(query, values)
    job_id = response[0][0] if response and response[0] else None
    return {"status": "created", "job_id": job_id}

def edit_job(schema, job_table, job_id, job_name, document_ids, updated_by, reference_document_id=None):
    query = f"""
        UPDATE {_table_ref(schema, job_table)}
        SET
            job_name = %s,
            document_id = %s::jsonb,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = %s,
            reference_document_id = %s
        WHERE id = %s
          AND COALESCE(delete_status, FALSE) = FALSE
    """
    values = (job_name.strip(), json.dumps(document_ids), updated_by, reference_document_id, job_id)
    response = insert_db(query, values)
    return {"status": "updated", "response": response}

def delete_job(schema, job_table, job_id):
    query = f"""
        UPDATE {_table_ref(schema, job_table)}
        SET
            delete_status = TRUE,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
          AND COALESCE(delete_status, FALSE) = FALSE
    """
    values = (job_id,)
    response = insert_db(query, values)
    return {"status": "deleted", "response": response}
