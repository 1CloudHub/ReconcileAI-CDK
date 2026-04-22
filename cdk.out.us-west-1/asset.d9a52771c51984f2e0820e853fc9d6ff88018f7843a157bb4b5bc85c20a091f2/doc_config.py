"""
Document type list queries with optional filters and pagination.
"""
from db import select_db


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


def list_document_types(
    schema,
    table,
    page=None,
    page_size=None,
    document_name=None,
    document_description=None,
    created_from=None,
    created_to=None,
):
    """
    List document types from document_table.

    Optional filters (partial ILIKE for text):
      - document_name, document_description
      - created_from / created_to: created_at range (inclusive)

    Pagination: applied only when `page` or `page_size` is provided; otherwise returns all rows.
    """
    tref = _table_ref(schema, table)
    filters = []
    params = []

    if document_name:
        filters.append("document_name ILIKE %s")
        params.append(f"%{document_name}%")

    if document_description:
        filters.append("document_description ILIKE %s")
        params.append(f"%{document_description}%")

    if created_from:
        filters.append("created_at >= %s")
        params.append(created_from)

    if created_to:
        filters.append("created_at <= %s")
        params.append(created_to)

    where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""
    base_params = tuple(params) if params else None

    paginate = page is not None or page_size is not None
    pagination_meta = None

    if paginate:
        p, ps = _normalize_pagination(
            page if page is not None else 1,
            page_size if page_size is not None else 10,
        )
        count_sql = f"SELECT COUNT(*) FROM {tref} {where_clause}"
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
        query = f"""
            SELECT id, document_name, needed_fields, document_description, created_at, document_key, document_key_is_enabled
            FROM {tref}
            {where_clause}
            ORDER BY id DESC
            LIMIT %s OFFSET %s
        """
        qparams = list(params) + [ps, offset]
        rows = select_db(query, tuple(qparams))
    else:
        query = f"""
            SELECT id, document_name, needed_fields, document_description, created_at, document_key, document_key_is_enabled
            FROM {tref}
            {where_clause}
            ORDER BY id DESC
        """
        rows = select_db(query, base_params)

    results = []
    for row in rows:
        results.append(
            {
                "id": row[0],
                "document_name": row[1],
                "needed_fields": row[2],
                "document_description": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
                "document_key": row[5],
                "document_key_is_enabled": row[6],
            }
        )

    out = {
        "status_code": 200,
        "result": results,
        "document_type": results,
    }
    if pagination_meta is not None:
        out["pagination"] = pagination_meta
    return out
