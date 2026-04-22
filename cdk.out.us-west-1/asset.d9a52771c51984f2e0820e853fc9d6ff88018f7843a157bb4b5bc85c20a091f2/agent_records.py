"""
Agent records data access functions.
Handles queries against session_table for the Reconcile Agent page.
"""

from db import select_db,execute_db

SCHEMA = "erp_no_sap"
SESSION_TABLE = "session_table"
JOB_TABLE = "job_table"
DOCUMENT_TABLE = "document_table"
SESSION_TABLE_TEST = "session_table_test"


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


# def list_all_sessions(
#     page: int = 1,
#     page_size: int = 10,
#     job_name: str = None,
#     reconcile_status: str = None,
#     document_type: str = None,
#     session_id: str = None,
#     created_by: str = None,
#     created_from: str = None,
#     created_to: str = None,
# ):
#     """
#     Return all reconciliation sessions joined with job and document names.

#     Optional filters:
#       - job_name:          partial case-insensitive match on job_table.job_name
#       - reconcile_status:  partial case-insensitive match on session_table.reconcile_status
#       - document_type:     partial case-insensitive match on document_table.document_name
#       - session_id:        partial case-insensitive match on session_table.session_id
#       - created_by:        partial case-insensitive match on session_table.created_by
#       - created_from:      created_at >= created_from
#       - created_to:        created_at <= created_to
#     """
#     page, page_size = _normalize_pagination(page, page_size)
#     filters = []
#     params = []

#     if job_name:
#         filters.append("j.job_name ILIKE %s")
#         params.append(f"%{job_name}%")

#     if reconcile_status:
#         filters.append("s.reconcile_status ILIKE %s")
#         params.append(f"%{reconcile_status}%")

#     if document_type:
#         filters.append("d.document_name ILIKE %s")
#         params.append(f"%{document_type}%")

#     if session_id:
#         filters.append("s.session_id ILIKE %s")
#         params.append(f"%{session_id}%")

#     if created_by:
#         filters.append("s.created_by ILIKE %s")
#         params.append(f"%{created_by}%")

#     if created_from:
#         filters.append("s.created_at >= %s")
#         params.append(created_from)

#     if created_to:
#         filters.append("s.created_at <= %s")
#         params.append(created_to)

#     where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""
#     offset = (page - 1) * page_size

#     count_query = f"""
#         SELECT COUNT(DISTINCT s.id)
#         FROM {SCHEMA}.{SESSION_TABLE} s
#         LEFT JOIN {SCHEMA}.{JOB_TABLE} j
#             ON j.id = s.job_id
#         LEFT JOIN {SCHEMA}.{DOCUMENT_TABLE} d
#             ON d.id IN (
#                 SELECT value::bigint
#                 FROM jsonb_array_elements_text(
#                     COALESCE(j.document_id, '[]'::jsonb)
#                 ) AS items(value)
#             )
#         {where_clause}
#     """
#     total_rows = select_db(count_query, params if params else None)
#     total = total_rows[0][0] if total_rows and total_rows[0] else 0

#     query = f"""
#         SELECT
#             s.id,
#             s.session_id,
#             s.job_id,
#             j.job_name,
#             s.reconcile_status,
#             s.reason_for_failure,
#             s.created_at,
#             s.created_by,
#             s.updated_at,
#             s.documents,
#             COALESCE(
#                 ARRAY_REMOVE(
#                     ARRAY_AGG(d.document_name ORDER BY d.document_name),
#                     NULL
#                 ),
#                 ARRAY[]::text[]
#             ) AS document_names
#         FROM {SCHEMA}.{SESSION_TABLE} s
#         LEFT JOIN {SCHEMA}.{JOB_TABLE} j
#             ON j.id = s.job_id
#         LEFT JOIN {SCHEMA}.{DOCUMENT_TABLE} d
#             ON d.id IN (
#                 SELECT value::bigint
#                 FROM jsonb_array_elements_text(
#                     COALESCE(j.document_id, '[]'::jsonb)
#                 ) AS items(value)
#             )
#         {where_clause}
#         GROUP BY
#             s.id,
#             s.session_id,
#             s.job_id,
#             j.job_name,
#             s.reconcile_status,
#             s.reason_for_failure,
#             s.created_at,
#             s.created_by,
#             s.updated_at,
#             s.documents
#         ORDER BY s.created_at DESC
#         LIMIT %s OFFSET %s
#     """

#     query_params = list(params)
#     query_params.extend([page_size, offset])
#     rows = select_db(query, query_params)

#     results = []
#     for row in rows:
#         results.append(
#             {
#                 "id": row[0],
#                 "session_id": row[1],
#                 "job_id": row[2],
#                 "job_name": row[3],
#                 "reconcile_status": row[4],
#                 "reason_for_failure": row[5],
#                 "created_at": row[6].isoformat() if row[6] else None,
#                 "created_by": row[7],
#                 "updated_at": row[8].isoformat() if row[8] else None,
#                 "documents": row[9] if row[9] is not None else {},
#                 "document_names": list(row[10]) if row[10] is not None else [],
#             }
#         )

#     total_pages = (total + page_size - 1) // page_size if total else 0

#     return {
#         "statusCode": 200,
#         "body": {
#             "sessions": results,
#             "pagination": {
#                 "page": page,
#                 "page_size": page_size,
#                 "total": total,
#                 "total_pages": total_pages,
#                 "has_next": page < total_pages,
#                 "has_prev": page > 1,
#             },
#         },
#     }
def list_all_sessions(
    page: int = 1,
    page_size: int = 10,
    job_name: str = None,
    reconcile_status: str = None,
    document_type: str = None,
    session_id: str = None,
    created_by: str = None,
    created_from: str = None,
    created_to: str = None,
):
    """
    Return all reconciliation sessions joined with job and document names.
    """

    page, page_size = _normalize_pagination(page, page_size)
    filters = []
    params = []

    if job_name:
        filters.append("j.job_name ILIKE %s")
        params.append(f"%{job_name}%")

    if reconcile_status:
        filters.append("s.reconcile_status ILIKE %s")
        params.append(f"%{reconcile_status}%")

    if document_type:
        filters.append("d.document_name ILIKE %s")
        params.append(f"%{document_type}%")

    if session_id:
        filters.append("s.session_id ILIKE %s")
        params.append(f"%{session_id}%")

    if created_by:
        filters.append("s.created_by ILIKE %s")
        params.append(f"%{created_by}%")

    if created_from:
        filters.append("s.created_at >= %s")
        params.append(created_from)

    if created_to:
        filters.append("s.created_at <= %s")
        params.append(created_to)

    where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""
    offset = (page - 1) * page_size

    # Count Query
    count_query = f"""
        SELECT COUNT(DISTINCT s.id)
        FROM {SCHEMA}.{SESSION_TABLE} s
        LEFT JOIN {SCHEMA}.{JOB_TABLE} j
            ON j.id = s.job_id
        LEFT JOIN {SCHEMA}.{DOCUMENT_TABLE} d
            ON d.id IN (
                SELECT value::bigint
                FROM jsonb_array_elements_text(
                    COALESCE(j.document_id, '[]'::jsonb)
                ) AS items(value)
            )
        {where_clause}
    """

    total_rows = select_db(count_query, params if params else None)
    total = total_rows[0][0] if total_rows and total_rows[0] else 0

    # Main Query
    query = f"""
        SELECT
            s.id,
            s.session_id,
            s.job_id,
            j.job_name,
            s.reconcile_status,
            s.reason_for_failure,
            s.created_at,
            s.created_by,
            s.updated_at,
            s.documents,
            s.extracted_fields,
            COALESCE(
                ARRAY_REMOVE(
                    ARRAY_AGG(DISTINCT d.document_name ORDER BY d.document_name),
                    NULL
                ),
                ARRAY[]::text[]
            ) AS document_names,
            rd.document_name AS reference_document_name
        FROM {SCHEMA}.{SESSION_TABLE} s
        LEFT JOIN {SCHEMA}.{JOB_TABLE} j
            ON j.id = s.job_id
        LEFT JOIN {SCHEMA}.{DOCUMENT_TABLE} d
            ON d.id IN (
                SELECT value::bigint
                FROM jsonb_array_elements_text(
                    COALESCE(j.document_id, '[]'::jsonb)
                ) AS items(value)
            )
        LEFT JOIN {SCHEMA}.{DOCUMENT_TABLE} rd
            ON rd.id = j.reference_document_id
        {where_clause}
        GROUP BY
            s.id,
            s.session_id,
            s.job_id,
            j.job_name,
            s.reconcile_status,
            s.reason_for_failure,
            s.created_at,
            s.created_by,
            s.updated_at,
            s.documents,
            s.extracted_fields,
            rd.document_name
        ORDER BY s.created_at DESC
        LIMIT %s OFFSET %s
    """

    query_params = list(params)
    query_params.extend([page_size, offset])
    rows = select_db(query, query_params)

    results = []
    for row in rows:
        doc_names = list(row[11]) if row[11] is not None else []
        ref_doc_name = row[12]

        # Merge reference document name into document_names if not already present
        if ref_doc_name and ref_doc_name not in doc_names:
            doc_names.append(ref_doc_name)

        results.append(
            {
                "id": row[0],
                "session_id": row[1],
                "job_id": row[2],
                "job_name": row[3],
                "reconcile_status": row[4],
                "reason_for_failure": row[5],
                "created_at": row[6].isoformat() if row[6] else None,
                "created_by": row[7],
                "updated_at": row[8].isoformat() if row[8] else None,
                "documents": row[9] if row[9] is not None else {},
                "extracted_fields": row[10] if row[10] is not None else {},
                "document_names": doc_names,
                "reference_document": ref_doc_name
            }
        )

    total_pages = (total + page_size - 1) // page_size if total else 0

    return {
        "statusCode": 200,
        "body": {
            "sessions": results,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1,
            },
        },
    }


def get_session_documents(session_id: str):
    """
    Given a session_id, return s3:// URIs for all documents,
    their original filenames, and the associated job id + job name.
    """
    query = f"""
        SELECT
            s.session_id,
            s.job_id,
            j.job_name,
            s.documents
        FROM {SCHEMA}.{SESSION_TABLE} s
        LEFT JOIN {SCHEMA}.{JOB_TABLE} j
            ON j.id = s.job_id
        WHERE s.session_id = %s
        LIMIT 1
    """
    rows = select_db(query, (session_id,))

    if not rows or not rows[0]:
        return {
            "statusCode": 404,
            "body": {"message": "Session not found"}
        }

    row = rows[0]
    fetched_session_id = row[0]
    job_id             = row[1]
    job_name           = row[2]
    documents          = row[3] if row[3] is not None else {}

    # documents = { "invoice": "s3://bucket/path/file.pdf", ... }
    s3_uris = {}
    original_names = {}

    for doc_type, s3_uri in documents.items():
        try:
            if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
                s3_uris[doc_type] = None
                original_names[doc_type] = None
                continue

            without_scheme = s3_uri[len("s3://"):]
            bucket, key = without_scheme.split("/", 1)

            original_names[doc_type] = key.split("/")[-1]
            s3_uris[doc_type] = s3_uri
        except Exception as e:
            print(f"Failed to parse S3 URI for {doc_type}: {e}")
            s3_uris[doc_type] = None
            original_names[doc_type] = None

    return {
        "statusCode": 200,
        "body": {
            "session_id": fetched_session_id,
            "job_id": job_id,
            "job_name": job_name,
            "s3_uris": s3_uris,
            "original_names": original_names,
        }
    }

def test_session_removal(session_id):
    """
    Delete session_id from session_table only.
    Used after "test existing SOP" runs without sub_flag — those rows land in session_table, not session_table_test.
    """
    query = f"DELETE FROM {SCHEMA}.{SESSION_TABLE} WHERE session_id = %s"
    return execute_db(query, (session_id,))