import json
import utils
from db import select_db, insert_db , update_db
from datetime import datetime
from zoneinfo import ZoneInfo


SOP_SCHEMA = "erp_no_sap"
SOP_TABLE = "sop_table"
SOP_SEQ = f"{SOP_SCHEMA}.sop_exception_seq"
SESSION_TABLE = "session_table"
SOP_TABLE_TEST = "sop_table_test"
SESSION_TABLE_TEST = "session_table_test"


def add_sop(exception_name: str, job_ids: list, filenames: list, created_by: str, test_sop: bool = False):
    """
    Add a new SOP record.
    - Generates exception_id via sequence
    - Generates presigned upload URLs for multiple files
    - Inserts into sop_table (or sop_table_test if test_sop=True)
    """
    try:
        # Check for duplicate exception_name
        if not test_sop:
            existing = select_db(
                f"SELECT id FROM {SOP_SCHEMA}.{SOP_TABLE} WHERE exception_name = %s AND COALESCE(delete_status, FALSE) = FALSE",
                (exception_name.strip(),)
            )
            if existing:
                return {"statusCode": 409, "message": f"SOP with name '{exception_name}' already exists"}

        # Step 1: Get next sequence value → build exception_id
        seq_result = select_db(f"SELECT nextval('{SOP_SEQ}')")
        next_num = seq_result[0][0]
        exception_id = f"EXC-{next_num:03d}"

        # Step 2: Generate presigned upload URLs for each file
        presigned_list = []
        s3_path_list = []
        for i, filename in enumerate(filenames, start=1):
            presigned = utils.generate_presigned_upload_url(
                exception_id, filename,
                reconcile_type="sop_test" if test_sop else "sop_control"
            )
            presigned_list.append({
                "filename": filename,
                "upload_url": presigned["upload_url"],
                "s3_key": presigned["s3_key"],
                "expiry": presigned["expiry"],
            })
            s3_path_list.append({
                "s3_uri": presigned["s3_uri"],
                "document_delete_status": False,
                "active_status": True,
                "version": i,
                "uploaded_at": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
            })

        # Step 3: Insert into sop_table or sop_table_test
        target_table = SOP_TABLE_TEST if test_sop else SOP_TABLE
        query = f"""
            INSERT INTO {SOP_SCHEMA}.{target_table} (
                exception_id,
                exception_name,
                job_id,
                s3_path,
                created_by,
                updated_by
            )
            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s)
            RETURNING id, exception_id
        """
        values = (
            exception_id,
            exception_name.strip(),
            json.dumps(job_ids),
            json.dumps(s3_path_list),
            created_by,
            created_by,
        )
        row = select_db(query, values)

        return {
            "statusCode": 200,
            "body": {
                "id": row[0][0],
                "exception_id": row[0][1],
                "exception_name": exception_name,
                "s3_path": s3_path_list,
                "upload_urls": presigned_list,
                "job_id": job_ids,
                "created_by": created_by,
            }
        }

    except Exception as e:
        print(f"Failed to add SOP: {e}")
        return {"statusCode": 500, "message": "Failed to add SOP"}

def update_sop(sop_id: int, updated_by: str, exception_name: str = None, job_ids: list = None, filename: str = None):
    try:
        set_clauses = []

        if exception_name is not None:
            # Duplicate name check
            duplicate = select_db(
                f"SELECT id FROM {SOP_SCHEMA}.{SOP_TABLE} WHERE LOWER(exception_name) = LOWER(%s) AND COALESCE(delete_status, FALSE) = FALSE AND id != %s",
                (exception_name.strip(), sop_id)
            )
            if duplicate:
                return {"statusCode": 409, "message": f"SOP with name '{exception_name}' already exists"}

            set_clauses.append(f"exception_name = '{exception_name.strip()}'")

        if job_ids is not None:
            set_clauses.append(f"job_id = '{json.dumps(job_ids)}'::jsonb")

        presigned = None
        if filename is not None:
            # Fetch existing exception_id and s3_path
            existing = select_db(
                f"SELECT exception_id, s3_path FROM {SOP_SCHEMA}.{SOP_TABLE} WHERE id = %s AND COALESCE(delete_status, FALSE) = FALSE",
                (sop_id,)
            )
            if not existing:
                return {"statusCode": 404, "message": "SOP not found"}

            exception_id = existing[0][0]
            current_s3_path = existing[0][1] or []

            # Generate presigned URL for new file
            presigned = utils.generate_presigned_upload_url(exception_id, filename)
            new_version = len(current_s3_path) + 1

            # Append new file to existing list
            current_s3_path.append({
                "s3_uri": presigned["s3_uri"],
                "document_delete_status": False,
                "active_status": False,
                "version": new_version,
                "uploaded_at": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
            })

            set_clauses.append(f"s3_path = '{json.dumps(current_s3_path)}'::jsonb")

        if not set_clauses:
            return {"statusCode": 400, "message": "No fields to update"}

        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        set_clauses.append(f"updated_by = '{updated_by}'")

        query = f"""
            UPDATE {SOP_SCHEMA}.{SOP_TABLE}
            SET {', '.join(set_clauses)}
            WHERE id = {sop_id}
              AND COALESCE(delete_status, FALSE) = FALSE
        """
        update_db(query)

        response = {
            "statusCode": 200,
            "body": {
                "id": sop_id,
                "updated_by": updated_by,
            }
        }

        if exception_name is not None:
            response["body"]["exception_name"] = exception_name
        if job_ids is not None:
            response["body"]["job_id"] = job_ids
        if presigned is not None:
            response["body"]["upload_url"] = presigned["upload_url"]
            response["body"]["s3_key"] = presigned["s3_key"]
            response["body"]["s3_path"] = current_s3_path
            response["body"]["expiry"] = presigned["expiry"]

        return response

    except Exception as e:
        print(f"Failed to update SOP: {e}")
        return {"statusCode": 500, "message": "Failed to update SOP"}

def delete_sop(sop_id: int, updated_by: str, s3_uri: str = None, version: int = None, test_sop: bool = False, exception_name: str = None):
    """
    - If s3_uri + version provided: soft delete that specific document version
    - If no s3_uri/version: soft delete the entire SOP and all its documents
    - If test_sop=True: soft deletes entire row in sop_table_test by exception_name
    """
    try:
        target_table = SOP_TABLE_TEST if test_sop else SOP_TABLE

        # ── Test mode: fetch and delete by exception_name
        if test_sop:
            existing = select_db(
                f"SELECT id, s3_path FROM {SOP_SCHEMA}.{target_table} WHERE exception_name = %s AND COALESCE(delete_status, FALSE) = FALSE",
                (exception_name.strip(),)
            )
            if not existing:
                return {"statusCode": 404, "message": "SOP test not found or already deleted"}

            s3_path = existing[0][1] or []
            for doc in s3_path:
                doc["document_delete_status"] = True

            update_db(
                f"""
                UPDATE {SOP_SCHEMA}.{target_table}
                SET
                    delete_status = TRUE,
                    s3_path = '{json.dumps(s3_path)}'::jsonb,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = '{updated_by}'
                WHERE exception_name = '{exception_name.strip()}'
                  AND COALESCE(delete_status, FALSE) = FALSE
                """
            )
            return {
                "statusCode": 200,
                "body": {
                    "exception_name": exception_name,
                    "message": "SOP test deleted successfully"
                }
            }

        existing = select_db(
            f"SELECT id, s3_path FROM {SOP_SCHEMA}.{target_table} WHERE id = %s AND COALESCE(delete_status, FALSE) = FALSE",
            (sop_id,)
        )
        if not existing:
            return {"statusCode": 404, "message": "SOP not found or already deleted"}

        s3_path = existing[0][1] or []

        if s3_uri and version is not None:
            # Soft delete a single document version
            found = False
            for doc in s3_path:
                if doc.get("s3_uri") == s3_uri and doc.get("version") == version:
                    doc["document_delete_status"] = True
                    found = True
                    break

            if not found:
                return {"statusCode": 404, "message": f"Document with s3_uri and version {version} not found in SOP"}

            update_db(
                f"""
                UPDATE {SOP_SCHEMA}.{target_table}
                SET
                    s3_path = '{json.dumps(s3_path)}'::jsonb,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = '{updated_by}'
                WHERE id = {sop_id}
                  AND COALESCE(delete_status, FALSE) = FALSE
                """
            )

            return {
                "statusCode": 200,
                "body": {
                    "id": sop_id,
                    "deleted_document": s3_uri,
                    "version": version,
                    "message": "Document version deleted successfully"
                }
            }

        else:
            # Soft delete entire SOP and all documents
            for doc in s3_path:
                doc["document_delete_status"] = True

            update_db(
                f"""
                UPDATE {SOP_SCHEMA}.{target_table}
                SET
                    delete_status = TRUE,
                    s3_path = '{json.dumps(s3_path)}'::jsonb,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = '{updated_by}'
                WHERE id = {sop_id}
                  AND COALESCE(delete_status, FALSE) = FALSE
                """
            )

            return {
                "statusCode": 200,
                "body": {
                    "id": sop_id,
                    "message": "SOP deleted successfully"
                }
            }

    except Exception as e:
        print(f"Failed to delete SOP: {e}")
        return {"statusCode": 500, "message": "Failed to delete SOP"}

def list_sops(page: int = 1, page_size: int = 10, exception_name: str = None, job_id: int = None, created_at: str = None, s3_uri: str = None):
    try:
        offset = (page - 1) * page_size

        filters = ["COALESCE(s.delete_status, FALSE) = FALSE"]

        if exception_name:
            filters.append(f"s.exception_name ILIKE '%{exception_name}%'")

        if job_id is not None:                                           # fix 2
            filters.append(f"s.job_id::jsonb @> '[{job_id}]'::jsonb")  # fix 1

        if created_at:
            filters.append(f"(s.created_at AT TIME ZONE 'Asia/Kolkata')::date = '{created_at}'")

        if s3_uri:
            filters.append(
                f"EXISTS ("
                f"SELECT 1 FROM jsonb_array_elements(COALESCE(s.s3_path::jsonb, '[]'::jsonb)) AS doc "
                f"WHERE doc->>'s3_uri' ILIKE '%{s3_uri}%'"
                f")"
            )

        where_clause = " AND ".join(filters)

        count_result = select_db(
            f"SELECT COUNT(*) FROM {SOP_SCHEMA}.{SOP_TABLE} s WHERE {where_clause}"
        )
        total_count = count_result[0][0]
        total_pages = (total_count + page_size - 1) // page_size

        query = f"""
            SELECT
                s.id,
                s.exception_id,
                s.exception_name,
                s.s3_path,
                s.job_id,
                s.created_at,
                s.created_by,
                s.updated_at,
                s.updated_by,
                COALESCE(
                    ARRAY_REMOVE(ARRAY_AGG(j.job_name ORDER BY j.job_name), NULL),
                    ARRAY[]::text[]
                ) AS job_names
            FROM {SOP_SCHEMA}.{SOP_TABLE} s
            LEFT JOIN LATERAL (                                          -- fix 3
                SELECT value::bigint AS jid
                FROM jsonb_array_elements_text(
                    COALESCE(s.job_id::jsonb, '[]'::jsonb)
                ) AS items(value)
            ) ids ON true
            LEFT JOIN {SOP_SCHEMA}.job_table j
                ON j.id = ids.jid AND j.delete_status = false
            WHERE {where_clause}
            GROUP BY
                s.id, s.exception_id, s.exception_name, s.s3_path,
                s.job_id, s.created_at, s.created_by, s.updated_at, s.updated_by
            ORDER BY s.created_at DESC
            LIMIT {page_size} OFFSET {offset}
        """
        rows = select_db(query)

        results = []
        for row in rows:
            s3_path = row[3] or []
            active_documents = [
                doc for doc in s3_path
                if not doc.get("document_delete_status", False)
            ]
            results.append({
                "id": row[0],
                "exception_id": row[1],
                "exception_name": row[2],
                "s3_path": active_documents,
                "job_id": row[4] if row[4] is not None else [],
                "job_names": row[9] if row[9] is not None else [],
                "created_at": row[5].isoformat() if row[5] else None,
                "created_by": row[6],
                "updated_at": row[7].isoformat() if row[7] else None,
                "updated_by": row[8],
            })

        return {
            "statusCode": 200,
            "body": {
                "data": results,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": total_count,
                    "total_pages": total_pages,
                }
            }
        }

    except Exception as e:
        print(f"Failed to list SOPs: {e}")
        return {"statusCode": 500, "message": "Failed to list SOPs"}

def set_active_sop_document(sop_id: int, version: int):
    try:
        # Fetch existing s3_path array
        existing = select_db(
            f"SELECT s3_path FROM {SOP_SCHEMA}.{SOP_TABLE} WHERE id = %s AND COALESCE(delete_status, FALSE) = FALSE",
            (sop_id,)
        )
        if not existing:
            return {"statusCode": 404, "message": "SOP not found"}

        current_s3_path = existing[0][0] or []

        # Check target version exists
        target = next((doc for doc in current_s3_path if doc.get("version") == version), None)
        if not target:
            return {"statusCode": 404, "message": f"Document with version {version} not found"}

        # Deactivate all, then activate the selected one
        for doc in current_s3_path:
            doc["active_status"] = False
        target["active_status"] = True

        # Write back
        update_db(
            f"UPDATE {SOP_SCHEMA}.{SOP_TABLE} SET s3_path = '{json.dumps(current_s3_path)}'::jsonb, updated_at = CURRENT_TIMESTAMP WHERE id = {sop_id} AND COALESCE(delete_status, FALSE) = FALSE"
        )

        return {
            "statusCode": 200,
            "body": {
                "sop_id": sop_id,
                "active_version": version,
                "s3_path": current_s3_path
            }
        }

    except Exception as e:
        print(f"Failed to set active SOP document: {e}")
        return {"statusCode": 500, "message": "Failed to set active SOP document"}

def document_poll_status(session_id: str, use_session_table: bool = False):
    """
    Poll reconciliation status for a session.

    use_session_table:
      False (default) — session_table_test (new SOP test, new version test; sub_flag=testing).
      True — session_table (existing SOP test without sub_flag; also normal reconcile agent runs).
    """
    try:
        if not session_id or not str(session_id).strip():
            return {"statusCode": 400, "message": "session_id is required"}

        sid = str(session_id).strip()
        table = SESSION_TABLE if use_session_table else SESSION_TABLE_TEST

        result = select_db(
            f"""
            SELECT reconcile_status, reason_for_failure, extracted_fields
            FROM {SOP_SCHEMA}.{table}
            WHERE session_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (sid,),
        )

        if not result:
            return {"statusCode": 404, "message": f"No session found for session_id: {session_id}"}

        reconcile_status = result[0][0]
        reason_for_failure_raw = result[0][1]
        extracted_fields_raw = result[0][2]

        # ── Still running
        if reconcile_status == "processing":
            return {
                "statusCode": 200,
                "body": {
                    "session_id": session_id,
                    "reconcile_status": "processing",
                    "message": "Reconciliation is in progress",
                },
            }

        def parse_json_maybe(raw):
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    return raw
            return raw

        reason_for_failure = parse_json_maybe(reason_for_failure_raw)
        extracted_fields = parse_json_maybe(extracted_fields_raw)

        # ── Completed (yes = no failures, no = has failures)
        if reconcile_status in ("yes", "no", "failed", "Validation Failed"):
            return {
                "statusCode": 200,
                "body": {
                    "session_id": session_id,
                    "reconcile_status": reconcile_status,
                    "reason_for_failure": reason_for_failure,
                    "extracted_fields": extracted_fields,
                },
            }

        # ── Unexpected value
        return {
            "statusCode": 200,
            "body": {
                "session_id": session_id,
                "reconcile_status": reconcile_status,
                "reason_for_failure": reason_for_failure,
                "extracted_fields": extracted_fields,
                "message": f"Unknown status: {reconcile_status}",
            },
        }

    except Exception as e:
        print(f"[document_poll_status] Error: {e}")
        return {"statusCode": 500, "message": "Failed to fetch reconciliation status"}