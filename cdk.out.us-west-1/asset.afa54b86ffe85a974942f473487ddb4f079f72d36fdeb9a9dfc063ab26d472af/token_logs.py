from db import select_db
from zoneinfo import ZoneInfo
import json
SOP_SCHEMA = "erp_no_sap"


def token_usage_listing(page: int = 1, page_size: int = 10, module: str = None, type: str = None, start_date: str = None, end_date: str = None):
    try:
        offset = (page - 1) * page_size

        filters = []

        if module:
            filters.append(f"module ILIKE '%{module}%'")

        if type:
            filters.append(f"type ILIKE '%{type}%'")

        if start_date:
            filters.append(f"(created_at AT TIME ZONE 'Asia/Kolkata')::date >= '{start_date}'")

        if end_date:
            filters.append(f"(created_at AT TIME ZONE 'Asia/Kolkata')::date <= '{end_date}'")

        where_clause = " AND ".join(filters) if filters else "1=1"

        count_result = select_db(
            f"SELECT COUNT(*) FROM {SOP_SCHEMA}.token_log_table WHERE {where_clause}"
        )
        total_count = count_result[0][0]
        total_pages = (total_count + page_size - 1) // page_size

        query = f"""
            SELECT
                id,
                module,
                type,
                document_name,
                input_tokens,
                output_tokens,
                total_tokens,
                created_by,
                created_at
            FROM {SOP_SCHEMA}.token_log_table
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT {page_size} OFFSET {offset}
        """
        rows = select_db(query)

        results = []
        for row in rows:

            document_name_raw = row[3]
            if isinstance(document_name_raw, str):
                try:
                    document_name = json.loads(document_name_raw)
                except (json.JSONDecodeError, TypeError):
                    document_name = document_name_raw
            else:
                document_name = document_name_raw or []

            results.append({
                "id": row[0],
                "module": row[1],
                "type": row[2],
                "document_name": document_name,
                "input_tokens": row[4],
                "output_tokens": row[5],
                "total_tokens": row[6],
                "created_by": row[7],
                "created_at": row[8].isoformat() if row[8] else None,
            })
        
        print("resultsssss", results)

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
        print(f"Failed to list token usage: {e}")
        return {"statusCode": 500, "message": "Failed to list token usage"}
            