import psycopg2
import os
import json
import urllib.request
import boto3


def send_cfn_response(event, context, status, reason=""):
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
    })
    req = urllib.request.Request(
        url=event["ResponseURL"],
        data=body.encode("utf-8"),
        method="PUT",
        headers={"Content-Type": ""}
    )
    urllib.request.urlopen(req)


def handler(event, context):
    print("Event:", json.dumps(event))

    if event.get("RequestType") == "Delete":
        send_cfn_response(event, context, "SUCCESS")
        return

    try:
        # ── Read SQL from S3 instead of env var ───────────────────────────────
        s3_client = boto3.client("s3")
        bucket = os.environ["SQL_BUCKET"]
        key = os.environ["SQL_KEY"]
        print(f"Reading SQL from s3://{bucket}/{key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        sql = obj["Body"].read().decode("utf-8")

        # ── Connect to RDS ────────────────────────────────────────────────────
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", "5432")),
            dbname=os.environ.get("DB_NAME", "postgres"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "postgres123"),
            connect_timeout=10,
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(sql)

        cur.close()
        conn.close()
        print("Schema initialized successfully")
        send_cfn_response(event, context, "SUCCESS")

    except Exception as e:
        print("Error:", str(e))
        send_cfn_response(event, context, "FAILED", reason=str(e))