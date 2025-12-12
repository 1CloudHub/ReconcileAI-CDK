import json
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import os

S3_BUCKET = os.environ.get("S3_BUCKET", "sap-mcp-data-server")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")


try:
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
except NoCredentialsError:
    s3_client = boto3.client('s3', region_name=AWS_REGION)


def download_file_from_s3(bucket_name, file_key, local_path):
    print(f"{bucket_name}/{file_key}")
    try:
        s3_client.download_file(bucket_name, file_key, local_path)
        print(f"File downloaded successfully: {local_path}")
    except ClientError as e:
        print(f"Failed to download file: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")




def fetch_s3_data(po_number: str) -> dict:
    os.makedirs("needed_doc", exist_ok=True)
    meta_data = [
  {
    "po": "4500000001",
    "grn": "5100000450",
    "invoice": ["90005001"]
  },
  {
    "po": "4500018901",
    "grn": "5100044444",
    "invoice": ["90011111"]
  },
  {
    "po": "4500020000",
    "grn": "5100055555",
    "invoice": ["90013333", "90014444"]
  },
  {
    "po": "4500012345",
    "grn": "5100011111",
    "invoice": ["90007777"]
  },
  {
    "po": "4500016789",
    "grn": "5100022222",
    "invoice": ["90008888"]
  },
    {
    "po": "4500017890",
    "grn": "5100033333",
    "invoice": ["90009999"]
  },
  {
    "po": None,
    "grn": "5100000525",
    "invoice": ["90012222"]
  }
    
]
    
    status_bundle = "pending"
    needed_ele = meta_data[-1]
    for i in meta_data:
        if i["po"] == po_number:
            needed_ele = i

    po_path = "Not available"
    if needed_ele["po"] != None:
        po_s3_key = f"data/data_json/PO_{needed_ele["po"]}.json"
        po_path = f"needed_doc/PO_{needed_ele["po"]}.json"
        download_file_from_s3(S3_BUCKET, po_s3_key, f"needed_doc/PO_{needed_ele["po"]}.json")
    gr_s3_key = f"data/data_json/GRN_{needed_ele["grn"]}.json"
    gr_path = f"needed_doc/GRN_{needed_ele["grn"]}.json"
    download_file_from_s3(S3_BUCKET, gr_s3_key, f"needed_doc/GRN_{needed_ele["grn"]}.json")


    invoice_path = []
    for i in needed_ele["invoice"]:
        in_s3_key = f"data/data_json/INV_{i}.json"
        op = f"needed_doc/INV_{i}.json"
        invoice_path.append(op)
        download_file_from_s3(S3_BUCKET, in_s3_key, f"needed_doc/INV_{i}.json")


    final_map = {"bundle_status": status_bundle, "purchase_order": po_path, "goods_receipt": gr_path, "invoice": invoice_path}

    with open("map.json", 'w') as f:
        json.dump(final_map, f)

    print("WWWWWWWWWWWWWWw", final_map)
    return final_map

fetch_s3_data("4500016789")