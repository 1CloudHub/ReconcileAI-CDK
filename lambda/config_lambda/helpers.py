"""
Helper functions for Lambda: PDF/images, S3, encoding.
All functions are stateless and take required clients/config as arguments.
"""
import base64
import io

import fitz  # PyMuPDF
from PIL import Image


def pdf_to_base64_images(s3_client, bucket_name, s3_path):
    """Download PDF from S3, convert each page to base64 PNG. Returns list of base64 strings."""
    print("PDF TO BASE64 CALLED")
    print("BUCKET_NAME", bucket_name)
    print("S3 PATHHHH", s3_path)
    pdf_obj = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
    pdf_bytes = pdf_obj["Body"].read()
    doc = fitz.open("pdf", pdf_bytes)
    base64_images = []
    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)
        pix = page.get_pixmap()
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        img_buffer = io.BytesIO()
        img.save(img_buffer, format="PNG")
        img_buffer.seek(0)
        img_base64 = base64.b64encode(img_buffer.read()).decode("utf-8")
        base64_images.append(img_base64)
    return base64_images


def encode_image_to_base64(s3_client, bucket_name, doc_type, doc_id, file_extension):
    """Fetch image from S3 and return base64-encoded string."""
    key = f"era_demo/{doc_type}/INPUT/{doc_id}.{file_extension}"
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    image_data = response["Body"].read()
    return base64.b64encode(image_data).decode("utf-8")


def create_s3_folder(s3_client, bucket_name, folder_name):
    """Create a folder (prefix) in the S3 bucket."""
    s3_client.put_object(Bucket=bucket_name, Key=f"{folder_name}/")
    print(f"Folder '{folder_name}' created in bucket '{bucket_name}'.")
