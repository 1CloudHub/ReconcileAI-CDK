

import os
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from strands import Agent, tool
from strands.models import BedrockModel
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging
import random



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sap_agent.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()


DB_HOST = os.environ.get("db_host")
DB_PORT = int(os.environ.get("db_port", 5432))
DB_USER = os.environ.get("db_user")
DB_PASSWORD = os.environ.get("db_password")
DB_DATABASE = os.environ.get("db_database")

logger.info(f"Attempting to connect to database: {DB_HOST}:{DB_PORT}/{DB_DATABASE}")
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    dbname=DB_DATABASE
)
logger.info("Database connection established successfully")
cursor = conn.cursor()

logger.info("ABOUT TO INSERT")

test_query = """INSERT INTO "data".invoice (
    invoice_number, po_number, invoiced_quantity, unit_price, 
    invoice_date, currency, created_at, updated_at, exception_id
)
VALUES
('INV-001', 'PO-1001', 120, 55.75, '2025-10-10', 'INR', '2025-10-10 11:23:45.123', '2025-10-11 09:30:12.456', 'EXC-001-0001'),
('INV-002', 'PO-1002', 250, 32.40, '2025-10-12', 'USD', '2025-10-12 14:15:25.789', '2025-10-13 10:05:34.321', 'EXC-002-0001'),
('INV-003', 'PO-1003', 90, 78.90, '2025-10-14', 'EUR', '2025-10-14 09:50:15.222', '2025-10-14 17:10:55.847', 'EXC-003-0001'),
('INV-004', 'PO-1004', 310, 45.00, '2025-10-15', 'INR', '2025-10-15 13:40:32.654', '2025-10-16 08:20:11.943', 'EXC-004-0001'),
('INV-005', 'PO-1005', 175, 28.60, '2025-10-16', 'GBP', '2025-10-16 15:05:27.198', '2025-10-17 10:42:59.881', 'EXC-005-0001');
 """


fetch_query = '''SELECT SUM(records_processed) FROM "public".exceptions;'''

logger.info("""INSERT INTO "data".invoice (
    invoice_number, po_number, invoiced_quantity, unit_price, 
    invoice_date, currency, created_at, updated_at, exception_id
)
VALUES
('INV-001', 'PO-1001', 120, 55.75, '2025-10-10', 'INR', '2025-10-10 11:23:45.123', '2025-10-11 09:30:12.456', 'EXC-001-0001'),
('INV-002', 'PO-1002', 250, 32.40, '2025-10-12', 'USD', '2025-10-12 14:15:25.789', '2025-10-13 10:05:34.321', 'EXC-002-0001'),
('INV-003', 'PO-1003', 90, 78.90, '2025-10-14', 'EUR', '2025-10-14 09:50:15.222', '2025-10-14 17:10:55.847', 'EXC-003-0001'),
('INV-004', 'PO-1004', 310, 45.00, '2025-10-15', 'INR', '2025-10-15 13:40:32.654', '2025-10-16 08:20:11.943', 'EXC-004-0001'),
('INV-005', 'PO-1005', 175, 28.60, '2025-10-16', 'GBP', '2025-10-16 15:05:27.198', '2025-10-17 10:42:59.881', 'EXC-005-0001');
 """)


ll = cursor.execute("""INSERT INTO "data".invoice (
    invoice_number, po_number, invoiced_quantity, unit_price, 
    invoice_date, currency, created_at, updated_at, exception_id
)
VALUES
('INV-001', 'PO-1001', 120, 55.75, '2025-10-10', 'INR', '2025-10-10 11:23:45.123', '2025-10-11 09:30:12.456', 'EXC-001-0001'),
('INV-002', 'PO-1002', 250, 32.40, '2025-10-12', 'USD', '2025-10-12 14:15:25.789', '2025-10-13 10:05:34.321', 'EXC-002-0001'),
('INV-003', 'PO-1003', 90, 78.90, '2025-10-14', 'EUR', '2025-10-14 09:50:15.222', '2025-10-14 17:10:55.847', 'EXC-003-0001'),
('INV-004', 'PO-1004', 310, 45.00, '2025-10-15', 'INR', '2025-10-15 13:40:32.654', '2025-10-16 08:20:11.943', 'EXC-004-0001'),
('INV-005', 'PO-1005', 175, 28.60, '2025-10-16', 'GBP', '2025-10-16 15:05:27.198', '2025-10-17 10:42:59.881', 'EXC-005-0001');
 """)

logger.info(f"INSERTED: {ll}")


print(ll)