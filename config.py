"""
Configuration module for Human Resource Analytics Dashboard Using Cloud Storage.
Manages AWS S3 credentials, bucket settings, and local/cloud paths.
"""

import os
from pathlib import Path

# Default placeholder values for local development; override through environment variables
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-2")

# Base Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Dataset file names
EMPLOYEES_FILE = "employees.csv"
ATTENDANCE_FILE = "attendance.csv"
ATTENDANCE_SUMMARY_FILE = "attendance_summary.csv"
LEAVES_FILE = "leaves.csv"

# AWS Configuration (Can be overridden by environment variables or .env file)
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-2")  # Default to Hyderabad (ap-south-2)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")

# Default S3 Bucket Name
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "hr-analytics-cloud-storage-bda")
S3_RAW_PREFIX = "data/raw/"
S3_PROCESSED_PREFIX = "data/processed/"

# Check if AWS credentials are configured
def is_aws_configured() -> bool:
    """Returns True if AWS credentials or IAM role are likely configured."""
    return bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) or bool(os.getenv("AWS_ROLE_ARN"))

