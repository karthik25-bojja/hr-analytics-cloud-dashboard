"""
AWS S3 Cloud Storage Manager for HR Analytics Project.
Handles bucket creation, uploading raw datasets to AWS S3,
listing objects, and downloading processed results.
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class S3Manager:
    def __init__(self, bucket_name: Optional[str] = None, region: Optional[str] = None):
        self.bucket_name = bucket_name or config.S3_BUCKET_NAME
        self.region = region or config.AWS_REGION
        self.s3_client = None
        self._init_client()

    def _init_client(self):
        """Initializes the boto3 S3 client using environment or configured credentials."""
        if not BOTO3_AVAILABLE:
            return

        try:
            if config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
                self.s3_client = boto3.client(
                    "s3",
                    region_name=self.region,
                    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
                    aws_session_token=config.AWS_SESSION_TOKEN if config.AWS_SESSION_TOKEN else None
                )
            else:
                # Fallback to default AWS credential chain (~/.aws/credentials or IAM role)
                self.s3_client = boto3.client("s3", region_name=self.region)
        except Exception:
            self.s3_client = None

    def is_connected(self) -> bool:
        """Checks if S3 client is authenticated and can communicate with AWS."""
        if not self.s3_client:
            return False
        try:
            self.s3_client.list_buckets()
            return True
        except Exception:
            return False

    def create_bucket_if_not_exists(self) -> bool:
        """Creates the target S3 bucket if it does not already exist."""
        if not self.s3_client:
            return False
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"[*] S3 Bucket '{self.bucket_name}' already exists.")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code in ["404", "NoSuchBucket"]:
                try:
                    print(f"[*] Creating S3 Bucket '{self.bucket_name}' in region '{self.region}'...")
                    if self.region == "us-east-1":
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={"LocationConstraint": self.region}
                        )
                    print(f"[OK] Successfully created bucket '{self.bucket_name}'.")
                    return True
                except Exception as ce:
                    print(f"[!] Failed to create bucket: {ce}")
                    return False
            else:
                print(f"[!] Bucket access error: {e}")
                return False

    def upload_file(self, local_file_path: Path, s3_key: str) -> bool:
        """Uploads a single local file to S3."""
        if not self.s3_client:
            print("[!] S3 client is not connected.")
            return False

        try:
            print(f"[*] Uploading '{local_file_path.name}' to s3://{self.bucket_name}/{s3_key}...")
            self.s3_client.upload_file(str(local_file_path), self.bucket_name, s3_key)
            print(f"[OK] Upload completed: s3://{self.bucket_name}/{s3_key}")
            return True
        except Exception as e:
            print(f"[!] Failed to upload {local_file_path.name}: {e}")
            return False

    def upload_raw_datasets(self) -> Dict[str, bool]:
        """Uploads all raw CSV datasets to AWS S3 under data/raw/."""
        results = {}
        files_to_upload = [
            (config.RAW_DATA_DIR / config.EMPLOYEES_FILE, f"{config.S3_RAW_PREFIX}{config.EMPLOYEES_FILE}"),
            (config.RAW_DATA_DIR / config.ATTENDANCE_FILE, f"{config.S3_RAW_PREFIX}{config.ATTENDANCE_FILE}"),
            (config.RAW_DATA_DIR / config.ATTENDANCE_SUMMARY_FILE, f"{config.S3_RAW_PREFIX}{config.ATTENDANCE_SUMMARY_FILE}"),
            (config.RAW_DATA_DIR / config.LEAVES_FILE, f"{config.S3_RAW_PREFIX}{config.LEAVES_FILE}"),
        ]

        if not self.is_connected():
            print("[!] AWS S3 is not connected or credentials not provided.")
            return {f[0].name: False for f in files_to_upload}

        self.create_bucket_if_not_exists()

        for local_path, s3_key in files_to_upload:
            if local_path.exists():
                results[local_path.name] = self.upload_file(local_path, s3_key)
            else:
                print(f"[!] Local file not found: {local_path}")
                results[local_path.name] = False

        return results

    def list_s3_files(self, prefix: str = "") -> List[Dict[str, Any]]:
        """Lists files inside the S3 bucket with given prefix."""
        if not self.s3_client:
            return []
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            items = []
            if "Contents" in response:
                for obj in response["Contents"]:
                    items.append({
                        "key": obj["Key"],
                        "size_bytes": obj["Size"],
                        "size_kb": round(obj["Size"] / 1024, 2),
                        "last_modified": obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
                    })
            return items
        except Exception as e:
            print(f"[!] Error listing objects from S3: {e}")
            return []

    def download_file(self, s3_key: str, local_destination: Path) -> bool:
        """Downloads a file from S3 to local storage."""
        if not self.s3_client:
            return False
        try:
            local_destination.parent.mkdir(parents=True, exist_ok=True)
            self.s3_client.download_file(self.bucket_name, s3_key, str(local_destination))
            return True
        except Exception as e:
            print(f"[!] Failed to download s3://{self.bucket_name}/{s3_key}: {e}")
            return False


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AWS S3 Cloud Storage Manager")
    parser.add_argument("--upload", action="store_true", help="Upload raw HR datasets to S3")
    parser.add_argument("--list", action="store_true", help="List files in S3 bucket")
    parser.add_argument("--bucket", type=str, default=config.S3_BUCKET_NAME, help="S3 Bucket Name")
    parser.add_argument("--region", type=str, default=config.AWS_REGION, help="AWS Region")

    args = parser.parse_args()
    mgr = S3Manager(bucket_name=args.bucket, region=args.region)

    print(f"Connecting to AWS S3 (Bucket: {args.bucket}, Region: {args.region})...")
    connected = mgr.is_connected()
    print(f"AWS S3 Connection Status: {'CONNECTED [✓]' if connected else 'DISCONNECTED [!] (Check AWS credentials)'}")

    if args.upload:
        if connected:
            mgr.upload_raw_datasets()
        else:
            print("Please configure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to upload.")

    if args.list:
        if connected:
            objects = mgr.list_s3_files()
            print(f"\nFiles in s3://{args.bucket}/:")
            for o in objects:
                print(f" - {o['key']} ({o['size_kb']} KB) - Last modified: {o['last_modified']}")
        else:
            print("Cannot list files: AWS S3 not connected.")
