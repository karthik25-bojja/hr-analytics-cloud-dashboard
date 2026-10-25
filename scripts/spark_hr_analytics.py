"""
Apache Spark (PySpark) Data Processing Pipeline for HR Analytics.
Reads employee, attendance, and leave data from AWS S3 (or local path),
performs Spark SQL transformations, cleans data, computes KPIs, and
exports aggregated metrics for the HR Analytics Dashboard.
"""

import os
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

def run_pyspark_pipeline(s3_bucket: str = None, use_s3: bool = False):
    """
    Executes the PySpark data processing pipeline using Apache Spark DataFrame API & Spark SQL.
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, IntegerType, DateType
    except ImportError:
        print("[!] PySpark is not installed in the current Python environment.")
        return False

    print("=" * 60)
    print("  Starting Apache Spark (PySpark) HR Analytics Pipeline")
    print("=" * 60)

    # 1. Initialize SparkSession
    builder = SparkSession.builder \
        .appName("HR_Analytics_Cloud_Storage") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g")

    # If reading directly from S3 using s3a protocol
    if use_s3 and s3_bucket:
        print(f"[*] Configuring S3A Hadoop FileSystem for bucket: {s3_bucket}")
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        if config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
            builder = builder \
                .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID) \
                .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY)

    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print(f"[OK] SparkSession successfully initialized. Spark Version: {spark.version}")
    except Exception as e:
        print("\n" + "!" * 60)
        print("[!] Java runtime (JAVA_HOME) not found on this local machine.")
        print("[*] Note: Apache Spark requires Java (OpenJDK 11/17).")
        print("[*] On AWS EC2, run 'bash scripts/setup_ec2.sh' to install Java and PySpark.")
        print("[*] Automatically switching to local Big Data analytics engine (data_processor)...")
        print("!" * 60 + "\n")
        from scripts.data_processor import process_hr_data
        return process_hr_data()

    try:
        # Determine paths
        if use_s3 and s3_bucket:
            raw_base = f"s3a://{s3_bucket}/{config.S3_RAW_PREFIX}"
        else:
            raw_base = str(config.RAW_DATA_DIR).replace("\\", "/") + "/"

        emp_path = f"{raw_base}{config.EMPLOYEES_FILE}"
        att_path = f"{raw_base}{config.ATTENDANCE_FILE}"
        att_sum_path = f"{raw_base}{config.ATTENDANCE_SUMMARY_FILE}"
        leaves_path = f"{raw_base}{config.LEAVES_FILE}"

        print(f"[*] Reading datasets from: {raw_base}")

        # 2. Read Datasets into Spark DataFrames
        df_emp = spark.read.option("header", "true").option("inferSchema", "true").csv(emp_path)
        df_att = spark.read.option("header", "true").option("inferSchema", "true").csv(att_path)
        df_att_sum = spark.read.option("header", "true").option("inferSchema", "true").csv(att_sum_path)
        df_leaves = spark.read.option("header", "true").option("inferSchema", "true").csv(leaves_path)

        print(f"[OK] Employees Count: {df_emp.count()}")
        print(f"[OK] Attendance Records Count: {df_att.count()}")
        print(f"[OK] Leaves Records Count: {df_leaves.count()}")

        # 3. Data Cleaning & Feature Engineering
        # Register views for Spark SQL queries
        df_emp.createOrReplaceTempView("employees")
        df_att.createOrReplaceTempView("attendance")
        df_att_sum.createOrReplaceTempView("attendance_summary")
        df_leaves.createOrReplaceTempView("leaves")

        # Spark SQL: Enriched Employee Data
        df_emp_enriched = spark.sql("""
            SELECT 
                Employee_ID,
                Employee_Name,
                Gender,
                Date_of_Birth,
                Age,
                Email,
                Department,
                Job_Role,
                Joining_Date,
                Employment_Status,
                Resignation_Date,
                Monthly_Salary_INR,
                Annual_Salary_INR,
                Work_Mode,
                Performance_Rating,
                Training_Hours_Yearly,
                Attrition_Status,
                CASE WHEN Attrition_Status = 'Yes' THEN 1 ELSE 0 END AS Attrition_Flag,
                COALESCE(Reason_for_Leaving, 'N/A (Active)') AS Reason_for_Leaving,
                CASE 
                    WHEN Monthly_Salary_INR < 50000 THEN 'Entry (< 50K)'
                    WHEN Monthly_Salary_INR BETWEEN 50000 AND 100000 THEN 'Mid (50K - 100K)'
                    ELSE 'Senior (> 100K)'
                END AS Salary_Band,
                CASE 
                    WHEN Age < 30 THEN '< 30'
                    WHEN Age BETWEEN 30 AND 40 THEN '30 - 40'
                    WHEN Age BETWEEN 41 AND 50 THEN '41 - 50'
                    ELSE '> 50'
                END AS Age_Group
            FROM employees
        """)

        # 4. Compute Executive KPIs
        total_emp = df_emp_enriched.count()
        active_emp = df_emp_enriched.filter(F.col("Employment_Status") == "Active").count()
        resigned_emp = df_emp_enriched.filter(F.col("Attrition_Status") == "Yes").count()
        attrition_rate = round((resigned_emp / total_emp) * 100, 2) if total_emp > 0 else 0.0

        salary_stats = df_emp_enriched.select(
            F.round(F.avg("Monthly_Salary_INR"), 2).alias("avg_monthly_salary"),
            F.round(F.avg("Annual_Salary_INR"), 2).alias("avg_annual_salary"),
            F.round(F.avg("Training_Hours_Yearly"), 2).alias("avg_training_hours")
        ).collect()[0]

        att_stats = df_att_sum.select(
            F.round(F.avg("Attendance_Percentage"), 2).alias("avg_attendance_pct")
        ).collect()[0]

        kpis = {
            "total_employees": total_emp,
            "active_employees": active_emp,
            "resigned_employees": resigned_emp,
            "attrition_rate_pct": attrition_rate,
            "avg_monthly_salary_inr": salary_stats["avg_monthly_salary"],
            "avg_annual_salary_inr": salary_stats["avg_annual_salary"],
            "avg_training_hours": salary_stats["avg_training_hours"],
            "avg_attendance_pct": att_stats["avg_attendance_pct"]
        }

        # 5. Department Level Aggregations
        dept_metrics = df_emp_enriched.groupBy("Department").agg(
            F.count("Employee_ID").alias("total_employees"),
            F.sum("Attrition_Flag").alias("resigned_employees"),
            F.round((F.sum("Attrition_Flag") / F.count("Employee_ID")) * 100, 2).alias("attrition_rate_pct"),
            F.round(F.avg("Monthly_Salary_INR"), 2).alias("avg_monthly_salary_inr"),
            F.min("Monthly_Salary_INR").alias("min_monthly_salary_inr"),
            F.max("Monthly_Salary_INR").alias("max_monthly_salary_inr"),
            F.round(F.avg("Training_Hours_Yearly"), 2).alias("avg_training_hours")
        ).orderBy(F.desc("total_employees"))

        # 6. Attrition by Reason & Department
        attrition_reasons = df_emp_enriched.filter(F.col("Attrition_Status") == "Yes") \
            .groupBy("Reason_for_Leaving").count() \
            .withColumnRenamed("count", "resigned_count") \
            .orderBy(F.desc("resigned_count"))

        # 7. Performance & Training Analytics
        perf_metrics = df_emp_enriched.groupBy("Performance_Rating").agg(
            F.count("Employee_ID").alias("employee_count"),
            F.round(F.avg("Monthly_Salary_INR"), 2).alias("avg_monthly_salary_inr"),
            F.round(F.avg("Training_Hours_Yearly"), 2).alias("avg_training_hours")
        )

        # 8. Work Mode Distribution
        work_mode_metrics = df_emp_enriched.groupBy("Work_Mode").agg(
            F.count("Employee_ID").alias("employee_count"),
            F.sum("Attrition_Flag").alias("resigned_count"),
            F.round((F.sum("Attrition_Flag") / F.count("Employee_ID")) * 100, 2).alias("attrition_rate_pct")
        )

        # 9. Attendance & Leave Aggregations
        leaves_summary = df_leaves.groupBy("Leave_Type", "Approval_Status").agg(
            F.count("Leave_ID").alias("leave_count"),
            F.sum("Leave_Days").alias("total_leave_days")
        )

        # 10. Export results to processed directory
        out_dir = config.PROCESSED_DATA_DIR
        out_dir.mkdir(parents=True, exist_ok=True)

        with open(out_dir / "kpi_summary.json", "w") as f:
            json.dump(kpis, f, indent=4)

        df_emp_enriched.toPandas().to_csv(out_dir / "employees_enriched.csv", index=False)
        dept_metrics.toPandas().to_csv(out_dir / "department_metrics.csv", index=False)
        attrition_reasons.toPandas().to_csv(out_dir / "attrition_reasons.csv", index=False)
        perf_metrics.toPandas().to_csv(out_dir / "performance_metrics.csv", index=False)
        work_mode_metrics.toPandas().to_csv(out_dir / "work_mode_metrics.csv", index=False)
        leaves_summary.toPandas().to_csv(out_dir / "leaves_summary.csv", index=False)

        print("[OK] All aggregated metrics successfully computed and written to data/processed/")
        return True

    except Exception as e:
        print(f"[!] PySpark Pipeline Error: {e}")
        return False
    finally:
        if "spark" in locals() and spark:
            spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Apache Spark HR Analytics Pipeline")
    parser.add_argument("--s3", action="store_true", help="Read directly from AWS S3")
    parser.add_argument("--bucket", type=str, default=config.S3_BUCKET_NAME, help="AWS S3 Bucket Name")
    args = parser.parse_args()

    success = run_pyspark_pipeline(s3_bucket=args.bucket, use_s3=args.s3)
    if not success:
        print("[*] Note: To run without Java/Spark locally, the project also includes data_processor.py")
