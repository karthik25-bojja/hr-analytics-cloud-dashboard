"""
HR Analytics Data Processor.
Computes Big Data KPIs, department metrics, attrition analysis,
performance insights, and attendance metrics.
Works identically to the PySpark pipeline, enabling instant local development
and testing on laptops before EC2 deployment.
"""

import os
import sys
import json
from pathlib import Path
import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config


def process_hr_data():
    """Reads raw datasets and computes aggregated analytics metrics."""
    raw_dir = config.RAW_DATA_DIR
    out_dir = config.PROCESSED_DATA_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  Processing HR Analytics Dataset")
    print("=" * 60)

    # 1. Read Raw CSVs
    emp_df = pd.read_csv(raw_dir / config.EMPLOYEES_FILE)
    att_df = pd.read_csv(raw_dir / config.ATTENDANCE_FILE)
    att_sum_df = pd.read_csv(raw_dir / config.ATTENDANCE_SUMMARY_FILE)
    leaves_df = pd.read_csv(raw_dir / config.LEAVES_FILE)

    print(f"Loaded {len(emp_df)} employee records.")
    print(f"Loaded {len(att_df)} attendance logs.")
    print(f"Loaded {len(leaves_df)} leave records.")

    # 2. Enrich Employees
    emp_df["Attrition_Flag"] = emp_df["Attrition_Status"].apply(lambda x: 1 if str(x).strip().lower() == "yes" else 0)
    emp_df["Reason_for_Leaving"] = emp_df["Reason_for_Leaving"].fillna("Active / None")

    # Salary Band
    def categorize_salary(salary):
        if salary < 50000:
            return "Entry (< 50K)"
        elif salary <= 100000:
            return "Mid (50K - 100K)"
        else:
            return "Senior (> 100K)"

    emp_df["Salary_Band"] = emp_df["Monthly_Salary_INR"].apply(categorize_salary)

    # Age Group
    def categorize_age(age):
        if age < 30:
            return "< 30"
        elif age <= 40:
            return "30 - 40"
        elif age <= 50:
            return "41 - 50"
        else:
            return "> 50"

    emp_df["Age_Group"] = emp_df["Age"].apply(categorize_age)

    # Merge Attendance Percentage into employee dataframe
    emp_enriched = emp_df.merge(
        att_sum_df[["Employee_ID", "Attendance_Percentage", "Present_Days", "Absent_Days", "Leave_Days"]],
        on="Employee_ID",
        how="left"
    )

    # 3. Overall Executive KPIs
    total_emp = int(len(emp_df))
    active_emp = int((emp_df["Employment_Status"].str.strip().str.lower() == "active").sum())
    resigned_emp = int(emp_df["Attrition_Flag"].sum())
    attrition_rate = round((resigned_emp / total_emp) * 100, 2) if total_emp > 0 else 0.0

    avg_monthly_salary = round(float(emp_df["Monthly_Salary_INR"].mean()), 2)
    avg_annual_salary = round(float(emp_df["Annual_Salary_INR"].mean()), 2)
    avg_training_hours = round(float(emp_df["Training_Hours_Yearly"].mean()), 2)
    avg_attendance_pct = round(float(att_sum_df["Attendance_Percentage"].mean()), 2)

    kpis = {
        "total_employees": total_emp,
        "active_employees": active_emp,
        "resigned_employees": resigned_emp,
        "attrition_rate_pct": attrition_rate,
        "avg_monthly_salary_inr": avg_monthly_salary,
        "avg_annual_salary_inr": avg_annual_salary,
        "avg_training_hours": avg_training_hours,
        "avg_attendance_pct": avg_attendance_pct
    }

    # 4. Department Metrics
    dept_grp = emp_enriched.groupby("Department")
    dept_metrics = dept_grp.agg(
        total_employees=("Employee_ID", "count"),
        resigned_employees=("Attrition_Flag", "sum"),
        avg_monthly_salary_inr=("Monthly_Salary_INR", "mean"),
        min_monthly_salary_inr=("Monthly_Salary_INR", "min"),
        max_monthly_salary_inr=("Monthly_Salary_INR", "max"),
        avg_training_hours=("Training_Hours_Yearly", "mean"),
        avg_attendance_pct=("Attendance_Percentage", "mean")
    ).reset_index()

    dept_metrics["attrition_rate_pct"] = (
        (dept_metrics["resigned_employees"] / dept_metrics["total_employees"]) * 100
    ).round(2)
    dept_metrics["avg_monthly_salary_inr"] = dept_metrics["avg_monthly_salary_inr"].round(2)
    dept_metrics["avg_training_hours"] = dept_metrics["avg_training_hours"].round(2)
    dept_metrics["avg_attendance_pct"] = dept_metrics["avg_attendance_pct"].round(2)
    dept_metrics = dept_metrics.sort_values(by="total_employees", ascending=False)

    # 5. Attrition Reasons
    attrition_df = emp_df[emp_df["Attrition_Flag"] == 1]
    reasons = attrition_df["Reason_for_Leaving"].value_counts().reset_index()
    reasons.columns = ["Reason_for_Leaving", "resigned_count"]

    # 6. Performance Metrics
    perf_grp = emp_df.groupby("Performance_Rating").agg(
        employee_count=("Employee_ID", "count"),
        avg_monthly_salary_inr=("Monthly_Salary_INR", "mean"),
        avg_training_hours=("Training_Hours_Yearly", "mean")
    ).reset_index()
    perf_grp["avg_monthly_salary_inr"] = perf_grp["avg_monthly_salary_inr"].round(2)
    perf_grp["avg_training_hours"] = perf_grp["avg_training_hours"].round(2)

    # 7. Work Mode Metrics
    wm_grp = emp_df.groupby("Work_Mode").agg(
        employee_count=("Employee_ID", "count"),
        resigned_count=("Attrition_Flag", "sum")
    ).reset_index()
    wm_grp["attrition_rate_pct"] = ((wm_grp["resigned_count"] / wm_grp["employee_count"]) * 100).round(2)

    # 8. Leaves Summary
    leaves_summary = leaves_df.groupby(["Leave_Type", "Approval_Status"]).agg(
        leave_count=("Leave_ID", "count"),
        total_leave_days=("Leave_Days", "sum")
    ).reset_index()

    # 9. Daily Attendance Trends
    att_df["Attendance_Date"] = pd.to_datetime(att_df["Attendance_Date"])
    daily_att = att_df.groupby(["Attendance_Date", "Attendance_Status"]).size().unstack(fill_value=0).reset_index()
    if "Present" in daily_att.columns:
        daily_att["Daily_Present_Pct"] = (
            daily_att["Present"] / (daily_att.sum(axis=1, numeric_only=True)) * 100
        ).round(2)

    # 10. Save all to processed folder
    with open(out_dir / "kpi_summary.json", "w") as f:
        json.dump(kpis, f, indent=4)

    emp_enriched.to_csv(out_dir / "employees_enriched.csv", index=False)
    dept_metrics.to_csv(out_dir / "department_metrics.csv", index=False)
    reasons.to_csv(out_dir / "attrition_reasons.csv", index=False)
    perf_grp.to_csv(out_dir / "performance_metrics.csv", index=False)
    wm_grp.to_csv(out_dir / "work_mode_metrics.csv", index=False)
    leaves_summary.to_csv(out_dir / "leaves_summary.csv", index=False)
    daily_att.to_csv(out_dir / "attendance_trends.csv", index=False)

    print(f"[OK] Successfully processed and saved all outputs to {out_dir}")
    print(f"Summary KPIs: {kpis}")
    return True


if __name__ == "__main__":
    process_hr_data()
