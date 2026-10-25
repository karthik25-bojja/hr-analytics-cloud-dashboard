"""
Human Resource Analytics Dashboard Using Cloud Storage.
Big Data Framework: Apache Spark (PySpark)
Cloud Provider: AWS S3 + EC2
Interactive Web Dashboard built with Streamlit & Plotly.
"""

import os
import sys
import json
from pathlib import Path
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Setup paths
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import config
from scripts.aws_s3_manager import S3Manager, BOTO3_AVAILABLE
from scripts.data_processor import process_hr_data

# Page Configuration
st.set_page_config(
    page_title="HR Analytics Dashboard | Big Data & Cloud Storage",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Modern Executive UI
st.markdown("""
<style>
    /* Metric Cards */
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
        margin-bottom: 15px;
    }
    .metric-card-title {
        font-size: 0.85rem;
        color: #94a3b8;
        text-transform: uppercase;
        font-weight: 600;
        letter-spacing: 0.5px;
        margin-bottom: 5px;
    }
    .metric-card-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #f8fafc;
    }
    .metric-card-sub {
        font-size: 0.8rem;
        color: #38bdf8;
        margin-top: 4px;
    }
    .badge-cloud {
        display: inline-block;
        background-color: #0284c7;
        color: white;
        padding: 4px 10px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 8px;
    }
    .badge-spark {
        display: inline-block;
        background-color: #ea580c;
        color: white;
        padding: 4px 10px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 8px;
    }
    .badge-ec2 {
        display: inline-block;
        background-color: #16a34a;
        color: white;
        padding: 4px 10px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=60)
def load_data():
    """Loads processed HR analytics datasets."""
    proc_dir = config.PROCESSED_DATA_DIR
    raw_dir = config.RAW_DATA_DIR

    # If processed files don't exist yet, run processing
    if not (proc_dir / "kpi_summary.json").exists() or not (proc_dir / "employees_enriched.csv").exists():
        process_hr_data()

    with open(proc_dir / "kpi_summary.json", "r") as f:
        kpi_summary = json.load(f)

    emp_df = pd.read_csv(proc_dir / "employees_enriched.csv")
    dept_df = pd.read_csv(proc_dir / "department_metrics.csv")
    reasons_df = pd.read_csv(proc_dir / "attrition_reasons.csv")
    perf_df = pd.read_csv(proc_dir / "performance_metrics.csv")
    work_mode_df = pd.read_csv(proc_dir / "work_mode_metrics.csv")
    leaves_df = pd.read_csv(proc_dir / "leaves_summary.csv")
    att_trends_df = pd.read_csv(proc_dir / "attendance_trends.csv") if (proc_dir / "attendance_trends.csv").exists() else pd.DataFrame()

    raw_att = pd.read_csv(raw_dir / config.ATTENDANCE_FILE) if (raw_dir / config.ATTENDANCE_FILE).exists() else pd.DataFrame()
    raw_leaves = pd.read_csv(raw_dir / config.LEAVES_FILE) if (raw_dir / config.LEAVES_FILE).exists() else pd.DataFrame()

    return {
        "kpi": kpi_summary,
        "emp": emp_df,
        "dept": dept_df,
        "reasons": reasons_df,
        "perf": perf_df,
        "work_mode": work_mode_df,
        "leaves": leaves_df,
        "att_trends": att_trends_df,
        "raw_att": raw_att,
        "raw_leaves": raw_leaves
    }


data = load_data()

# ----------------- SIDEBAR -----------------
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg", width=160)
st.sidebar.title("HR Analytics Engine")
st.sidebar.markdown(
    """
    <div style="margin-bottom: 15px;">
        <span class="badge-cloud">AWS S3</span>
        <span class="badge-spark">PySpark</span>
        <span class="badge-ec2">AWS EC2</span>
    </div>
    """,
    unsafe_allow_html=True
)

st.sidebar.header("🔍 Global Filters")

# Filter: Department
all_depts = sorted(data["emp"]["Department"].dropna().unique().tolist())
selected_depts = st.sidebar.multiselect("Department", options=all_depts, default=all_depts)

# Filter: Work Mode
all_modes = sorted(data["emp"]["Work_Mode"].dropna().unique().tolist())
selected_modes = st.sidebar.multiselect("Work Mode", options=all_modes, default=all_modes)

# Filter: Gender
all_genders = sorted(data["emp"]["Gender"].dropna().unique().tolist())
selected_genders = st.sidebar.multiselect("Gender", options=all_genders, default=all_genders)

# Filter: Performance Rating
all_ratings = sorted(data["emp"]["Performance_Rating"].dropna().unique().tolist())
selected_ratings = st.sidebar.multiselect("Performance Rating", options=all_ratings, default=all_ratings)

# Filter: Attrition Status
attrition_option = st.sidebar.radio("Attrition Filter", options=["All", "Active Only", "Resigned Only"])

# Apply filters to employees dataframe
filtered_emp = data["emp"].copy()
if selected_depts:
    filtered_emp = filtered_emp[filtered_emp["Department"].isin(selected_depts)]
if selected_modes:
    filtered_emp = filtered_emp[filtered_emp["Work_Mode"].isin(selected_modes)]
if selected_genders:
    filtered_emp = filtered_emp[filtered_emp["Gender"].isin(selected_genders)]
if selected_ratings:
    filtered_emp = filtered_emp[filtered_emp["Performance_Rating"].isin(selected_ratings)]
if attrition_option == "Active Only":
    filtered_emp = filtered_emp[filtered_emp["Attrition_Status"] == "No"]
elif attrition_option == "Resigned Only":
    filtered_emp = filtered_emp[filtered_emp["Attrition_Status"] == "Yes"]

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Re-run Big Data Pipeline"):
    with st.spinner("Processing HR Big Data with transformations..."):
        process_hr_data()
        st.cache_data.clear()
        st.sidebar.success("Pipeline executed successfully!")
        st.rerun()

st.sidebar.info("💡 Project: Human Resource Analytics Dashboard Using Cloud Storage (AWS S3 + EC2 & PySpark)")

# ----------------- MAIN HEADER -----------------
st.title("💼 Human Resource Analytics Dashboard")
st.markdown(
    """
    **Architecture:** `HR Dataset (CSV)` ➔ `AWS S3 (Cloud Object Storage)` ➔ `Apache Spark (PySpark on AWS EC2)` ➔ `Streamlit Cloud Dashboard`
    """
)
st.markdown("---")

# ----------------- TABS -----------------
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Executive KPIs",
    "🚪 Attrition Analytics",
    "⏱️ Attendance & Work Mode",
    "🌟 Performance & Training",
    "🏖️ Leave Management",
    "🔍 Employee Directory",
    "☁️ Cloud Architecture & S3"
])

# ================= TAB 1: EXECUTIVE KPIS =================
with tab1:
    st.subheader("Key Performance Indicators (Executive Summary)")

    # Dynamic KPI Calculations based on filtered data
    total_count = len(filtered_emp)
    resigned_count = int(filtered_emp["Attrition_Flag"].sum()) if total_count > 0 else 0
    active_count = total_count - resigned_count
    attrition_rate = round((resigned_count / total_count * 100), 2) if total_count > 0 else 0.0

    avg_monthly_sal = round(filtered_emp["Monthly_Salary_INR"].mean(), 2) if total_count > 0 else 0.0
    avg_annual_sal = round(filtered_emp["Annual_Salary_INR"].mean(), 2) if total_count > 0 else 0.0
    avg_train_hrs = round(filtered_emp["Training_Hours_Yearly"].mean(), 2) if total_count > 0 else 0.0
    avg_att_pct = round(filtered_emp["Attendance_Percentage"].mean(), 2) if "Attendance_Percentage" in filtered_emp.columns and total_count > 0 else 0.0

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    with kpi_col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-card-title">👥 Total Headcount</div>
            <div class="metric-card-value">{total_count:,}</div>
            <div class="metric-card-sub">Active: {active_count} | Resigned: {resigned_count}</div>
        </div>
        """, unsafe_allow_html=True)

    with kpi_col2:
        attrition_color = "#ef4444" if attrition_rate > 10 else "#38bdf8"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-card-title">📉 Attrition Rate</div>
            <div class="metric-card-value" style="color: {attrition_color}">{attrition_rate}%</div>
            <div class="metric-card-sub">{resigned_count} total resignations</div>
        </div>
        """, unsafe_allow_html=True)

    with kpi_col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-card-title">💵 Avg Monthly Salary</div>
            <div class="metric-card-value">₹{avg_monthly_sal:,.0f}</div>
            <div class="metric-card-sub">Annual CTC: ₹{avg_annual_sal / 100000:.2f} Lakhs</div>
        </div>
        """, unsafe_allow_html=True)

    with kpi_col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-card-title">📈 Avg Attendance & Training</div>
            <div class="metric-card-value">{avg_att_pct}%</div>
            <div class="metric-card-sub">Avg Training: {avg_train_hrs:.1f} hrs/yr</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### 🏢 Department & Workforce Breakdown")
    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        dept_dist = filtered_emp["Department"].value_counts().reset_index()
        dept_dist.columns = ["Department", "Employee_Count"]
        fig_dept = px.bar(
            dept_dist,
            x="Department",
            y="Employee_Count",
            color="Department",
            text="Employee_Count",
            title="Employee Count by Department",
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig_dept.update_layout(showlegend=False, xaxis_title="", yaxis_title="Employees")
        st.plotly_chart(fig_dept, use_container_width=True)

    with col_chart2:
        wm_dist = filtered_emp["Work_Mode"].value_counts().reset_index()
        wm_dist.columns = ["Work_Mode", "Count"]
        fig_wm = px.pie(
            wm_dist,
            names="Work_Mode",
            values="Count",
            hole=0.45,
            title="Work Mode Distribution (Office vs Hybrid vs WFH)",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig_wm, use_container_width=True)

    st.markdown("### 💰 Compensation Distribution by Department")
    fig_salary_box = px.box(
        filtered_emp,
        x="Department",
        y="Monthly_Salary_INR",
        color="Department",
        points="all",
        title="Monthly Salary Distribution & Outliers Across Departments (INR)",
        color_discrete_sequence=px.colors.qualitative.Safe
    )
    fig_salary_box.update_layout(yaxis_title="Monthly Salary (₹)")
    st.plotly_chart(fig_salary_box, use_container_width=True)


# ================= TAB 2: ATTRITION ANALYTICS =================
with tab2:
    st.subheader("🚪 Employee Attrition & Retention Deep Dive")
    st.markdown("Analyze turnover patterns, department attrition hotspots, and root causes for employee resignations.")

    att_col1, att_col2 = st.columns(2)

    with att_col1:
        # Attrition rate by department
        dept_att = filtered_emp.groupby("Department").agg(
            Total=("Employee_ID", "count"),
            Resigned=("Attrition_Flag", "sum")
        ).reset_index()
        dept_att["Attrition_Rate_%"] = ((dept_att["Resigned"] / dept_att["Total"]) * 100).round(2)
        dept_att = dept_att.sort_values(by="Attrition_Rate_%", ascending=True)

        fig_dept_att = px.bar(
            dept_att,
            y="Department",
            x="Attrition_Rate_%",
            orientation="h",
            color="Attrition_Rate_%",
            color_continuous_scale="Reds",
            text="Attrition_Rate_%",
            title="Attrition Rate by Department (%)"
        )
        fig_dept_att.update_layout(xaxis_title="Attrition Rate (%)", yaxis_title="")
        st.plotly_chart(fig_dept_att, use_container_width=True)

    with att_col2:
        # Reasons for leaving
        resigned_records = filtered_emp[filtered_emp["Attrition_Flag"] == 1]
        if len(resigned_records) > 0:
            reasons_count = resigned_records["Reason_for_Leaving"].value_counts().reset_index()
            reasons_count.columns = ["Reason", "Count"]
            fig_reasons = px.pie(
                reasons_count,
                names="Reason",
                values="Count",
                title="Primary Reasons for Resignation",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            st.plotly_chart(fig_reasons, use_container_width=True)
        else:
            st.info("No resigned employees matching current filter criteria.")

    col_att3, col_att4 = st.columns(2)

    with col_att3:
        # Attrition by Work Mode
        wm_att = filtered_emp.groupby("Work_Mode").agg(
            Total=("Employee_ID", "count"),
            Resigned=("Attrition_Flag", "sum")
        ).reset_index()
        wm_att["Attrition_Rate_%"] = ((wm_att["Resigned"] / wm_att["Total"]) * 100).round(2)
        fig_wm_att = px.bar(
            wm_att,
            x="Work_Mode",
            y="Attrition_Rate_%",
            color="Work_Mode",
            text="Attrition_Rate_%",
            title="Attrition Rate by Work Mode (%)",
            color_discrete_sequence=px.colors.qualitative.Vivid
        )
        fig_wm_att.update_layout(showlegend=False, yaxis_title="Attrition Rate (%)")
        st.plotly_chart(fig_wm_att, use_container_width=True)

    with col_att4:
        # Attrition by Salary Band
        sal_att = filtered_emp.groupby("Salary_Band").agg(
            Total=("Employee_ID", "count"),
            Resigned=("Attrition_Flag", "sum")
        ).reset_index()
        sal_att["Attrition_Rate_%"] = ((sal_att["Resigned"] / sal_att["Total"]) * 100).round(2)
        fig_sal_att = px.bar(
            sal_att,
            x="Salary_Band",
            y="Attrition_Rate_%",
            color="Salary_Band",
            text="Attrition_Rate_%",
            title="Attrition Rate by Salary Bracket",
            color_discrete_sequence=px.colors.qualitative.Antique
        )
        fig_sal_att.update_layout(showlegend=False, yaxis_title="Attrition Rate (%)")
        st.plotly_chart(fig_sal_att, use_container_width=True)


# ================= TAB 3: ATTENDANCE & WORK MODE =================
with tab3:
    st.subheader("⏱️ Attendance, Working Hours & Absenteeism Analytics")

    col_att_m1, col_att_m2 = st.columns(2)

    with col_att_m1:
        # Attendance % by department
        dept_att_pct = filtered_emp.groupby("Department")["Attendance_Percentage"].mean().reset_index()
        dept_att_pct["Attendance_Percentage"] = dept_att_pct["Attendance_Percentage"].round(2)
        fig_dept_att_bar = px.bar(
            dept_att_pct,
            x="Department",
            y="Attendance_Percentage",
            color="Attendance_Percentage",
            color_continuous_scale="Teal",
            text="Attendance_Percentage",
            title="Average Attendance Percentage by Department (%)"
        )
        fig_dept_att_bar.add_hline(y=85.0, line_dash="dash", line_color="orange", annotation_text="Target 85%")
        fig_dept_att_bar.update_layout(yaxis_title="Attendance %")
        st.plotly_chart(fig_dept_att_bar, use_container_width=True)

    with col_att_m2:
        # Daily Attendance Status Trends
        if not data["att_trends"].empty:
            att_trend_df = data["att_trends"].copy()
            fig_daily_trend = px.line(
                att_trend_df,
                x="Attendance_Date",
                y="Daily_Present_Pct" if "Daily_Present_Pct" in att_trend_df.columns else "Present",
                title="Company-Wide Daily Attendance Percentage Trend (Q1 2026)",
                markers=True
            )
            fig_daily_trend.update_layout(xaxis_title="Date", yaxis_title="Present %")
            st.plotly_chart(fig_daily_trend, use_container_width=True)
        else:
            st.info("Attendance trend logs not available.")

    # Working Hours Analysis by Work Mode
    if not data["raw_att"].empty:
        st.markdown("### 🕒 Average Daily Working Hours by Work Mode")
        work_hours_summary = data["raw_att"].groupby("Work_Mode")["Working_Hours"].agg(
            ["mean", "min", "max", "count"]
        ).reset_index()
        work_hours_summary.columns = ["Work_Mode", "Avg_Hours", "Min_Hours", "Max_Hours", "Log_Count"]
        work_hours_summary["Avg_Hours"] = work_hours_summary["Avg_Hours"].round(2)

        fig_wh = px.bar(
            work_hours_summary,
            x="Work_Mode",
            y="Avg_Hours",
            color="Work_Mode",
            text="Avg_Hours",
            title="Average Working Hours Logged (Daily)",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_wh.add_hline(y=8.0, line_dash="dot", line_color="red", annotation_text="Standard 8 Hours")
        fig_wh.update_layout(showlegend=False, yaxis_title="Average Hours")
        st.plotly_chart(fig_wh, use_container_width=True)


# ================= TAB 4: PERFORMANCE & TRAINING =================
with tab4:
    st.subheader("🌟 Performance Ratings & Training Effectiveness")

    perf_col1, perf_col2 = st.columns(2)

    with perf_col1:
        # Performance Distribution
        perf_counts = filtered_emp["Performance_Rating"].value_counts().reset_index()
        perf_counts.columns = ["Performance_Rating", "Count"]
        fig_perf_pie = px.pie(
            perf_counts,
            names="Performance_Rating",
            values="Count",
            title="Workforce Performance Rating Distribution",
            hole=0.45,
            color_discrete_sequence=px.colors.qualitative.Dark24
        )
        st.plotly_chart(fig_perf_pie, use_container_width=True)

    with perf_col2:
        # Average Salary by Performance Rating
        perf_sal = filtered_emp.groupby("Performance_Rating")["Monthly_Salary_INR"].mean().reset_index()
        perf_sal["Monthly_Salary_INR"] = perf_sal["Monthly_Salary_INR"].round(2)
        fig_perf_sal = px.bar(
            perf_sal,
            x="Performance_Rating",
            y="Monthly_Salary_INR",
            color="Performance_Rating",
            text="Monthly_Salary_INR",
            title="Average Monthly Salary by Performance Rating (INR)",
            color_discrete_sequence=px.colors.qualitative.Prism
        )
        fig_perf_sal.update_layout(showlegend=False, yaxis_title="Avg Monthly Salary (₹)")
        st.plotly_chart(fig_perf_sal, use_container_width=True)

    # Scatter Plot: Training Hours vs Monthly Salary
    st.markdown("### 🎯 Training Hours vs Performance & Compensation")
    fig_scatter = px.scatter(
        filtered_emp,
        x="Training_Hours_Yearly",
        y="Monthly_Salary_INR",
        color="Performance_Rating",
        size="Age",
        hover_data=["Employee_ID", "Employee_Name", "Department", "Job_Role"],
        title="Training Hours vs Monthly Salary (Color: Performance, Size: Age)",
        color_discrete_sequence=px.colors.qualitative.Bold
    )
    fig_scatter.update_layout(xaxis_title="Training Hours (Yearly)", yaxis_title="Monthly Salary (₹)")
    st.plotly_chart(fig_scatter, use_container_width=True)


# ================= TAB 5: LEAVE MANAGEMENT =================
with tab5:
    st.subheader("🏖️ Employee Leave Utilization & Approval Tracking")

    leave_col1, leave_col2 = st.columns(2)

    with leave_col1:
        if not data["raw_leaves"].empty:
            type_counts = data["raw_leaves"]["Leave_Type"].value_counts().reset_index()
            type_counts.columns = ["Leave_Type", "Count"]
            fig_ltype = px.pie(
                type_counts,
                names="Leave_Type",
                values="Count",
                hole=0.4,
                title="Leave Application Requests by Leave Type",
                color_discrete_sequence=px.colors.qualitative.Pastel1
            )
            st.plotly_chart(fig_ltype, use_container_width=True)
        else:
            st.info("No leave records found.")

    with leave_col2:
        if not data["raw_leaves"].empty:
            approval_counts = data["raw_leaves"]["Approval_Status"].value_counts().reset_index()
            approval_counts.columns = ["Approval_Status", "Count"]
            fig_approval = px.bar(
                approval_counts,
                x="Approval_Status",
                y="Count",
                color="Approval_Status",
                text="Count",
                title="Leave Approval Status Distribution",
                color_discrete_map={"Approved": "#10b981", "Pending": "#f59e0b", "Rejected": "#ef4444"}
            )
            fig_approval.update_layout(showlegend=False, yaxis_title="Requests Count")
            st.plotly_chart(fig_approval, use_container_width=True)

    # Total Leave Days Taken
    if not data["raw_leaves"].empty:
        st.markdown("### 📊 Leave Days Consumed by Type")
        days_by_type = data["raw_leaves"].groupby("Leave_Type")["Leave_Days"].sum().reset_index()
        fig_days = px.bar(
            days_by_type,
            x="Leave_Type",
            y="Leave_Days",
            color="Leave_Type",
            text="Leave_Days",
            title="Total Leave Days Consumed by Leave Category",
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig_days.update_layout(showlegend=False, yaxis_title="Total Days")
        st.plotly_chart(fig_days, use_container_width=True)


# ================= TAB 6: EMPLOYEE DIRECTORY =================
with tab6:
    st.subheader("🔍 Employee Directory & Interactive Data Explorer")

    search_query = st.text_input("Search by Employee Name, ID, or Job Role", "")
    display_df = filtered_emp.copy()

    if search_query:
        mask = (
            display_df["Employee_Name"].str.contains(search_query, case=False, na=False) |
            display_df["Employee_ID"].str.contains(search_query, case=False, na=False) |
            display_df["Job_Role"].str.contains(search_query, case=False, na=False)
        )
        display_df = display_df[mask]

    st.write(f"Showing **{len(display_df)}** matching employee records:")

    cols_to_show = [
        "Employee_ID", "Employee_Name", "Department", "Job_Role", "Gender", "Age",
        "Monthly_Salary_INR", "Work_Mode", "Performance_Rating", "Training_Hours_Yearly",
        "Attendance_Percentage", "Employment_Status", "Attrition_Status"
    ]
    st.dataframe(display_df[cols_to_show], use_container_width=True)

    csv_data = display_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Filtered Employee Data (CSV)",
        data=csv_data,
        file_name="filtered_hr_data.csv",
        mime="text/csv"
    )


# ================= TAB 7: CLOUD ARCHITECTURE & S3 =================
with tab7:
    st.subheader("☁️ Cloud Architecture & AWS S3 Storage Verification")
    st.markdown(
        """
        This tab provides real-time verification of the **AWS S3 Cloud Object Storage** and **Apache Spark (PySpark)**
        compute pipeline for faculty evaluations and project vivas.
        """
    )

    s3_mgr = S3Manager()
    is_s3_live = s3_mgr.is_connected()

    # Cloud Status Banner
    st.markdown(f"""
    <div style="background-color: #0f172a; padding: 16px; border-radius: 8px; border-left: 5px solid {'#10b981' if is_s3_live else '#f59e0b'}; margin-bottom: 20px;">
        <h4 style="margin: 0; color: #f8fafc;">Cloud Storage Engine: Amazon Web Services (AWS S3)</h4>
        <p style="margin: 4px 0 0 0; color: #94a3b8;">
            <b>Bucket:</b> <code>{s3_mgr.bucket_name}</code> | <b>Region:</b> <code>{s3_mgr.region}</code> | 
            <b>Status:</b> {'🟢 CONNECTED & AUTHENTICATED' if is_s3_live else '🟡 LOCAL TESTING / SIMULATION MODE (Configure AWS credentials in config.py or AWS CLI)'}
        </p>
    </div>
    """, unsafe_allow_html=True)

    col_arch1, col_arch2 = st.columns(2)

    with col_arch1:
        st.markdown("### 🏗️ End-to-End Big Data Architecture")
        st.code("""
+-------------------------------------------------------+
|  1. HR Dataset Generation (4 Relational CSV Files)    |
|     employees.csv, attendance.csv, leaves.csv, etc.  |
+--------------------------+----------------------------+
                           |
                           v  (Boto3 / AWS CLI Upload)
+-------------------------------------------------------+
|  2. Cloud Object Storage: AWS S3 Bucket               |
|     s3://hr-analytics-cloud-storage-bda/data/raw/    |
+--------------------------+----------------------------+
                           |
                           v  (S3A Protocol & PySpark Reader)
+-------------------------------------------------------+
|  3. Big Data Processing: Apache Spark on AWS EC2      |
|     - Schema Inference, Cleaning, Spark SQL           |
|     - Scalable Aggregations, Feature Engineering      |
|     - Output exported to s3://.../data/processed/     |
+--------------------------+----------------------------+
                           |
                           v  (Dynamic Data Loading)
+-------------------------------------------------------+
|  4. Interactive Presentation: Streamlit Dashboard     |
|     - Real-Time Executive KPIs, Attrition Heatmaps    |
|     - Hosted on EC2 (Public IP: 8501) or Local Web    |
+-------------------------------------------------------+
        """, language="text")

    with col_arch2:
        st.markdown("### 📂 S3 Cloud Bucket Objects")
        if is_s3_live:
            s3_files = s3_mgr.list_s3_files()
            if s3_files:
                st.dataframe(pd.DataFrame(s3_files), use_container_width=True)
            else:
                st.info("Bucket is connected, but no files uploaded yet. Click upload below.")
        else:
            # Show simulated / local raw files mapped to S3 URIs
            mock_s3_files = [
                {"S3 Key / Object URI": f"s3://{s3_mgr.bucket_name}/data/raw/employees.csv", "Category": "Employee Master", "Size": "15.4 KB", "Status": "Ready for S3 Sync"},
                {"S3 Key / Object URI": f"s3://{s3_mgr.bucket_name}/data/raw/attendance.csv", "Category": "Attendance Logs (5,888 Rows)", "Size": "289.7 KB", "Status": "Ready for S3 Sync"},
                {"S3 Key / Object URI": f"s3://{s3_mgr.bucket_name}/data/raw/attendance_summary.csv", "Category": "Attendance Summary", "Size": "4.2 KB", "Status": "Ready for S3 Sync"},
                {"S3 Key / Object URI": f"s3://{s3_mgr.bucket_name}/data/raw/leaves.csv", "Category": "Leave Logs (157 Rows)", "Size": "9.3 KB", "Status": "Ready for S3 Sync"},
                {"S3 Key / Object URI": f"s3://{s3_mgr.bucket_name}/data/processed/kpi_summary.json", "Category": "Spark Aggregated KPIs", "Size": "0.3 KB", "Status": "Ready for S3 Sync"}
            ]
            st.dataframe(pd.DataFrame(mock_s3_files), use_container_width=True)
            st.caption("ℹ️ To sync with live AWS S3, set AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY.")

    st.markdown("### 🚀 AWS & PySpark College Viva / Interview Q&A")
    with st.expander("❓ Why Apache Spark (PySpark) instead of traditional SQL / Pandas?"):
        st.write(
            "Apache Spark processes data in-memory and distributes computation across a cluster of nodes. "
            "While Pandas loads everything into a single machine's RAM and fails on large enterprise datasets (millions of attendance logs), "
            "PySpark's Resilient Distributed Datasets (RDDs) and DataFrames scale horizontally on cloud compute instances like AWS EC2."
        )

    with st.expander("❓ Why AWS S3 for storage instead of local disk?"):
        st.write(
            "AWS S3 provides 99.999999999% (11 9's) durability, virtually infinite scalability, and decouples storage from compute. "
            "This means compute clusters (EC2/EMR) can be spun up on demand, process the data, write results back to S3, and be shut down, "
            "drastically reducing cloud infrastructure costs."
        )

    with st.expander("❓ How to run this PySpark script on an AWS EC2 instance?"):
        st.code("""
# 1. Connect to your EC2 instance via SSH:
ssh -i "your-key.pem" ubuntu@ec2-xx-xx-xx-xx.compute-1.amazonaws.com

# 2. Run the automated EC2 setup script:
bash scripts/setup_ec2.sh

# 3. Run the Apache Spark Pipeline:
spark-submit scripts/spark_hr_analytics.py --s3 --bucket hr-analytics-cloud-storage-bda

# 4. Launch the Streamlit dashboard on port 8501:
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
        """, language="bash")
