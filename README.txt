Human Resource Analytics Dashboard - Sample Dataset

This dataset contains fictional/synthetic employee data for an academic project.
No real employee personal information is included.

Files:
1. employees.csv - one row per employee with demographic, employment, salary,
   work mode, performance and attrition fields.
2. attendance.csv - weekday attendance records from 2026-01-01 to 2026-03-31,
   including present/absent/leave status, check-in/out time, work hours and mode.
3. leaves.csv - sample leave requests with leave type, dates, days and approval status.

Suggested workflow:
- Upload these CSV files to an AWS S3 bucket.
- Use Apache Spark (PySpark) to read, clean and analyze the data.
- Export aggregated results for a dashboard in Tableau or Power BI.

Note:
- All names and records are fictional.
- Attendance and leave records are randomly generated for demonstration only.
- Resignation dates are blank for active employees.

4. attendance_summary.csv - one row per employee with present, absent, leave,
   recorded days, and Attendance_Percentage. Formula:
   Present_Days / Total_Recorded_Days * 100. This simple project metric counts
   recorded leave days as non-present days.
