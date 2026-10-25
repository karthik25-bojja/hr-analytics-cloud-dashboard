#!/bin/bash
# ==============================================================================
# AWS EC2 Setup Script for Human Resource Analytics Dashboard
# Big Data Framework: Apache Spark (PySpark)
# Cloud Storage: AWS S3
# Dashboard: Streamlit on Port 8501
# ==============================================================================

set -e

echo "=================================================================="
echo " Starting AWS EC2 Setup for HR Big Data Analytics System"
echo "=================================================================="

# 1. Update OS Packages
echo "[*] Updating package repositories..."
if [ -f /etc/debian_version ]; then
    sudo apt update -y && sudo apt upgrade -y
    echo "[*] Installing OpenJDK 11, Python3, and Pip on Ubuntu/Debian..."
    sudo apt install -y openjdk-11-jdk python3 python3-pip python3-venv git
elif [ -f /etc/redhat-release ] || [ -f /etc/system-release ]; then
    sudo dnf update -y || sudo yum update -y
    echo "[*] Installing Java 11 and Python3 on Amazon Linux / RHEL..."
    sudo dnf install -y java-11-amazon-corretto python3 python3-pip git || \
    sudo yum install -y java-11-openjdk python3 python3-pip git
fi

# 2. Verify Java installation (Required for Apache Spark)
echo "[*] Checking Java version..."
java -version
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
echo "export JAVA_HOME=${JAVA_HOME}" >> ~/.bashrc
echo "JAVA_HOME set to: ${JAVA_HOME}"

# 3. Create Python Virtual Environment
echo "[*] Setting up Python virtual environment..."
python3 -m venv ~/hr_analytics_env
source ~/hr_analytics_env/bin/activate

# 4. Install Big Data & Web Dashboard Dependencies
echo "[*] Installing PySpark, Boto3, Streamlit, and Plotly..."
pip install --upgrade pip
pip install pyspark boto3 pandas numpy plotly streamlit

# 5. Run Apache Spark Pipeline to Verify Execution
echo "[*] Running PySpark pipeline..."
python scripts/spark_hr_analytics.py

echo "=================================================================="
echo " [OK] Setup Completed Successfully!"
echo " To run the Streamlit Dashboard, execute:"
echo "   streamlit run app.py --server.port 8501 --server.address 0.0.0.0"
echo ""
echo " IMPORTANT: Ensure your EC2 Security Group allows Inbound TCP on"
echo " port 8501 from 0.0.0.0/0 so you can access the dashboard in your"
echo " web browser at: http://<EC2-PUBLIC-IP>:8501"
echo "=================================================================="
