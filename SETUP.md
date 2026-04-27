# 🛠️ Setup Guide — Snowflake FinOps Toolkit

**Author:** Shailesh Chalke — Senior Snowflake Consultant  
**Audience:** Engineers setting up this toolkit for the first time.

---

## Prerequisites

| Requirement          | Version / Details                          |
|----------------------|--------------------------------------------|
| Python               | 3.10 or higher                             |
| pip                  | Latest (comes with Python)                 |
| Git                  | Any recent version                         |
| Git Bash (Windows)   | Recommended for Windows users              |
| Snowflake Account    | Trial account works: snowflake.com/trial   |
| Snowflake Role       | SYSADMIN (or role with CREATE DATABASE)    |

---

## Step 1 — Clone the Repository

```bash
git clone https://github.com/shaileshchalke/snowflake-finops-toolkit.git
cd snowflake-finops-toolkit
```

---

## Step 2 — Create Virtual Environment

```bash
# Windows (Git Bash)
python -m venv venv
source venv/Scripts/activate

# Mac / Linux
python3 -m venv venv
source venv/bin/activate
```

---

## Step 3 — Install Dependencies

```bash
pip install -r requirements.txt
```

Expected output: All packages installed with no errors.  
Total install time: ~2-3 minutes.

---

## Step 4 — Configure Environment Variables

```bash
# Copy the template
cp .env.example .env

# Edit with your credentials
notepad .env          # Windows
# OR
nano .env             # Mac/Linux
```

Fill in these required values in `.env`:

```
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=SYSADMIN
```

### How to find your Snowflake Account Identifier

1. Log into Snowflake Web UI
2. Click your username (bottom left)
3. Click "Copy Account Identifier"
4. Format: `orgname-accountname`

---

## Step 5 — Generate Sample Data

This script creates the `FINOPS_DEMO` database, `FINOPS_SAMPLE` schema,  
and uploads 12 warehouses × 28 days of realistic demo data.

```bash
make setup-sample-data
# OR
python src/generate_sample_data.py
```

Expected output:
```
✅ Connected to Snowflake.
✅ Schema ready.
✅ Metering data uploaded.
✅ User attribution data uploaded.
🎉 Sample data generation complete!
```

---

## Step 6 — Launch the Dashboard

```bash
make run
# OR
streamlit run app/streamlit_app.py
```

Open your browser at: **http://localhost:8501**

---

## Step 7 — Run Tests

```bash
make test
```

All 35+ tests should pass. Tests use mock connectors — no live Snowflake needed.

---

## Connecting to ACCOUNT_USAGE (Production Mode)

If your Snowflake account has ACCOUNTADMIN or GOVERNANCE_VIEWER role,  
the toolkit automatically detects and switches to ACCOUNT_USAGE mode.

To enable:
1. Set `SNOWFLAKE_ROLE=ACCOUNTADMIN` in `.env`
2. Restart the dashboard

No code changes needed — mode detection is automatic.

---

## Troubleshooting

| Error                              | Solution                                              |
|------------------------------------|-------------------------------------------------------|
| `SNOWFLAKE_ACCOUNT not found`      | Check `.env` file exists and has correct account ID   |
| `250001: Could not connect`        | Verify account identifier format: `orgname-accountname` |
| `002043: SQL not found`            | Run `make setup-sample-data` first                    |
| `ModuleNotFoundError`              | Run `pip install -r requirements.txt` again           |
| Dashboard loads but shows no data  | Check Snowflake role has SELECT on FINOPS_DEMO schema |

---

## Git Bash — Upload to GitHub

```bash
# Navigate to project folder
cd snowflake-finops-toolkit

# Initialize git (first time only)
git init

# Stage all files
git add .

# Commit
git commit -m "Initial commit: Snowflake FinOps Toolkit v1.0"

# Add GitHub remote (replace with your actual repo URL)
git remote add origin https://github.com/shaileshchalke/snowflake-finops-toolkit.git

# Push to GitHub
git push -u origin main
```

If you get `rejected` error:
```bash
git pull origin main --allow-unrelated-histories
git push origin main
```