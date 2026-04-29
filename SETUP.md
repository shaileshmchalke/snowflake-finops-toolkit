# SETUP Guide — Snowflake FinOps Toolkit

## Prerequisites

| Tool | Version | Download |
|------|---------|----------|
| Python | 3.10 or higher | https://python.org |
| Git | Any recent | https://git-scm.com |
| Snowflake Account | Free trial OK | https://snowflake.com/try-snowflake |

---

## Step 1 — Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/snowflake-finops-toolkit.git
cd snowflake-finops-toolkit
```

---

## Step 2 — Create Virtual Environment

**Windows (Command Prompt):**
```cmd
python -m venv venv
venv\Scripts\activate.bat
```

**Windows (Git Bash / PowerShell):**
```bash
python -m venv venv
source venv/Scripts/activate
```

**Mac / Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

You should see `(venv)` in your terminal prompt.

---

## Step 3 — Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Step 4 — Configure Snowflake Credentials

```bash
# Copy the template
cp .env.example .env
```

Open `.env` in any text editor and fill in your values:

```
SNOWFLAKE_ACCOUNT=myorg-myaccount
SNOWFLAKE_USER=myusername
SNOWFLAKE_PASSWORD=mypassword
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=FINOPS_DEMO
SNOWFLAKE_SCHEMA=FINOPS_SAMPLE
SNOWFLAKE_ROLE=SYSADMIN
```

**How to find your SNOWFLAKE_ACCOUNT:**
- Log in to app.snowflake.com
- Go to Admin > Accounts
- Copy the value in format: `orgname-accountname`

---

## Step 5 — Generate Sample Data

```bash
python src/generate_sample_data.py
```

Expected output:
```
INFO  Connected: account=myorg-myaccount, database=FINOPS_DEMO
INFO  Creating FINOPS_DEMO database and FINOPS_SAMPLE schema...
INFO  Creating tables...
INFO  Uploading 336 metering rows...
INFO  Uploading user attribution rows...
INFO  WH_METERING_HISTORY rows: 336
INFO  USER_ATTRIBUTION rows: 392
INFO  Sample data generation complete!
INFO  Run: streamlit run app/streamlit_app.py
```

---

## Step 6 — Launch Dashboard

```bash
streamlit run app/streamlit_app.py
```

Open your browser at: **http://localhost:8501**

---

## Step 7 — Run Tests (Optional)

```bash
pytest tests/ -v
```

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `ModuleNotFoundError: snowflake` | Run `pip install -r requirements.txt` |
| `250001: Could not connect` | Check SNOWFLAKE_ACCOUNT format: `orgname-accountname` |
| `002003: Object does not exist` | Run `python src/generate_sample_data.py` first |
| `Permission denied on ACCOUNT_USAGE` | Use SYSADMIN or ACCOUNTADMIN role |
| Dashboard blank / no data | Refresh browser, check .env values |