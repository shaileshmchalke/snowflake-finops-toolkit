# ❄️ Snowflake FinOps Toolkit

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat&logo=python)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.44-FF4B4B?style=flat&logo=streamlit)](https://streamlit.io)
[![Snowflake](https://img.shields.io/badge/Snowflake-Compatible-29B5E8?style=flat&logo=snowflake)](https://snowflake.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-Pytest-green)](tests/)

A production-ready Streamlit dashboard for **Snowflake cost optimization and FinOps**.  
Identifies warehouse waste, auto-suspend inefficiencies, right-sizing opportunities, and cost anomalies.

---

## 💰 What It Does

| Feature | Savings Identified |
|---|---|
| Auto-suspend optimization | 35–50% of idle billing |
| Warehouse right-sizing | 25–40% on over-provisioned warehouses |
| Multi-cluster waste detection | 30% idle cluster reduction |
| Anomaly detection (Z-Score) | Catches unexpected spend spikes |
| What-If Simulator | Model savings before any change |

> **Demo result:** 12 warehouses × 28 days of sample data identifies **~$744,000/year** in potential savings.

---

## 🖥️ Dashboard Pages

| Page | Description |
|---|---|
| 📊 Cost Overview | MTD/YTD spend, 28-day trend, top warehouses, user attribution |
| 🏭 Warehouse Optimizer | Per-warehouse savings with detailed calculation breakdowns |
| 🚨 Anomaly Detection | Z-score analysis, spike alerts, slow creep detection |
| ⚙️ Bulk Configurator | Grouped ALTER SQL + downloadable rollback script |
| 🔮 What-If Simulator | Scenario modeling with BI cache penalty awareness |

---

## 🏗️ Project Structure

```
snowflake-finops-toolkit/
│
├── app/
│   ├── __init__.py
│   └── streamlit_app.py          # Main dashboard (5 pages)
│
├── src/
│   ├── __init__.py
│   ├── snowflake_connector.py    # Password + key-pair auth
│   ├── cost_analyzer.py          # MTD/YTD/daily trends/idle waste
│   ├── warehouse_optimizer.py    # Auto-suspend + right-sizing + multi-cluster
│   ├── anomaly_detector.py       # Z-score spike + slow creep detection
│   ├── bulk_configurator.py      # Grouped ALTER + rollback SQL
│   └── generate_sample_data.py   # 12-WH × 28-day demo data upload
│
├── tests/
│   ├── __init__.py
│   ├── test_cost_analyzer.py
│   ├── test_warehouse_optimizer.py
│   └── test_anomaly_detector.py  # Z-score division-by-zero fix tested
│
├── .streamlit/
│   └── config.toml               # Dark theme configuration
│
├── .env.example                  # Credentials template (copy to .env)
├── .gitignore                    # Excludes .env, venv, __pycache__
├── requirements.txt              # Pinned dependencies
├── Makefile                      # make run / make test / make data
├── SETUP.md                      # Detailed setup for Windows/Mac/Linux
├── LICENSE                       # MIT License
└── README.md
```

---

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/snowflake-finops-toolkit.git
cd snowflake-finops-toolkit

# 2. Virtual environment
python -m venv venv
source venv/bin/activate        # Mac/Linux
# source venv/Scripts/activate  # Windows Git Bash

# 3. Install
pip install -r requirements.txt

# 4. Configure credentials
cp .env.example .env
# Edit .env with your Snowflake account details

# 5. Upload sample data
python src/generate_sample_data.py

# 6. Launch dashboard
streamlit run app/streamlit_app.py
```

Open: **http://localhost:8501**

For detailed setup instructions see [SETUP.md](SETUP.md).

---

## ⚙️ Configuration

Edit `.env` (copy from `.env.example`):

```
SNOWFLAKE_ACCOUNT=orgname-accountname
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=FINOPS_DEMO
SNOWFLAKE_SCHEMA=FINOPS_SAMPLE
SNOWFLAKE_ROLE=SYSADMIN
```

The dashboard sidebar lets you adjust the **credit price** (USD) at runtime — all cost calculations update instantly.

---

## 🧠 How Savings Are Calculated

**Auto-Suspend Savings:**
```
Savings = idle_sessions/day × minutes_saved/session × credits/hr × 365
```

**Right-Sizing Savings:**
```
Savings = (current_cph - recommended_cph) × hours/year
Triggers when: avg_utilization < 25% of warehouse capacity
```

**Multi-Cluster Waste:**
```
Savings = (min_cluster - 1) × credits/hr × hours/year × 30% idle
```

All calculations are shown in the Warehouse Optimizer expand panel for full transparency.

---

## 🔬 Workload Classification

| Workload | Keyword Match | Recommended Auto-Suspend |
|---|---|---|
| BI | bi, report, tableau, looker, dashboard | 300s (5 min) |
| ETL | etl, pipeline, dbt, fivetran, airflow | 120s (2 min) |
| AD_HOC | adhoc, dev, test, sandbox, explore | 60s (1 min) |
| DS | ds, ml, notebook, jupyter, model | 600s (10 min) |

---

## 🧪 Running Tests

```bash
# All tests
pytest tests/ -v

# With coverage report
pytest tests/ -v --cov=src --cov-report=term-missing

# Single file
pytest tests/test_warehouse_optimizer.py -v
```

All tests use mock connectors — no live Snowflake account needed.

---

## 🤝 Contributing

1. Fork this repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes and add tests
4. Run `pytest tests/ -v` — all tests must pass
5. Submit a Pull Request

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for full text.

---

## 👤 Author

**Shailesh Chalke**  
FinOps & Snowflake Cost Optimization Specialist  
[LinkedIn](https://linkedin.com/in/shaileshchalke) | [GitHub](https://github.com/shaileshmchalke)