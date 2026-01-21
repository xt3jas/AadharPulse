# AadharPulse - Operational Intelligence Platform

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109+-green.svg)](https://fastapi.tiangolo.com/)
[![Polars](https://img.shields.io/badge/Polars-0.20+-orange.svg)](https://pola.rs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A specialized **Lakehouse** application that ingests Aadhar operational logs and transforms them into strategic insights. The system identifies **Migration**, **Fraud**, and **Operational Volatility** patterns using advanced statistical frameworks.

![Dashboard Preview](docs/dashboard-preview.png)

## 🎯 Key Features

- **Gatekeeper Engine**: Auto-detects CSV schemas and validates data integrity
- **Pulse Analytics**: Calculates 5 niche parameters for operational intelligence
- **Lakehouse Architecture**: Bronze → Silver → Gold data layers with Delta Lake
- **Interactive Dashboard**: 3-tab Streamlit app with drill-downs and exports

## 📊 The 5 Niche Parameters

| Parameter | Name | Purpose | Threshold |
|-----------|------|---------|-----------|
| **OVS** | Operational Volatility Score | Detect temporary camps vs permanent centers | > 4.0 = Camp |
| **MII** | Migration Impact Index | Identify migration hotspots | > 0.40 = Hotspot |
| **DHR** | Data Hygiene Ratio | Flag potential fraud patterns | > 1.5 = High Risk |
| **TLP** | Temporal Load Profile | Optimize staffing schedules | Weekend/School patterns |
| **SML** | Saturation Maturity Level | Classify district maturity | K-Means clustering |

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- pip or conda

### Installation

```bash
# Clone the repository
cd aadhar_pulse

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the API

```bash
# Start the FastAPI server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# API documentation available at:
# http://localhost:8000/docs
```

### Running the Dashboard

```bash
# In a new terminal
cd dashboard
streamlit run dashboard.py --server.port 8501

# Dashboard available at:
# http://localhost:8501
```

## 📤 Data Ingestion

### CSV Formats

The system auto-detects three types of CSV files:

**Enrolment CSV:**
```csv
date,state,district,pincode,age_0_5,age_5_17,age_18_greater
2024-01-15,KARNATAKA,BANGALORE,560001,100,200,300
```

**Biometric Update CSV:**
```csv
date,state,district,pincode,bio_age_5_17,bio_age_17_
2024-01-15,KARNATAKA,BANGALORE,560001,50,150
```

**Demographic Update CSV:**
```csv
date,state,district,pincode,demo_age_5_17,demo_age_17_
2024-01-15,KARNATAKA,BANGALORE,560001,30,80
```

### Upload via API

```bash
# Upload a CSV file
curl -X POST "http://localhost:8000/api/v1/ingest" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@enrolment_data.csv"
```

### Transform Data

```bash
# Transform Bronze to Silver
curl -X POST "http://localhost:8000/api/v1/transform/silver?schema_type=enrolment"
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    AadharPulse Architecture                  │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   CSV       │───▶│   FastAPI   │───▶│   Polars    │     │
│  │   Upload    │    │   Gateway   │    │   Engine    │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│                                              │               │
│                                              ▼               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                 Delta Lake Storage                   │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │   │
│  │  │  Bronze  │─▶│  Silver  │─▶│   Gold   │          │   │
│  │  │  (Raw)   │  │ (Clean)  │  │(Features)│          │   │
│  │  └──────────┘  └──────────┘  └──────────┘          │   │
│  └─────────────────────────────────────────────────────┘   │
│                                              │               │
│                                              ▼               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Streamlit Dashboard                      │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │   │
│  │  │ Command  │  │Operation │  │  Risk &  │          │   │
│  │  │ Center   │  │  Intel   │  │Governance│          │   │
│  │  └──────────┘  └──────────┘  └──────────┘          │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
aadhar_pulse/
├── app/
│   ├── main.py                  # FastAPI entry point
│   ├── api/
│   │   ├── routes.py            # API endpoints
│   │   └── schemas.py           # Pydantic models
│   ├── core/
│   │   ├── config.py            # Configuration
│   │   └── constants.py         # Thresholds
│   ├── services/
│   │   ├── ingestion.py         # Data ingestion
│   │   ├── analytics.py         # InsightGenerator
│   │   └── clustering.py        # K-Means SML
│   └── utils/
│       ├── delta_ops.py         # Delta Lake ops
│       └── date_parser.py       # Date utilities
├── dashboard/
│   ├── dashboard.py             # Streamlit app
│   └── components/
│       ├── maps.py              # Altair charts
│       └── metrics.py           # KPI widgets
├── data/
│   ├── bronze/                  # Raw data
│   ├── silver/                  # Cleaned data
│   └── gold/                    # Feature tables
├── tests/
│   └── unit/
│       ├── test_ingestion.py
│       └── test_analytics.py
├── requirements.txt
├── Dockerfile
└── README.md
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/unit/test_analytics.py::TestOVSCalculation::test_ovs_high_volatility_spike_pattern -v

# Run with coverage
pytest tests/ --cov=app --cov-report=html
```

## 🐳 Docker

```bash
# Build the image
docker build -t aadhar-pulse .

# Run the container
docker run -p 8000:8000 -p 8501:8501 aadhar-pulse
```

## 📖 API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/ingest` | POST | Upload and validate CSV file |
| `/api/v1/transform/silver` | POST | Transform Bronze to Silver |
| `/api/v1/metrics/summary` | GET | National summary metrics |
| `/api/v1/metrics/district/{name}` | GET | District-level insights |
| `/api/v1/metrics/pincode/{code}` | GET | Pincode-level insights |
| `/api/v1/health` | GET | Health check |

## 📊 Dashboard Tabs

### Tab 1: Command Center
- National KPI ticker
- District Maturity Map (interactive scatter plot)
- Cluster distribution summary

### Tab 2: Operational Intelligence
- Searchable pincode table sorted by OVS
- Temporal Load Profile chart for selected pincode
- Staffing recommendations

### Tab 3: Risk & Governance
- High-risk district watchlist (DHR > 1.5 or MII > 0.4)
- Vigilance report generation (PDF export)

## 🔧 Configuration

Environment variables (or `.env` file):

```env
APP_NAME=AadharPulse
DEBUG=false
API_HOST=0.0.0.0
API_PORT=8000
```

## 📈 Performance

- **Polars**: Rust-based DataFrame engine for 10-100x faster processing than Pandas
- **Delta Lake**: ACID transactions, time travel, and efficient upserts
- **Async API**: FastAPI with async/await for high concurrency

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Submit a pull request

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

- UIDAI for Aadhar infrastructure
- Polars team for the incredible DataFrame library
- Delta Lake team for the Lakehouse format
