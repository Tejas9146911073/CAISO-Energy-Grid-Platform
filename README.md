# ⚡ CAISO Real-Time Energy Grid & Market Analytics Platform

[![Live Dashboard](https://img.shields.io/badge/Live_Dashboard-tencercloud.site-10B981?style=for-the-badge&logo=vercel)](https://tencercloud.site)
[![API Docs](https://img.shields.io/badge/Swagger_API-api.tencercloud.site-3B82F6?style=for-the-badge&logo=fastapi)](https://api.tencercloud.site/docs)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-18-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://react.dev)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Aiven_Managed-231F20?style=for-the-badge&logo=apachekafka)](https://aiven.io)
[![PostgreSQL](https://img.shields.io/badge/AWS_RDS-PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://aws.amazon.com/rds/)

An enterprise-grade, lightweight, and cost-efficient ($0/month) real-time data engineering platform that ingests, streams, and visualizes 5-minute interval electrical grid data from the **California Independent System Operator (CAISO)**.

The platform captures wholesale electricity prices (**Locational Marginal Pricing - LMPs**) and **System Load** (grid demand vs. day-ahead forecast) across major transmission hubs, storing them in a time-series optimized **AWS RDS PostgreSQL** instance with sub-millisecond B-Tree indexing and serving them to an interactive **React** dashboard via **FastAPI**.

---

## 🌐 Live Production Deployments

* **🖥️ Web Dashboard (Frontend)**: [https://tencercloud.site](https://tencercloud.site) *(Hosted on Vercel Global Edge CDN)*
* **⚡ REST API Documentation (Backend)**: [https://api.tencercloud.site/docs](https://api.tencercloud.site/docs) *(FastAPI + Nginx on AWS EC2)*

---

## 🏗️ System Architecture & Data Flow

```mermaid
graph TD
    subgraph INGESTION ["1. Ingestion Layer (AWS EC2)"]
        CAISO["CAISO OASIS Public API"] -->|5-min Real-Time Settlement| Producer["producer.py (Python Daemon)"]
        Cron["Linux Cron (23:59 UTC)"] -->|Daily Reconciliation| Backfill["backfill.py (Self-Healing Upsert)"]
    end

    subgraph STREAMING ["2. Event Streaming (Aiven Cloud)"]
        Producer -->|Publish (SASL_SSL)| Kafka["Aiven Managed Apache Kafka"]
    end

    subgraph STORAGE ["3. Time-Series Storage (AWS RDS)"]
        Kafka -->|Consume Stream| Consumer["consumer.py (Python Daemon)"]
        Consumer -->|Bulk Insert| RDS[("AWS RDS PostgreSQL<br/>(B-Tree Composite Indexing)")]
        Backfill -->|Gap-Filling Upsert| RDS
    end

    subgraph SERVING ["4. Backend API (AWS EC2)"]
        RDS -->|SQL Range Queries (<2ms)| FastAPI["FastAPI Service (api/main.py)"]
        FastAPI -->|Reverse Proxy| Nginx["Nginx (Port 80)"]
        Nginx -->|SSL Proxy| Cloudflare["Cloudflare (api.tencercloud.site)"]
    end

    subgraph PRESENTATION ["5. Frontend UI (Vercel Edge)"]
        Cloudflare -->|HTTPS REST API / JSON| ReactApp["React 18 Dashboard (tencercloud.site)<br/>• Recharts Time-Series<br/>• Node Selectors (NP15, SP15, ZP26)<br/>• 30s Auto-Polling"]
    end
```

---

## 🚀 Key Features

### 1. High-Frequency Grid Ingestion
* Ingests 5-minute Real-Time Dispatch (RTD) wholesale electricity prices from CAISO OASIS for the three primary California transmission hubs:
  * **`TH_NP15`** (Northern California / Bay Area)
  * **`TH_SP15`** (Southern California / Los Angeles & San Diego)
  * **`TH_ZP26`** (Central California / Fresno & Central Valley)
* Multiplexes **Total LMP ($/MWh)**, **Marginal Congestion Cost (MCC)**, and **Marginal Loss Cost (MCL)** components into an event stream.
* Tracks live grid power demand (MW) alongside CAISO's day-ahead load forecast.

### 2. Time-Series Database Optimization
* Structured in a **Star Schema** on **Amazon RDS PostgreSQL (`db.t4g.micro`)**.
* Employs **Composite B-Tree Indexes** on `(node, event_timestamp DESC)` to eliminate expensive in-memory database sorting and achieve **sub-2ms query response times**.

### 3. Decoupled Jamstack Architecture
* **Frontend**: React 18 + Tailwind CSS + Recharts deployed on Vercel's global edge network. Uses **0 MB of EC2 RAM**, ensuring the server never runs out of memory.
* **Backend**: Asynchronous FastAPI service running under Systemd on EC2, reverse-proxied via Nginx with automated Cloudflare SSL encryption.

### 4. Self-Healing & Daily Reconciliation
* A nightly cron job runs `backfill.py` at 23:59 UTC, executing SQL `UPSERT` statements to reconcile and heal any data gaps caused by temporary network interruptions.

---

## 🗄️ Database Schema Design

```sql
-- 1. Transmission Nodes Dimension Table
CREATE TABLE dim_caiso_nodes (
    node_id VARCHAR(30) PRIMARY KEY,
    node_name VARCHAR(100) NOT NULL,
    location VARCHAR(50) NOT NULL,
    voltage_level_kv NUMERIC(5, 1) NOT NULL
);

-- 2. Fact Table: 5-Minute Locational Marginal Pricing (LMP)
CREATE TABLE fact_caiso_lmp (
    node VARCHAR(30) NOT NULL REFERENCES dim_caiso_nodes(node_id),
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    lmp_type VARCHAR(10) NOT NULL, -- 'LMP' (Total), 'MCC' (Congestion), 'MCL' (Loss)
    price_per_mwh NUMERIC(10, 2) NOT NULL,
    date DATE NOT NULL,
    PRIMARY KEY (node, event_timestamp, lmp_type)
);

-- Composite B-Tree Index for sub-millisecond range queries
CREATE INDEX idx_caiso_lmp_time ON fact_caiso_lmp (node, event_timestamp DESC);

-- 3. Fact Table: System Load Demand vs. Forecast
CREATE TABLE fact_caiso_load (
    event_timestamp TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    actual_load_mw INTEGER NOT NULL,
    forecast_load_mw INTEGER NOT NULL,
    date DATE NOT NULL
);
```

---

## 🔌 REST API Endpoints

The backend is built with FastAPI and exposes the following production endpoints at `https://api.tencercloud.site`:

| Method | Endpoint | Description | Query Parameters |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/status` | System health, latest timestamps, database row counts | None |
| `GET` | `/api/prices` | 5-minute historical & live LMP price curves for charting | `node` (e.g. `TH_SP15`), `lmp_type` (default `LMP`), `limit` (default `288`) |
| `GET` | `/api/load` | 5-minute actual power demand vs. day-ahead forecast (MW) | `limit` (default `288`) |
| `GET` | `/api/nodes` | Metadata catalog for transmission hubs (`dim_caiso_nodes`) | None |
| `GET` | `/docs` | Interactive Swagger UI API documentation | None |

---

## 📁 Repository Structure

```text
├── api/
│   └── main.py              # FastAPI backend application & CORS configuration
├── dashboard/               # React 18 frontend dashboard
│   ├── src/
│   │   ├── App.jsx          # Interactive UI, Recharts graphs & live polling
│   │   ├── main.jsx         # React DOM root mounting
│   │   └── index.css        # Tailwind CSS imports & styling
│   ├── index.html           # HTML5 template
│   ├── package.json         # React dependencies
│   └── vite.config.js       # Vite build tooling
├── kafka/
│   ├── caiso_bootstrap.py   # Historical 7-day bootstrap & index builder
│   ├── producer.py          # 24/7 CAISO poller & Aiven Kafka publisher
│   ├── consumer.py          # 24/7 Kafka-to-PostgreSQL streaming loader
│   └── backfill.py          # Daily self-healing reconciliation script
├── db_setup.py              # Database schema initialization script
├── requirements.txt         # Python backend dependencies
└── README.md                # Project documentation
```

---

## 🛠️ Local Development & Setup

### 1. Prerequisites
* Python 3.10+
* Node.js 18+ and npm
* PostgreSQL Instance or AWS RDS
* Apache Kafka Broker (or Aiven Kafka)

### 2. Environment Configuration
Create a `.env` file in the project root:
```env
# AWS RDS POSTGRESQL
RDS_POSTGRES_HOST=your-rds-endpoint.amazonaws.com
RDS_POSTGRES_USER=postgres
RDS_POSTGRES_PASSWORD=your_master_password

# AIVEN KAFKA
AIVEN_KAFKA_BOOTSTRAP_SERVER=your_kafka_uri:port
AIVEN_KAFKA_CA_CERT_PATH=/path/to/ca.pem
AIVEN_KAFKA_USERNAME=avnadmin
AIVEN_KAFKA_PASSWORD=your_aiven_password
```

### 3. Backend Setup
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Initialize database schema
python db_setup.py

# Run historical bootstrap
python kafka/caiso_bootstrap.py

# Start FastAPI backend server
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### 4. Frontend Setup
```bash
cd dashboard

# Install dependencies
npm install

# Start Vite dev server
npm run dev
```
Navigate to `http://localhost:5173` in your browser.

---

## ☁️ Production Deployment Guide (AWS EC2 & Vercel)

### 1. Systemd Background Services (EC2)
Configure the Producer, Consumer, and FastAPI backend as 24/7 services in `/etc/systemd/system/`:

```bash
# Enable and start all services
sudo systemctl daemon-reload
sudo systemctl enable --now caiso-producer
sudo systemctl enable --now caiso-consumer
sudo systemctl enable --now caiso-api
```

### 2. Daily Cron Job (EC2)
Add the daily backfill to crontab (`crontab -e`):
```bash
59 23 * * * /home/ubuntu/CAISO-Energy-Grid-Platform/.venv/bin/python /home/ubuntu/CAISO-Energy-Grid-Platform/kafka/backfill.py >> /home/ubuntu/backfill.log 2>&1
```

### 3. Nginx Reverse Proxy (EC2)
Configure `/etc/nginx/sites-available/default` to route port 80 to FastAPI on port 8000:
```nginx
server {
    listen 80 default_server;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### 4. Vercel Frontend Deployment
1. Connect your GitHub repository to [Vercel](https://vercel.com).
2. Set the **Root Directory** to `dashboard`.
3. Add your custom domain (`tencercloud.site`) in Vercel settings and link the DNS records via Cloudflare.

---

## 📜 License
This project is open-source and available under the [MIT License](LICENSE).
