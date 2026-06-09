<div align="center">

# 🎬 YouTube Trending Data Pipeline

### Production-grade cloud ETL on AWS — live data, real architecture, zero shortcuts.

![AWS](https://img.shields.io/badge/AWS-Cloud-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Glue-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Step Functions](https://img.shields.io/badge/Step_Functions-Orchestrated-FF4F8B?style=for-the-badge&logo=amazonaws&logoColor=white)

**Built by [Sudheendra Nekkanti](https://github.com/Sudheendra66)**

</div>

---

## 💡 What Makes This Different

Most data engineering projects pull a static CSV and call it a pipeline. This one doesn't.

This pipeline hits the **YouTube Data API v3 live**, ingests trending video data across **10 countries**, enforces strict data quality gates, and delivers clean analytics tables — fully automated, every 6 hours, on AWS.

No manual steps. No hardcoded data. If it fails, an alert fires. If the data is stale or dirty, Gold never runs.

That's what a real pipeline looks like.

---

## 🏗️ Architecture

![Architecture Diagram](YouTube%20Trending%20Data%20Pipeline.png)

The pipeline follows **Medallion Architecture** — an industry-standard pattern used at companies like Databricks and Netflix:

```
YouTube API v3
      │
      ▼
  🥉 BRONZE          Raw JSON/CSV → S3  (partitioned by region/date/hour)
      │
      ▼
  🥈 SILVER          PySpark Glue jobs → type casting, dedup, engagement metrics, Parquet
      │
      ▼
  ✅ DQ GATE         Row count · Null % · Schema · Value ranges · Freshness
      │                         │
      │                    FAIL → SNS Alert → Pipeline halts
      ▼
  🥇 GOLD            3 analytics tables → Athena → QuickSight
```

**Orchestration:** AWS Step Functions handles the full DAG — parallel execution, retry logic (3x with exponential backoff), and failure routing.

---

## 📊 What Gets Produced

Three production-ready analytics tables land in the Gold layer:

| Table | What It Answers |
|---|---|
| `trending_analytics` | Which regions are trending what? Views, likes, engagement — daily. |
| `channel_analytics` | Which channels dominate trending? Ranked per region. |
| `category_analytics` | What categories own the most views? With % share. |

All queryable via **Athena SQL** in seconds. Plug straight into QuickSight or any BI tool.

---

## 🛠️ Tech Stack

| | Technology |
|---|---|
| **Compute** | AWS Lambda · AWS Glue (PySpark) |
| **Storage** | Amazon S3 — Parquet + Snappy compression |
| **Orchestration** | AWS Step Functions |
| **Scheduling** | Amazon EventBridge (runs every 6 hours) |
| **Metadata** | AWS Glue Data Catalog |
| **Query Engine** | Amazon Athena |
| **Alerting** | Amazon SNS |
| **Monitoring** | Amazon CloudWatch |
| **Languages** | Python 3 · PySpark · SQL |
| **Libraries** | Pandas · AWS Wrangler · Boto3 |

---

## 🔍 Data Quality — Not an Afterthought

Before a single row reaches the Gold layer, a dedicated Lambda runs 5 checks on Silver data:

| Check | Threshold |
|---|---|
| Row count | ≥ 10 rows |
| Null % on critical columns | ≤ 5% |
| Schema validation | All required columns present |
| Value ranges | Views sanity check |
| Data freshness | Last load < 48 hours ago |

Fail any check → SNS alert fires with full details → Gold job does **not** run. Bad data never reaches analytics.

---

## 📁 Project Structure

```
youtube-data-pipeline/
├── lambdas/
│   ├── youtube_api_integstion/    # Live ingestion from YouTube API v3
│   └── json_to_parquet/           # Category reference data transformer
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py   # PySpark: cleanse + enrich
│   └── silver_to_gold_analytics.py      # PySpark: aggregate to 3 tables
├── data_quality/
│   └── dq_lambda.py               # 5-point quality gate
├── step_functions/
│   └── pipeline_orchestation.json # Full DAG definition
├── scripts/
│   ├── aws_copy.sh                # Load historical Kaggle data to Bronze
│   └── information.md             # AWS resource reference
├── data/                          # 10-region CSV + JSON reference files
└── YouTube Trending Data Pipeline.png
```

---

## 🚀 Deploy It Yourself

### Prerequisites
- AWS Account (Lambda · Glue · S3 · Step Functions · SNS · Athena · EventBridge · IAM)
- [YouTube Data API v3 key](https://console.cloud.google.com/apis/credentials)
- AWS CLI configured
- Python 3.9+

### 1 — Provision Infrastructure

```bash
# S3 buckets
aws s3 mb s3://yt-data-pipeline-bronze-<region>-<env>
aws s3 mb s3://yt-data-pipeline-silver-<region>-<env>
aws s3 mb s3://yt-data-pipeline-gold-<region>-<env>
aws s3 mb s3://yt-data-pipeline-script-<region>-<env>

# Glue databases
aws glue create-database --database-input '{"Name": "yt_pipeline_bronze_<env>"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_silver_<env>"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_gold_<env>"}'

# SNS alerts
aws sns create-topic --name yt-data-pipeline-alerts-<env>
aws sns subscribe --topic-arn <topic-arn> --protocol email --notification-endpoint <your-email>
```

### 2 — Deploy Lambdas

```bash
cd lambdas/youtube_api_integstion
zip -r function.zip lambda_function.py
aws lambda create-function \
  --function-name yt-data-pipeline-youtube-ingestion-<env> \
  --runtime python3.9 \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --role <lambda-execution-role-arn> \
  --timeout 300 --memory-size 256
# Repeat for json_to_parquet and dq_lambda
```

### 3 — Upload Glue Scripts & Create Jobs

```bash
aws s3 cp glue_jobs/bronze_to_silver_statistics.py s3://yt-data-pipeline-script-<region>-<env>/glue_jobs/
aws s3 cp glue_jobs/silver_to_gold_analytics.py s3://yt-data-pipeline-script-<region>-<env>/glue_jobs/

aws glue create-job \
  --name yt-data-pipeline-bronze-to-silver-<env> \
  --role <glue-role-arn> \
  --command '{"Name":"glueetl","ScriptLocation":"s3://yt-data-pipeline-script-<region>-<env>/glue_jobs/bronze_to_silver_statistics.py"}' \
  --glue-version "4.0" --number-of-workers 2 --worker-type G.1X
```

### 4 — Deploy Step Functions & Schedule

```bash
aws stepfunctions create-state-machine \
  --name yt-data-pipeline \
  --definition file://step_functions/pipeline_orchestation.json \
  --role-arn <step-functions-role-arn>

aws events put-rule --name yt-pipeline-schedule --schedule-expression "rate(6 hours)"
aws events put-targets --rule yt-pipeline-schedule \
  --targets '[{"Id":"1","Arn":"<state-machine-arn>","RoleArn":"<eventbridge-role-arn>"}]'
```

### 5 — (Optional) Backfill with Kaggle Data

```bash
cd data && bash ../scripts/aws_copy.sh
```

---

## ▶️ Run It

```bash
# Manual trigger
aws stepfunctions start-execution --state-machine-arn <state-machine-arn>
```

**Execution flow:**
```
Ingestion → Silver (parallel) → DQ Gate → Gold → SNS Notification
```
Each step retries 3× with exponential backoff. One SNS notification on success or failure either way.

---

## 🌍 Supported Regions

US · GB · CA · DE · FR · IN · JP · KR · MX · RU

---

## 🙏 Credit

Project inspired by [Darshil Parmar's](https://www.youtube.com/@DarshilParmar) YouTube walkthrough. Independently built, deployed, and extended as part of my data engineering portfolio.

---

<div align="center">

**Sudheendra Nekkanti** · [GitHub @Sudheendra66](https://github.com/Sudheendra66)

*If you found this useful, drop a ⭐ — it means a lot!*

</div>
