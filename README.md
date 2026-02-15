# Data Analysis of Pushshift Reddit Comments using Apache Spark

**An end-to-end data pipeline for streaming, cleaning, and analyzing millions of Reddit comments at scale.**

## Overview

Reddit generates massive volumes of text data daily across thousands of communities. This project builds a complete ETL pipeline that streams Reddit comments from a 292 GB dataset, processes them with Apache Spark, engineers analytical features, and outputs structured data for visualization and reporting.

The pipeline processes **3 million+ comments** from the [Pushshift Reddit Comments dataset](https://huggingface.co/datasets/fddemarco/pushshift-reddit-comments) (1.85 billion total records, 2005–2023) without requiring a full download.

## Tech Stack

| Tool | Purpose |
|------|---------|
| **Apache Spark (PySpark)** | Distributed data processing and aggregation |
| **HuggingFace Datasets** | Streaming extraction from cloud-hosted data |
| **Matplotlib** | Data visualization |
| **Python** | Pipeline orchestration and feature engineering |

## Pipeline Architecture

```
HuggingFace Dataset (292 GB)
        │
        ▼
┌─────────────────┐
│   EXTRACTION    │  Streaming via HuggingFace library
│   (JSONL)       │  No full download required
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  TRANSFORMATION │  Schema validation, cleaning,
│  & CLEANING     │  type casting, null filtering
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    FEATURE      │  12 engineered features:
│   ENGINEERING   │  text, temporal, engagement, sentiment
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AGGREGATION    │  4 analytical views:
│  & OUTPUT       │  subreddits, hourly, sentiment, engagement
└────────┬────────┘
         │
         ▼
   Parquet / CSV / JSON / PNG
```

## Features Engineered

The pipeline generates 12 analytical features from raw comment data:

| Category | Features |
|----------|----------|
| **Text** | `comment_length`, `word_count` |
| **Temporal** | `created_ts`, `year`, `month`, `day`, `hour`, `day_of_week` |
| **Engagement** | `engagement_rate` (score / comment length + 1) |
| **Sentiment** | `sentiment_proxy` (positive / negative / neutral based on score thresholds) |
| **Boolean** | `is_long_comment` (>500 chars), `is_controversial` |

## Aggregations

Four analytical views are generated for dashboard use:

1. **Top Subreddits** — Comment counts, average scores, engagement rates per community
2. **Hourly Activity** — Comment volume and average scores by hour (0–23)
3. **Sentiment Distribution** — Breakdown of positive, negative, and neutral comments
4. **Engagement Metrics** — Average engagement rates and controversial comment counts by subreddit

## Visualizations

### Reddit Activity by Hour
<img width="2000" height="1000" alt="hourly_activity" src="https://github.com/user-attachments/assets/863fd6c8-5513-48d2-ac59-d06157c42caf" />

Peak activity occurs during US evening hours (16:00–22:00 UTC), with a dip during late morning.

### Sentiment Distribution
<img width="1600" height="1200" alt="sentiment_distribution" src="https://github.com/user-attachments/assets/fbc30677-b895-4370-ab70-8ea6a4b8a3c9" />

85.2% neutral, 13.7% positive, 1.1% negative — most comments fall within a moderate score range.

### Top 20 Subreddits by Comment Count
<img width="2400" height="1600" alt="top20_subreddits" src="https://github.com/user-attachments/assets/2d289b8c-bb36-4ddb-922a-6a900747d246" />

AskReddit dominates with 370K+ comments, followed by funny, pics, and gaming.

## Output Formats

| Format | Purpose |
|--------|---------|
| **Parquet** | Partitioned by year/month for efficient querying |
| **CSV** | Summary aggregations for external tools |
| **JSON** | Dashboard-ready top 50 results |
| **PNG** | Static visualizations (200 DPI) |

## How to Run

### Prerequisites
- Python 3.10+
- Apache Spark / PySpark
- Virtual environment recommended

### Setup
```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run the Pipeline
```bash
python cloud_computing_project.py
```

The pipeline will stream comments from HuggingFace, process them, and output results to the `output/` directory.

## Data Quality

The pipeline includes a `DataQualityMetrics` class that tracks records at each stage — initial count, post-cleaning count, and records removed — saved to `quality_reports/pipeline_quality.json` for reproducibility verification.
