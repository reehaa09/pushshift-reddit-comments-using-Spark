import json
import sys
import logging
from pathlib import Path
from typing import Optional, Dict, Tuple
from datetime import datetime

from datasets import load_dataset
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Config
DATASET_NAME = "fddemarco/pushshift-reddit-comments"
DATASET_URL = "https://huggingface.co/datasets/fddemarco/pushshift-reddit-comments"

# Output locations
BASE_DIR = Path("/Users/reehaalthaf/Downloads/Cloud Project")
RAW_DIR = BASE_DIR / "raw"
EXTRACT_JSONL = RAW_DIR / "reddit_extracted.jsonl"
PARQUET_ANALYTICAL = BASE_DIR / "reddit_analytical"
SUMMARY_DIR = BASE_DIR / "summaries"
VIZ_DIR = BASE_DIR / "visualizations"
QUALITY_DIR = BASE_DIR / "quality_reports"
DASHBOARD_DATA = BASE_DIR / "dashboard_data"

# Spark configuration optimized for local processing
SPARK_CONFIG = {
    "spark.app.name": "RedditAnalysisPipeline",
    "spark.driver.memory": "8g",
    "spark.executor.memory": "8g",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
    "spark.sql.shuffle.partitions": "200",
    "spark.default.parallelism": "200",
    "spark.sql.files.maxPartitionBytes": "128m",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}


# Data Quality Metrics
class DataQualityMetrics:
    """Track and report data quality throughout the pipeline"""
    
    def __init__(self):
        self.metrics = {}
        self.start_time = datetime.now()
    
    def add_metric(self, stage: str, metric_name: str, value):
        """Add a quality metric"""
        if stage not in self.metrics:
            self.metrics[stage] = {}
        self.metrics[stage][metric_name] = value
        logger.info(f"[{stage}] {metric_name}: {value}")
    
    def save_report(self, output_path: Path):
        """Save quality report to JSON"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        report = {
            "pipeline_start": self.start_time.isoformat(),
            "pipeline_end": datetime.now().isoformat(),
            "metrics": self.metrics
        }
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Quality report saved to {output_path}")


# Spark Session
def build_spark_session(config: Optional[dict] = None) -> SparkSession:
    """
    Build and configure Spark session with optimizations.
    
    JUSTIFICATION: Local[*] uses all available cores for parallel processing.
    Adaptive execution automatically optimizes shuffle partitions and joins.
    """
    logger.info("Creating SparkSession with optimized configuration...")
    builder = SparkSession.builder.master("local[*]")
    
    conf = config or SPARK_CONFIG
    for k, v in conf.items():
        builder = builder.config(k, v)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Available cores: {spark.sparkContext.defaultParallelism}")
    
    return spark


# Extraction
def extract_stream_to_jsonl(
    dataset_name: str = DATASET_NAME,
    jsonl_path: Path = EXTRACT_JSONL,
    limit: Optional[int] = 1_000_000,
    batch_log: int = 100_000,
    quality_metrics: Optional[DataQualityMetrics] = None,
) -> Path:

    logger.info(f"Extraction Stage")
    logger.info(f"Dataset: {dataset_name}")
    logger.info(f"Target: {jsonl_path}")
    logger.info(f"Limit: {limit if limit else 'ALL'}")
    
    try:
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        
        if jsonl_path.exists():
            logger.warning(f"Removing existing file: {jsonl_path}")
            jsonl_path.unlink()
        
        stream = load_dataset(dataset_name, split="train", streaming=True)
        
        count = 0
        null_fields = 0
        deleted_removed = 0
        
        with jsonl_path.open("w", encoding="utf-8") as outf:
            for record in stream:
                # Track data quality issues
                if not record.get("body"):
                    null_fields += 1
                    continue
                
                if record.get("body", "").lower() in ["[deleted]", "[removed]"]:
                    deleted_removed += 1
                    continue
                
                slim = {
                    "id": record.get("id"),
                    "body": record.get("body"),
                    "score": record.get("score"),
                    "created_utc": record.get("created_utc"),
                    "subreddit": record.get("subreddit"),
                    "subreddit_id": record.get("subreddit_id"),
                    "author": record.get("author"),
                    "controversiality": record.get("controversiality"),
                }
                
                outf.write(json.dumps(slim) + "\n")
                count += 1
                
                if batch_log and count % batch_log == 0:
                    logger.info(f"Extracted {count:,} rows...")
                
                if limit is not None and count >= limit:
                    break
        
        # Record quality metrics
        if quality_metrics:
            quality_metrics.add_metric("extraction", "total_records", count)
            quality_metrics.add_metric("extraction", "null_body_records", null_fields)
            quality_metrics.add_metric("extraction", "deleted_removed_records", deleted_removed)
            quality_metrics.add_metric("extraction", "file_size_mb", 
                                      round(jsonl_path.stat().st_size / 1024 / 1024, 2))
        
        logger.info(f"Extraction complete: {count:,} records")
        return jsonl_path
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}", exc_info=True)
        raise


# Transformation
def clean_transform_and_aggregate(
    spark: SparkSession, 
    jsonl_path: Path,
    quality_metrics: Optional[DataQualityMetrics] = None
) -> Dict[str, DataFrame]:
    logger.info(f"Transformation Stage")
    logger.info(f"Reading JSONL: {jsonl_path}")
    
    try:
        # Read with error handling
        df = spark.read.json(str(jsonl_path))
        initial_count = df.count()
        logger.info(f"Initial records: {initial_count:,}")
        
        if quality_metrics:
            quality_metrics.add_metric("transformation", "initial_records", initial_count)
        
        # Schema validation
        logger.info("Initial schema:")
        df.printSchema()
        
        # Select and validate columns
        selected = ["id", "body", "score", "created_utc", "subreddit", 
                   "subreddit_id", "author", "controversiality"]
        existing_cols = [c for c in selected if c in df.columns]
        df = df.select(*existing_cols)
        
        # Cleaning: Remove nulls and deleted content
        logger.info("Applying data cleaning filters...")
        df = df.filter(F.col("body").isNotNull())
        df = df.filter(~F.lower(F.col("body")).isin([F.lit("[deleted]"), F.lit("[removed]")]))
        
        after_clean = df.count()
        logger.info(f"After cleaning: {after_clean:,} records ({initial_count - after_clean:,} removed)")
        
        if quality_metrics:
            quality_metrics.add_metric("transformation", "after_cleaning", after_clean)
            quality_metrics.add_metric("transformation", "cleaning_removed", 
                                      initial_count - after_clean)
        
        # Type Conversions
        logger.info("Converting data types...")
        if "created_utc" in df.columns:
            df = df.withColumn("created_utc_long", F.col("created_utc").cast("long"))
            df = df.filter(F.col("created_utc_long").isNotNull())
        else:
            df = df.withColumn("created_utc_long", F.lit(None).cast("long"))
        
        if "score" in df.columns:
            df = df.withColumn("score_num", F.col("score").cast("double"))
            df = df.filter(F.col("score_num").isNotNull())
        else:
            df = df.withColumn("score_num", F.lit(0.0).cast("double"))
        
        # Feature Engineering
        logger.info("Engineering features...")
        
        # Text features
        df = df.withColumn("comment_length", F.length(F.col("body")))
        df = df.withColumn("word_count", F.size(F.split(F.col("body"), r"\s+")))
        
        # Temporal features (keeping for data partitioning, but not for visualization)
        df = df.withColumn("created_ts", F.from_unixtime(F.col("created_utc_long")))
        df = df.withColumn("year", F.year(F.col("created_ts")))
        df = df.withColumn("month", F.month(F.col("created_ts")))
        df = df.withColumn("day", F.dayofmonth(F.col("created_ts")))
        df = df.withColumn("hour", F.hour(F.col("created_ts")))
        df = df.withColumn("day_of_week", F.dayofweek(F.col("created_ts")))
        
        # Boolean indicators
        df = df.withColumn("is_long_comment", 
                          F.when(F.col("comment_length") > 500, 1).otherwise(0))
        
        if "controversiality" in df.columns:
            df = df.withColumn("is_controversial", 
                             F.when(F.col("controversiality").cast("int") == 1, 1).otherwise(0))
        else:
            df = df.withColumn("is_controversial", F.lit(0))
        
        # Engagement metric
        df = df.withColumn("engagement_rate", 
                          F.col("score_num") / (F.col("comment_length") + F.lit(1)))
        
        # Sentiment proxy (simple: positive score = positive sentiment)
        df = df.withColumn("sentiment_proxy", 
                          F.when(F.col("score_num") > 5, "positive")
                           .when(F.col("score_num") < -5, "negative")
                           .otherwise("neutral"))
        
        # Final analytical DataFrame
        analytical_cols = [
            "id", "subreddit", "subreddit_id", "author",
            "created_ts", "created_utc_long", "year", "month", "day", "hour", "day_of_week",
            "score_num", "comment_length", "word_count",
            "is_long_comment", "is_controversial", "engagement_rate", "sentiment_proxy",
            "body"
        ]
        analytical_cols_existing = [c for c in analytical_cols if c in df.columns]
        analytical_df = df.select(*analytical_cols_existing)
        
        # Optimize partitioning for downstream queries
        analytical_df = analytical_df.repartition(20, "year", "month")
        
        final_count = analytical_df.count()
        logger.info(f"Final analytical records: {final_count:,}")
        
        if quality_metrics:
            quality_metrics.add_metric("transformation", "final_records", final_count)
            quality_metrics.add_metric("transformation", "feature_columns", 
                                      len(analytical_cols_existing))
        
        logger.info("Transformed schema:")
        analytical_df.printSchema()
        
        # aggregations for analytics
        logger.info("Computing aggregations")
        
        top_subreddits = (
            analytical_df.groupBy("subreddit")
            .agg(
                F.count("*").alias("total_comments"),
                F.avg("score_num").alias("avg_score"),
                F.avg("comment_length").alias("avg_comment_length"),
                F.sum("score_num").alias("total_score"),
                F.avg("engagement_rate").alias("avg_engagement")
            )
            .orderBy(F.desc("total_comments"))
        )
        
        hourly_activity = (
            analytical_df.groupBy("hour")
            .agg(
                F.count("*").alias("comments"),
                F.avg("score_num").alias("avg_score")
            )
            .orderBy("hour")
        )
        
        sentiment_distribution = (
            analytical_df.groupBy("sentiment_proxy")
            .agg(F.count("*").alias("count"))
            .orderBy(F.desc("count"))
        )
        
        engagement = (
            analytical_df.groupBy("subreddit")
            .agg(
                F.avg("engagement_rate").alias("avg_engagement"),
                F.sum("is_controversial").alias("controversial_count"),
                F.avg("word_count").alias("avg_words"),
            )
            .orderBy(F.desc("avg_engagement"))
        )
        
        results = {
            "analytical_df": analytical_df,
            "top_subreddits": top_subreddits,
            "hourly_activity": hourly_activity,
            "sentiment_distribution": sentiment_distribution,
            "engagement": engagement,
        }
        
        logger.info("Transformation complete")
        return results
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}", exc_info=True)
        raise


# Persistence
def persist_results(
    results: Dict[str, DataFrame],
    parquet_path: Path = PARQUET_ANALYTICAL,
    summary_dir: Path = SUMMARY_DIR,
    viz_dir: Path = VIZ_DIR,
    dashboard_dir: Path = DASHBOARD_DATA,
    quality_metrics: Optional[DataQualityMetrics] = None
):
   
    logger.info(f"Persistence Stage")
    
    try:
        analytical_df = results["analytical_df"]
        
        # 1. PARQUET (partitioned for query optimization)
        logger.info(f"Saving analytical parquet to: {parquet_path}")
        parquet_path.mkdir(parents=True, exist_ok=True)
        analytical_df.write.mode("overwrite").partitionBy("year", "month").parquet(str(parquet_path))
        logger.info("Parquet saved")
        
        # 2. CSV SUMMARIES
        summary_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Writing summary CSVs")
        
        for name, df in results.items():
            if name != "analytical_df":
                output_dir = summary_dir / name
                df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_dir))
                logger.info(f"{name}.csv saved")
        
        # 3. DASHBOARD DATA (JSON for fast loading)
        dashboard_dir.mkdir(parents=True, exist_ok=True)
        
        # Top 50 subreddits for dashboard
        top50 = results["top_subreddits"].limit(50).toPandas()
        top50.to_json(dashboard_dir / "top_subreddits.json", orient="records", indent=2)
        
        # Hourly activity
        hourly = results["hourly_activity"].toPandas()
        hourly.to_json(dashboard_dir / "hourly_activity.json", orient="records", indent=2)
        
        # Sentiment distribution
        sentiment = results["sentiment_distribution"].toPandas()
        sentiment.to_json(dashboard_dir / "sentiment_distribution.json", orient="records", indent=2)
        
        # Top engagement
        engagement = results["engagement"].limit(50).toPandas()
        engagement.to_json(dashboard_dir / "engagement.json", orient="records", indent=2)
        
        logger.info(f"Dashboard data saved to {dashboard_dir}")
        
        # 4. VISUALIZATIONS 
        viz_dir.mkdir(parents=True, exist_ok=True)
        
        # Top 20 subreddits
        if not top50.empty:
            plt.figure(figsize=(12, 8))
            top20 = top50.head(20)
            plt.barh(top20["subreddit"], top20["total_comments"])
            plt.gca().invert_yaxis()
            plt.xlabel("Total Comments")
            plt.title("Top 20 Subreddits by Comment Count")
            plt.tight_layout()
            plt.savefig(viz_dir / "top20_subreddits.png", dpi=200)
            plt.close()
            logger.info("top20_subreddits.png")
        
        # Hourly activity
        if not hourly.empty:
            plt.figure(figsize=(10, 5))
            plt.plot(hourly["hour"], hourly["comments"], marker="o", linewidth=2)
            plt.xlabel("Hour of Day")
            plt.ylabel("Number of Comments")
            plt.title("Reddit Activity by Hour")
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(viz_dir / "hourly_activity.png", dpi=200)
            plt.close()
            logger.info("hourly_activity.png")
        
        # Sentiment distribution
        if not sentiment.empty:
            plt.figure(figsize=(8, 6))
            plt.pie(sentiment["count"], labels=sentiment["sentiment_proxy"], autopct='%1.1f%%')
            plt.title("Sentiment Distribution")
            plt.tight_layout()
            plt.savefig(viz_dir / "sentiment_distribution.png", dpi=200)
            plt.close()
            logger.info("sentiment_distribution.png")
        
        logger.info("Persistence complete")
        
        if quality_metrics:
            quality_metrics.add_metric("persistence", "parquet_partitions", 
                                      len(list(parquet_path.glob("year=*"))))
            quality_metrics.add_metric("persistence", "csv_files", 
                                      len(list(summary_dir.glob("*"))))
            quality_metrics.add_metric("persistence", "visualizations", 
                                      len(list(viz_dir.glob("*.png"))))
        
    except Exception as e:
        logger.error(f"Persistence failed: {str(e)}", exc_info=True)
        raise


# Main Pipelines
def run_pipeline(
    sample_limit: int = 3_000_000,
    jsonl_path: Path = EXTRACT_JSONL,
    parquet_path: Path = PARQUET_ANALYTICAL,
):

    logger.info("REDDIT ETL PIPELINE - STARTING")
    logger.info(f"Sample limit: {sample_limit if sample_limit else 'ALL'}")
    logger.info(f"Output directory: {BASE_DIR}")
    
    quality = DataQualityMetrics()
    spark = None
    
    try:
        # Stage 1: Extraction
        jsonl = extract_stream_to_jsonl(
            limit=sample_limit, 
            jsonl_path=jsonl_path,
            quality_metrics=quality
        )
        
        # Stage 2: Spark initialization
        spark = build_spark_session()
        
        # Stage 3: Transformation
        results = clean_transform_and_aggregate(spark, jsonl, quality_metrics=quality)
        
        # Stage 4: Persistence
        persist_results(results, parquet_path=parquet_path, quality_metrics=quality)
        
        # Stage 5: Quality report
        quality.save_report(QUALITY_DIR / "pipeline_quality.json")
        
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Outputs saved to: {BASE_DIR}")
        logger.info(f"  - Parquet: {parquet_path}")
        logger.info(f"  - Summaries: {SUMMARY_DIR}")
        logger.info(f"  - Visualizations: {VIZ_DIR}")
        logger.info(f"  - Dashboard data: {DASHBOARD_DATA}")
        logger.info(f"  - Quality report: {QUALITY_DIR}")
        logger.info("")
        
        return True
        
    except Exception as e:
        logger.error("PIPELINE FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        
        # Save partial quality report
        try:
            quality.save_report(QUALITY_DIR / "pipeline_quality_failed.json")
        except:
            pass
        
        return False
        
    finally:
        if spark:
            logger.info("Stopping SparkSession...")
            spark.stop()


# CLI
if __name__ == "__main__":
    # Parse command line arguments
    arg_limit = None
    if len(sys.argv) > 1:
        try:
            arg_limit = int(sys.argv[1])
        except ValueError:
            logger.warning("Invalid argument for limit; using default")
            arg_limit = None
    
    default_limit = 3_000_000
    success = run_pipeline(sample_limit=(arg_limit if arg_limit is not None else default_limit))
    
    sys.exit(0 if success else 1)
