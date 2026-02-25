
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType, DateType
)


def create_spark_session(app_name: str, mode: str = "batch") -> SparkSession:
    """Create Spark session optimised for batch or streaming."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Snowflake connector
        .config("spark.jars.packages",
                "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,"
                "net.snowflake:snowflake-jdbc:3.14.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
        # Delta Lake
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    if mode == "streaming":
        builder = builder.config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")

    return builder.getOrCreate()



FLIGHT_EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),    False),
    StructField("event_ts",         TimestampType(), False),
    StructField("flight_number",    StringType(),    True),
    StructField("airline_code",     StringType(),    True),
    StructField("origin_iata",      StringType(),    True),
    StructField("dest_iata",        StringType(),    True),
    StructField("sched_dep_ts",     TimestampType(), True),
    StructField("actual_dep_ts",    TimestampType(), True),
    StructField("dep_delay_min",    IntegerType(),   True),
    StructField("gate_number",      StringType(),    True),
    StructField("tail_number",      StringType(),    True),
    StructField("status",           StringType(),    True),  
])

RESERVATION_SCHEMA = StructType([
    StructField("booking_ref",      StringType(),    False),
    StructField("passenger_id",     StringType(),    True),
    StructField("flight_number",    StringType(),    True),
    StructField("cabin_class",      StringType(),    True),
    StructField("total_fare",       DoubleType(),    True),
    StructField("booking_channel",  StringType(),    True),
    StructField("booking_ts",       TimestampType(), True),
])


class BatchProcessor:
    """
    Reads historical BTS data from Delta Lake, applies
    transformations and writes results to Snowflake.
    """

    def __init__(self, spark: SparkSession, snowflake_opts: dict):
        self.spark = spark
        self.sf_opts = snowflake_opts

    # ── Read ────────────────────────────────────────────────
    def read_raw_flights(self, path: str) -> DataFrame:
        df = (
            self.spark.read
            .format("delta")
            .load(path)
        )
        print(f"[Batch] Loaded {df.count():,} raw flight records")
        return df

    # ── Transform ───────────────────────────────────────────
    def transform_flights(self, df: DataFrame) -> DataFrame:
        """Apply business transformations at scale."""

        # 1. Derive date / time keys
        df = (df
            .withColumn("date_key",
                F.date_format("sched_dep_ts", "yyyyMMdd").cast(IntegerType()))
            .withColumn("dep_hour",
                F.hour("sched_dep_ts"))
        )

        # 2. Classify delay severity
        df = df.withColumn("delay_severity",
            F.when(F.col("arr_delay_min") <= 0,  "On Time")
             .when(F.col("arr_delay_min") <= 15,  "Minor (<15m)")
             .when(F.col("arr_delay_min") <= 60,  "Moderate (15-60m)")
             .when(F.col("arr_delay_min") <= 180, "Severe (1-3h)")
             .otherwise("Critical (>3h)")
        )

        # 3. Identify primary delay cause
        delay_cols = {
            "carrier_delay_min":   "Carrier",
            "weather_delay_min":   "Weather",
            "nas_delay_min":       "NAS",
            "security_delay_min":  "Security",
            "late_aircraft_delay": "Late Aircraft",
        }
        greatest_col = F.greatest(*[F.col(c) for c in delay_cols])
        df = df.withColumn("primary_delay_cause",
            F.when(F.col("carrier_delay_min")   == greatest_col, "Carrier")
             .when(F.col("weather_delay_min")   == greatest_col, "Weather")
             .when(F.col("nas_delay_min")        == greatest_col, "NAS")
             .when(F.col("security_delay_min")   == greatest_col, "Security")
             .when(F.col("late_aircraft_delay")  == greatest_col, "Late Aircraft")
             .otherwise("Unknown")
        )

        # 4. Window functions: rolling avg delay per airline/airport
        window_7d = (
            Window
            .partitionBy("airline_code", "origin_iata")
            .orderBy("date_key")
            .rowsBetween(-7, 0)
        )
        df = df.withColumn("rolling_7d_avg_delay",
            F.avg("arr_delay_min").over(window_7d))

        # 5. Percentile rank of flight delay within airline
        window_airline = Window.partitionBy("airline_code", "date_key")
        df = df.withColumn("delay_percentile",
            F.percent_rank().over(
                window_airline.orderBy("arr_delay_min")))

        return df

    def compute_route_kpis(self, df: DataFrame) -> DataFrame:
        """Aggregate route-level KPIs."""
        return (
            df.groupBy("airline_code", "origin_iata", "dest_iata")
            .agg(
                F.count("*")                           .alias("total_flights"),
                F.sum(F.when(F.col("arr_delay_min") > 15, 1).otherwise(0))
                                                       .alias("delayed_flights"),
                F.sum(F.col("is_cancelled").cast("int")).alias("cancelled"),
                F.avg("arr_delay_min")                 .alias("avg_arr_delay"),
                F.avg("dep_delay_min")                 .alias("avg_dep_delay"),
                F.avg("load_factor")                   .alias("avg_load_factor"),
                F.sum("total_revenue")                 .alias("total_revenue"),
                F.avg("distance_miles")                .alias("avg_distance"),
            )
            .withColumn("on_time_pct",
                F.round(
                    100 * (F.col("total_flights") - F.col("delayed_flights") - F.col("cancelled"))
                    / F.col("total_flights"), 2
                )
            )
        )

    # ── Write ────────────────────────────────────────────────
    def write_to_snowflake(self, df: DataFrame, table: str, mode: str = "append"):
        """Write Spark DataFrame → Snowflake via Spark connector."""
        (
            df.write
            .format("net.snowflake.spark.snowflake")
            .options(**self.sf_opts)
            .option("dbtable", table)
            .mode(mode)
            .save()
        )
        print(f"[Batch] Written to Snowflake: {table}")

    def write_to_delta(self, df: DataFrame, path: str, partition_by: list = None):
        """Write to Delta Lake with optional partitioning."""
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        print(f"[Batch] Written to Delta: {path}")

    def run(self, raw_path: str, process_date: str):
        """Execute full batch pipeline."""
        print(f"\n{'='*50}")
        print(f"Batch Pipeline | {process_date}")

        df_raw       = self.read_raw_flights(raw_path)
        df_filtered  = df_raw.filter(F.col("date_key") == int(process_date.replace("-", "")))
        df_transform = self.transform_flights(df_filtered)
        df_kpis      = self.compute_route_kpis(df_transform)

        # Write fact table
        self.write_to_snowflake(df_transform, "FACT_FLIGHT")
        # Write pre-aggregated KPIs
        self.write_to_snowflake(df_kpis, "AGG_ROUTE_KPIS", mode="overwrite")
        # Write to Delta for downstream ML
        self.write_to_delta(
            df_transform,
            "s3://airline-dw/delta/fact_flight",
            partition_by=["date_key", "airline_code"]
        )


class StreamingProcessor:
    """
    Consumes real-time flight events from Kafka.
    Computes live delay metrics with 5-minute micro-batches.
    """

    KAFKA_BROKERS = "kafka-broker:9092"
    TOPICS = {
        "flight_events":    "airline.flight.events",
        "gate_changes":     "airline.gate.changes",
        "weather_alerts":   "airline.weather.alerts",
    }

    def __init__(self, spark: SparkSession, snowflake_opts: dict):
        self.spark  = spark
        self.sf_opts = snowflake_opts

    def read_flight_stream(self) -> DataFrame:
        """Read flight events from Kafka."""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.KAFKA_BROKERS)
            .option("subscribe", self.TOPICS["flight_events"])
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 100_000)
            .load()
            .select(
                F.col("timestamp").alias("kafka_ts"),
                F.from_json(
                    F.col("value").cast(StringType()),
                    FLIGHT_EVENT_SCHEMA
                ).alias("data")
            )
            .select("kafka_ts", "data.*")
        )

    def apply_watermark_and_windows(self, df: DataFrame) -> DataFrame:
        """Apply event-time watermark for late data tolerance."""
        return (
            df
            # Allow up to 10 min of late data
            .withWatermark("event_ts", "10 minutes")
            # Tumbling 5-minute window for real-time KPIs
            .groupBy(
                F.window("event_ts", "5 minutes"),
                "airline_code",
                "origin_iata",
            )
            .agg(
                F.count("*")                           .alias("flights_in_window"),
                F.avg("dep_delay_min")                 .alias("avg_dep_delay"),
                F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))
                                                       .alias("cancellations"),
                F.countDistinct("tail_number")         .alias("active_aircraft"),
                F.max("dep_delay_min")                 .alias("max_delay"),
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end",   F.col("window.end"))
            .drop("window")
        )

    def detect_delay_cascade(self, df: DataFrame) -> DataFrame:
        """
        Detect delay cascades: an airport with >3 flights
        delayed >30 min within 15 minutes = system alert.
        """
        return (
            df
            .withWatermark("event_ts", "5 minutes")
            .groupBy(
                F.window("event_ts", "15 minutes", "5 minutes"),  # Sliding window
                "origin_iata"
            )
            .agg(
                F.sum(
                    F.when(F.col("dep_delay_min") > 30, 1).otherwise(0)
                ).alias("severe_delays"),
                F.avg("dep_delay_min").alias("avg_delay"),
            )
            .filter(F.col("severe_delays") >= 3)
            .withColumn("alert_level",
                F.when(F.col("severe_delays") >= 10, "CRITICAL")
                 .when(F.col("severe_delays") >= 5, "HIGH")
                 .otherwise("MEDIUM")
            )
            .withColumn("alert_ts", F.current_timestamp())
        )

    def write_stream_to_snowflake(
        self,
        df: DataFrame,
        table: str,
        checkpoint: str,
        trigger_interval: str = "5 minutes",
    ):
        """Write streaming DataFrame to Snowflake via foreachBatch."""
        sf_opts = self.sf_opts

        def write_batch(batch_df: DataFrame, epoch_id: int):
            if batch_df.isEmpty():
                return
            (
                batch_df.write
                .format("net.snowflake.spark.snowflake")
                .options(**sf_opts)
                .option("dbtable", table)
                .mode("append")
                .save()
            )
            print(f"[Stream] epoch={epoch_id} → {table} ({batch_df.count()} rows)")

        return (
            df.writeStream
            .foreachBatch(write_batch)
            .trigger(processingTime=trigger_interval)
            .option("checkpointLocation", checkpoint)
            .outputMode("update")
            .start()
        )

    def run(self):
        """Launch streaming pipeline."""
        print("\n[Stream] Starting streaming pipeline...")

        # Stream 1: Real-time flight metrics
        flight_stream   = self.read_flight_stream()
        windowed_kpis   = self.apply_watermark_and_windows(flight_stream)
        q1 = self.write_stream_to_snowflake(
            windowed_kpis,
            table="STREAM_AIRPORT_REALTIME_KPI",
            checkpoint="/checkpoints/airport_kpi",
        )

        # Stream 2: Delay cascade alerts
        alerts_stream = self.detect_delay_cascade(flight_stream)
        q2 = self.write_stream_to_snowflake(
            alerts_stream,
            table="STREAM_DELAY_ALERTS",
            checkpoint="/checkpoints/delay_alerts",
            trigger_interval="1 minute",
        )

        print("[Stream] Queries running. Awaiting termination...")
        q1.awaitTermination()
        q2.awaitTermination()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "stream", "both"], default="batch")
    parser.add_argument("--date", default="2024-12-01")
    args = parser.parse_args()

    SF_OPTS = {
        "sfURL":        "xy12345.snowflakecomputing.com",
        "sfUser":       "etl_svc",
        "sfPassword":   "***",
        "sfDatabase":   "AIRLINE_DW",
        "sfSchema":     "PUBLIC",
        "sfWarehouse":  "ETL_WH",
        "sfRole":       "ETL_ROLE",
    }

    if args.mode in ("batch", "both"):
        spark = create_spark_session("AirlineBatch", "batch")
        bp = BatchProcessor(spark, SF_OPTS)
        bp.run("/data/delta/raw_flights", args.date)

    if args.mode in ("stream", "both"):
        spark = create_spark_session("AirlineStream", "streaming")
        sp = StreamingProcessor(spark, SF_OPTS)
        sp.run()
