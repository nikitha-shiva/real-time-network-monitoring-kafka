"""
Spark Streaming Processor for Real-Time Network Monitoring
Processes network logs from Kafka with anomaly detection and auto-scaling
Author: Nikitha Shiva
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NetworkStreamProcessor:
    """
    Real-time network log processing with Spark Streaming
    Handles high-throughput log processing with anomaly detection
    """
    
    def __init__(self, app_name: str = "NetworkMonitoring", config: Dict = None):
        """
        Initialize Spark Streaming processor
        
        Args:
            app_name: Spark application name
            config: Additional Spark configuration
        """
        self.app_name = app_name
        self.config = config or {}
        self.spark = self._create_spark_session()
        
        # Define schema for network logs
        self.log_schema = self._get_log_schema()
        
        # Anomaly detection thresholds
        self.anomaly_thresholds = {
            'high_volume_threshold': 1000,  # logs per minute
            'high_latency_threshold': 5000,  # milliseconds
            'failed_connection_rate': 0.1,   # 10% failure rate
            'suspicious_port_access': [22, 23, 3389, 1433, 5432],
            'data_exfiltration_threshold': 10485760  # 10MB
        }
        
        logger.info(f"NetworkStreamProcessor initialized: {app_name}")
    
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for streaming"""
        
        # Base configuration for high-performance streaming
        spark_config = {
            "spark.app.name": self.app_name,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            
            # Streaming specific configurations
            "spark.sql.streaming.checkpointLocation": "/checkpoints/network-monitoring",
            "spark.sql.streaming.stateStore.maintenanceInterval": "60s",
            "spark.streaming.kafka.maxRatePerPartition": "1000",
            "spark.streaming.backpressure.enabled": "true",
            
            # Memory optimization
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
            "spark.executor.instances": "3",
            "spark.driver.memory": "2g",
            "spark.driver.cores": "1",
            
            # Kafka integration
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        }
        
        # Override with custom config
        spark_config.update(self.config)
        
        # Build Spark session
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def _get_log_schema(self) -> StructType:
        """Define schema for network log data"""
        return StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("source_ip", StringType(), True),
            StructField("destination_ip", StringType(), True),
            StructField("source_port", IntegerType(), True),
            StructField("destination_port", IntegerType(), True),
            StructField("protocol", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("message", StringType(), True),
            StructField("log_level", StringType(), True),
            StructField("bytes_sent", LongType(), True),
            StructField("bytes_received", LongType(), True),
            StructField("response_time_ms", DoubleType(), True),
            StructField("connection_status", StringType(), True),
            StructField("ingestion_time", TimestampType(), True),
            StructField("processing_date", StringType(), True),
            StructField("log_source", StringType(), True),
            StructField("hostname", StringType(), True),
            StructField("data_version", StringType(), True),
            StructField("is_internal_ip", BooleanType(), True),
            StructField("traffic_direction", StringType(), True),
            StructField("risk_score", DoubleType(), True)
        ])
    
    def create_kafka_stream(self, kafka_servers: str, topics: List[str]) -> DataFrame:
        """
        Create streaming DataFrame from Kafka topics
        
        Args:
            kafka_servers: Comma-separated Kafka broker addresses
            topics: List of Kafka topics to consume
            
        Returns:
            Streaming DataFrame with parsed log data
        """
        # Read from Kafka
        kafka_df = (self.spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", kafka_servers)
                   .option("subscribe", ",".join(topics))
                   .option("startingOffsets", "latest")
                   .option("maxOffsetsPerTrigger", 10000)
                   .option("kafka.consumer.group.id", "network-monitoring-consumer")
                   .load())
        
        # Parse JSON data and apply schema
        parsed_df = (kafka_df
                    .select(
                        col("topic"),
                        col("partition"),
                        col("offset"),
                        col("timestamp").alias("kafka_timestamp"),
                        from_json(col("value").cast("string"), self.log_schema).alias("data")
                    )
                    .select("topic", "partition", "offset", "kafka_timestamp", "data.*")
                    .filter(col("timestamp").isNotNull()))
        
        return parsed_df
    
    def create_windowed_aggregations(self, stream_df: DataFrame) -> DataFrame:
        """
        Create windowed aggregations for real-time analytics
        
        Args:
            stream_df: Streaming DataFrame with log data
            
        Returns:
            DataFrame with windowed metrics
        """
        # 5-minute sliding windows with 1-minute slide
        windowed_metrics = (stream_df
                           .withWatermark("timestamp", "2 minutes")
                           .groupBy(
                               window("timestamp", "5 minutes", "1 minute"),
                               "source_ip",
                               "traffic_direction",
                               "severity"
                           )
                           .agg(
                               count("*").alias("log_count"),
                               sum("bytes_sent").alias("total_bytes_sent"),
                               sum("bytes_received").alias("total_bytes_received"),
                               avg("response_time_ms").alias("avg_response_time"),
                               max("response_time_ms").alias("max_response_time"),
                               min("response_time_ms").alias("min_response_time"),
                               countDistinct("destination_ip").alias("unique_destinations"),
                               countDistinct("destination_port").alias("unique_ports"),
                               sum(when(col("connection_status") == "FAILED", 1).otherwise(0)).alias("failed_connections"),
                               avg("risk_score").alias("avg_risk_score"),
                               max("risk_score").alias("max_risk_score")
                           ))
        
        # Add derived metrics
        enhanced_metrics = (windowed_metrics
                           .withColumn("failure_rate", 
                                     col("failed_connections").cast("double") / col("log_count"))
                           .withColumn("bytes_per_second",
                                     (col("total_bytes_sent") + col("total_bytes_received")) / 300)  # 5 min window
                           .withColumn("is_high_volume",
                                     col("log_count") > self.anomaly_thresholds['high_volume_threshold'])
                           .withColumn("is_high_latency",
                                     col("avg_response_time") > self.anomaly_thresholds['high_latency_threshold'])
                           .withColumn("is_suspicious_failure_rate",
                                     col("failure_rate") > self.anomaly_thresholds['failed_connection_rate']))
        
        return enhanced_metrics
    
    def detect_anomalies(self, metrics_df: DataFrame) -> DataFrame:
        """
        Detect network anomalies based on multiple criteria
        
        Args:
            metrics_df: DataFrame with windowed metrics
            
        Returns:
            DataFrame with anomaly flags and scores
        """
        # Define anomaly conditions
        anomaly_conditions = [
            # High volume anomaly
            (col("log_count") > self.anomaly_thresholds['high_volume_threshold']).alias("high_volume_anomaly"),
            
            # High latency anomaly
            (col("avg_response_time") > self.anomaly_thresholds['high_latency_threshold']).alias("high_latency_anomaly"),
            
            # High failure rate anomaly
            (col("failure_rate") > self.anomaly_thresholds['failed_connection_rate']).alias("high_failure_rate_anomaly"),
            
            # Data exfiltration anomaly
            (col("total_bytes_sent") > self.anomaly_thresholds['data_exfiltration_threshold']).alias("data_exfiltration_anomaly"),
            
            # Port scanning anomaly
            (col("unique_ports") > 50).alias("port_scanning_anomaly"),
            
            # High risk score anomaly
            (col("max_risk_score") > 0.8).alias("high_risk_anomaly")
        ]
        
        # Add anomaly flags
        anomaly_df = metrics_df.select("*", *anomaly_conditions)
        
        # Calculate overall anomaly score
        anomaly_df = (anomaly_df
                     .withColumn("anomaly_score",
                               (col("high_volume_anomaly").cast("int") * 0.2 +
                                col("high_latency_anomaly").cast("int") * 0.15 +
                                col("high_failure_rate_anomaly").cast("int") * 0.25 +
                                col("data_exfiltration_anomaly").cast("int") * 0.3 +
                                col("port_scanning_anomaly").cast("int") * 0.05 +
                                col("high_risk_anomaly").cast("int") * 0.05))
                     .withColumn("is_anomaly", col("anomaly_score") > 0.3)
                     .withColumn("anomaly_level",
                               when(col("anomaly_score") > 0.7, "CRITICAL")
                               .when(col("anomaly_score") > 0.5, "HIGH")
                               .when(col("anomaly_score") > 0.3, "MEDIUM")
                               .otherwise("LOW"))
                     .withColumn("detected_at", current_timestamp()))
        
        return anomaly_df
    
    def create_real_time_alerts(self, anomaly_df: DataFrame) -> DataFrame:
        """
        Create real-time alerts for detected anomalies
        
        Args:
            anomaly_df: DataFrame with anomaly detection results
            
        Returns:
            DataFrame with alert information
        """
        # Filter only anomalies
        alerts_df = (anomaly_df
                    .filter(col("is_anomaly") == True)
                    .select(
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),
                        "source_ip",
                        "traffic_direction", 
                        "severity",
                        "log_count",
                        "failure_rate",
                        "avg_response_time",
                        "total_bytes_sent",
                        "unique_destinations",
                        "unique_ports",
                        "anomaly_score",
                        "anomaly_level",
                        "detected_at",
                        "high_volume_anomaly",
                        "high_latency_anomaly", 
                        "high_failure_rate_anomaly",
                        "data_exfiltration_anomaly",
                        "port_scanning_anomaly",
                        "high_risk_anomaly"
                    )
                    .withColumn("alert_id", monotonically_increasing_id())
                    .withColumn("alert_message", 
                              concat(lit("Network anomaly detected from "), col("source_ip"),
                                   lit(" - Level: "), col("anomaly_level"),
                                   lit(" - Score: "), col("anomaly_score"))))
        
        return alerts_df
    
    def process_alerts_batch(self, alerts_df: DataFrame, batch_id: int):
        """
        Process alerts in batch mode for actions
        
        Args:
            alerts_df: DataFrame with alerts
            batch_id: Batch identifier
        """
        if alerts_df.count() > 0:
            logger.info(f"Processing {alerts_df.count()} alerts in batch {batch_id}")
            
            # Collect alerts for processing
            alerts = alerts_df.collect()
            
            for alert in alerts:
                alert_dict = alert.asDict()
                
                # Log alert
                logger.warning(f"ALERT: {alert_dict['alert_message']}")
                
                # Send to monitoring dashboard (implement based on your system)
                self._send_to_dashboard(alert_dict)
                
                # Trigger auto-scaling if critical
                if alert_dict['anomaly_level'] == 'CRITICAL':
                    self._trigger_autoscaling(alert_dict)
                
                # Send notifications (implement based on your notification system)
                self._send_notification(alert_dict)
    
    def _send_to_dashboard(self, alert: Dict):
        """Send alert to monitoring dashboard"""
        # Implement dashboard integration
        logger.info(f"Dashboard alert: {alert['alert_id']}")
    
    def _trigger_autoscaling(self, alert: Dict):
        """Trigger infrastructure auto-scaling"""
        # Implement auto-scaling logic
        logger.info(f"Auto-scaling triggered by alert: {alert['alert_id']}")
    
    def _send_notification(self, alert: Dict):
        """Send notification via email/Slack/Teams"""
        # Implement notification system
        logger.info(f"Notification sent for alert: {alert['alert_id']}")
    
    def start_streaming_pipeline(self, kafka_servers: str, topics: List[str], 
                                output_path: str = None) -> List[StreamingQuery]:
        """
        Start the complete streaming pipeline
        
        Args:
            kafka_servers: Kafka broker addresses
            topics: Kafka topics to consume
            output_path: Path for storing processed data
            
        Returns:
            List of streaming queries
        """
        logger.info("Starting network monitoring streaming pipeline...")
        
        # Create Kafka stream
        stream_df = self.create_kafka_stream(kafka_servers, topics)
        
        # Create windowed aggregations
        metrics_df = self.create_windowed_aggregations(stream_df)
        
        # Detect anomalies
        anomaly_df = self.detect_anomalies(metrics_df)
        
        # Create alerts
        alerts_df = self.create_real_time_alerts(anomaly_df)
        
        # Start streaming queries
        queries = []
        
        # Query 1: Process alerts in real-time
        alert_query = (alerts_df.writeStream
                      .foreachBatch(self.process_alerts_batch)
                      .outputMode("update")
                      .trigger(processingTime='30 seconds')
                      .option("checkpointLocation", "/checkpoints/alerts")
                      .start())
        queries.append(alert_query)
        
        # Query 2: Store metrics to storage (if path provided)
        if output_path:
            metrics_query = (metrics_df.writeStream
                           .format("parquet")
                           .option("path", f"{output_path}/metrics")
                           .option("checkpointLocation", "/checkpoints/metrics")
                           .outputMode("append")
                           .trigger(processingTime='1 minute')
                           .start())
            queries.append(metrics_query)
        
        # Query 3: Console output for monitoring (development)
        if os.getenv("ENVIRONMENT") == "development":
            console_query = (alerts_df.writeStream
                           .outputMode("update")
                           .format("console")
                           .option("truncate", False)
                           .trigger(processingTime='30 seconds')
                           .start())
            queries.append(console_query)
        
        logger.info(f"Started {len(queries)} streaming queries")
        return queries
    
    def stop_streaming(self, queries: List[StreamingQuery]):
        """Stop all streaming queries"""
        for query in queries:
            query.stop()
        logger.info("All streaming queries stopped")
    
    def get_streaming_metrics(self, queries: List[StreamingQuery]) -> Dict:
        """Get metrics from running streaming queries"""
        metrics = {}
        for i, query in enumerate(queries):
            if query.isActive:
                progress = query.lastProgress
                metrics[f"query_{i}"] = {
                    "name": query.name,
                    "batch_id": progress.get("batchId"),
                    "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                    "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
                    "batch_duration": progress.get("batchDuration", 0),
                    "state_operators": progress.get("stateOperators", [])
                }
        return metrics


# Main execution
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Network Monitoring Streaming Processor")
    parser.add_argument("--kafka-servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topics", required=True, nargs="+", help="Kafka topics to consume")
    parser.add_argument("--output-path", help="Output path for storing processed data")
    parser.add_argument("--app-name", default="NetworkMonitoring", help="Spark application name")
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = NetworkStreamProcessor(args.app_name)
    
    try:
        # Start streaming pipeline
        queries = processor.start_streaming_pipeline(
            kafka_servers=args.kafka_servers,
            topics=args.topics,
            output_path=args.output_path
        )
        
        # Wait for termination
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Stopping streaming pipeline...")
        processor.stop_streaming(queries)
    except Exception as e:
        logger.error(f"Streaming pipeline error: {e}")
        raise
    finally:
        processor.spark.stop()
