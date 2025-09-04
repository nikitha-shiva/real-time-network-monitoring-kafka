"""
Network Log Ingestion with Kafka
Real-time network log processing for monitoring pipeline
Author: Nikitha Shiva
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Generator
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import socket
import threading
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NetworkLogIngester:
    """
    High-performance network log ingestion using Apache Kafka
    Handles real-time log streaming with intelligent routing and error handling
    """
    
    def __init__(self, bootstrap_servers: str, config: Dict = None):
        """
        Initialize Kafka producer with optimized configuration
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            config: Additional Kafka producer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.config = config or {}
        
        # Optimized producer configuration for high throughput
        producer_config = {
            'bootstrap_servers': bootstrap_servers.split(','),
            'value_serializer': lambda x: json.dumps(x, default=str).encode('utf-8'),
            'key_serializer': lambda x: x.encode('utf-8') if x else None,
            'batch_size': 32768,  # 32KB batches for efficiency
            'linger_ms': 5,       # Small delay to batch messages
            'buffer_memory': 67108864,  # 64MB buffer
            'acks': 'all',        # Wait for all replicas
            'retries': 5,         # Retry failed sends
            'retry_backoff_ms': 100,
            'request_timeout_ms': 30000,
            'compression_type': 'snappy'  # Fast compression
        }
        
        # Override with custom config
        producer_config.update(self.config)
        
        try:
            self.producer = KafkaProducer(**producer_config)
            logger.info(f"Kafka producer initialized successfully: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
        
        # Topic routing configuration
        self.topic_mapping = {
            'CRITICAL': 'network-logs-critical',
            'ERROR': 'network-logs-error',
            'WARNING': 'network-logs-warning',
            'INFO': 'network-logs-info',
            'DEBUG': 'network-logs-debug'
        }
        
        # Performance metrics
        self.metrics = {
            'messages_sent': 0,
            'messages_failed': 0,
            'bytes_sent': 0,
            'start_time': time.time()
        }
    
    def enrich_log_data(self, log_data: Dict) -> Dict:
        """
        Enrich incoming log data with metadata and standardization
        
        Args:
            log_data: Raw log data dictionary
            
        Returns:
            Enriched log data with additional metadata
        """
        current_time = datetime.utcnow()
        
        enriched_log = {
            # Original fields
            'timestamp': log_data.get('timestamp', current_time.isoformat()),
            'source_ip': log_data.get('source_ip'),
            'destination_ip': log_data.get('destination_ip'),
            'source_port': log_data.get('source_port'),
            'destination_port': log_data.get('destination_port'),
            'protocol': log_data.get('protocol', 'TCP').upper(),
            'severity': log_data.get('severity', 'INFO').upper(),
            'message': log_data.get('message', ''),
            'log_level': log_data.get('log_level', 'INFO').upper(),
            
            # Network metrics
            'bytes_sent': int(log_data.get('bytes_sent', 0)),
            'bytes_received': int(log_data.get('bytes_received', 0)),
            'response_time_ms': float(log_data.get('response_time', 0)),
            'connection_status': log_data.get('connection_status', 'UNKNOWN'),
            
            # Metadata
            'ingestion_time': current_time.isoformat(),
            'processing_date': current_time.strftime('%Y-%m-%d'),
            'log_source': log_data.get('log_source', 'unknown'),
            'hostname': log_data.get('hostname', socket.gethostname()),
            'data_version': '1.0',
            
            # Derived fields for analysis
            'is_internal_ip': self._is_internal_ip(log_data.get('source_ip')),
            'traffic_direction': self._determine_traffic_direction(log_data),
            'risk_score': self._calculate_risk_score(log_data)
        }
        
        return enriched_log
    
    def _is_internal_ip(self, ip_address: Optional[str]) -> bool:
        """Check if IP address is internal/private"""
        if not ip_address:
            return False
        
        private_ranges = [
            '10.', '192.168.', '172.16.', '172.17.', '172.18.', '172.19.',
            '172.20.', '172.21.', '172.22.', '172.23.', '172.24.', '172.25.',
            '172.26.', '172.27.', '172.28.', '172.29.', '172.30.', '172.31.'
        ]
        
        return any(ip_address.startswith(prefix) for prefix in private_ranges)
    
    def _determine_traffic_direction(self, log_data: Dict) -> str:
        """Determine traffic direction based on IP addresses"""
        source_ip = log_data.get('source_ip', '')
        dest_ip = log_data.get('destination_ip', '')
        
        source_internal = self._is_internal_ip(source_ip)
        dest_internal = self._is_internal_ip(dest_ip)
        
        if source_internal and dest_internal:
            return 'internal'
        elif source_internal and not dest_internal:
            return 'outbound'
        elif not source_internal and dest_internal:
            return 'inbound'
        else:
            return 'external'
    
    def _calculate_risk_score(self, log_data: Dict) -> float:
        """Calculate basic risk score based on log characteristics"""
        risk_score = 0.0
        
        # High risk ports
        high_risk_ports = [22, 23, 3389, 1433, 3306, 5432]
        dest_port = log_data.get('destination_port')
        if dest_port in high_risk_ports:
            risk_score += 0.3
        
        # External connections
        if not self._is_internal_ip(log_data.get('source_ip')):
            risk_score += 0.2
        
        # High data transfer
        bytes_sent = int(log_data.get('bytes_sent', 0))
        if bytes_sent > 1000000:  # > 1MB
            risk_score += 0.2
        
        # Failed connections
        if log_data.get('connection_status') == 'FAILED':
            risk_score += 0.3
        
        return min(risk_score, 1.0)  # Cap at 1.0
    
    def get_topic_by_severity(self, severity: str) -> str:
        """Route logs to different topics based on severity for prioritized processing"""
        return self.topic_mapping.get(severity.upper(), 'network-logs-info')
    
    def get_partition_key(self, log_data: Dict) -> str:
        """Generate partition key for optimal load balancing"""
        source_ip = log_data.get('source_ip', 'unknown')
        # Use last octet of IP for better distribution
        if '.' in source_ip:
            return source_ip.split('.')[-1]
        return source_ip
    
    def send_log(self, log_data: Dict) -> bool:
        """
        Send individual log entry to Kafka
        
        Args:
            log_data: Log data dictionary
            
        Returns:
            Success status
        """
        try:
            # Enrich log data
            enriched_log = self.enrich_log_data(log_data)
            
            # Determine topic and partition key
            topic = self.get_topic_by_severity(enriched_log['severity'])
            partition_key = self.get_partition_key(enriched_log)
            
            # Send to Kafka
            future = self.producer.send(
                topic=topic,
                key=partition_key,
                value=enriched_log
            )
            
            # Optional: Add callback for monitoring
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            # Update metrics
            self.metrics['messages_sent'] += 1
            self.metrics['bytes_sent'] += len(json.dumps(enriched_log))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send log: {e}")
            self.metrics['messages_failed'] += 1
            return False
    
    def batch_send_logs(self, log_batch: List[Dict]) -> Dict:
        """
        Send multiple logs in batch for improved throughput
        
        Args:
            log_batch: List of log data dictionaries
            
        Returns:
            Processing results with success/failure counts
        """
        results = {
            'total': len(log_batch),
            'success': 0,
            'failed': 0,
            'start_time': time.time()
        }
        
        for log_entry in log_batch:
            if self.send_log(log_entry):
                results['success'] += 1
            else:
                results['failed'] += 1
        
        # Flush producer to ensure delivery
        self.producer.flush(timeout=30)
        
        results['processing_time'] = time.time() - results['start_time']
        results['throughput_msg_sec'] = results['total'] / results['processing_time']
        
        logger.info(f"Batch processed: {results}")
        return results
    
    def _on_send_success(self, record_metadata):
        """Callback for successful message delivery"""
        logger.debug(f"Message sent to topic: {record_metadata.topic}, "
                    f"partition: {record_metadata.partition}, "
                    f"offset: {record_metadata.offset}")
    
    def _on_send_error(self, exception):
        """Callback for failed message delivery"""
        logger.error(f"Message delivery failed: {exception}")
        self.metrics['messages_failed'] += 1
    
    def get_metrics(self) -> Dict:
        """Get performance metrics"""
        runtime = time.time() - self.metrics['start_time']
        throughput = self.metrics['messages_sent'] / runtime if runtime > 0 else 0
        
        return {
            **self.metrics,
            'runtime_seconds': runtime,
            'throughput_msg_sec': throughput,
            'success_rate': (self.metrics['messages_sent'] / 
                           (self.metrics['messages_sent'] + self.metrics['messages_failed'])) 
                           if (self.metrics['messages_sent'] + self.metrics['messages_failed']) > 0 else 0
        }
    
    def close(self):
        """Clean up resources"""
        try:
            self.producer.flush(timeout=30)
            self.producer.close(timeout=30)
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")


class NetworkLogConsumer:
    """
    Kafka consumer for network logs with intelligent processing
    """
    
    def __init__(self, bootstrap_servers: str, topics: List[str], group_id: str):
        """
        Initialize Kafka consumer
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            topics: List of topics to consume from
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        
        consumer_config = {
            'bootstrap_servers': bootstrap_servers.split(','),
            'group_id': group_id,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
            'key_deserializer': lambda x: x.decode('utf-8') if x else None,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'auto_commit_interval_ms': 5000,
            'session_timeout_ms': 30000,
            'max_poll_records': 1000,
            'fetch_max_bytes': 52428800,  # 50MB
            'consumer_timeout_ms': 1000
        }
        
        self.consumer = KafkaConsumer(*topics, **consumer_config)
        logger.info(f"Consumer initialized for topics: {topics}")
    
    def consume_logs(self) -> Generator[Dict, None, None]:
        """
        Consume logs from Kafka topics
        
        Yields:
            Processed log messages with metadata
        """
        try:
            for message in self.consumer:
                yield {
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp': message.timestamp,
                    'key': message.key,
                    'value': message.value,
                    'headers': dict(message.headers) if message.headers else {}
                }
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.close()
    
    def close(self):
        """Clean up consumer resources"""
        try:
            self.consumer.close()
            logger.info("Kafka consumer closed successfully")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")


# Example usage and testing
if __name__ == "__main__":
    # Configuration
    KAFKA_SERVERS = "localhost:9092"
    
    # Initialize ingester
    ingester = NetworkLogIngester(KAFKA_SERVERS)
    
    # Sample log data
    sample_logs = [
        {
            "timestamp": datetime.utcnow().isoformat(),
            "source_ip": "192.168.1.100",
            "destination_ip": "8.8.8.8",
            "source_port": 54321,
            "destination_port": 80,
            "protocol": "TCP",
            "severity": "INFO",
            "message": "HTTP request to external server",
            "bytes_sent": 1024,
            "bytes_received": 2048,
            "response_time": 150,
            "connection_status": "SUCCESS",
            "log_source": "firewall"
        },
        {
            "timestamp": datetime.utcnow().isoformat(),
            "source_ip": "10.0.1.50",
            "destination_ip": "10.0.1.100",
            "source_port": 22,
            "destination_port": 22,
            "protocol": "TCP",
            "severity": "WARNING",
            "message": "SSH connection attempt",
            "bytes_sent": 512,
            "connection_status": "FAILED",
            "log_source": "server"
        }
    ]
    
    # Send sample logs
    results = ingester.batch_send_logs(sample_logs)
    print(f"Batch processing results: {results}")
    
    # Print metrics
    metrics = ingester.get_metrics()
    print(f"Ingester metrics: {metrics}")
    
    # Clean up
    ingester.close()
