"""
Unit Tests for Kafka Network Log Ingester
Comprehensive testing suite for the NetworkLogIngester class
Author: Nikitha Shiva
"""

import pytest
import json
import time
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta
from kafka.errors import KafkaError
import sys
import os

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from kafka_network_ingester import NetworkLogIngester, NetworkLogConsumer

class TestNetworkLogIngester:
    """Test suite for NetworkLogIngester class"""
    
    @pytest.fixture
    def sample_log_data(self):
        """Sample log data for testing"""
        return {
            "timestamp": "2024-01-15T10:30:00Z",
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
        }
    
    @pytest.fixture
    def mock_kafka_producer(self):
        """Mock Kafka producer for testing"""
        with patch('kafka_network_ingester.KafkaProducer') as mock_producer:
            producer_instance = Mock()
            mock_producer.return_value = producer_instance
            yield producer_instance
    
    @pytest.fixture
    def ingester(self, mock_kafka_producer):
        """NetworkLogIngester instance for testing"""
        return NetworkLogIngester("localhost:9092")
    
    def test_initialization_success(self, mock_kafka_producer):
        """Test successful initialization of NetworkLogIngester"""
        ingester = NetworkLogIngester("localhost:9092")
        
        assert ingester.bootstrap_servers == "localhost:9092"
        assert ingester.metrics['messages_sent'] == 0
        assert ingester.metrics['messages_failed'] == 0
        assert 'start_time' in ingester.metrics
        
        # Verify topic mapping
        expected_topics = {
            'CRITICAL': 'network-logs-critical',
            'ERROR': 'network-logs-error',
            'WARNING': 'network-logs-warning',
            'INFO': 'network-logs-info',
            'DEBUG': 'network-logs-debug'
        }
        assert ingester.topic_mapping == expected_topics
    
    def test_initialization_failure(self):
        """Test initialization failure with invalid Kafka configuration"""
        with patch('kafka_network_ingester.KafkaProducer') as mock_producer:
            mock_producer.side_effect = KafkaError("Connection failed")
            
            with pytest.raises(KafkaError):
                NetworkLogIngester("invalid:9092")
    
    def test_enrich_log_data_basic(self, ingester, sample_log_data):
        """Test basic log data enrichment"""
        enriched = ingester.enrich_log_data(sample_log_data)
        
        # Verify original fields are preserved
        assert enriched['source_ip'] == sample_log_data['source_ip']
        assert enriched['destination_ip'] == sample_log_data['destination_ip']
        assert enriched['severity'] == sample_log_data['severity']
        
        # Verify metadata fields are added
        assert 'ingestion_time' in enriched
        assert 'processing_date' in enriched
        assert 'data_version' in enriched
        assert enriched['data_version'] == '1.0'
        
        # Verify derived fields
        assert 'is_internal_ip' in enriched
        assert 'traffic_direction' in enriched
        assert 'risk_score' in enriched
        assert isinstance(enriched['risk_score'], float)
        assert 0.0 <= enriched['risk_score'] <= 1.0
    
    def test_enrich_log_data_defaults(self, ingester):
        """Test log enrichment with minimal input data"""
        minimal_log = {"source_ip": "10.0.0.1"}
        enriched = ingester.enrich_log_data(minimal_log)
        
        # Verify defaults are applied
        assert enriched['severity'] == 'INFO'
        assert enriched['protocol'] == 'TCP'
        assert enriched['bytes_sent'] == 0
        assert enriched['bytes_received'] == 0
        assert enriched['response_time_ms'] == 0.0
        assert enriched['connection_status'] == 'UNKNOWN'
        assert enriched['log_source'] == 'unknown'
    
    def test_is_internal_ip(self, ingester):
        """Test internal IP address detection"""
        # Test internal IPs
        assert ingester._is_internal_ip("192.168.1.1") == True
        assert ingester._is_internal_ip("10.0.0.1") == True
        assert ingester._is_internal_ip("172.16.0.1") == True
        assert ingester._is_internal_ip("172.31.255.255") == True
        
        # Test external IPs
        assert ingester._is_internal_ip("8.8.8.8") == False
        assert ingester._is_internal_ip("1.1.1.1") == False
        assert ingester._is_internal_ip("173.16.0.1") == False  # Outside private range
        
        # Test edge cases
        assert ingester._is_internal_ip(None) == False
        assert ingester._is_internal_ip("") == False
        assert ingester._is_internal_ip("invalid") == False
    
    def test_determine_traffic_direction(self, ingester):
        """Test traffic direction determination"""
        # Internal to internal
        log_data = {"source_ip": "192.168.1.1", "destination_ip": "10.0.0.1"}
        assert ingester._determine_traffic_direction(log_data) == "internal"
        
        # Internal to external (outbound)
        log_data = {"source_ip": "192.168.1.1", "destination_ip": "8.8.8.8"}
        assert ingester._determine_traffic_direction(log_data) == "outbound"
        
        # External to internal (inbound)
        log_data = {"source_ip": "8.8.8.8", "destination_ip": "192.168.1.1"}
        assert ingester._determine_traffic_direction(log_data) == "inbound"
        
        # External to external
        log_data = {"source_ip": "8.8.8.8", "destination_ip": "1.1.1.1"}
        assert ingester._determine_traffic_direction(log_data) == "external"
    
    def test_calculate_risk_score(self, ingester):
        """Test risk score calculation"""
        # Low risk log
        low_risk_log = {
            "destination_port": 80,
            "source_ip": "192.168.1.1",
            "bytes_sent": 1024,
            "connection_status": "SUCCESS"
        }
        risk_score = ingester._calculate_risk_score(low_risk_log)
        assert 0.0 <= risk_score <= 0.3
        
        # High risk log
        high_risk_log = {
            "destination_port": 22,  # High risk port
            "source_ip": "8.8.8.8",  # External IP
            "bytes_sent": 2000000,   # High data transfer
            "connection_status": "FAILED"  # Failed connection
        }
        risk_score = ingester._calculate_risk_score(high_risk_log)
        assert risk_score >= 0.8
    
    def test_get_topic_by_severity(self, ingester):
        """Test topic routing by severity level"""
        assert ingester.get_topic_by_severity("CRITICAL") == "network-logs-critical"
        assert ingester.get_topic_by_severity("ERROR") == "network-logs-error"
        assert ingester.get_topic_by_severity("WARNING") == "network-logs-warning"
        assert ingester.get_topic_by_severity("INFO") == "network-logs-info"
        assert ingester.get_topic_by_severity("DEBUG") == "network-logs-debug"
        
        # Test case insensitivity
        assert ingester.get_topic_by_severity("critical") == "network-logs-critical"
        assert ingester.get_topic_by_severity("Critical") == "network-logs-critical"
        
        # Test unknown severity defaults to INFO
        assert ingester.get_topic_by_severity("UNKNOWN") == "network-logs-info"
        assert ingester.get_topic_by_severity("") == "network-logs-info"
    
    def test_get_partition_key(self, ingester):
        """Test partition key generation"""
        # Test normal IP address
        log_data = {"source_ip": "192.168.1.100"}
        partition_key = ingester.get_partition_key(log_data)
        assert partition_key == "100"
        
        # Test IP without dots
        log_data = {"source_ip": "localhost"}
        partition_key = ingester.get_partition_key(log_data)
        assert partition_key == "localhost"
        
        # Test missing source_ip
        log_data = {}
        partition_key = ingester.get_partition_key(log_data)
        assert partition_key == "unknown"
    
    def test_send_log_success(self, ingester, mock_kafka_producer, sample_log_data):
        """Test successful log sending"""
        # Mock successful send
        future_mock = Mock()
        mock_kafka_producer.send.return_value = future_mock
        
        result = ingester.send_log(sample_log_data)
        
        assert result == True
        assert ingester.metrics['messages_sent'] == 1
        assert ingester.metrics['messages_failed'] == 0
        assert ingester.metrics['bytes_sent'] > 0
        
        # Verify send was called with correct parameters
        mock_kafka_producer.send.assert_called_once()
        call_args = mock_kafka_producer.send.call_args
        assert call_args[1]['topic'] == 'network-logs-info'
        assert call_args[1]['key'] == '100'  # Last octet of IP
    
    def test_send_log_failure(self, ingester, mock_kafka_producer, sample_log_data):
        """Test log sending failure"""
        # Mock send failure
        mock_kafka_producer.send.side_effect = KafkaError("Send failed")
        
        result = ingester.send_log(sample_log_data)
        
        assert result == False
        assert ingester.metrics['messages_sent'] == 0
        assert ingester.metrics['messages_failed'] == 1
    
    def test_batch_send_logs_success(self, ingester, mock_kafka_producer, sample_log_data):
        """Test successful batch log sending"""
        # Create batch of logs
        log_batch = [sample_log_data, sample_log_data.copy(), sample_log_data.copy()]
        
        # Mock successful sends
        future_mock = Mock()
        mock_kafka_producer.send.return_value = future_mock
        
        results = ingester.batch_send_logs(log_batch)
        
        assert results['total'] == 3
        assert results['success'] == 3
        assert results['failed'] == 0
        assert results['throughput_msg_sec'] > 0
        assert 'processing_time' in results
        
        # Verify flush was called
        mock_kafka_producer.flush.assert_called_once()
    
    def test_batch_send_logs_partial_failure(self, ingester, mock_kafka_producer, sample_log_data):
        """Test batch sending with partial failures"""
        log_batch = [sample_log_data, sample_log_data.copy()]
        
        # Mock first success, second failure
        def send_side_effect(*args, **kwargs):
            if mock_kafka_producer.send.call_count == 1:
                return Mock()
            else:
                raise KafkaError("Send failed")
        
        mock_kafka_producer.send.side_effect = send_side_effect
        
        results = ingester.batch_send_logs(log_batch)
        
        assert results['total'] == 2
        assert results['success'] == 1
        assert results['failed'] == 1
    
    def test_on_send_success(self, ingester):
        """Test successful send callback"""
        record_metadata = Mock()
        record_metadata.topic = "test-topic"
        record_metadata.partition = 0
        record_metadata.offset = 12345
        
        # Should not raise exception
        ingester._on_send_success(record_metadata)
    
    def test_on_send_error(self, ingester):
        """Test error send callback"""
        initial_failed_count = ingester.metrics['messages_failed']
        
        exception = KafkaError("Send failed")
        ingester._on_send_error(exception)
        
        assert ingester.metrics['messages_failed'] == initial_failed_count + 1
    
    def test_get_metrics(self, ingester):
        """Test metrics retrieval"""
        # Simulate some activity
        ingester.metrics['messages_sent'] = 100
        ingester.metrics['messages_failed'] = 5
        time.sleep(0.1)  # Small delay to ensure runtime > 0
        
        metrics = ingester.get_metrics()
        
        assert metrics['messages_sent'] == 100
        assert metrics['messages_failed'] == 5
        assert 'runtime_seconds' in metrics
        assert metrics['runtime_seconds'] > 0
        assert 'throughput_msg_sec' in metrics
        assert 'success_rate' in metrics
        
        expected_success_rate = 100 / (100 + 5)
        assert abs(metrics['success_rate'] - expected_success_rate) < 0.01
    
    def test_close(self, ingester, mock_kafka_producer):
        """Test ingester cleanup"""
        ingester.close()
        
        mock_kafka_producer.flush.assert_called_once_with(timeout=30)
        mock_kafka_producer.close.assert_called_once_with(timeout=30)
    
    def test_close_with_error(self, ingester, mock_kafka_producer):
        """Test ingester cleanup with error"""
        mock_kafka_producer.close.side_effect = KafkaError("Close failed")
        
        # Should not raise exception
        ingester.close()
        
        mock_kafka_producer.close.assert_called_once()


class TestNetworkLogConsumer:
    """Test suite for NetworkLogConsumer class"""
    
    @pytest.fixture
    def mock_kafka_consumer(self):
        """Mock Kafka consumer for testing"""
        with patch('kafka_network_ingester.KafkaConsumer') as mock_consumer:
            consumer_instance = Mock()
            mock_consumer.return_value = consumer_instance
            yield consumer_instance
    
    @pytest.fixture
    def consumer(self, mock_kafka_consumer):
        """NetworkLogConsumer instance for testing"""
        return NetworkLogConsumer("localhost:9092", ["test-topic"], "test-group")
    
    def test_initialization(self, mock_kafka_consumer):
        """Test consumer initialization"""
        consumer = NetworkLogConsumer("localhost:9092", ["topic1", "topic2"], "test-group")
        
        assert consumer.bootstrap_servers == "localhost:9092"
        assert consumer.topics == ["topic1", "topic2"]
        assert consumer.group_id == "test-group"
    
    def test_consume_logs_success(self, consumer, mock_kafka_consumer):
        """Test successful log consumption"""
        # Mock messages
        mock_message1 = Mock()
        mock_message1.topic = "test-topic"
        mock_message1.partition = 0
        mock_message1.offset = 100
        mock_message1.timestamp = 1642248600000  # Unix timestamp
        mock_message1.key = "test-key"
        mock_message1.value = {"test": "data"}
        mock_message1.headers = [("header1", b"value1")]
        
        mock_message2 = Mock()
        mock_message2.topic = "test-topic"
        mock_message2.partition = 1
        mock_message2.offset = 200
        mock_message2.timestamp = 1642248660000
        mock_message2.key = None
        mock_message2.value = {"test": "data2"}
        mock_message2.headers = None
        
        mock_kafka_consumer.__iter__ = Mock(return_value=iter([mock_message1, mock_message2]))
        
        # Consume messages
        messages = list(consumer.consume_logs())
        
        assert len(messages) == 2
        
        # Verify first message
        msg1 = messages[0]
        assert msg1['topic'] == "test-topic"
        assert msg1['partition'] == 0
        assert msg1['offset'] == 100
        assert msg1['key'] == "test-key"
        assert msg1['value'] == {"test": "data"}
        assert msg1['headers'] == {"header1": b"value1"}
        
        # Verify second message
        msg2 = messages[1]
        assert msg2['topic'] == "test-topic"
        assert msg2['partition'] == 1
        assert msg2['offset'] == 200
        assert msg2['key'] is None
        assert msg2['value'] == {"test": "data2"}
        assert msg2['headers'] == {}
    
    def test_consume_logs_keyboard_interrupt(self, consumer, mock_kafka_consumer):
        """Test consumer handling of keyboard interrupt"""
        mock_kafka_consumer.__iter__ = Mock(side_effect=KeyboardInterrupt())
        
        # Should handle KeyboardInterrupt gracefully
        messages = list(consumer.consume_logs())
        assert len(messages) == 0
        
        mock_kafka_consumer.close.assert_called_once()
    
    def test_consume_logs_exception(self, consumer, mock_kafka_consumer):
        """Test consumer handling of general exceptions"""
        mock_kafka_consumer.__iter__ = Mock(side_effect=KafkaError("Consumer error"))
        
        # Should handle exception gracefully
        messages = list(consumer.consume_logs())
        assert len(messages) == 0
        
        mock_kafka_consumer.close.assert_called_once()
    
    def test_close(self, consumer, mock_kafka_consumer):
        """Test consumer cleanup"""
        consumer.close()
        mock_kafka_consumer.close.assert_called_once()
    
    def test_close_with_error(self, consumer, mock_kafka_consumer):
        """Test consumer cleanup with error"""
        mock_kafka_consumer.close.side_effect = KafkaError("Close failed")
        
        # Should not raise exception
        consumer.close()
        mock_kafka_consumer.close.assert_called_once()


class TestIntegration:
    """Integration tests for the complete ingestion pipeline"""
    
    @pytest.fixture
    def integration_config(self):
        """Configuration for integration tests"""
        return {
            'kafka_servers': 'localhost:9092',
            'topics': ['network-logs-info', 'network-logs-warning', 'network-logs-error'],
            'consumer_group': 'test-integration-group'
        }
    
    def test_end_to_end_flow(self, integration_config):
        """Test complete end-to-end flow (requires running Kafka)"""
        pytest.skip("Integration test requires running Kafka cluster")
        
        # This test would:
        # 1. Create ingester and consumer
        # 2. Send test messages
        # 3. Consume and verify messages
        # 4. Check metrics and health
    
    def test_performance_benchmarks(self):
        """Performance benchmarks for the ingester"""
        pytest.skip("Performance test - run manually when needed")
        
        # This test would:
        # 1. Generate large volume of test data
        # 2. Measure throughput and latency
        # 3. Verify system behavior under load


class TestConfiguration:
    """Tests for configuration handling and validation"""
    
    def test_custom_configuration(self):
        """Test ingester with custom configuration"""
        custom_config = {
            'batch_size': 64000,
            'linger_ms': 20,
            'acks': 1,
            'retries': 10
        }
        
        with patch('kafka_network_ingester.KafkaProducer') as mock_producer:
            ingester = NetworkLogIngester("localhost:9092", custom_config)
            
            # Verify custom config was passed to producer
            call_args = mock_producer.call_args[1]
            assert call_args['batch_size'] == 64000
            assert call_args['linger_ms'] == 20
            assert call_args['acks'] == 1
            assert call_args['retries'] == 10
    
    def test_topic_mapping_customization(self):
        """Test custom topic mapping"""
        with patch('kafka_network_ingester.KafkaProducer'):
            ingester = NetworkLogIngester("localhost:9092")
            
            # Modify topic mapping
            ingester.topic_mapping['CUSTOM'] = 'custom-logs'
            
            assert ingester.get_topic_by_severity('CUSTOM') == 'custom-logs'


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
