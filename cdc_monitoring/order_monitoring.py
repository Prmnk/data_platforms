import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, push_to_gateway, CollectorRegistry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration for Kafka (Redpanda)
KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092'
KAFKA_TOPIC = 'dbz.public.orders'

# Configuration for Prometheus
PROMETHEUS_GATEWAY = 'localhost:9091'  # Default pushgateway port
PROMETHEUS_JOB = 'order_monitoring'

# Window configuration
WINDOW_SIZE_MINUTES = 5
AGGREGATION_INTERVAL_SECONDS = 30

# Thread-safe data structures
class OrderWindow:
    """Thread-safe sliding window for order data"""
    def __init__(self, window_minutes=5):
        self.window_minutes = window_minutes
        self.orders = deque()  # Store (timestamp, order_id, total) tuples
        self.lock = threading.Lock()
    
    def add_order(self, order_id, total):
        """Add an order to the window"""
        with self.lock:
            now = datetime.now()
            self.orders.append((now, order_id, total))
            self._cleanup_old_orders()
    
    def remove_order(self, order_id, total):
        """Remove an order from the window (for deletes)"""
        with self.lock:
            # Find and remove the most recent matching order
            for i in range(len(self.orders) - 1, -1, -1):
                if self.orders[i][1] == order_id:
                    self.orders.remove(self.orders[i])
                    break
    
    def _cleanup_old_orders(self):
        """Remove orders older than the window"""
        cutoff_time = datetime.now() - timedelta(minutes=self.window_minutes)
        while self.orders and self.orders[0][0] < cutoff_time:
            self.orders.popleft()
    
    def get_aggregates(self):
        """Get order count and total sum for the current window"""
        with self.lock:
            self._cleanup_old_orders()
            order_count = len(self.orders)
            total_sum = sum(order[2] for order in self.orders)
            return order_count, total_sum

# Global window instance
order_window = OrderWindow(window_minutes=WINDOW_SIZE_MINUTES)

# Prometheus metrics registry
registry = CollectorRegistry()
order_count_gauge = Gauge(
    'order_count_last_5min',
    'Number of orders in the last 5 minutes',
    registry=registry
)
order_sum_gauge = Gauge(
    'order_sum_last_5min',
    'Total sum of orders in the last 5 minutes',
    registry=registry
)

def process_message(msg):
    """Process a Kafka message and update the order window"""
    try:
        value = msg.value
        # Skip if value is None (tombstone message)
        if value is None:
            logger.debug("Skipping tombstone message")
            return
        
        # Decode message if it is bytes
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        
        payload = json.loads(value)
        before = None
        after = None
        
        # Handle Debezium envelope (common for JSON)
        if "payload" in payload:
            before = payload["payload"].get("before")
            after = payload["payload"].get("after")
        elif "before" in payload and "after" in payload:
            before = payload["before"]
            after = payload["after"]
        
        # Process insert: before is None, after is not None
        if not before and after:
            order_id = after.get("id") or after.get("order_id")
            total = after.get("total")
            if order_id is not None and total is not None:
                order_window.add_order(order_id, float(total))
                logger.debug(f"Added order {order_id} with total {total}")
        
        # Process delete: before is not None, after is None
        elif before and not after:
            order_id = before.get("id") or before.get("order_id")
            total = before.get("total")
            if order_id is not None and total is not None:
                order_window.remove_order(order_id, float(total))
                logger.debug(f"Removed order {order_id} with total {total}")
        
        # Process update: both before and after exist
        elif before and after:
            # For updates, we need to adjust: remove old, add new
            old_order_id = before.get("id") or before.get("order_id")
            old_total = before.get("total")
            new_order_id = after.get("id") or after.get("order_id")
            new_total = after.get("total")
            
            if old_order_id is not None and old_total is not None:
                order_window.remove_order(old_order_id, float(old_total))
            if new_order_id is not None and new_total is not None:
                order_window.add_order(new_order_id, float(new_total))
            logger.debug(f"Updated order from {old_order_id} to {new_order_id}")
            
    except Exception as e:
        logger.error(f"Failed to process message: {e} - {msg.value}")

def push_metrics_to_prometheus():
    """Push aggregated metrics to Prometheus Pushgateway"""
    try:
        order_count, total_sum = order_window.get_aggregates()
        
        # Update gauge values
        order_count_gauge.set(order_count)
        order_sum_gauge.set(total_sum)
        
        # Push to Prometheus gateway
        push_to_gateway(
            PROMETHEUS_GATEWAY,
            job=PROMETHEUS_JOB,
            registry=registry
        )
        
        logger.info(
            f"Pushed metrics to Prometheus - "
            f"Order Count: {order_count}, Total Sum: {total_sum:.2f}"
        )
    except Exception as e:
        logger.error(f"Failed to push metrics to Prometheus: {e}")

def metrics_pusher_thread():
    """Background thread that pushes metrics every 30 seconds"""
    while True:
        time.sleep(AGGREGATION_INTERVAL_SECONDS)
        push_metrics_to_prometheus()

def main():
    """Main function to start the consumer and metrics pusher"""
    # Set up consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="order-monitoring-group",
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode('utf-8') if isinstance(v, bytes) else v,
    )
    
    # Start metrics pusher thread
    metrics_thread = threading.Thread(target=metrics_pusher_thread, daemon=True)
    metrics_thread.start()
    logger.info("Started metrics pusher thread")
    
    logger.info(f"Listening for messages from topic {KAFKA_TOPIC}...")
    logger.info(
        f"Aggregating orders for {WINDOW_SIZE_MINUTES} minute windows "
        f"and pushing to Prometheus every {AGGREGATION_INTERVAL_SECONDS} seconds"
    )
    
    try:
        for msg in consumer:
            if msg.value is None:
                logger.debug("Consumed tombstone message (value is None), skipping...")
                continue
            
            logger.debug(f"Consumed message from partition {msg.partition}, offset {msg.offset}")
            process_message(msg)
            
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()
