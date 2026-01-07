"""
The "TV" - Netflix Data Generator
==================================
In a distributed system, different users are processed by different workers.
With multiple users, you'll see:
- Users being distributed across 3 TaskManagers
- Parallel session tracking
- How keyBy() routes users to workers
"""

import json
import time
import os
import threading
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'viewing-events')

def create_producer():
    print(f"🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Add retry logic because Kafka might not be ready immediately
        retries=10,
        retry_backoff_ms=1000
    )
    print("Connected to Kafka successfully!")
    return producer

def send_heartbeat(producer, user_id, show_name):
    event = {
        "user_id": user_id,
        "show": show_name,
        "timestamp": int(time.time() * 1000),  # Unix timestamp in milliseconds
        "type": "HEARTBEAT"
    }
    
    producer.send(KAFKA_TOPIC, value=event)
    # Format the timestamp for readability
    readable_time = datetime.fromtimestamp(event['timestamp'] / 1000).strftime('%H:%M:%S')
    print(f"[{readable_time}] {user_id} watching {show_name}")
    
    return event

def simulate_viewing_session():
    """
    Simulate MULTIPLE users watching simultaneously to show distributed processing.
    
    Users:
    - Erin: Watches "Derry Girls"
    - Clare: Watches "The Crown"  
    - Michelle: Watches "Stranger Things"
    - James: Watches "Breaking Bad"
    - Orla: Watches "Bridgerton"
    
    Each user has a different viewing pattern to make it interesting!
    """
    producer = create_producer()
    
    print("\n" + "="*80)
    print(" STARTING DISTRIBUTED NETFLIX VIEWING SIMULATION")
    print("="*80)
    print("   - 5 USERS watching simultaneously")
    print("   - Flink's keyBy() will route each user to a TaskManager")
    print("   - Different session patterns per user")
    
    # Define our users and their shows
    users = [
        {"user_id": "Erin", "show": "Derry Girls", "heartbeats": 15, "pause": 35},
        {"user_id": "Clare", "show": "The Crown", "heartbeats": 20, "pause": 40},
        {"user_id": "Michelle", "show": "Stranger Things", "heartbeats": 12, "pause": 45},
        {"user_id": "James", "show": "Breaking Bad", "heartbeats": 18, "pause": 38},
        {"user_id": "Orla", "show": "Bridgerton", "heartbeats": 10, "pause": 50},
    ]
    
    def simulate_user(user_data):
        """Simulate one user's viewing in a separate thread"""
        user_id = user_data["user_id"]
        show = user_data["show"]
        num_heartbeats = user_data["heartbeats"]
        pause_duration = user_data["pause"]
        
        try:
            # Add a random start delay so users don't all start at exact same time
            start_delay = random.uniform(0, 3)
            time.sleep(start_delay)
            
            # SESSION 1
            print(f"\n{user_id} started watching {show}")
            for i in range(num_heartbeats):
                send_heartbeat(producer, user_id, show)
                time.sleep(1)
            
            print(f"\n⏸{user_id} paused for {pause_duration} seconds")
            time.sleep(pause_duration)
            
            # SESSION 2
            print(f"\n{user_id} resumed watching {show}")
            for i in range(num_heartbeats):
                send_heartbeat(producer, user_id, show)
                time.sleep(1)
            
            print(f"\n{user_id} finished watching")
            
        except Exception as e:
            print(f"Error for {user_id}: {e}")
    
    try:
        # Start a thread for each user - they all watch simultaneously!
        threads = []
        for user in users:
            thread = threading.Thread(target=simulate_user, args=(user,))
            thread.start()
            threads.append(thread)
        
        # Wait for all users to finish
        for thread in threads:
            thread.join()
        
        print("\n\n All users finished! Waiting for final sessions to close...")
        time.sleep(35)
        
    except KeyboardInterrupt:
        print("\n\n  Stopped by user")
    finally:
        producer.close()
        print("Generator shut down cleanly")

if __name__ == "__main__":
    # Wait a moment to ensure Kafka is fully ready
    print("Waiting for Kafka to be ready...")
    time.sleep(5)
    
    simulate_viewing_session()
