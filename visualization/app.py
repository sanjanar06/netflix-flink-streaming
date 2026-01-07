"""
Netflix Distributed Processing Visualization Dashboard
=======================================================
This shows you IN REAL-TIME how Flink distributes work across TaskManagers.

You'll see:
- Which users are assigned to which TaskManagers
- Active sessions per worker
- Real-time heartbeats and session closures
"""

from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json
import threading
import os
import time
from collections import defaultdict, deque

app = Flask(__name__)

# Store metrics in memory
metrics_queue = deque(maxlen=100)  # Last 100 events
taskmanager_assignments = {}  # user_id -> taskmanager
active_sessions = defaultdict(dict)  # taskmanager -> {user_id: session_data}
session_history = deque(maxlen=50)  # Recent closed sessions

def consume_metrics():
    """Background thread that reads metrics from Kafka"""
    time.sleep(15)  # Wait for Kafka to be ready
    
    print("🔌 Connecting to Kafka for metrics...")
    consumer = KafkaConsumer(
        'session-metrics',
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='visualization-dashboard',
        auto_offset_reset='latest'
    )
    
    print("✅ Connected! Listening for metrics...")
    
    for message in consumer:
        try:
            metric = message.value
            metrics_queue.append(metric)
            
            user_id = metric.get('userId')
            event_type = metric.get('eventType')
            task_manager = metric.get('taskManager', 'unknown')
            
            # Track which TaskManager handles which user
            if user_id:
                taskmanager_assignments[user_id] = task_manager
            
            # Update active sessions
            if event_type == 'SESSION_START':
                if task_manager not in active_sessions:
                    active_sessions[task_manager] = {}
                active_sessions[task_manager][user_id] = {
                    'userId': user_id,
                    'showName': metric.get('showName'),
                    'heartbeats': 0,
                    'startTime': metric.get('timestamp')
                }
                print(f"🟢 [{task_manager}] {user_id} started session")
                
            elif event_type == 'HEARTBEAT':
                if task_manager in active_sessions and user_id in active_sessions[task_manager]:
                    active_sessions[task_manager][user_id]['heartbeats'] = metric.get('heartbeatCount', 0)
                
            elif event_type == 'SESSION_END':
                if task_manager in active_sessions and user_id in active_sessions[task_manager]:
                    session_data = active_sessions[task_manager][user_id]
                    session_data['durationSeconds'] = metric.get('durationSeconds')
                    session_data['endTime'] = metric.get('timestamp')
                    session_history.append({
                        'taskManager': task_manager,
                        **session_data
                    })
                    del active_sessions[task_manager][user_id]
                    print(f"🎬 [{task_manager}] {user_id} ended session")
        
        except Exception as e:
            print(f"Error processing metric: {e}")

# Start background thread
metrics_thread = threading.Thread(target=consume_metrics, daemon=True)
metrics_thread.start()

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/status')
def get_status():
    """API endpoint for real-time dashboard updates"""
    
    # Count total active sessions per TaskManager
    taskmanager_stats = {}
    for tm, sessions in active_sessions.items():
        taskmanager_stats[tm] = {
            'name': tm,
            'activeSessions': len(sessions),
            'users': list(sessions.keys())
        }
    
    return jsonify({
        'taskmanagers': taskmanager_stats,
        'activeSessions': {
            tm: list(sessions.values())
            for tm, sessions in active_sessions.items()
        },
        'recentMetrics': list(metrics_queue)[-10:],  # Last 10 events
        'sessionHistory': list(session_history)[-10:],  # Last 10 closed sessions
        'assignments': taskmanager_assignments
    })

if __name__ == '__main__':
    print("\n" + "="*80)
    print("🎯 NETFLIX DISTRIBUTED PROCESSING VISUALIZATION")
    print("="*80)
    print("\n📊 Dashboard running at http://localhost:5001")
    print("   This shows REAL-TIME distribution of users across TaskManagers\n")
    
    app.run(host='0.0.0.0', port=5000, debug=False)
