# 🚀 Quick Start - Distributed Mode

This guide will get your distributed Netflix streaming system running in under 5 minutes!

## What You're About to See

- **3 TaskManagers** (Flink workers) running in parallel
- **5 Users** watching Netflix simultaneously  
- **Real-time visualization** showing how Flink distributes users across workers
- **Live metrics** showing session starts, heartbeats, and closures

## Prerequisites

- Docker Desktop installed and running
- Java 11+ and Maven installed
- 8GB+ RAM available for Docker

## Step-by-Step Setup

### 1. Build the Flink Job

```bash
cd flink-job
mvn clean package
cd ..
```

You should see: `BUILD SUCCESS`

### 2. Start All Infrastructure

```bash
# Start Kafka, Zookeeper, and Flink cluster with 3 TaskManagers
docker-compose up -d
```

Wait 30 seconds for everything to start.

### 3. Verify Flink is Running

```bash
docker ps
```

You should see 6 containers running:
- zookeeper
- kafka
- jobmanager
- taskmanager-1
- taskmanager-2  
- taskmanager-3
- data-generator
- visualization

### 4. Submit the Flink Job

```bash
# Copy JAR to JobManager
docker cp flink-job/target/viewing-session-analyzer-1.0-SNAPSHOT.jar jobmanager:/tmp/

# Submit the job (parallelism=5 means 5 parallel processing instances)
docker exec jobmanager flink run /tmp/viewing-session-analyzer-1.0-SNAPSHOT.jar
```

### 5. Open the Visualization Dashboard

Open your browser to: **http://localhost:5001**

You'll see the real-time dashboard showing:
- Which users are on which TaskManagers
- Active sessions per worker
- Live heartbeat counts
- Session completions

### 6. Watch the Magic! ✨

In the dashboard, you'll see:

1. **Initial Distribution** (first ~5 seconds):
   - Each of the 5 users gets assigned to a TaskManager
   - You'll see "SESSION_START" events with TaskManager IDs

2. **Active Processing** (next ~60 seconds):
   - Heartbeat counters increasing for each user
   - Notice: Each user ALWAYS stays on the same TaskManager (keyBy magic!)
   - Different TaskManagers process different users in parallel

3. **First Session Closures** (after ~35-50 seconds):
   - Users start pausing (40-50 second gaps in the code)
   - After 30 seconds of no activity, Flink closes sessions
   - You'll see "SESSION_END" with duration and heartbeat count

4. **Second Wave** (after ~45-60 seconds):
   - Users resume watching (new sessions)
   - New "SESSION_START" events
   - Same TaskManager assignments! (because same user_id)

5. **Final Closures** (after ~90-110 seconds):
   - All sessions complete
   - Dashboard shows completed sessions

## Monitoring Options

### Option A: Flink Web UI
http://localhost:8081
- See job graph
- Monitor TaskManager resource usage
- View task distribution

### Option B: Watch TaskManager Logs
```bash
# See logs from all TaskManagers
docker logs -f taskmanager-1
# OR
docker logs -f taskmanager-2
# OR
docker logs -f taskmanager-3
```

You'll see messages like:
```
🟢 [Detect Sessions (1/5)#0] NEW SESSION for: Erin
   ➕ [Detect Sessions (1/5)#0] Heartbeat #2 for Erin
🎬 [Detect Sessions (1/5)#0] SESSION CLOSED: Erin watched Derry Girls
```

The `(1/5)#0` means: "subtask 1 of 5 parallel tasks, running on TaskManager 0"

### Option C: Watch Data Generator
```bash
docker logs -f data-generator
```

See all 5 users sending heartbeats simultaneously.

## Understanding the Distribution

### Key Concept: keyBy() Routing

```java
.keyBy(event -> event.userId)  // Routes by user_id
```

**What happens:**
1. Flink hashes the user_id (e.g., "Erin" → hash value)
2. `hash % num_parallel_instances` determines which TaskManager
3. ALL events for "Erin" go to the same TaskManager forever

**Why this matters:**
- State management: Each TaskManager maintains sessions only for its users
- Parallel processing: Different users processed simultaneously
- Fault tolerance: If TaskManager crashes, Flink reassigns its users to another worker

### Parallelism = 5 with 3 TaskManagers

- **3 TaskManagers** × **2 slots each** = **6 total slots**
- **5 parallel instances** of the session detector
- Distribution might be: TM1 (2 users), TM2 (2 users), TM3 (1 user)

## Troubleshooting

### Dashboard shows "Waiting for Flink job to start"

Wait 10 more seconds. The generator starts after a 15-second delay.

### No containers running

```bash
# Check logs
docker-compose logs

# Restart
docker-compose down
docker-compose up -d
```

### Job submission fails

```bash
# Check JobManager is running
docker ps | grep jobmanager

# Check JobManager logs
docker logs jobmanager

# Verify JAR exists
docker exec jobmanager ls -lh /tmp/viewing-session-analyzer-1.0-SNAPSHOT.jar
```

### Port already in use

Change ports in `docker-compose.yml`:
- Flink UI: `8081` → `8082`
- Visualization: Already on `5001`

## Stopping Everything

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## What's Next?

Now that you see distributed processing in action, try:

1. **Scale Up**: Add more TaskManagers in docker-compose.yml
2. **More Users**: Modify generator.py to add 10+ users
3. **Increase Parallelism**: Change `env.setParallelism(5)` to `env.setParallelism(10)`
4. **Failure Testing**: Kill a TaskManager mid-processing and watch Flink recover

## Key Takeaways

✅ **You just ran a real distributed streaming system!**

- keyBy() ensures correct routing for stateful processing
- Multiple TaskManagers process users in parallel
- Each worker maintains state only for its assigned users
- Real-time event processing with session detection
- Visualization shows the distributed nature clearly

This is the SAME architecture Netflix uses, just at smaller scale!

---

**Questions?** Check the main [README.md](README.md) for deep-dive explanations.
