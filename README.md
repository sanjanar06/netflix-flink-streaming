# 🎬 Netflix Mini-Streaming: Distributed Systems Learning Project

**Learn distributed stream processing by building a real-time viewing session analyzer**

---

## 🚀 Quick Start (5 minutes)

See **[QUICKSTART.md](QUICKSTART.md)** for step-by-step instructions.

**TL;DR:**
```bash
# 1. Build
cd flink-job && mvn clean package && cd ..

# 2. Start infrastructure (3 TaskManagers + Kafka + Visualization)
docker-compose up -d

# 3. Submit Flink job
docker cp flink-job/target/viewing-session-analyzer-1.0-SNAPSHOT.jar jobmanager:/tmp/
docker exec jobmanager flink run /tmp/viewing-session-analyzer-1.0-SNAPSHOT.jar

# 4. Watch the magic
# Visualization: http://localhost:5001
# Flink UI: http://localhost:8081
```

---

## 🎯 What You're Building

A **distributed stream processing system** that:
- Processes **5 users** watching Netflix simultaneously
- Distributes work across **3 TaskManagers** (Flink workers)
- Merges viewing events into sessions in real-time
- Visualizes how Flink routes users to workers

### The Architecture

```
5 Users → Kafka → Flink (3 workers) → Real-time Dashboard
         (events)   (keyBy routing)    (see distribution)
```

### Key Learning Goals

✅ **Distributed Processing**: See how `keyBy()` routes users to workers  
✅ **Stateful Streaming**: Watch Flink maintain session state per user  
✅ **Event Time**: Understand timers and session detection  
✅ **Visualization**: See the distribution in real-time on http://localhost:5001

---

## 📚 ANSWERING YOUR INTERN QUESTIONS

### Question 1: Distributed vs. Local - Why can't I just sort a list in Python?

**Short Answer:** Because the data is arriving continuously on multiple machines, and by the time you "sort" it, new data has already arrived.

**The Detailed Explanation:**

#### In LeetCode (Local Merging):
```python
intervals = [[1,3], [2,6], [8,10], [15,18]]
intervals.sort()  # This works because:
                  # 1. All data is HERE (one machine, one memory)
                  # 2. Data is STATIC (not changing while you process)
                  # 3. You can see ALL of it at once
```

#### In Netflix Production (Distributed Merging):

Imagine **10 million users** watching simultaneously:

```
User "Erin" events arriving:
  Machine A receives: [heartbeat at 10:00:01]
  Machine B receives: [heartbeat at 10:00:02]
  Machine C receives: [heartbeat at 10:00:03]
```

**Problems with a Python sort approach:**

1. **Data is on different machines** 
   - You'd need to collect all data to one machine first
   - That machine would run out of memory instantly
   - Network costs would be massive

2. **Data never stops arriving**
   - While you're "sorting", new heartbeats arrive
   - When do you stop waiting for more data?
   - How do you know when Erin's session is "done"?

3. **Latency requirements**
   - Netflix needs to show viewing stats in REAL-TIME
   - Can't wait to "collect and sort" all day's data
   - Need to process as events arrive

**What Flink Does Differently:**

```java
.keyBy(event -> event.userId)  // Send all Erin's events to ONE machine
.process(new SessionDetectionFunction())  // Process incrementally
```

- **Partitioning:** Routes all events for one user to the same worker
- **Stateful Processing:** Each worker maintains a "session notebook" for its users
- **Event-Time Semantics:** Uses timestamps to handle out-of-order arrival
- **Windowing/Timers:** Automatically detects when to "close" a session

**The Key Insight:**  
You're not merging intervals in a list.  
You're merging intervals **as they flow through a distributed pipeline**, making decisions with partial information.

---

### Question 2: The State Analogy - Where is the "concierge's notebook"?

**The Concierge Analogy:**

Think of a hotel concierge tracking packages:
- Guest "Erin" has a package arriving
- Concierge writes in notebook: "Erin - 1 package - 10:00 AM"
- Another package for Erin arrives at 10:05 AM
- Concierge updates: "Erin - 2 packages - last at 10:05 AM"
- If no package arrives for 30 minutes, deliver them all

**In the Code - Here's the Notebook:**

```java
// DECLARING the notebook (in SessionDetectionFunction):
private transient ValueState<ViewingSession> currentSessionState;

@Override
public void open(Configuration parameters) {
    // Create a "notebook page" for tracking sessions
    ValueStateDescriptor<ViewingSession> descriptor = 
        new ValueStateDescriptor<>(
            "current-session",  // ← Name of this state variable
            TypeInformation.of(ViewingSession.class)  // ← What we're storing
        );
    
    currentSessionState = getRuntimeContext().getState(descriptor);
}
```

**What We Write in the Notebook:**

```java
public static class ViewingSession {
    public String userId;          // Who is watching?
    public String showName;        // What show?
    public long startTime;         // When did session start?
    public long lastHeartbeat;     // Most recent heartbeat timestamp
    public int heartbeatCount;     // How many heartbeats total?
}
```

**Using the Notebook:**

```java
@Override
public void processElement(ViewingEvent event, Context ctx, Collector<String> out) {
    // READ from the notebook
    ViewingSession session = currentSessionState.value();
    
    if (session == null) {
        // Start a new notebook entry
        session = new ViewingSession();
        session.userId = event.userId;
        session.startTime = event.timestamp;
    } else {
        // UPDATE the existing entry
        session.heartbeatCount++;
        session.lastHeartbeat = event.timestamp;
    }
    
    // WRITE back to the notebook
    currentSessionState.update(session);
}
```

**Why Not Just Use a Java HashMap?**

```java
// ❌ DON'T DO THIS:
private Map<String, ViewingSession> sessions = new HashMap<>();
```

**Problems:**
1. **Lost on Failure:** If the machine crashes, HashMap is gone
2. **Not Distributed:** Can't be shared across machines
3. **No Checkpointing:** Flink can't save/restore it
4. **Memory Issues:** Grows unbounded (no cleanup)

**Flink State Benefits:**
- ✅ **Fault Tolerance:** Automatically saved to disk (checkpoints)
- ✅ **Recovery:** If worker crashes, state is restored
- ✅ **Partitioning:** Each worker only stores its keys' state
- ✅ **TTL:** Can auto-expire old state
- ✅ **Scalability:** State is sharded across workers

**The Exact Location in Code:**

| Code Location | Purpose |
|---------------|---------|
| `Line 121: private transient ValueState<ViewingSession>` | Declaration of the state variable |
| `Line 136-143: open()` | Creating/registering the state |
| `Line 161: currentSessionState.value()` | Reading from state |
| `Line 193: currentSessionState.update(session)` | Writing to state |
| `Line 220: currentSessionState.clear()` | Clearing state when done |

---

### Question 3: Timers - How does Flink handle the gap if events arrive out of order?

**The Challenge:**

Imagine events arrive like this:

```
Expected Order:    10:00:01, 10:00:02, 10:00:03
Actual Arrival:    10:00:01, 10:00:03, 10:00:02  ← Out of order!
```

If you set a "30 seconds of silence" timer after the first event, it might fire too early!

**Flink's Solution: Event Time + Watermarks**

#### 1. Event Time vs. Processing Time

```java
DataStream<String> rawEvents = env.fromSource(
    source,
    WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)),
    //                                                                    ↑
    //                                      "Wait up to 5 seconds for late events"
    "Kafka Source"
);
```

- **Event Time:** The timestamp IN the event (when user actually watched)
- **Processing Time:** When Flink processes it (could be seconds later)
- **Watermark:** Flink's way of saying "I'm now at time T, events older than T-5 are late"

#### 2. How Timers Work with Event Time

```java
@Override
public void processElement(ViewingEvent event, Context ctx, Collector<String> out) {
    // Set timer for 30 seconds AFTER this event's timestamp
    long timerTime = event.timestamp + 30_000;
    ctx.timerService().registerEventTimeTimer(timerTime);
    //                  ↑
    //                  Uses EVENT TIME, not wall-clock time
}
```

**Example Timeline:**

```
Events arrive:
  10:00:01 → Set timer for 10:00:31
  10:00:03 → Set timer for 10:00:33 (OVERWRITES previous timer)
  10:00:02 → Set timer for 10:00:32 (late, but still processed!)
             (OVERWRITES the 10:00:33 timer)

Watermark reaches 10:00:32:
  → Timer for 10:00:32 fires
  → Session is closed
```

#### 3. The "Snooze Button" Effect

Every time a new heartbeat arrives, you register a NEW timer:

```java
// Heartbeat at 10:00:01 → Timer at 10:00:31
ctx.timerService().registerEventTimeTimer(10:00:31);

// Heartbeat at 10:00:02 → Timer at 10:00:32 (overwrites 10:00:31)
ctx.timerService().registerEventTimeTimer(10:00:32);

// Heartbeat at 10:00:03 → Timer at 10:00:33 (overwrites 10:00:32)
ctx.timerService().registerEventTimeTimer(10:00:33);

// No more heartbeats...
// Watermark advances...
// When watermark reaches 10:00:33, timer fires → Session closes!
```

**It's like hitting snooze on an alarm** - each heartbeat pushes the "close session" timer 30 seconds forward.

#### 4. Handling Late Events

What if an event arrives VERY late?

```java
WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
```

This tells Flink: "Wait up to 5 seconds for stragglers, but after that, drop them."

**Trade-offs:**
- **Smaller buffer (1 second):** Faster results, but might miss late events
- **Larger buffer (10 seconds):** Catch more late events, but higher latency

**In Production:** Netflix might use different strategies:
- **Session detection:** Short buffer (fast closing)
- **Billing:** Longer buffer (accuracy over speed)

#### 5. Visual Timeline

```
Time →  00:01  00:02  00:03  00:04  ...  00:33  00:34
        ─────────────────────────────────────────────
Events:  ❤️     ❤️     ❤️           [gap]
         
Timers:  ⏰31  ⏰32   ⏰33
        (cancel)(cancel)(ACTIVE)
        
                                         FIRE! 🔥
                                     (30 sec gap detected)
```

**Code Location for Timers:**

| Line | Purpose |
|------|---------|
| `Line 56-58` | WatermarkStrategy configuration (handles out-of-order) |
| `Line 187-189` | Registering the event-time timer |
| `Line 199-222` | `onTimer()` - Callback when timer fires |

---

## 🚀 Running the Project

### Prerequisites
- Docker Desktop installed and running
- Java 11+ (for building the Flink job)
- Maven (for building)

### Step 1: Build the Flink Job

```bash
cd flink-job
mvn clean package
```

This creates: `target/viewing-session-analyzer-1.0-SNAPSHOT.jar`

### Step 2: Start the Infrastructure

```bash
# From project root
docker-compose up -d zookeeper kafka jobmanager taskmanager
```

Wait 30 seconds for services to be ready.

### Step 3: Submit the Flink Job

```bash
# Copy the JAR into the Flink container
docker cp flink-job/target/viewing-session-analyzer-1.0-SNAPSHOT.jar jobmanager:/tmp/

# Submit the job
docker exec -it jobmanager flink run /tmp/viewing-session-analyzer-1.0-SNAPSHOT.jar
```

### Step 4: Start the Data Generator

```bash
docker-compose up data-generator
```

### Step 5: Watch the Magic! 🎬

Open the Flink UI: http://localhost:8081

Watch the logs:
```bash
docker logs -f taskmanager
```

You should see:
```
🟢 NEW SESSION started for user: Erin
   ➕ Heartbeat #2 added to session for Erin
   ➕ Heartbeat #3 added to session for Erin
   ...
🎬 SESSION CLOSED: User 'Erin' watched 'Derry Girls' for 10 seconds (10 heartbeats)

[40 second gap]

🟢 NEW SESSION started for user: Erin
   ...
🎬 SESSION CLOSED: User 'Erin' watched 'Derry Girls' for 10 seconds (10 heartbeats)
```

---

## 🎓 Key Concepts You've Learned

### 1. **KeyBy (Partitioning)**
   - Ensures all events for one user go to the same worker
   - Foundation of stateful processing in distributed systems

### 2. **Managed State**
   - Fault-tolerant storage for intermediate results
   - Not just a Java variable - it's checkpointed and recoverable

### 3. **Event Time Processing**
   - Use event timestamps, not processing time
   - Handles out-of-order and late-arriving data

### 4. **Timers**
   - Schedule actions based on event time progress
   - Enable gap detection and session timeouts

### 5. **Watermarks**
   - Tell Flink "time is progressing" in the event stream
   - Balance between latency and completeness

---

## 🔧 Troubleshooting

**Flink job fails to connect to Kafka:**
```bash
# Check Kafka is running
docker logs kafka

# Wait longer - Kafka takes ~30 seconds to start
sleep 30
```

**No events appearing:**
```bash
# Check generator logs
docker logs data-generator

# Manually test Kafka
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic viewing-events \
  --from-beginning
```

**Build errors:**
```bash
# Ensure Java 11+
java -version

# Clean build
cd flink-job
mvn clean install -U
```

---

## 📖 Next Steps for Your Learning

1. **Add a Second User:**
   - Modify `generator.py` to simulate "Clare" watching "The Crown"
   - Watch how Flink processes both users in parallel

2. **Change the Session Timeout:**
   - In `ViewingSessionJob.java` line 187, change `30_000` to `10_000`
   - See how it affects session detection

3. **Simulate Out-of-Order Events:**
   - Modify generator to send events with shuffled timestamps
   - Watch how watermarks handle it

4. **Add More TaskManagers:**
   - In `docker-compose.yml`, set `scale: 3` for taskmanager
   - Increase parallelism to 3 in the Flink job
   - See distributed processing in action!

5. **Store Results in a Database:**
   - Add a Postgres sink
   - Write closed sessions to a table
   - Build a "viewing history" API

---

## 🎬 Real Netflix at Scale

What you've built is simplified, but the **concepts are identical** to production:

| Your Project | Netflix Production |
|--------------|-------------------|
| 1-2 users | 200M+ subscribers |
| 1 Kafka topic | Thousands of topics |
| 1 TaskManager | Thousands of servers |
| Print to console | Write to data lake (S3/Delta) |
| 30-second timeout | ML-driven session detection |
| Manual deployment | CI/CD pipelines |

**The core concepts (keyBy, state, timers) are the same!**

---

## 🙏 Acknowledgments

This project demonstrates production patterns used at:
- Netflix (viewing analytics)
- Uber (trip tracking)
- Spotify (listening sessions)
- Any company doing real-time event processing at scale

---

## 📝 Cheat Sheet

**Start everything:**
```bash
docker-compose up -d
```

**Build & deploy Flink job:**
```bash
cd flink-job && mvn package && cd ..
docker cp flink-job/target/viewing-session-analyzer-1.0-SNAPSHOT.jar jobmanager:/tmp/
docker exec -it jobmanager flink run /tmp/viewing-session-analyzer-1.0-SNAPSHOT.jar
```

**Watch output:**
```bash
docker logs -f taskmanager
```

**Stop everything:**
```bash
docker-compose down
```

---

**Questions?** Review the code comments - every critical section has detailed explanations for interns! 🎓
