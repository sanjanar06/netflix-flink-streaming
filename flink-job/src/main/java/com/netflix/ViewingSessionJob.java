package com.netflix;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This Flink job demonstrates the "Merge Intervals" problem at SCALE.
 * 
 * On LeetCode, you merge intervals in a sorted array in one place.
 * Here, we merge intervals across:
 *   - Multiple machines (distributed)
 *   - Arriving events in real-time (streaming)
 *   - Potentially out-of-order data
 * 
 * This is the fundamental difference between coding interviews and production!
 */
public class ViewingSessionJob {
    
    public static void main(String[] args) throws Exception {
        // Step 1: Create the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Parallelism of 5 means 5 parallel tasks
        // With 3 TaskManagers (2 slots each = 6 total slots), we can run 5 parallel instances
        // This distributes our 5 users across the 3 TaskManagers!
        env.setParallelism(5);
        
        System.out.println("🚀 Starting with parallelism: 5 (distributed across 3 TaskManagers)");
        
        // Step 2: Create a Kafka source to read viewing events
        System.out.println("🔌 Setting up Kafka source...");
        KafkaSource<String> source = KafkaSource
            .<String>builder()
            .setBootstrapServers("kafka:29092")  // Connect to our Kafka broker
            .setTopics("viewing-events")          // Read from the viewing-events topic
            .setGroupId("flink-session-analyzer") // Consumer group (for Kafka offset tracking)
            .setStartingOffsets(OffsetsInitializer.earliest()) // Read from the beginning
            .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize as strings
            .build();
        
        // Step 3: Create the data stream from Kafka
        DataStream<String> rawEvents = env.fromSource(
            source,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // Handle out-of-order events upto 5 seconds
                .withIdleness(Duration.ofSeconds(30)),
            "Kafka Source"
        );
        
        // Step 4: Parse JSON strings into ViewingEvent objects
        DataStream<ViewingEvent> events = rawEvents
            .map(jsonString -> ViewingEvent.fromJson(jsonString))
            .name("Parse JSON");
        
        // ========================================================================
        // CRITICAL STEP: keyBy - This ensures all events for the same user
        // go to the SAME TaskManager (worker machine)
        // ========================================================================
        // 
        // WHY? If Erin's events went to 3 different machines, each machine would
        // only see PART of the session and couldn't merge properly.
        // 
        // keyBy() is like saying: "Dear Flink, please send all events with the
        // same user_id to the same worker so we can track their session."
        //
        // With 5 users and parallelism 5, each user gets their own parallel instance!
        //
        DataStream<SessionMetric> metrics = events
            .keyBy(event -> event.userId)  // ← THIS IS THE MAGIC
            .process(new SessionDetectionFunction())
            .name("Detect Sessions");
        
        // Step 5: Create Kafka sink for visualization
        KafkaSink<String> metricsSink = KafkaSink.<String>builder()
            .setBootstrapServers("kafka:29092")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("session-metrics")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        
        // Convert metrics to JSON and send to Kafka for visualization
        metrics
            .map(metric -> metric.toJson())
            .sinkTo(metricsSink)
            .name("Send to Visualization");
        
        // Also print to console for debugging
        metrics.map(metric -> metric.toHumanReadable()).print();
        
        // Step 6: Execute the job
        System.out.println("🚀 Starting Flink job: Viewing Session Analyzer");
        System.out.println("📊 Visualization available at: http://localhost:5001");
        env.execute("Netflix Viewing Session Analyzer");
    }
    
    /**
     * ========================================================================
     * THE "BRAIN" - KeyedProcessFunction
     * ========================================================================
     * 
     * This is where the merge logic happens. This function processes events
     * for ONE user (thanks to keyBy).
     * 
     * Think of each KeyedProcessFunction instance as a dedicated agent
     * watching ONE user's activity.
     * 
     * NOW WITH METRICS: We track WHICH TaskManager is processing each user!
     */
    public static class SessionDetectionFunction 
            extends KeyedProcessFunction<String, ViewingEvent, SessionMetric> {
        
        // ====================================================================
        // THE "CONCIERGE'S NOTEBOOK" - Flink State
        // ====================================================================
        // 
        // LEARNING NOTE: This is NOT a regular Java variable!
        // 
        // Why? Because regular variables would be lost if:
        //   - The machine crashes
        //   - Flink moves the task to another machine
        //   - The job restarts
        // 
        // ValueState is MANAGED by Flink. It's:
        //   - Automatically saved (checkpointed)
        //   - Automatically restored on failure
        //   - Partitioned by key (each user has their own state)
        //
        private transient ValueState<ViewingSession> currentSessionState;
        private String taskManagerId;
        
        /**
         * This method runs ONCE when the function is initialized.
         * Here we "declare" what we want to store in our state.
         */
        @Override
        public void open(Configuration parameters) {
            // Capture which TaskManager this runs on - KEY for visualization!
            taskManagerId = getRuntimeContext().getTaskNameWithSubtasks();
            System.out.println("🔧 SessionDetector initialized on: " + taskManagerId);
            
            // Define the "notebook page" where we'll track the current session
            ValueStateDescriptor<ViewingSession> descriptor = 
                new ValueStateDescriptor<>(
                    "current-session",           // Name of the state
                    TypeInformation.of(ViewingSession.class) // What type we're storing
                );
            
            currentSessionState = getRuntimeContext().getState(descriptor);
            
            System.out.println("📓 Session detector initialized - notebook ready!");
        }
        
        /**
         * This method is called for EVERY event that arrives for this user.
         * 
         * @param event     The viewing event (heartbeat)
         * @param ctx       Context object (used for timers and timestamps)
         * @param out       Output collector (to emit results)
         */
        @Override
        public void processElement(
                ViewingEvent event,
                Context ctx,
                Collector<SessionMetric> out) throws Exception {
            
            // Read the current session from state (our "notebook")
            ViewingSession session = currentSessionState.value();
            
            if (session == null) {
                // No active session - START A NEW ONE!
                System.out.println("[" + taskManagerId + "] NEW SESSION for: " + event.userId);
                
                session = new ViewingSession();
                session.userId = event.userId;
                session.showName = event.show;
                session.startTime = event.timestamp;
                session.lastHeartbeat = event.timestamp;
                session.heartbeatCount = 1;
                
                // Emit SESSION_START metric for visualization
                SessionMetric metric = new SessionMetric();
                metric.eventType = "SESSION_START";
                metric.userId = event.userId;
                metric.showName = event.show;
                metric.taskManager = taskManagerId;
                metric.timestamp = System.currentTimeMillis();
                out.collect(metric);
                
            } else {
                // Active session exists - ADD THIS HEARTBEAT TO IT
                session.lastHeartbeat = event.timestamp;
                session.heartbeatCount++;
                
                System.out.println("   ➕ [" + taskManagerId + "] Heartbeat #" + 
                                 session.heartbeatCount + " for " + event.userId);
                
                // Emit HEARTBEAT metric periodically (every 5 heartbeats)
                if (session.heartbeatCount % 5 == 0) {
                    SessionMetric metric = new SessionMetric();
                    metric.eventType = "HEARTBEAT";
                    metric.userId = event.userId;
                    metric.showName = event.show;
                    metric.taskManager = taskManagerId;
                    metric.heartbeatCount = session.heartbeatCount;
                    metric.timestamp = System.currentTimeMillis();
                    out.collect(metric);
                }
            }
            
            // ================================================================
            // EVENT TIME TIMER - The "alarm clock" that closes sessions
            // ================================================================
            // 
            // Set a timer to fire 30 seconds AFTER this event's timestamp.
            // If NO new events arrive before the timer fires, we'll close
            // the session in onTimer() below.
            // 
            // LEARNING NOTE: Every time we get a new heartbeat, we set a NEW
            // timer. The old timer is automatically "overwritten" because we're
            // using the same namespace. This is like hitting "snooze" on an alarm.
            //
            long timerTime = event.timestamp + 30_000; // 30 seconds in milliseconds
            ctx.timerService().registerEventTimeTimer(timerTime);
            
            // Save the updated session back to state (write to the "notebook")
            currentSessionState.update(session);
        }
        
        /**
         * ================================================================
         * THE TIMER CALLBACK - This fires when 30 seconds pass with no events
         * ================================================================
         * 
         * This is called when the timer we set in processElement() fires.
         * It means: "30 seconds have passed since the last heartbeat."
         * Time to close the session!
         * 
         * @param timestamp  The timestamp of the timer
         * @param ctx        Context object
         * @param out        Output collector to emit the closed session
         */
        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<SessionMetric> out) throws Exception {
            
            ViewingSession session = currentSessionState.value();
            
            if (session != null) {
                // Calculate how long they watched
                long durationMs = session.lastHeartbeat - session.startTime;
                long durationSeconds = durationMs / 1000;
                
                System.out.println("🎬 [" + taskManagerId + "] SESSION CLOSED: " + 
                                 session.userId + " watched " + session.showName);
                
                // Emit SESSION_END metric for visualization
                SessionMetric metric = new SessionMetric();
                metric.eventType = "SESSION_END";
                metric.userId = session.userId;
                metric.showName = session.showName;
                metric.taskManager = taskManagerId;
                metric.durationSeconds = durationSeconds;
                metric.heartbeatCount = session.heartbeatCount;
                metric.timestamp = System.currentTimeMillis();
                out.collect(metric);
                
                // Clear the state - session is over!
                currentSessionState.clear();
            }
        }
    }
    
    /**
     * Simple POJO to represent a viewing event
     */
    public static class ViewingEvent {
        public String userId;
        public String show;
        public long timestamp;
        public String type;
        
        // Parse JSON into ViewingEvent
        public static ViewingEvent fromJson(String json) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(json);
                
                ViewingEvent event = new ViewingEvent();
                event.userId = node.get("user_id").asText();
                event.show = node.get("show").asText();
                event.timestamp = node.get("timestamp").asLong();
                event.type = node.get("type").asText();
                
                return event;
            } catch (Exception e) {
                System.err.println("Failed to parse JSON: " + json);
                throw new RuntimeException(e);
            }
        }
    }
    
    /**
     * Represents an active viewing session for a user.
     * This is what we store in Flink State.
     */
    public static class ViewingSession {
        public String userId;
        public String showName;
        public long startTime;
        public long lastHeartbeat;
        public int heartbeatCount;
    }
    
    /**
     * Metric class for visualization dashboard.
     * Shows what's happening on each TaskManager in real-time!
     */
    public static class SessionMetric {
        public String eventType;  // SESSION_START, HEARTBEAT, SESSION_END
        public String userId;
        public String showName;
        public String taskManager;  // Which worker is processing this user
        public long timestamp;
        public int heartbeatCount;
        public long durationSeconds;
        
        public String toJson() {
            try {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.writeValueAsString(this);
            } catch (Exception e) {
                return "{}";
            }
        }
        
        public String toHumanReadable() {
            switch (eventType) {
                case "SESSION_START":
                    return String.format("[%s] %s started watching %s", 
                                       taskManager, userId, showName);
                case "HEARTBEAT":
                    return String.format("[%s] %s: %d heartbeats", 
                                       taskManager, userId, heartbeatCount);
                case "SESSION_END":
                    return String.format("[%s] %s watched %s for %d seconds (%d heartbeats)", 
                                       taskManager, userId, showName, durationSeconds, heartbeatCount);
                default:
                    return eventType;
            }
        }
    }
}