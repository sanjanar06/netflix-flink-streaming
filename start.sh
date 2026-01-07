#!/bin/bash

# Netflix Flink Streaming - Quick Start Script
# This script helps you get everything running quickly

set -e  # Exit on error

echo "🎬 Netflix Mini-Streaming Quick Start"
echo "======================================"
echo ""

# Check prerequisites
echo "📋 Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { echo "❌ Docker not found. Please install Docker Desktop."; exit 1; }
command -v mvn >/dev/null 2>&1 || { echo "❌ Maven not found. Please install Maven."; exit 1; }
command -v java >/dev/null 2>&1 || { echo "❌ Java not found. Please install Java 11+."; exit 1; }

echo "✅ All prerequisites found!"
echo ""

# Build the Flink job
echo "🔨 Building Flink job..."
cd flink-job
mvn clean package -q
cd ..
echo "✅ Flink job built successfully!"
echo ""

# Start infrastructure
echo "🚀 Starting infrastructure (Kafka, Zookeeper, Flink cluster)..."
docker-compose up -d zookeeper kafka jobmanager taskmanager-1 taskmanager-2 taskmanager-3 visualization
echo "⏳ Waiting 30 seconds for services to be ready..."
sleep 30
echo "✅ Infrastructure is ready!"
echo ""

# Submit Flink job
echo "📤 Submitting Flink job..."
docker cp flink-job/target/viewing-session-analyzer-1.0-SNAPSHOT.jar jobmanager:/tmp/
docker exec -d jobmanager flink run /tmp/viewing-session-analyzer-1.0-SNAPSHOT.jar
echo "✅ Flink job submitted!"
echo ""

# Start data generator
echo "📺 Starting data generator (5 users)..."
docker-compose up -d data-generator
echo "✅ Generator started!"
echo ""

echo "🎉 Everything is running!"
echo ""
echo "📊 VISUALIZATION DASHBOARD: http://localhost:5001"
echo "   ↑ Open this to see distributed processing in real-time!"
echo ""
echo "Other monitoring:"
echo "  • Flink UI: http://localhost:8081"
echo "  • TaskManager logs: docker logs -f taskmanager-1"
echo ""
echo "What to expect:"
echo "  • 5 users distributed across 3 TaskManagers"
echo "  • Two sessions per user (with ~40s gap between)"
echo "  • Total runtime: ~2 minutes"
echo ""
echo "To stop everything: docker-compose down"
