# EmoStream 
**Real-time Emoji Broadcast for Live Events**

EmoStream is a high-concurrency, event-driven system that captures, processes, and broadcasts **billions of emoji reactions** in real time during live sporting events.

---

## Overview
- Collects emoji reactions from millions of users.
- Processes them using **Apache Kafka** + **Apache Spark Streaming** (2s micro-batches).
- Aggregates duplicates (1000+ identical emojis → 1 in swarm).
- Broadcasts to all clients via a **Pub-Sub delivery system** with low latency.

---

## Architecture

### Data Ingestion
Clients → API (Flask/Express) → Kafka Producer (0.5s flush).
![Stage 1](Application%20Architecture.png)

---

### Real-Time Processing
Spark Streaming → Aggregates emoji counts → Pushes results back to Kafka.
![Spark Streaming](spark%20streaming.png)

---

### Broadcasting
Main Publisher → Cluster Publishers → Subscribers → Clients.
![Pub-Sub](pub%20sub%20model.png)

---

## Tech Stack
- **Backend API:** Flask / Express.js
- **Streaming:** Apache Kafka
- **Processing:** Apache Spark Streaming
- **Messaging:** Kafka / RabbitMQ (Pub-Sub)

---

## Features
- Handles **billions of concurrent emoji events**.
- Ultra-low latency (< 2s end-to-end).
- Auto-scaling, fault-tolerant architecture.
- UI-friendly aggregation algorithm.

---
