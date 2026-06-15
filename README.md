# StreamPulse — Real-Time Financial Intelligence Platform

> Event-driven financial analytics platform processing live stock ticks through Apache Kafka, with Python ML microservices for price forecasting and anomaly detection, SignalR WebSocket delivery to an Angular 21 dashboard, deployed on AWS EC2 with GitHub Actions CI/CD.

## Tech Stack
`Apache Kafka` `ASP.NET Core 10` `Angular 21 + PrimeNG Aura` `Python FastAPI` `TimescaleDB` `Redis` `SignalR WebSockets` `Docker` `AWS EC2` `GitHub Actions`

## Architecture
12 services across .NET, Python, and Angular communicating via 4 Kafka topics. See /docker/architecture.md for full service map.

## Services

| Service | Tech | Port | Description |
|---|---|---|---|
| Data Ingestion | Python FastAPI | 8001 | Simulates live stock ticks for 5 symbols → raw-ticks topic |
| Stream Processor | ASP.NET Core 10 Worker | 5100 | Aggregates OHLCV candles → ohlcv-aggregated topic + TimescaleDB |
| Anomaly Detection | Python FastAPI + Isolation Forest | 8003 | Scores ticks for anomalies → alerts topic |
| Apache Kafka | Confluent 7.6 | 9092 | Central event backbone |
| TimescaleDB | PostgreSQL + Timescale | 5432 | Time-series OHLCV storage |
| Redis | Redis 7 | 6380 | Cache (Day 6+) |
| Kafka UI | provectuslabs | 8081 | Topic browser |

## Database
TimescaleDB stores all OHLCV candle data in the `ohlcv_candles` hypertable, partitioned automatically by `candle_time`. Supports time-series queries via `time_bucket()`.

| Table | Type | Partition Key | Description |
|---|---|---|---|
| ohlcv_candles | Hypertable | candle_time | 1-min OHLCV candles per symbol |

## Build Progress
- ✅ Day 1 — Infrastructure (Zookeeper, Kafka, TimescaleDB, Redis, Kafka UI)
- ✅ Day 2 — Data Ingestion Service (Python FastAPI, 5 symbols, raw-ticks topic)
- ✅ Day 3 — Stream Processor (ASP.NET Core 10 Worker, OHLCV aggregation, ohlcv-aggregated topic)
- ✅ Day 4 — TimescaleDB Persistence (Npgsql, ohlcv_candles hypertable, upsert on flush)
- ✅ Day 5 — Anomaly Detection Service (Python FastAPI + Isolation Forest, alerts topic)

## Status
🚧 Day 5 complete — Anomaly detection scoring live ticks via Isolation Forest
