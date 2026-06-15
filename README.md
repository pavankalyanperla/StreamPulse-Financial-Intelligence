# StreamPulse — Real-Time Financial Intelligence Platform

> Event-driven financial analytics platform processing live stock ticks through Apache Kafka, with Python ML microservices for price forecasting and anomaly detection, SignalR WebSocket delivery to an Angular 21 dashboard, deployed on AWS EC2 with GitHub Actions CI/CD.

## Tech Stack
`Apache Kafka` `ASP.NET Core 10` `Angular 21 + PrimeNG Aura` `Python FastAPI` `TimescaleDB` `Redis` `SignalR WebSockets` `Docker` `AWS EC2` `GitHub Actions`

## Architecture
12 services across .NET, Python, and Angular communicating via 4 Kafka topics. See /docker/architecture.md for full service map.

## Database
TimescaleDB (PostgreSQL + Timescale extension) stores all OHLCV candle data in the `ohlcv_candles` hypertable, partitioned automatically by `candle_time`. Supports time-series queries via `time_bucket()`.

| Table | Type | Partition Key | Description |
|---|---|---|---|
| ohlcv_candles | Hypertable | candle_time | 1-min OHLCV candles per symbol |

## Build Progress
- ✅ Day 1 — Infrastructure (Zookeeper, Kafka, TimescaleDB, Redis, Kafka UI)
- ✅ Day 2 — Data Ingestion Service (Python FastAPI, 5 symbols, raw-ticks topic)
- ✅ Day 3 — Stream Processor (ASP.NET Core 10 Worker, OHLCV aggregation, ohlcv-aggregated topic)
- ✅ Day 4 — TimescaleDB Persistence (Npgsql, ohlcv_candles hypertable, upsert on flush)

## Status
🚧 Day 4 complete — OHLCV candles persisting to TimescaleDB
