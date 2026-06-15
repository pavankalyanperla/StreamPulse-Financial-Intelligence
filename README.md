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
| Price Forecasting | Python FastAPI + Keras LSTM | 8002 | Predicts next close price → ml-forecasts topic |
| News Sentiment | Python FastAPI + TF-IDF + LR | 8004 | RSS news sentiment per symbol → sentiment_scores table |
| Alert Service | ASP.NET Core 10 Worker + API | 5200 | Consumes alerts topic → TimescaleDB + MailKit email |
| Apache Kafka | Confluent 7.6 | 9092 | Central event backbone |
| TimescaleDB | PostgreSQL + Timescale | 5432 | Time-series OHLCV storage |
| Redis | Redis 7 | 6380 | Cache (Day 7+) |
| Kafka UI | provectuslabs | 8081 | Topic browser |

## Database
TimescaleDB stores all OHLCV candle data in the `ohlcv_candles` hypertable, partitioned automatically by `candle_time`. Supports time-series queries via `time_bucket()`.

| Table | Type | Partition Key | Description |
|---|---|---|---|
| ohlcv_candles | Hypertable | candle_time | 1-min OHLCV candles per symbol |

## Kafka Topics

| Topic | Producer | Consumers | Data |
|---|---|---|---|
| raw-ticks | Ingestion | Stream Processor, Anomaly Detection | Raw price tick per symbol |
| ohlcv-aggregated | Stream Processor | Forecasting, Gateway (future) | 1-min OHLCV candles |
| ml-forecasts | Forecasting | Gateway (future) | LSTM next-close predictions |
| alerts | Anomaly Detection | Alert Service, Gateway (future) | Anomaly type + severity |

## Build Progress
- ✅ Day 1 — Infrastructure (Zookeeper, Kafka, TimescaleDB, Redis, Kafka UI)
- ✅ Day 2 — Data Ingestion Service (Python FastAPI, 5 symbols, raw-ticks topic)
- ✅ Day 3 — Stream Processor (ASP.NET Core 10 Worker, OHLCV aggregation, ohlcv-aggregated topic)
- ✅ Day 4 — TimescaleDB Persistence (Npgsql, ohlcv_candles hypertable, upsert on flush)
- ✅ Day 5 — Anomaly Detection Service (Python FastAPI + Isolation Forest, alerts topic)
- ✅ Day 6 — LSTM Price Forecasting Service (Keras LSTM, ml-forecasts topic, historical warm-start)
- ✅ Day 7 — News Sentiment Service (TF-IDF + Logistic Regression, Yahoo Finance RSS, sentiment_scores hypertable)
- ✅ Day 8 — Alert Service (ASP.NET Core 10, MailKit, alert_records hypertable, REST query API)

## Status
🚧 Day 8 complete — Alert Service consuming Kafka alerts topic, persisting to TimescaleDB, optional MailKit email for HIGH alerts
