# StreamPulse — Real-Time Financial Intelligence Platform

> Event-driven financial analytics platform processing live stock ticks through Apache Kafka, with Python ML microservices for price forecasting and anomaly detection, SignalR WebSocket delivery to an Angular 21 dashboard, deployed on AWS EC2 with GitHub Actions CI/CD.

## Tech Stack
`Apache Kafka` `ASP.NET Core 10` `Angular 21 + PrimeNG Aura` `Python FastAPI` `TimescaleDB` `Redis` `SignalR WebSockets` `NewsAPI` `Docker` `AWS EC2` `GitHub Actions`

## Architecture
12 services across .NET, Python, and Angular communicating via 4 Kafka topics. See /docker/architecture.md for full service map.

## Services

| Service | Tech | Port | Description |
|---|---|---|---|
| Data Ingestion | Python FastAPI | 8001 | Simulates live stock ticks for 5 symbols → raw-ticks topic |
| Stream Processor | ASP.NET Core 10 Worker + API | 5100 | Aggregates OHLCV candles → ohlcv-aggregated topic + TimescaleDB; REST /candles endpoint |
| Anomaly Detection | Python FastAPI + Isolation Forest | 8003 | Scores ticks for anomalies → alerts topic |
| Price Forecasting | Python FastAPI + Keras LSTM | 8002 | Predicts next close price → ml-forecasts topic |
| News Sentiment | Python FastAPI + TF-IDF + LR | 8004 | NewsAPI real headlines + TF-IDF sentiment → sentiment_scores table |
| Alert Service | ASP.NET Core 10 Worker + API | 5200 | Consumes alerts topic → TimescaleDB + MailKit email |
| API Gateway | ASP.NET Core 10 + Ocelot + SignalR | 5000 | Ocelot proxy + SignalR WebSocket hub + Redis tick cache |
| Apache Kafka | Confluent 7.6 | 9092 | Central event backbone |
| TimescaleDB | PostgreSQL + Timescale | 5432 | Time-series OHLCV storage |
| Redis | Redis 7 | 6380 | Tick cache (60s TTL per symbol) |
| Kafka UI | provectuslabs | 8081 | Topic browser |
| Angular Dashboard | Angular 21 + PrimeNG Aura | 4200 | Live ticker, SignalR WebSocket client, flash animations |

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
- ✅ Day 8 — Alert Service (ASP.NET Core 10, MailKit, alert_records hypertable, REST query API) — HIGH severity anomaly alerts trigger real email notifications via Gmail SMTP (MailKit StartTls)
- ✅ Day 9 — API Gateway + SignalR Hub (Ocelot proxy, SignalR WebSocket hub, Redis tick cache, Kafka consumers for ml-forecasts + alerts + raw-ticks)
- ✅ Day 10 — End-to-End Integration Tests (NUnit 4 + FluentAssertions, full pipeline trace, Kafka topic validation, TimescaleDB queries, gateway routing + SignalR)
- ✅ Day 11 — Angular 21 Dashboard + Live Ticker (PrimeNG Aura theme, SignalR WebSocket client, live price cards for all 5 symbols with flash animations, reconnection badge)
- ✅ Day 12 — Live Candlestick Chart (Chart.js + chartjs-chart-financial, ML forecast dashed overlay, symbol selector, 1m/5m/15m timeframe aggregation, historical load + live append via SignalR)
- ✅ Day 13 — Alert Feed + Sentiment Panel (real-time side panel: scrolling anomaly alerts with severity colors + PrimeNG Toast for HIGH alerts, news sentiment score bars auto-refreshing every 30s)
- ✅ Day 14 — Dashboard Polish (dark/light theme toggle with CSS variable system, shimmer loading skeletons on ticker and chart, responsive layout for tablet and mobile, hover lift on ticker cards, fadeSlideIn on alerts)
- ✅ Day 15 — Prometheus + Grafana Monitoring (metrics endpoints on all 7 services, Prometheus scrapes every 15s, auto-provisioned Grafana dashboard with 7 panels: tick throughput, anomaly rate, forecast confidence, sentiment scores, connected clients, candles published, alert rate)

## Integration Tests

NUnit 4 test suite at `tests/integration/` traces the full data path end-to-end against a live Docker stack.

```bash
cd tests/integration/StreamPulse.Integration.Tests
dotnet test --logger "console;verbosity=normal"
```

| Test Class | Tests | What it verifies |
|---|---|---|
| `PipelineTraceTests` | 2 | New OHLCV rows + alerts written in 65 s; sentiment scores in [-1,1] for all 5 symbols |
| `KafkaIntegrationTests` | 4 | raw-ticks (Latest), ohlcv-aggregated, ml-forecasts, alerts (Earliest) topics have valid messages |
| `DatabaseIntegrationTests` | 4 | Hypertables exist; recent OHLCV rows; time_bucket(); per-symbol alert counts |
| `GatewayRoutingTests` | 5 | /health, /api/alerts, /api/sentiment/AAPL, /api/forecasts/AAPL, SignalR tick via JoinAllSymbols |

See [tests/integration/README.md](tests/integration/README.md) for prerequisites and troubleshooting.

GitHub Actions workflow at `.github/workflows/integration-test.yml` — manual trigger (`workflow_dispatch`) on a self-hosted runner with Docker and .NET 10.

## Monitoring
| Tool | URL | Credentials |
|---|---|---|
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / StreamPulse@2026 |

Prometheus scrapes all 7 StreamPulse services every 15s. Grafana auto-provisions the **StreamPulse Financial Intelligence** dashboard with tick throughput, anomaly rate, forecast confidence, sentiment scores, connected clients, candles published, and alert rate panels.

## Status
✅ Day 15 complete — full observability stack with Prometheus scraping all 7 services and Grafana auto-provisioned dashboard
