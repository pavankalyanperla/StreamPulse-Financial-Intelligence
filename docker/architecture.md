# StreamPulse — Architecture Reference

## Service Map

| Service | Tech | Port | Role |
|---|---|---|---|
| Data Ingestion | Python FastAPI | 8001 | Fetches stock ticks, publishes to raw-ticks Kafka topic |
| Stream Processor | ASP.NET Core 10 | 5100 | Consumes raw-ticks, aggregates OHLCV candles, writes to TimescaleDB |
| Forecasting Service | Python FastAPI + Keras LSTM | 8002 | Predicts next price, publishes to ml-forecasts topic |
| Anomaly Detection | Python FastAPI + Isolation Forest | 8003 | Detects price/volume spikes, publishes to alerts topic |
| Sentiment Service | Python FastAPI + TF-IDF | 8004 | News sentiment scoring per ticker |
| Alert Service | ASP.NET Core 10 | 5200 | Consumes alerts, persists to DB, sends email for HIGH severity |
| API Gateway + SignalR Hub | ASP.NET Core 10 + Ocelot | 5000 | Routes requests + broadcasts live data via WebSockets |
| Angular Dashboard | Angular 21 + PrimeNG Aura | 4200 | Live candlestick charts, alert feed, ML prediction panel |
| Apache Kafka | Kafka + Zookeeper | 9092 | Central event backbone |
| TimescaleDB | PostgreSQL + Timescale | 5432 | Time-series stock data storage |
| Redis | Redis 7 | 6379 | Latest tick cache, WebSocket state |
| Kafka UI | provectuslabs/kafka-ui | 8080 | Visual Kafka topic browser (dev only) |

## Kafka Topics

| Topic | Producer | Consumers | Data |
|---|---|---|---|
| raw-ticks | Ingestion Service | Stream Processor, Anomaly Detector | Raw price + volume per tick |
| ohlcv-aggregated | Stream Processor | Forecasting Service, Gateway | 1-min OHLCV candles |
| ml-forecasts | Forecasting Service | Gateway | Predicted next price + confidence score |
| alerts | Anomaly Service | Alert Service, Gateway | Anomaly type, severity, ticker symbol |
