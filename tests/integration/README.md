# StreamPulse Integration Tests

End-to-end tests that verify the full live pipeline — from raw-tick ingestion through Kafka,
OHLCV aggregation, ML forecasting, anomaly detection, and gateway routing.

## Prerequisites

All 12 Docker containers must be running before executing tests:

```
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected containers:
- streampulse-zookeeper, streampulse-kafka, streampulse-kafka-ui
- streampulse-timescaledb, streampulse-redis
- streampulse-ingestion, streampulse-processor, streampulse-anomaly
- streampulse-forecasting, streampulse-sentiment, streampulse-alert
- streampulse-gateway

Start all services if not running:

```
docker compose up -d
```

## Running the Tests

```bash
# From the repo root
cd tests/integration/StreamPulse.Integration.Tests
dotnet test --logger "console;verbosity=normal"
```

To run a single test class:

```bash
dotnet test --filter "FullyQualifiedName~GatewayRoutingTests"
```

## Test Classes

| Class | Tests | Timeout | Purpose |
|-------|-------|---------|---------|
| `PipelineTraceTests` | 2 | 90 s / 15 s | Wait 65 s; assert new OHLCV+alert rows written; verify sentiment for all 5 symbols |
| `KafkaIntegrationTests` | 4 | 30 s each | Consume raw-ticks (Latest), ohlcv-aggregated, ml-forecasts, alerts (Earliest) |
| `DatabaseIntegrationTests` | 4 | 15 s each | Hypertables exist; recent OHLCV rows; time_bucket(); alert stats per symbol |
| `GatewayRoutingTests` | 5 | 15–30 s | /health, /api/alerts, /api/sentiment/AAPL, /api/forecasts/AAPL, SignalR tick |

## Connection Constants

| Service | Address |
|---------|---------|
| TimescaleDB | `localhost:5432` / db `streampulse_db` |
| Kafka | `localhost:9092` |
| Gateway | `http://localhost:5000` |
| SignalR Hub | `http://localhost:5000/hubs/stock` |

## Troubleshooting

**Tests time out waiting for Kafka messages**
- Restart ingestion if it lost connection after a `docker compose up`: `docker restart streampulse-ingestion`

**Kafka partition conflict** — if you run the gateway or alert service locally AND in Docker, both consumers share the same consumer group and fight for the partition.
- Stop the Docker container before running locally: `docker stop streampulse-alert`

**SignalR test fails to receive ticks**
- Confirm the gateway's KafkaTickConsumer is streaming: `docker logs streampulse-gateway | Select-String "Broadcasting"`

**PowerShell pipe adds BOM to Kafka test messages**
- Use bash `echo '...' | docker exec -i streampulse-kafka kafka-console-producer` instead of PowerShell pipe when injecting test messages manually.
