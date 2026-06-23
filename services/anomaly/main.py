import asyncio
import json
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import numpy as np
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Gauge, make_asgi_app
from pydantic import BaseModel
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_TOPIC = "raw-ticks"
PRODUCER_TOPIC = "alerts"
CONSUMER_GROUP = "anomaly-group"
BUFFER_SIZE = 100
MIN_TRAIN_TICKS = 20
RETRAIN_EVERY = 50

# ── Prometheus metrics ────────────────────────────────────────────────────────
TICKS_SCORED = Counter(
    "streampulse_ticks_scored_total",
    "Total ticks scored",
    ["symbol"],
)
ANOMALIES_DETECTED = Counter(
    "streampulse_anomalies_detected_total",
    "Total anomalies detected",
    ["symbol", "severity"],
)
MODELS_TRAINED = Gauge(
    "streampulse_models_trained",
    "Model trained flag",
    ["symbol"],
)


# ── Models ──────────────────────────────────────────────────────────────────

class StockTick(BaseModel):
    symbol: str
    price: float
    volume: int
    timestamp: str
    change_pct: float


class AnomalyAlert(BaseModel):
    symbol: str
    alert_type: str
    severity: str
    price: float
    volume: int
    change_pct: float
    timestamp: str
    anomaly_score: float


# ── Per-symbol state ─────────────────────────────────────────────────────────

class SymbolState:
    def __init__(self) -> None:
        self.buffer: deque[tuple[float, int, float]] = deque(maxlen=BUFFER_SIZE)
        self.model: IsolationForest | None = None
        self.is_trained: bool = False
        self.ticks_since_train: int = 0
        self.total_anomalies: int = 0


# ── Anomaly Detector ─────────────────────────────────────────────────────────

class AnomalyDetector:
    def __init__(self) -> None:
        self._states: dict[str, SymbolState] = {}

    def _state(self, symbol: str) -> SymbolState:
        if symbol not in self._states:
            self._states[symbol] = SymbolState()
        return self._states[symbol]

    def _train(self, symbol: str, state: SymbolState) -> None:
        X = np.array(list(state.buffer))
        state.model = IsolationForest(
            contamination=0.05, random_state=42, n_estimators=100
        )
        state.model.fit(X)
        state.is_trained = True
        state.ticks_since_train = 0
        MODELS_TRAINED.labels(symbol=symbol).set(1.0)
        logger.info("[MODEL] %s Isolation Forest trained on %d ticks", symbol, len(state.buffer))

    def detect(self, tick: StockTick) -> AnomalyAlert | None:
        state = self._state(tick.symbol)
        state.buffer.append((tick.price, tick.volume, tick.change_pct))
        state.ticks_since_train += 1

        n = len(state.buffer)

        if n < MIN_TRAIN_TICKS:
            return None

        if not state.is_trained:
            self._train(tick.symbol, state)
        elif state.ticks_since_train >= RETRAIN_EVERY:
            self._train(tick.symbol, state)

        X_current = np.array([[tick.price, tick.volume, tick.change_pct]])
        prediction = state.model.predict(X_current)[0]
        score = float(state.model.decision_function(X_current)[0])

        if prediction != -1:
            return None

        # Classify alert type
        avg_volume = float(np.mean([b[1] for b in state.buffer]))
        price_spike = abs(tick.change_pct) > 0.5
        volume_spike = tick.volume > avg_volume * 2

        if price_spike and volume_spike:
            alert_type = "COMBINED"
        elif price_spike:
            alert_type = "PRICE_SPIKE"
        elif volume_spike:
            alert_type = "VOLUME_SPIKE"
        else:
            alert_type = "PRICE_SPIKE"

        # Severity from score (more negative = more anomalous)
        if score < -0.15:
            severity = "HIGH"
        elif score < -0.08:
            severity = "MEDIUM"
        else:
            severity = "LOW"

        state.total_anomalies += 1

        return AnomalyAlert(
            symbol=tick.symbol,
            alert_type=alert_type,
            severity=severity,
            price=tick.price,
            volume=tick.volume,
            change_pct=tick.change_pct,
            timestamp=tick.timestamp,
            anomaly_score=round(score, 6),
        )

    def health(self) -> dict[str, bool]:
        return {sym: st.is_trained for sym, st in self._states.items()}

    def stats(self) -> dict[str, dict[str, Any]]:
        return {
            sym: {
                "buffer_size": len(st.buffer),
                "is_trained": st.is_trained,
                "total_anomalies_detected": st.total_anomalies,
            }
            for sym, st in self._states.items()
        }

    def latest_price(self, symbol: str) -> float | None:
        state = self._states.get(symbol)
        if state and state.buffer:
            return state.buffer[-1][0]
        return None


# ── Global state ──────────────────────────────────────────────────────────────

detector = AnomalyDetector()
producer: KafkaProducer | None = None


def _make_producer() -> KafkaProducer | None:
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode(),
        )
        logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
        return p
    except KafkaError as exc:
        logger.error("Kafka producer failed: %s", exc)
        return None


def _publish_alert(alert: AnomalyAlert) -> None:
    if producer is None:
        return
    try:
        producer.send(PRODUCER_TOPIC, key=alert.symbol, value=alert.model_dump())
        producer.flush()
    except KafkaError as exc:
        logger.error("Failed to publish alert for %s: %s", alert.symbol, exc)


async def consume_and_detect() -> None:
    loop = asyncio.get_event_loop()

    def _blocking_consume() -> None:
        consumer = KafkaConsumer(
            CONSUMER_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode()),
            consumer_timeout_ms=float("inf"),
        )
        logger.info("Kafka consumer subscribed to %s", CONSUMER_TOPIC)

        for msg in consumer:
            try:
                tick = StockTick(**msg.value)
                TICKS_SCORED.labels(symbol=tick.symbol).inc()
                alert = detector.detect(tick)
                if alert:
                    ANOMALIES_DETECTED.labels(symbol=alert.symbol, severity=alert.severity).inc()
                    _publish_alert(alert)
                    logger.info(
                        "[ALERT] %s %s — %s | $%.2f | %+.4f%% | score: %.3f",
                        alert.severity, alert.alert_type,
                        alert.symbol, alert.price, alert.change_pct, alert.anomaly_score,
                    )
            except Exception as exc:
                logger.error("Error processing message: %s", exc)

    await loop.run_in_executor(None, _blocking_consume)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = _make_producer()
    asyncio.create_task(consume_and_detect())
    yield
    if producer:
        producer.close()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="StreamPulse Anomaly Detection Service", lifespan=lifespan)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "anomaly-detection",
        "models_trained": detector.health(),
    }


@app.get("/stats")
def stats():
    return detector.stats()


@app.post("/test/anomaly/{symbol}")
def test_anomaly(symbol: str):
    current_price = detector.latest_price(symbol) or 180.0
    fake_tick = StockTick(
        symbol=symbol,
        price=round(current_price * 1.05, 2),
        volume=999999,
        change_pct=5.0,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    alert = detector.detect(fake_tick)
    if alert is None:
        return {"message": "no anomaly detected (model not trained yet)"}
    _publish_alert(alert)
    logger.info(
        "[ALERT] %s %s — %s | $%.2f | %+.4f%% | score: %.3f",
        alert.severity, alert.alert_type,
        alert.symbol, alert.price, alert.change_pct, alert.anomaly_score,
    )
    return alert.model_dump()
