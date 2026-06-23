import asyncio
import json
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import psycopg2
from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Gauge, make_asgi_app
from pydantic import BaseModel, ConfigDict, Field
from sklearn.preprocessing import MinMaxScaler

# Suppress TF noise before import
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
os.environ.setdefault("TF_ENABLE_ONEDNN_OPTS", "0")

import tensorflow as tf  # noqa: E402
from tensorflow.keras.layers import Dense, LSTM  # noqa: E402
from tensorflow.keras.models import Sequential  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://streampulse:StreamPulse%402026@localhost:5432/streampulse_db",
)
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL,GOOGL,MSFT,INFY,TCS").split(",")]
CONSUMER_TOPIC = "ohlcv-aggregated"
PRODUCER_TOPIC = "ml-forecasts"
CONSUMER_GROUP = "forecasting-group"
SEQUENCE_LENGTH = 10
MIN_TRAIN_CANDLES = 15
BUFFER_MAXLEN = 50

# ── Prometheus metrics ────────────────────────────────────────────────────────
FORECASTS_PUBLISHED = Counter(
    "streampulse_forecasts_published_total",
    "Total forecasts published",
    ["symbol"],
)
FORECAST_CONFIDENCE = Gauge(
    "streampulse_forecast_confidence",
    "Forecast confidence per symbol",
    ["symbol"],
)


# ── Models ────────────────────────────────────────────────────────────────────

class OhlcvCandle(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    symbol: str
    open_price: float = Field(alias="openPrice")
    high_price: float = Field(alias="highPrice")
    low_price: float = Field(alias="lowPrice")
    close_price: float = Field(alias="closePrice")
    total_volume: int = Field(alias="totalVolume")
    candle_time: str = Field(alias="candleTime")
    tick_count: int = Field(alias="tickCount")


class PriceForecast(BaseModel):
    symbol: str
    predicted_close: float
    current_close: float
    confidence: float
    direction: str
    candle_time: str
    predicted_for: str
    model_trained: bool


# ── LSTM Forecaster ───────────────────────────────────────────────────────────

class LSTMForecaster:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.model: Sequential | None = None
        self.buffer: deque[float] = deque(maxlen=BUFFER_MAXLEN)
        self.is_trained = False
        self.confidence = 0.0

    def _build_model(self) -> Sequential:
        model = Sequential([
            LSTM(32, input_shape=(SEQUENCE_LENGTH, 1), return_sequences=False),
            Dense(16, activation="relu"),
            Dense(1),
        ])
        model.compile(optimizer="adam", loss="mse")
        return model

    def train(self, candles: list[dict]) -> bool:
        closes = [float(c["close_price"]) for c in candles]
        if len(closes) < MIN_TRAIN_CANDLES:
            return False

        arr = np.array(closes).reshape(-1, 1)
        scaled = self.scaler.fit_transform(arr).flatten()

        X, y = [], []
        for i in range(len(scaled) - SEQUENCE_LENGTH):
            X.append(scaled[i: i + SEQUENCE_LENGTH])
            y.append(scaled[i + SEQUENCE_LENGTH])

        if len(X) == 0:
            return False

        X = np.array(X).reshape(-1, SEQUENCE_LENGTH, 1)
        y = np.array(y)

        self.model = self._build_model()
        history = self.model.fit(
            X, y,
            epochs=20,
            batch_size=8,
            verbose=0,
            validation_split=0.1,
        )

        val_loss = history.history.get("val_loss", [1.0])[-1]
        self.confidence = round(max(0.0, 1.0 - min(float(val_loss) * 10, 1.0)), 4)
        self.is_trained = True

        logger.info(
            "[LSTM] %s model trained on %d candles, confidence: %.2f",
            self.symbol, len(candles), self.confidence,
        )
        return True

    def predict(self, candle: OhlcvCandle) -> PriceForecast:
        self.buffer.append(candle.close_price)

        try:
            next_minute = (
                datetime.fromisoformat(candle.candle_time.replace("Z", "+00:00"))
                + timedelta(minutes=1)
            ).isoformat()
        except Exception:
            next_minute = candle.candle_time

        flat = PriceForecast(
            symbol=candle.symbol,
            predicted_close=round(candle.close_price, 4),
            current_close=round(candle.close_price, 4),
            confidence=self.confidence,
            direction="FLAT",
            candle_time=candle.candle_time,
            predicted_for=next_minute,
            model_trained=self.is_trained,
        )

        if not self.is_trained or len(self.buffer) < SEQUENCE_LENGTH:
            return flat

        seq = np.array(list(self.buffer)[-SEQUENCE_LENGTH:]).reshape(-1, 1)
        seq_scaled = self.scaler.transform(seq).reshape(1, SEQUENCE_LENGTH, 1)

        raw = self.model.predict(seq_scaled, verbose=0)[0][0]
        predicted = float(self.scaler.inverse_transform([[raw]])[0][0])
        predicted = round(predicted, 4)
        current = candle.close_price

        if predicted > current * 1.001:
            direction = "UP"
        elif predicted < current * 0.999:
            direction = "DOWN"
        else:
            direction = "FLAT"

        return PriceForecast(
            symbol=candle.symbol,
            predicted_close=predicted,
            current_close=round(current, 4),
            confidence=self.confidence,
            direction=direction,
            candle_time=candle.candle_time,
            predicted_for=next_minute,
            model_trained=True,
        )


# ── Database loader ───────────────────────────────────────────────────────────

def load_candles(symbol: str) -> list[dict]:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, open_price, high_price, low_price, close_price,
                   total_volume, candle_time, tick_count
            FROM ohlcv_candles
            WHERE symbol = %s
            ORDER BY candle_time ASC
            """,
            (symbol,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        logger.info("[DB] Loaded %d candles for %s from TimescaleDB", len(rows), symbol)
        return rows
    except Exception as exc:
        logger.error("[DB] Failed to load candles for %s: %s", symbol, exc)
        return []


# ── Global state ──────────────────────────────────────────────────────────────

forecasters: dict[str, LSTMForecaster] = {sym: LSTMForecaster(sym) for sym in SYMBOLS}
latest_forecasts: dict[str, PriceForecast] = {}
producer: KafkaProducer | None = None


def _make_producer() -> KafkaProducer | None:
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode(),
        )
        logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP)
        return p
    except KafkaError as exc:
        logger.error("Kafka producer failed: %s", exc)
        return None


def _publish_forecast(forecast: PriceForecast) -> None:
    if producer is None:
        return
    try:
        producer.send(PRODUCER_TOPIC, key=forecast.symbol, value=forecast.model_dump())
        producer.flush()
        FORECASTS_PUBLISHED.labels(symbol=forecast.symbol).inc()
        FORECAST_CONFIDENCE.labels(symbol=forecast.symbol).set(forecast.confidence)
    except KafkaError as exc:
        logger.error("Failed to publish forecast for %s: %s", forecast.symbol, exc)


async def consume_and_forecast() -> None:
    loop = asyncio.get_event_loop()

    def _blocking_consume() -> None:
        consumer = KafkaConsumer(
            CONSUMER_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode()),
            consumer_timeout_ms=float("inf"),
        )
        logger.info("Kafka consumer subscribed to %s", CONSUMER_TOPIC)

        for msg in consumer:
            try:
                candle = OhlcvCandle(**msg.value)
                fc = forecasters[candle.symbol]

                # Trigger training on arrival if not yet trained and enough data
                if not fc.is_trained and len(fc.buffer) >= MIN_TRAIN_CANDLES:
                    rows = [{"close_price": p} for p in list(fc.buffer)]
                    fc.train(rows)

                forecast = fc.predict(candle)
                latest_forecasts[candle.symbol] = forecast

                if forecast.model_trained:
                    _publish_forecast(forecast)
                    logger.info(
                        "[FORECAST] %s | current: $%.4f | predicted: $%.4f | %s | confidence: %.2f",
                        forecast.symbol, forecast.current_close,
                        forecast.predicted_close, forecast.direction, forecast.confidence,
                    )
            except Exception as exc:
                logger.error("Error processing candle: %s", exc)

    await loop.run_in_executor(None, _blocking_consume)


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    logger.info("[LSTM] Starting forecasting service, loading historical data...")

    for symbol in SYMBOLS:
        fc = forecasters[symbol]
        candles = load_candles(symbol)
        for c in candles:
            fc.buffer.append(float(c["close_price"]))

        if len(candles) >= MIN_TRAIN_CANDLES:
            fc.train(candles)
        else:
            logger.info(
                "[LSTM] %s — insufficient data (%d candles), will train as data arrives",
                symbol, len(candles),
            )

    producer = _make_producer()
    asyncio.create_task(consume_and_forecast())
    yield
    if producer:
        producer.close()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="StreamPulse Forecasting Service", lifespan=lifespan)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "forecasting",
        "models": {
            sym: {
                "trained": fc.is_trained,
                "confidence": fc.confidence,
                "buffer_size": len(fc.buffer),
            }
            for sym, fc in forecasters.items()
        },
    }


@app.get("/forecast/all")
def forecast_all():
    return {sym: f.model_dump() for sym, f in latest_forecasts.items()}


@app.get("/forecast/{symbol}")
def forecast_symbol(symbol: str):
    symbol = symbol.upper()
    if symbol not in forecasters:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not tracked")
    if symbol not in latest_forecasts:
        raise HTTPException(status_code=404, detail="No forecast yet — waiting for next candle")
    return latest_forecasts[symbol].model_dump()
