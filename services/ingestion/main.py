import asyncio
import json
import logging
import os
import random
from datetime import datetime, timezone

from fastapi import FastAPI
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Gauge, make_asgi_app
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SYMBOLS_ENV = os.getenv("SYMBOLS", "AAPL,GOOGL,MSFT,INFY,TCS")
SYMBOLS = [s.strip() for s in SYMBOLS_ENV.split(",")]
TOPIC = "raw-ticks"

BASE_PRICES: dict[str, float] = {
    "AAPL": 180.0,
    "GOOGL": 140.0,
    "MSFT": 420.0,
    "INFY": 18.0,
    "TCS": 22.0,
}
last_prices: dict[str, float] = dict(BASE_PRICES)

producer: KafkaProducer | None = None

# ── Prometheus metrics ────────────────────────────────────────────────────────
TICKS_PUBLISHED = Counter(
    "streampulse_ticks_published_total",
    "Total ticks published to Kafka",
    ["symbol"],
)
CURRENT_PRICE = Gauge(
    "streampulse_current_price",
    "Current price per symbol",
    ["symbol"],
)


class Tick(BaseModel):
    symbol: str
    price: float
    volume: int
    timestamp: str
    change_pct: float


def create_producer() -> KafkaProducer | None:
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return p
    except KafkaError as exc:
        logger.error(f"Kafka connection failed: {exc} — ticks will not be published")
        return None


def generate_tick(symbol: str) -> Tick:
    old_price = last_prices[symbol]
    delta = random.uniform(-0.002, 0.002)
    new_price = round(old_price * (1 + delta), 2)
    change_pct = round((new_price - old_price) / old_price * 100, 4)
    last_prices[symbol] = new_price
    return Tick(
        symbol=symbol,
        price=new_price,
        volume=random.randint(100, 10000),
        timestamp=datetime.now(timezone.utc).isoformat(),
        change_pct=change_pct,
    )


def publish_tick(tick: Tick) -> None:
    if producer is None:
        return
    try:
        producer.send(TOPIC, key=tick.symbol, value=tick.model_dump())
    except KafkaError as exc:
        logger.error(f"Failed to publish tick for {tick.symbol}: {exc}")


async def tick_loop() -> None:
    while True:
        for symbol in SYMBOLS:
            tick = generate_tick(symbol)
            publish_tick(tick)
            TICKS_PUBLISHED.labels(symbol=symbol).inc()
            CURRENT_PRICE.labels(symbol=symbol).set(tick.price)
            sign = "+" if tick.change_pct >= 0 else ""
            logger.info(
                f"[TICK] {tick.symbol:<5} | ${tick.price:<9} | vol: {tick.volume:<5} | {sign}{tick.change_pct}%"
            )
        if producer:
            producer.flush()
        await asyncio.sleep(1)


app = FastAPI(title="StreamPulse Ingestion Service")

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.on_event("startup")
async def startup() -> None:
    global producer
    producer = create_producer()
    asyncio.create_task(tick_loop())


@app.get("/health")
def health():
    return {"status": "ok", "service": "ingestion", "symbols": SYMBOLS}


@app.get("/ticks/latest")
def ticks_latest():
    return {symbol: last_prices[symbol] for symbol in SYMBOLS}
