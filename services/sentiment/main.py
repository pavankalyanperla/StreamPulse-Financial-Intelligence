import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import httpx
import numpy as np
import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://streampulse:StreamPulse%402026@localhost:5432/streampulse_db",
)
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL,GOOGL,MSFT,INFY,TCS").split(",")]
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")
REFRESH_INTERVAL = 300


# ── Models ────────────────────────────────────────────────────────────────────

class NewsHeadline(BaseModel):
    symbol: str
    headline: str
    source: str
    published_at: str
    sentiment_score: float
    sentiment_label: str


class SymbolSentiment(BaseModel):
    symbol: str
    score: float
    label: str
    headline_count: int
    latest_headline: str
    latest_published: str
    last_updated: str


# ── Sentiment Classifier ──────────────────────────────────────────────────────

class SentimentClassifier:
    _TRAINING_DATA = [
        ("strong earnings beat", 1),
        ("revenue growth exceeded forecasts", 1),
        ("raised guidance for full year", 1),
        ("acquisition approved by regulators", 1),
        ("dividend increase announced", 1),
        ("market outperform rating maintained", 1),
        ("bullish outlook from analysts", 1),
        ("record profits reported this quarter", 1),
        ("share buyback program expanded", 1),
        ("upgraded to buy by analysts", 1),
        ("strong demand drives revenue", 1),
        ("beat expectations on earnings per share", 1),
        ("raised price target significantly", 1),
        ("positive outlook for next quarter", 1),
        ("strategic partnership announced", 1),
        ("new product launch drives growth", 1),
        ("cost cuts succeed improving margins", 1),
        ("debt reduced ahead of schedule", 1),
        ("margin expansion beats estimates", 1),
        ("analyst upgrade boosts shares", 1),
        ("quarterly results surpass estimates", 1),
        ("investors cheer strong guidance", 1),
        ("robust demand lifts forecast", 1),
        ("cash flow hits all-time high", 1),
        ("international expansion accelerates growth", 1),
        ("missed earnings expectations this quarter", 0),
        ("revenue decline reported year over year", 0),
        ("lowered guidance for next quarter", 0),
        ("lawsuit filed against company", 0),
        ("dividend cut announced", 0),
        ("bearish outlook from analysts", 0),
        ("profit warning issued to investors", 0),
        ("layoffs announced companywide", 0),
        ("debt downgrade by rating agency", 0),
        ("regulatory fine imposed on company", 0),
        ("class action lawsuit filed", 0),
        ("CEO resignation effective immediately", 0),
        ("missed expectations on revenue and profit", 0),
        ("lowered price target by analysts", 0),
        ("negative outlook issued for sector", 0),
        ("supply chain issues hurt margins", 0),
        ("margin compression weighs on earnings", 0),
        ("earnings miss disappoints investors", 0),
        ("analyst downgrade cuts target price", 0),
        ("market underperform rating assigned", 0),
        ("shares slide on weak results", 0),
        ("cash burn accelerates raising concerns", 0),
        ("guidance slashed amid uncertainty", 0),
        ("inventory buildup signals demand weakness", 0),
        ("write-down announced on failed acquisition", 0),
    ]

    def __init__(self) -> None:
        texts = [t for t, _ in self._TRAINING_DATA]
        labels = [l for _, l in self._TRAINING_DATA]
        self._pipeline = Pipeline([
            ("tfidf", TfidfVectorizer(ngram_range=(1, 2), max_features=500)),
            ("clf", LogisticRegression(max_iter=1000)),
        ])
        self._pipeline.fit(texts, labels)
        logger.info("[SENTIMENT] Classifier trained on %d examples", len(texts))

    def score(self, headline: str) -> tuple[float, str]:
        prob = float(self._pipeline.predict_proba([headline])[0][1])
        shifted = round((prob - 0.5) * 2, 4)
        if shifted > 0.1:
            label = "BULLISH"
        elif shifted < -0.1:
            label = "BEARISH"
        else:
            label = "NEUTRAL"
        return shifted, label


# ── News Fetcher ──────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _synthetic_headlines(symbol: str) -> list[dict]:
    return [
        {"headline": f"{symbol} trading within normal range", "source": "synthetic", "published_at": _now_iso()},
        {"headline": f"{symbol} market activity observed", "source": "synthetic", "published_at": _now_iso()},
        {"headline": f"Analysts watching {symbol} closely", "source": "synthetic", "published_at": _now_iso()},
    ]


def fetch_headlines(symbol: str) -> list[dict]:
    if not NEWSAPI_KEY:
        logger.warning("[SENTIMENT] %s — NEWSAPI_KEY not set, using synthetic fallback", symbol)
        return _synthetic_headlines(symbol)

    try:
        resp = httpx.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": symbol,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 5,
                "apiKey": NEWSAPI_KEY,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            raise ValueError(f"HTTP {resp.status_code}")

        articles = resp.json().get("articles", [])
        if not articles:
            raise ValueError("empty articles list")

        results = [
            {
                "headline": a.get("title") or f"{symbol} market update",
                "source": (a.get("source") or {}).get("name", "NewsAPI"),
                "published_at": a.get("publishedAt", _now_iso()),
            }
            for a in articles
        ]
        logger.info("[SENTIMENT] %s — fetched %d real headlines from NewsAPI", symbol, len(results))
        return results

    except Exception as exc:
        logger.warning("[SENTIMENT] %s — NewsAPI failed (%s), using synthetic fallback", symbol, exc)
        return _synthetic_headlines(symbol)


# ── Database ──────────────────────────────────────────────────────────────────

def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def init_db() -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sentiment_scores (
                    recorded_at  TIMESTAMPTZ  NOT NULL,
                    symbol       VARCHAR(10)  NOT NULL,
                    score        NUMERIC(6,4) NOT NULL,
                    label        VARCHAR(10)  NOT NULL,
                    headline     TEXT         NOT NULL,
                    source       VARCHAR(50)  NOT NULL,
                    PRIMARY KEY (recorded_at, symbol)
                );
            """)
            cur.execute("""
                SELECT create_hypertable(
                    'sentiment_scores', 'recorded_at', if_not_exists => TRUE
                );
            """)
        conn.commit()
    logger.info("[SENTIMENT] DB initialised — sentiment_scores hypertable ready")


def save_to_db(symbol: str, score: float, label: str, headline: str, source: str) -> None:
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO sentiment_scores (recorded_at, symbol, score, label, headline, source)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (recorded_at, symbol) DO NOTHING
                    """,
                    (datetime.now(timezone.utc), symbol, score, label, headline[:500], source[:50]),
                )
            conn.commit()
    except Exception as exc:
        logger.error("[SENTIMENT] DB save failed for %s: %s", symbol, exc)


def load_from_db(symbol: str) -> list[dict]:
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, score, label, headline, source, recorded_at
                    FROM sentiment_scores
                    WHERE symbol = %s
                    ORDER BY recorded_at DESC
                    LIMIT 10
                    """,
                    (symbol,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("[SENTIMENT] DB load failed for %s: %s", symbol, exc)
        return []


# ── Global state ──────────────────────────────────────────────────────────────

classifier: SentimentClassifier
sentiment_cache: dict[str, SymbolSentiment] = {}


# ── Core refresh logic ────────────────────────────────────────────────────────

def refresh_symbol(symbol: str) -> Optional[SymbolSentiment]:
    headlines = fetch_headlines(symbol)
    if not headlines:
        return None

    scores: list[float] = []
    latest_headline = ""
    latest_published = _now_iso()

    for item in headlines:
        sc, lbl = classifier.score(item["headline"])
        save_to_db(symbol, sc, lbl, item["headline"], item["source"])
        scores.append(sc)
        if not latest_headline:
            latest_headline = item["headline"]
            latest_published = item["published_at"]

    avg_score = round(float(np.mean(scores)), 4)
    if avg_score > 0.1:
        agg_label = "BULLISH"
    elif avg_score < -0.1:
        agg_label = "BEARISH"
    else:
        agg_label = "NEUTRAL"

    sentiment = SymbolSentiment(
        symbol=symbol,
        score=avg_score,
        label=agg_label,
        headline_count=len(scores),
        latest_headline=latest_headline,
        latest_published=latest_published,
        last_updated=_now_iso(),
    )
    sentiment_cache[symbol] = sentiment

    logger.info(
        '[SENTIMENT] %s | score: %+.4f | %s | "%s"',
        symbol, avg_score, agg_label, latest_headline[:60],
    )
    return sentiment


async def _background_refresh() -> None:
    while True:
        loop = asyncio.get_event_loop()
        for symbol in SYMBOLS:
            await loop.run_in_executor(None, refresh_symbol, symbol)
        await asyncio.sleep(REFRESH_INTERVAL)


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global classifier
    classifier = SentimentClassifier()
    init_db()
    asyncio.create_task(_background_refresh())
    yield


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="StreamPulse Sentiment Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "sentiment",
        "symbols_cached": list(sentiment_cache.keys()),
    }


@app.get("/sentiment/all")
def sentiment_all():
    return {sym: s.model_dump() for sym, s in sentiment_cache.items()}


@app.post("/sentiment/{symbol}/refresh")
def sentiment_refresh(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not tracked")
    result = refresh_symbol(symbol)
    if result is None:
        raise HTTPException(status_code=500, detail="Refresh failed")
    return result.model_dump()


@app.get("/sentiment/{symbol}/headlines")
def sentiment_headlines(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not tracked")
    rows = load_from_db(symbol)
    return [
        NewsHeadline(
            symbol=r["symbol"],
            headline=r["headline"],
            source=r["source"],
            published_at=str(r["recorded_at"]),
            sentiment_score=float(r["score"]),
            sentiment_label=r["label"],
        ).model_dump()
        for r in rows
    ]


@app.get("/sentiment/{symbol}")
def sentiment_symbol(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not tracked")
    if symbol in sentiment_cache:
        return sentiment_cache[symbol].model_dump()
    rows = load_from_db(symbol)
    if not rows:
        raise HTTPException(status_code=404, detail="No sentiment data yet — refresh in progress")
    scores = [float(r["score"]) for r in rows]
    avg = round(float(np.mean(scores)), 4)
    label = "BULLISH" if avg > 0.1 else ("BEARISH" if avg < -0.1 else "NEUTRAL")
    s = SymbolSentiment(
        symbol=symbol,
        score=avg,
        label=label,
        headline_count=len(rows),
        latest_headline=rows[0]["headline"],
        latest_published=str(rows[0]["recorded_at"]),
        last_updated=_now_iso(),
    )
    sentiment_cache[symbol] = s
    return s.model_dump()
