export interface LiveTick {
  symbol: string;
  price: number;
  volume: number;
  changePct: number;
  timestamp: string;
}

export interface OhlcvCandle {
  symbol: string;
  openPrice: number;
  highPrice: number;
  lowPrice: number;
  closePrice: number;
  totalVolume: number;
  candleTime: string;
  tickCount: number;
}

export interface PriceForecast {
  symbol: string;
  predictedClose: number;
  currentClose: number;
  confidence: number;
  direction: 'UP' | 'DOWN' | 'FLAT';
  candleTime: string;
  predictedFor: string;
  modelTrained: boolean;
}

export interface AnomalyAlert {
  symbol: string;
  alertType: string;
  severity: 'LOW' | 'MEDIUM' | 'HIGH';
  price: number;
  volume: number;
  changePct: number;
  timestamp: string;
  anomalyScore: number;
}

export interface SymbolSentiment {
  symbol: string;
  score: number;
  label: string;
  headlineCount: number;
  latestHeadline: string;
  lastUpdated: string;
}

// Extends LiveTick with previousPrice for change-direction animation
export interface TickState extends LiveTick {
  previousPrice: number;
}
