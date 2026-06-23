export interface CandlestickDataPoint {
  x: number; // timestamp ms
  o: number; // open
  h: number; // high
  l: number; // low
  c: number; // close
}

export interface ChartTimeframe {
  label: string;
  minutes: number;
  maxCandles: number;
}

export const TIMEFRAMES: ChartTimeframe[] = [
  { label: '1m',  minutes: 1,  maxCandles: 60 },
  { label: '5m',  minutes: 5,  maxCandles: 48 },
  { label: '15m', minutes: 15, maxCandles: 32 },
];
