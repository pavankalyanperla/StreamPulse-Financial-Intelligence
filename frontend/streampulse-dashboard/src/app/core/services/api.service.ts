import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';
import { AnomalyAlert, OhlcvCandle, PriceForecast, SymbolSentiment } from '../models/stock.models';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly base = environment.gatewayUrl;

  constructor(private http: HttpClient) {}

  // Direct gateway endpoint — returns symbol → price map from Redis cache
  getLatestTicks(): Observable<Record<string, number>> {
    return this.http.get<Record<string, number>>(`${this.base}/ticks`);
  }

  getAlerts(symbol?: string, severity?: string, limit = 20): Observable<AnomalyAlert[]> {
    let params = new HttpParams().set('limit', limit);
    if (symbol)   params = params.set('symbol', symbol);
    if (severity) params = params.set('severity', severity);
    return this.http.get<AnomalyAlert[]>(`${this.base}/api/alerts`, { params });
  }

  getAlertStats(): Observable<unknown> {
    return this.http.get(`${this.base}/api/alerts/stats`);
  }

  getHighAlerts(): Observable<AnomalyAlert[]> {
    return this.http.get<AnomalyAlert[]>(`${this.base}/api/alerts/high`);
  }

  getSentiment(symbol: string): Observable<SymbolSentiment> {
    return this.http.get<SymbolSentiment>(`${this.base}/api/sentiment/${symbol}`);
  }

  getAllSentiment(): Observable<SymbolSentiment[]> {
    return this.http.get<SymbolSentiment[]>(`${this.base}/api/sentiment/all`);
  }

  getForecast(symbol: string): Observable<PriceForecast> {
    return this.http.get<PriceForecast>(`${this.base}/api/forecasts/${symbol}`);
  }

  getAllForecasts(): Observable<PriceForecast[]> {
    return this.http.get<PriceForecast[]>(`${this.base}/api/forecasts/all`);
  }

  getRecentCandles(symbol: string, limit: number = 60): Observable<OhlcvCandle[]> {
    return this.http.get<OhlcvCandle[]>(`${this.base}/api/candles/${symbol}?limit=${limit}`);
  }
}
