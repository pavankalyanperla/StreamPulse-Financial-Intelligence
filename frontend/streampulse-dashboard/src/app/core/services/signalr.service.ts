import { Injectable, NgZone, OnDestroy } from '@angular/core';
import { BehaviorSubject, Subject } from 'rxjs';
import {
  HubConnection,
  HubConnectionBuilder,
  HubConnectionState
} from '@microsoft/signalr';
import { environment } from '../../../environments/environment';
import {
  AnomalyAlert,
  OhlcvCandle,
  PriceForecast,
  TickState
} from '../models/stock.models';

@Injectable({ providedIn: 'root' })
export class SignalRService implements OnDestroy {
  readonly SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'INFY', 'TCS'];

  readonly connectionState$ = new BehaviorSubject<string>('Disconnected');
  readonly liveTicks$ = new BehaviorSubject<Map<string, TickState>>(new Map());
  readonly liveCandles$ = new Subject<OhlcvCandle>();
  readonly liveForecasts$ = new BehaviorSubject<Map<string, PriceForecast>>(new Map());
  readonly liveAlerts$ = new Subject<AnomalyAlert>();
  readonly alertHistory$ = new BehaviorSubject<AnomalyAlert[]>([]);
  readonly MAX_ALERT_HISTORY = 50;

  private connection!: HubConnection;
  private destroyed = false;
  private connectionId = 0;

  constructor(private readonly ngZone: NgZone) {}

  async connect(): Promise<void> {
    const id = ++this.connectionId;
    this.connection = new HubConnectionBuilder()
      .withUrl(environment.signalRUrl)
      .withAutomaticReconnect([0, 2000, 5000, 10000])
      .withServerTimeout(30000)
      .build();

    // All SignalR callbacks run outside Angular's ngZone — wrap in ngZone.run() for CD
    this.connection.on('ReceiveTick', (raw: any) => {
      this.ngZone.run(() => {
        const current = this.liveTicks$.value;
        const prev = current.get(raw.symbol);
        const tick: TickState = {
          symbol:        raw.symbol,
          price:         raw.price,
          volume:        raw.volume,
          changePct:     raw.change_pct ?? raw.changePct ?? 0,
          timestamp:     raw.timestamp,
          previousPrice: prev?.price ?? raw.price,
        };
        const updated = new Map(current);
        updated.set(tick.symbol, tick);
        this.liveTicks$.next(updated);
      });
    });

    this.connection.on('ReceiveCandle', (candle: OhlcvCandle) => {
      this.ngZone.run(() => this.liveCandles$.next(candle));
    });

    this.connection.on('ReceiveForecast', (raw: any) => {
      this.ngZone.run(() => {
        const forecast: PriceForecast = {
          symbol:         raw.symbol,
          predictedClose: raw.predicted_close ?? raw.predictedClose ?? 0,
          currentClose:   raw.current_close   ?? raw.currentClose   ?? 0,
          confidence:     raw.confidence      ?? 0,
          direction:      raw.direction       ?? 'FLAT',
          candleTime:     raw.candle_time     ?? raw.candleTime     ?? '',
          predictedFor:   raw.predicted_for   ?? raw.predictedFor   ?? '',
          modelTrained:   raw.model_trained   ?? raw.modelTrained   ?? true,
        };
        const updated = new Map(this.liveForecasts$.value);
        updated.set(forecast.symbol, forecast);
        this.liveForecasts$.next(updated);
      });
    });

    this.connection.on('ReceiveAlert', (raw: any) => {
      const normalized: AnomalyAlert = {
        symbol:       raw.symbol,
        alertType:    raw.alertType    ?? raw.alert_type    ?? '',
        severity:     raw.severity     ?? '',
        price:        raw.price        ?? 0,
        volume:       raw.volume       ?? 0,
        changePct:    raw.changePct    ?? raw.change_pct    ?? 0,
        timestamp:    raw.timestamp    ?? '',
        anomalyScore: raw.anomalyScore ?? raw.anomaly_score ?? 0,
      };
      this.ngZone.run(() => {
        this.liveAlerts$.next(normalized);
        const current = [normalized, ...this.alertHistory$.value];
        if (current.length > this.MAX_ALERT_HISTORY) current.pop();
        this.alertHistory$.next(current);
      });
    });

    this.connection.onreconnecting(() => {
      if (id !== this.connectionId) return;
      this.ngZone.run(() => {
        console.log('[SignalR] Reconnecting...');
        this.connectionState$.next('Reconnecting');
      });
    });

    this.connection.onreconnected(async () => {
      if (id !== this.connectionId) return;
      this.ngZone.run(() => {
        console.log('[SignalR] Reconnected');
        this.connectionState$.next('Connected');
      });
      await this.joinAllSymbols();
    });

    this.connection.onclose(() => {
      if (id !== this.connectionId) return;
      if (this.destroyed) return;
      this.ngZone.run(() => {
        console.log('[SignalR] Connection closed — retrying in 5s');
        this.connectionState$.next('Reconnecting');
      });
      setTimeout(() => { if (!this.destroyed) this.connect(); }, 5000);
    });

    try {
      await this.connection.start();
      this.ngZone.run(() => {
        this.connectionState$.next('Connected');
        console.log('[SignalR] Connected');
      });
      await this.joinAllSymbols();
    } catch (err) {
      if (id !== this.connectionId) return;
      if (this.destroyed) return;
      this.ngZone.run(() => {
        console.error('[SignalR] Connection failed — retrying in 5s:', err);
        this.connectionState$.next('Reconnecting');
      });
      setTimeout(() => { if (!this.destroyed) this.connect(); }, 5000);
    }
  }

  async joinAllSymbols(): Promise<void> {
    if (this.connection?.state === HubConnectionState.Connected) {
      await this.connection.invoke('JoinAllSymbols');
      console.log('[SignalR] Joined ALL_SYMBOLS group');
    }
  }

  async disconnect(): Promise<void> {
    this.destroyed = true;
    if (this.connection) {
      await this.connection.stop();
      this.connectionState$.next('Disconnected');
    }
  }

  ngOnDestroy(): void {
    this.disconnect();
  }
}
