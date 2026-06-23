import {
  AfterViewInit,
  Component,
  ElementRef,
  OnDestroy,
  OnInit,
  ViewChild,
} from '@angular/core';
import { DecimalPipe, NgClass } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subscription } from 'rxjs';
import { Select } from 'primeng/select';
import {
  Chart,
  CategoryScale,
  Legend,
  LinearScale,
  LineController,
  LineElement,
  PointElement,
  TimeScale,
  Tooltip,
} from 'chart.js';
import {
  CandlestickController,
  CandlestickElement,
  OhlcElement,
} from 'chartjs-chart-financial';
import 'chartjs-adapter-date-fns';

import { ApiService } from '../../core/services/api.service';
import { SignalRService } from '../../core/services/signalr.service';
import { OhlcvCandle, PriceForecast } from '../../core/models/stock.models';
import { CandlestickDataPoint, ChartTimeframe, TIMEFRAMES } from '../../core/models/chart.models';
import { SkeletonComponent } from '../../shared/skeleton/skeleton.component';

@Component({
  selector: 'app-candlestick-chart',
  standalone: true,
  imports: [NgClass, DecimalPipe, FormsModule, Select, SkeletonComponent],
  templateUrl: './candlestick-chart.component.html',
  styleUrl: './candlestick-chart.component.scss',
})
export class CandlestickChartComponent implements OnInit, AfterViewInit, OnDestroy {
  @ViewChild('chartCanvas') canvasRef!: ElementRef<HTMLCanvasElement>;

  private static chartRegistered = false;

  chart: Chart | null = null;
  selectedSymbol = 'AAPL';
  selectedTimeframe: ChartTimeframe = TIMEFRAMES[0];
  candleData: CandlestickDataPoint[] = [];
  forecastPoints: { x: number; y: number }[] = [];
  latestCandle: OhlcvCandle | null = null;
  isLoading = true;

  readonly symbols = ['AAPL', 'GOOGL', 'MSFT', 'INFY', 'TCS'];
  readonly timeframes = TIMEFRAMES;

  private subs: Subscription[] = [];

  constructor(
    private readonly api: ApiService,
    private readonly signalR: SignalRService,
  ) {}

  ngOnInit(): void {
    if (!CandlestickChartComponent.chartRegistered) {
      Chart.register(
        CandlestickController, CandlestickElement, OhlcElement,
        TimeScale, LinearScale, CategoryScale,
        Tooltip, Legend,
        LineController, LineElement, PointElement,
      );
      CandlestickChartComponent.chartRegistered = true;
    }

    this.loadHistoricalCandles(this.selectedSymbol, this.selectedTimeframe);

    this.subs.push(
      this.signalR.liveCandles$.subscribe(candle => {
        if (candle.symbol === this.selectedSymbol) {
          this.appendLiveCandle(candle);
        }
      }),
      this.signalR.liveForecasts$.subscribe(forecasts => {
        this.updateForecastOverlay(forecasts);
      }),
    );
  }

  ngAfterViewInit(): void {
    setTimeout(() => this.initChart(this.canvasRef.nativeElement), 100);
  }

  private loadHistoricalCandles(symbol: string, tf: ChartTimeframe): void {
    const limit = tf.maxCandles * tf.minutes;
    const sub = this.api.getRecentCandles(symbol, limit).subscribe(candles => {
      this.candleData = this.aggregateCandles(candles, tf.minutes);
      this.latestCandle = candles[candles.length - 1] ?? null;
      this.isLoading = false;
      if (this.chart) {
        (this.chart.data.datasets[0] as any).data = this.candleData;
        this.chart.update('none');
      }
    });
    this.subs.push(sub);
  }

  private aggregateCandles(raw: OhlcvCandle[], tfMinutes: number): CandlestickDataPoint[] {
    if (tfMinutes === 1) {
      return raw.map(c => ({
        x: new Date(c.candleTime).getTime(),
        o: Number(c.openPrice),
        h: Number(c.highPrice),
        l: Number(c.lowPrice),
        c: Number(c.closePrice),
      }));
    }

    const buckets = new Map<number, CandlestickDataPoint>();
    const bucketMs = tfMinutes * 60_000;

    for (const c of raw) {
      const t = new Date(c.candleTime).getTime();
      const key = Math.floor(t / bucketMs) * bucketMs;
      const existing = buckets.get(key);
      if (!existing) {
        buckets.set(key, {
          x: key,
          o: Number(c.openPrice),
          h: Number(c.highPrice),
          l: Number(c.lowPrice),
          c: Number(c.closePrice),
        });
      } else {
        existing.h = Math.max(existing.h, Number(c.highPrice));
        existing.l = Math.min(existing.l, Number(c.lowPrice));
        existing.c = Number(c.closePrice);
      }
    }

    return Array.from(buckets.values()).sort((a, b) => a.x - b.x);
  }

  initChart(canvas: HTMLCanvasElement): void {
    this.chart?.destroy();

    this.chart = new Chart(canvas, {
      type: 'candlestick' as any,
      data: {
        datasets: [
          {
            type: 'candlestick' as any,
            label: this.selectedSymbol,
            data: this.candleData as any,
            color: { up: '#00ff88', down: '#ff4444', unchanged: '#888888' } as any,
            borderColor: { up: '#00ff88', down: '#ff4444', unchanged: '#888888' } as any,
          },
          {
            type: 'line',
            label: 'ML Forecast',
            data: this.forecastPoints as any,
            borderColor: '#00b4d8',
            borderDash: [5, 5],
            pointRadius: 3,
            pointBackgroundColor: '#00b4d8',
            tension: 0.3,
            fill: false,
            yAxisID: 'y',
          } as any,
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: {
            type: 'time',
            adapters: { date: {} },
            time: { unit: 'minute', displayFormats: { minute: 'HH:mm' } },
            ticks: { color: '#888', maxTicksLimit: 10 },
            grid: { color: '#1a1a3a' },
          },
          y: {
            ticks: { color: '#888' },
            grid: { color: '#2a2a4a' },
          },
        },
        plugins: {
          legend: { labels: { color: '#ccc' } },
          tooltip: {
            callbacks: {
              label: (ctx: any) => {
                const raw = ctx.raw as any;
                if (raw?.o !== undefined) {
                  return `O: ${raw.o?.toFixed(2)}  H: ${raw.h?.toFixed(2)}  L: ${raw.l?.toFixed(2)}  C: ${raw.c?.toFixed(2)}`;
                }
                return `${ctx.dataset.label}: ${ctx.parsed?.y?.toFixed(2)}`;
              },
            },
          },
        },
      } as any,
    });
  }

  private appendLiveCandle(candle: OhlcvCandle): void {
    const t = new Date(candle.candleTime).getTime();
    const bucketMs = this.selectedTimeframe.minutes * 60_000;
    const bucketKey = this.selectedTimeframe.minutes === 1
      ? t
      : Math.floor(t / bucketMs) * bucketMs;

    const last = this.candleData[this.candleData.length - 1];
    if (last && last.x === bucketKey) {
      last.h = Math.max(last.h, Number(candle.highPrice));
      last.l = Math.min(last.l, Number(candle.lowPrice));
      last.c = Number(candle.closePrice);
    } else {
      this.candleData.push({
        x: bucketKey,
        o: Number(candle.openPrice),
        h: Number(candle.highPrice),
        l: Number(candle.lowPrice),
        c: Number(candle.closePrice),
      });
      if (this.candleData.length > this.selectedTimeframe.maxCandles) {
        this.candleData.shift();
      }
    }

    this.latestCandle = candle;
    this.chart?.update('none');
  }

  private updateForecastOverlay(forecasts: Map<string, PriceForecast>): void {
    const forecast = forecasts.get(this.selectedSymbol);
    if (!forecast?.modelTrained) return;

    this.forecastPoints.push({ x: Date.now(), y: Number(forecast.predictedClose) });
    if (this.forecastPoints.length > 20) this.forecastPoints.shift();

    if (this.chart) {
      (this.chart.data.datasets[1] as any).data = this.forecastPoints;
      this.chart.update('none');
    }
  }

  onSymbolChange(symbol: string): void {
    this.selectedSymbol = symbol;
    this.candleData = [];
    this.forecastPoints = [];
    this.latestCandle = null;
    this.isLoading = true;
    this.chart?.destroy();
    this.chart = null;

    const limit = this.selectedTimeframe.maxCandles * this.selectedTimeframe.minutes;
    const sub = this.api.getRecentCandles(symbol, limit).subscribe(candles => {
      this.candleData = this.aggregateCandles(candles, this.selectedTimeframe.minutes);
      this.latestCandle = candles[candles.length - 1] ?? null;
      this.isLoading = false;
      this.initChart(this.canvasRef.nativeElement);
    });
    this.subs.push(sub);
  }

  onTimeframeChange(tf: ChartTimeframe): void {
    this.selectedTimeframe = tf;
    this.candleData = [];
    this.forecastPoints = [];
    this.isLoading = true;

    const limit = tf.maxCandles * tf.minutes;
    const sub = this.api.getRecentCandles(this.selectedSymbol, limit).subscribe(candles => {
      this.candleData = this.aggregateCandles(candles, tf.minutes);
      this.latestCandle = candles[candles.length - 1] ?? null;
      this.isLoading = false;
      this.initChart(this.canvasRef.nativeElement);
    });
    this.subs.push(sub);
  }

  ngOnDestroy(): void {
    this.subs.forEach(s => s.unsubscribe());
    this.chart?.destroy();
  }
}
