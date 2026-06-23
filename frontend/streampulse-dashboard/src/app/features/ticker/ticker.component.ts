import {
  ChangeDetectorRef,
  Component,
  OnDestroy,
  OnInit,
} from '@angular/core';
import { DatePipe, DecimalPipe } from '@angular/common';
import { Subscription } from 'rxjs';
import { SignalRService } from '../../core/services/signalr.service';
import { TickState } from '../../core/models/stock.models';
import { SkeletonComponent } from '../../shared/skeleton/skeleton.component';

@Component({
  selector: 'app-ticker',
  standalone: true,
  imports: [DatePipe, DecimalPipe, SkeletonComponent],
  templateUrl: './ticker.component.html',
  styleUrl: './ticker.component.scss',
})
export class TickerComponent implements OnInit, OnDestroy {
  readonly symbols = ['AAPL', 'GOOGL', 'MSFT', 'INFY', 'TCS'];
  readonly skeletonRange = [1, 2, 3, 4, 5];

  ticks = new Map<string, TickState>();
  flashState: Record<string, 'up' | 'down' | ''> = {};
  isLoading = true;

  private sub!: Subscription;
  private flashTimers: Record<string, ReturnType<typeof setTimeout>> = {};

  constructor(
    private readonly signalR: SignalRService,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.sub = this.signalR.liveTicks$.subscribe(ticks => {
      if (this.isLoading && ticks.size > 0) {
        this.isLoading = false;
      }
      for (const [symbol, tick] of ticks) {
        const prev = this.ticks.get(symbol);
        if (prev && prev.price !== tick.price) {
          this.triggerFlash(symbol, tick.price > prev.price ? 'up' : 'down');
        }
      }
      this.ticks = ticks;
    });
  }

  getTick(symbol: string): TickState | undefined {
    return this.ticks.get(symbol);
  }

  private triggerFlash(symbol: string, direction: 'up' | 'down'): void {
    const existing = this.flashTimers[symbol];
    if (existing) clearTimeout(existing);

    this.flashState = { ...this.flashState, [symbol]: direction };

    this.flashTimers[symbol] = setTimeout(() => {
      this.flashState = { ...this.flashState, [symbol]: '' };
      this.cdr.detectChanges();
    }, 600);
  }

  ngOnDestroy(): void {
    this.sub?.unsubscribe();
    for (const timer of Object.values(this.flashTimers)) {
      clearTimeout(timer);
    }
  }
}
