import { Component, OnDestroy, OnInit } from '@angular/core';
import { AsyncPipe, DecimalPipe, NgClass } from '@angular/common';
import { Subscription } from 'rxjs';
import { MessageService } from 'primeng/api';
import { SignalRService } from '../../core/services/signalr.service';
import { ApiService } from '../../core/services/api.service';
import { AnomalyAlert } from '../../core/models/stock.models';

@Component({
  selector: 'app-alert-feed',
  standalone: true,
  imports: [NgClass, DecimalPipe, AsyncPipe],
  templateUrl: './alert-feed.component.html',
  styleUrl: './alert-feed.component.scss',
})
export class AlertFeedComponent implements OnInit, OnDestroy {
  private toastSub!: Subscription;

  constructor(
    readonly signalR: SignalRService,
    private readonly api: ApiService,
    private readonly messageService: MessageService,
  ) {}

  ngOnInit(): void {
    // Pre-populate from REST only when the service history is empty
    // (avoids duplicating on re-mount after theme switch)
    if (this.signalR.alertHistory$.value.length === 0) {
      this.api.getHighAlerts().subscribe((raw: any[]) => {
        const normalized: AnomalyAlert[] = raw.map(a => ({
          symbol:       a.symbol,
          alertType:    a.alertType    ?? a.alert_type    ?? '',
          severity:     a.severity     ?? '',
          price:        a.price        ?? 0,
          volume:       a.volume       ?? 0,
          changePct:    a.changePct    ?? a.change_pct    ?? 0,
          timestamp:    a.alertTimestamp ?? a.timestamp   ?? new Date().toISOString(),
          anomalyScore: a.anomalyScore  ?? a.anomaly_score ?? 0,
        }));
        const merged = [...normalized, ...this.signalR.alertHistory$.value];
        this.signalR.alertHistory$.next(
          merged.slice(0, this.signalR.MAX_ALERT_HISTORY)
        );
      });
    }

    // Toast notifications for HIGH severity live alerts only
    this.toastSub = this.signalR.liveAlerts$.subscribe(alert => {
      if (alert.severity === 'HIGH') {
        this.messageService.add({
          severity: 'error',
          summary: 'HIGH ALERT — ' + alert.symbol,
          detail: alert.alertType + ' | $' + alert.price,
          life: 5000,
        });
      }
    });
  }

  formatTime(timestamp: string): string {
    if (!timestamp) return '--:--:--';
    return new Date(timestamp).toTimeString().slice(0, 8);
  }

  ngOnDestroy(): void {
    this.toastSub?.unsubscribe();
  }
}
