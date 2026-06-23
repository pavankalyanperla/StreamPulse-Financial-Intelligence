import { Component, OnDestroy, OnInit } from '@angular/core';
import { DatePipe, DecimalPipe, NgClass } from '@angular/common';
import { ApiService } from '../../core/services/api.service';
import { SymbolSentiment } from '../../core/models/stock.models';

@Component({
  selector: 'app-sentiment-panel',
  standalone: true,
  imports: [NgClass, DecimalPipe, DatePipe],
  templateUrl: './sentiment-panel.component.html',
  styleUrl: './sentiment-panel.component.scss',
})
export class SentimentPanelComponent implements OnInit, OnDestroy {
  sentimentList: SymbolSentiment[] = [];
  selectedSentiment: SymbolSentiment | null = null;
  lastUpdated: Date | null = null;

  private refreshInterval: any;

  constructor(private readonly api: ApiService) {}

  ngOnInit(): void {
    this.loadSentiment();
    this.refreshInterval = setInterval(() => this.loadSentiment(), 30_000);
  }

  loadSentiment(): void {
    this.api.getAllSentiment().subscribe((data: any) => {
      // API returns Record<string, obj> with snake_case keys — normalize to SymbolSentiment[]
      const list: SymbolSentiment[] = Object.values(data).map((s: any) => ({
        symbol:         s.symbol,
        score:          s.score,
        label:          s.label,
        headlineCount:  s.headline_count  ?? s.headlineCount  ?? 0,
        latestHeadline: s.latest_headline ?? s.latestHeadline ?? '',
        lastUpdated:    s.last_updated    ?? s.lastUpdated    ?? '',
      }));
      this.sentimentList = list.sort((a, b) => Math.abs(b.score) - Math.abs(a.score));
      this.selectedSentiment = this.sentimentList[0] ?? null;
      this.lastUpdated = new Date();
    });
  }

  getBarWidth(score: number): number {
    return Math.min(Math.abs(score) * 100, 100);
  }

  ngOnDestroy(): void {
    clearInterval(this.refreshInterval);
  }
}
