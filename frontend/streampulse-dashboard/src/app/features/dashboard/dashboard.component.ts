import { Component } from '@angular/core';
import { TickerComponent } from '../ticker/ticker.component';
import { CandlestickChartComponent } from '../chart/candlestick-chart.component';
import { AlertFeedComponent } from '../alerts/alert-feed.component';
import { SentimentPanelComponent } from '../sentiment/sentiment-panel.component';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [TickerComponent, CandlestickChartComponent, AlertFeedComponent, SentimentPanelComponent],
  templateUrl: './dashboard.component.html',
  styleUrl: './dashboard.component.scss',
})
export class DashboardComponent {}
