import { Component, OnInit } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { AsyncPipe, NgClass } from '@angular/common';
import { Toast } from 'primeng/toast';
import { SignalRService } from './core/services/signalr.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, NgClass, AsyncPipe, Toast],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App implements OnInit {
  isDarkMode = true;

  constructor(readonly signalR: SignalRService) {}

  ngOnInit(): void {
    document.documentElement.classList.add('dark-mode');
    setTimeout(() => this.signalR.connect(), 0);
  }

  toggleTheme(): void {
    this.isDarkMode = !this.isDarkMode;
    const root = document.documentElement;
    root.classList.toggle('dark-mode', this.isDarkMode);
    root.classList.toggle('light-mode', !this.isDarkMode);
  }
}
