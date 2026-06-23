import { Component, Input } from '@angular/core';

@Component({
  selector: 'app-skeleton',
  standalone: true,
  template: `<div class="skeleton" [style.width]="width" [style.height]="height"></div>`,
  styles: [`
    .skeleton {
      background: linear-gradient(
        90deg,
        var(--sp-bg-secondary) 25%,
        var(--sp-border) 50%,
        var(--sp-bg-secondary) 75%
      );
      background-size: 200% 100%;
      animation: shimmer 1.5s infinite;
      border-radius: 4px;
    }
    @keyframes shimmer {
      0%   { background-position: 200% 0; }
      100% { background-position: -200% 0; }
    }
  `],
})
export class SkeletonComponent {
  @Input() width  = '100%';
  @Input() height = '16px';
}
