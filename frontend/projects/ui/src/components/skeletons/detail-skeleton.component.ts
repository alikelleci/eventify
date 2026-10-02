import { Component } from '@angular/core';
import { SkeletonModule } from 'primeng/skeleton';

/** Placeholder matching the event and command detail panes. */
@Component({
  selector: 'app-detail-skeleton',
  standalone: true,
  imports: [SkeletonModule],
  host: { class: 'block' },
  template: `
    <div class="flex flex-col gap-4 pt-1">
      <p-skeleton width="40%" height="1rem" />
      <p-skeleton width="25%" height="0.75rem" />
      <p-skeleton width="60%" height="0.875rem" styleClass="mt-4" />
      <p-skeleton width="50%" height="0.875rem" />
      <p-skeleton height="10rem" styleClass="mt-4" />
    </div>
  `,
})
export class DetailSkeletonComponent {}
