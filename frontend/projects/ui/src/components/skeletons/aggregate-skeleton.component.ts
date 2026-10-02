import { Component } from '@angular/core';
import { SkeletonModule } from 'primeng/skeleton';
import { ListSkeletonComponent } from './list-skeleton.component';
import { DetailSkeletonComponent } from './detail-skeleton.component';

/** Placeholder for the whole aggregate view (tabs, list and detail pane) while an aggregate opens. */
@Component({
  selector: 'app-aggregate-skeleton',
  standalone: true,
  imports: [SkeletonModule, ListSkeletonComponent, DetailSkeletonComponent],
  host: { class: 'flex bg-surface-0 dark:bg-surface-900' },
  template: `
    <div class="flex flex-col h-full shrink-0 w-full lg:w-[480px] border-r border-surface-100 dark:border-surface-800 overflow-hidden">
      <!-- Same box as the real Events / Commands tabs, so nothing shifts when they appear -->
      <div class="px-4 shrink-0">
        <div class="flex items-center border-b border-surface-200 dark:border-surface-700">
          <div class="flex items-center h-[1.3125rem] px-4 py-3.5 box-content"><p-skeleton width="2.8rem" height="0.875rem" /></div>
          <div class="flex items-center h-[1.3125rem] px-4 py-3.5 box-content"><p-skeleton width="4.7rem" height="0.875rem" /></div>
          <p-skeleton shape="circle" size="1.25rem" styleClass="ml-auto mr-2.5" />
        </div>
      </div>
      <div class="flex-1 min-h-0 px-4 pt-3 pb-4">
        <app-list-skeleton class="max-h-full rounded-lg border border-surface-100 dark:border-surface-800 overflow-hidden" />
      </div>
    </div>
    <div class="hidden lg:block flex-1 px-6 py-5">
      <app-detail-skeleton />
    </div>
  `,
})
export class AggregateSkeletonComponent {}
