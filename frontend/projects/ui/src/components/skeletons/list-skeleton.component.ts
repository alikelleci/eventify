import { Component, computed, input } from '@angular/core';
import { SkeletonModule } from 'primeng/skeleton';
import { TimelineItemComponent } from '../timeline-item.component';

/** Placeholder timeline rows, the same height as a real events/commands list item. */
@Component({
  selector: 'app-list-skeleton',
  standalone: true,
  imports: [SkeletonModule, TimelineItemComponent],
  host: { class: 'block' },
  template: `
    @for (i of rowIndexes(); track i) {
      <app-timeline-item tone="placeholder" [interactive]="false" [first]="$first && !continues()" [last]="$last">
        <div class="flex flex-col gap-[14.5px] flex-1">
          <p-skeleton width="40%" height="0.875rem" /><p-skeleton width="60%" height="0.75rem" />
        </div>
      </app-timeline-item>
    }
  `,
})
export class ListSkeletonComponent {
  rows = input(8);
  /** Set when shown below already loaded items (loading more), so the line continues from them. */
  continues = input(false);
  protected rowIndexes = computed(() => Array.from({ length: this.rows() }, (_, i) => i));
}
