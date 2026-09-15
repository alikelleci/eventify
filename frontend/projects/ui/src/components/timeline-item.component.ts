import { Component, computed, input } from '@angular/core';

/** Colour of an item's dot: neutral for events, the outcome for commands, placeholder while loading. */
export type TimelineTone = 'neutral' | 'success' | 'failure' | 'placeholder';

/**
 * One row of a vertical timeline: a dot on a connecting line, with the row's content projected next to it.
 * Used by the events and commands lists and their skeleton, so the line and dots always line up.
 */
@Component({
  selector: 'app-timeline-item',
  standalone: true,
  host: {
    class: 'group relative flex items-center gap-3 pl-[42px] pr-4 py-3 transition-colors',
    '[class]': 'rowClass()',
  },
  template: `
    <span class="absolute left-[23px] w-px bg-surface-200 dark:bg-surface-700" [class]="lineClass()"></span>
    <span class="absolute left-[16px] top-1/2 -translate-y-1/2 h-[15px] w-[15px] rounded-full border-[3px] transition-colors" [class]="dotClass()"></span>
    <ng-content />
  `,
})
export class TimelineItemComponent {
  tone = input<TimelineTone>('neutral');
  selected = input(false);
  /** Clickable rows get a hover background and pointer. */
  interactive = input(true);
  /** The line starts at this row's dot (the newest item). */
  first = input(false);
  /** The line ends at this row's dot (the oldest loaded item, with nothing more to load). */
  last = input(false);

  rowClass = computed(() =>
    this.selected() ? 'cursor-pointer bg-primary-50 dark:bg-primary-950 shadow-[inset_2px_0_0_var(--p-primary-500)]'
      : this.interactive() ? 'cursor-pointer hover:bg-surface-50 dark:hover:bg-surface-800' : '');

  lineClass = computed(() => {
    if (this.first() && this.last()) return 'hidden';
    return (this.first() ? 'top-1/2 ' : 'top-0 ') + (this.last() ? 'bottom-1/2' : 'bottom-0');
  });

  dotClass = computed(() => {
    // A selected dot gets a thin ring in its own colour, like the newest event on the landing page.
    const [fill, ring] = {
      neutral: this.selected() ? ['bg-primary-500', 'ring-primary-500'] : ['bg-surface-300 dark:bg-surface-600', ''],
      success: ['bg-emerald-500', 'ring-emerald-500'],
      failure: ['bg-red-500', 'ring-red-500'],
      placeholder: ['bg-surface-200 dark:bg-surface-700', ''],
    }[this.tone()];
    // The dot's border matches the row background, so it looks like a gap in the line (and between dot and ring).
    const border = this.selected() ? `border-primary-50 dark:border-primary-950 ring-1 ${ring}`
      : this.interactive() ? 'border-surface-0 dark:border-surface-900 group-hover:border-surface-50 dark:group-hover:border-surface-800'
      : 'border-surface-0 dark:border-surface-900';
    return `${fill} ${border}`;
  });
}
