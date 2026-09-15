import { Component, computed, input } from '@angular/core';

/** Colour of an item's dot: neutral for events, the outcome for commands, placeholder while loading. */
export type TimelineTone = 'neutral' | 'success' | 'failure' | 'placeholder';

/**
 * One row of a vertical timeline: a dot on a connecting line, with the row's content projected next to it.
 * Used by the events and commands lists and their skeleton, so the line and dots always line up.
 * When selected, the line leading TO it turns green, and all older items have green lines flowing downward.
 * Failed commands keep their red dots even when highlighted, to preserve failure status visibility.
 */
@Component({
  selector: 'app-timeline-item',
  standalone: true,
  host: {
    class: 'group relative flex items-center gap-3 pl-[42px] pr-4 py-3 transition-colors',
    '[class]': 'rowClass()',
  },
  template: `
    <span class="absolute left-[23px] w-px transition-colors" 
          [class]="lineClass()"></span>
    <span class="absolute left-[16px] top-1/2 -translate-y-1/2 h-[15px] w-[15px] rounded-full border-[3px] transition-colors" [class]="dotClass()"></span>
    <ng-content />
  `,
})
export class TimelineItemComponent {
  tone = input<TimelineTone>('neutral');
  selected = input(false);
  /** Whether this older event (below selected) should have its line and dot highlighted. */
  highlightedConnector = input(false);
  /** Whether the next item in the list is selected. */
  nextItemSelected = input(false);
  filled = input(false);
  /** Clickable rows get a hover background and pointer. */
  interactive = input(true);
  /** The line starts at this row's dot (the newest item). */
  first = input(false);
  /** The line ends at this row's dot (the oldest loaded item, with nothing more to load). */
  last = input(false);

  rowClass = computed(() =>
    this.selected() ? 'cursor-pointer bg-emerald-50 dark:bg-emerald-950 shadow-[inset_2px_0_0_var(--p-emerald-500)]'
      : this.interactive() ? 'cursor-pointer hover:bg-surface-50 dark:hover:bg-surface-800' : '');

  lineClass = computed(() => {
    const hidden = this.first() && this.last();
    // Line goes from dot downward (not from above). Last item has no line below it.
    const positionClasses = hidden ? 'hidden' :
      (this.last() ? '' : 'top-1/2 bottom-0');

    // Green line if: this is selected or in the highlighted connector chain (older items below)
    const colorClasses = this.selected() || this.highlightedConnector()
      ? 'bg-emerald-500'
      : 'bg-surface-200 dark:bg-surface-700';

    return `${positionClasses} ${colorClasses}`.trim();
  });

  dotClass = computed(() => {
    const baseBorder = this.interactive()
      ? 'border-surface-0 dark:border-surface-900 group-hover:border-surface-50 dark:group-hover:border-surface-800'
      : 'border-surface-0 dark:border-surface-900';

    // Failed commands: always red, with red ring when selected
    if (this.tone() === 'failure') {
      const ringClasses = this.selected() ? 'ring-1 ring-red-500' : '';
      return `bg-red-500 ${baseBorder} ${ringClasses}`;
    }

    // Selected item: emerald dot with ring
    if (this.selected()) {
      return `bg-emerald-500 border-emerald-50 dark:border-emerald-950 ring-1 ring-emerald-500`;
    }

    // Highlighted items (older items in flow): emerald for success/neutral, no ring
    if (this.highlightedConnector()) {
      return `bg-emerald-500 ${baseBorder}`;
    }

    // Default colors by tone (not selected, not highlighted) — no rings except on selected
    const fill = {
      neutral: 'bg-surface-300 dark:bg-surface-600',
      success: 'bg-emerald-500',
      failure: 'bg-red-500',
      placeholder: 'bg-surface-200 dark:bg-surface-700',
    }[this.tone()];

    return `${fill} ${baseBorder}`;
  });
}
