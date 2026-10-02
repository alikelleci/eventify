import { Component, computed, input } from '@angular/core';

/** Colour of an item's dot: neutral for events, the outcome for commands, placeholder while loading. */
export type TimelineTone = 'neutral' | 'success' | 'failure' | 'placeholder';

/**
 * One row of a vertical timeline: a dot on a connecting line, with the row's content projected next to it.
 * Used by the events and commands lists and their skeleton, so the line and dots always line up.
 * Like the replay on the landing page, a list can fill the line from the oldest item up to the selected one (see `reached`).
 */
@Component({
  selector: 'app-timeline-item',
  standalone: true,
  host: {
    class: 'group relative flex items-center gap-3 pl-[42px] pr-4 py-3 transition-colors',
    '[class]': 'rowClass()',
  },
  template: `
    <!-- The line in two halves, to the newer row above and the older row below, so it can fill up to the selected dot -->
    @if (!first()) { <span class="absolute left-[23px] top-0 bottom-1/2 w-px transition-colors duration-300" [class]="upperLineClass()"></span> }
    @if (!last()) { <span class="absolute left-[23px] top-1/2 bottom-0 w-px transition-colors duration-300" [class]="lowerLineClass()"></span> }
    <span class="absolute left-[16px] top-1/2 -translate-y-1/2 h-[15px] w-[15px] rounded-full border-[3px] transition-all duration-300" [class]="dotClass()"></span>
    <ng-content />
  `,
})
export class TimelineItemComponent {
  tone = input<TimelineTone>('neutral');
  selected = input(false);
  /** On the way from the oldest item to the selected one (the selected item included): its line and dot are filled. */
  reached = input(false);
  /** Clickable rows get a hover background and pointer. */
  interactive = input(true);
  /** The line starts at this row's dot (the newest item). */
  first = input(false);
  /** The line ends at this row's dot (the oldest loaded item, with nothing more to load). */
  last = input(false);

  rowClass = computed(() =>
    this.selected() ? 'cursor-pointer bg-primary-50 dark:bg-primary-950 shadow-[inset_2px_0_0_var(--p-primary-500)]'
      : this.interactive() ? 'cursor-pointer hover:bg-surface-50 dark:hover:bg-surface-800' : '');

  private readonly filledLine = 'bg-primary-500';
  private readonly emptyLine = 'bg-surface-200 dark:bg-surface-700';

  /** Filled when the row above is reached too, which is the case for every reached row except the selected one. */
  upperLineClass = computed(() => this.reached() && !this.selected() ? this.filledLine : this.emptyLine);

  lowerLineClass = computed(() => this.reached() ? this.filledLine : this.emptyLine);

  dotClass = computed(() => {
    // Commands keep their outcome colour; events are filled once reached, or when selected.
    const [fill, ring] = {
      neutral: this.reached() || this.selected() ? ['bg-primary-500', 'ring-primary-500/20'] : ['bg-surface-300 dark:bg-surface-600', ''],
      success: ['bg-emerald-500', 'ring-emerald-500/20'],
      failure: ['bg-red-500', 'ring-red-500/20'],
      placeholder: ['bg-surface-200 dark:bg-surface-700', ''],
    }[this.tone()];
    // The dot's border matches the row background, so it looks like a gap in the line.
    // The selected dot is enlarged with a soft ring, like the current event on the landing page.
    const border = this.selected() ? `border-primary-50 dark:border-primary-950 scale-125 ring-4 ${ring}`
      : this.interactive() ? 'border-surface-0 dark:border-surface-900 group-hover:border-surface-50 dark:group-hover:border-surface-800'
      : 'border-surface-0 dark:border-surface-900';
    return `${fill} ${border}`;
  });
}
