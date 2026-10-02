import { Component, input, output } from '@angular/core';

export interface ShowcaseStep {
  title: string;
  text: string;
}

/**
 * The steps of a showcase in a row, above it. The current one is marked, and while it plays its line fills over its time.
 * On a phone they stack, with the line on their left, and only the current one shows its text.
 */
@Component({
  selector: 'app-showcase-steps',
  standalone: true,
  template: `
    <ol class="grid gap-x-8 gap-y-3" [class]="steps().length === 2 ? 'sm:grid-cols-2' : 'sm:grid-cols-3'">
      @for (step of steps(); track step.title; let i = $index) {
        <li>
          <button type="button" (click)="pick.emit(i)" [attr.aria-current]="i === active()"
                  class="group relative block w-full cursor-pointer border-l-2 py-1 pl-4 text-left transition-colors duration-300 sm:border-t-2 sm:border-l-0 sm:py-0 sm:pt-4 sm:pl-0"
                  [class]="lineClass(i)">
            @if (i === active() && playing()) {
              @for (r of [run()]; track r) {
                <span class="step-progress absolute -top-0.5 left-0 hidden h-0.5 bg-primary-500 sm:block" [style.animation-duration.ms]="duration()"></span>
              }
            }
            <span class="block text-sm font-semibold transition-colors duration-300"
                  [class]="i === active() ? 'text-surface-900 dark:text-surface-0' : 'text-surface-500 group-hover:text-surface-800 dark:text-surface-400 dark:group-hover:text-surface-100'">{{ step.title }}</span>
            <span class="mt-1 text-sm leading-relaxed text-surface-500 sm:block dark:text-surface-400" [class]="i === active() ? 'block' : 'hidden'">{{ step.text }}</span>
          </button>
        </li>
      }
    </ol>
  `,
  styles: `
    .step-progress { animation: step-progress linear both; }
    @keyframes step-progress { from { width: 0; } to { width: 100%; } }
  `,
})
export class ShowcaseStepsComponent {
  readonly steps = input.required<ShowcaseStep[]>();
  readonly active = input.required<number>();
  readonly playing = input(false);
  /** Changes every time a step starts, so the progress starts over. */
  readonly run = input(0);
  /** How long the current step plays, in ms. */
  readonly duration = input(0);
  readonly pick = output<number>();

  /** The current step's line: filled, or a light track for the progress while playing. */
  lineClass(index: number): string {
    if (index !== this.active()) return 'border-surface-200 hover:border-surface-400 dark:border-surface-700 dark:hover:border-surface-500';
    return this.playing() ? 'border-primary-500 sm:border-primary-100 sm:dark:border-primary-950' : 'border-primary-500';
  }
}
