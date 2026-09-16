import { Component, computed, input } from '@angular/core';
import { StatusTone } from '../status';

/** An application in the picture, with a short note under its name, e.g. its number of instances. */
export interface ConnectedApp {
  name: string;
  note?: string;
  /** How it is doing, as the colour of its dot. Without one, the dot is green. */
  tone?: StatusTone;
}

/** At most this many boxes; with more applications the last box counts the rest. */
const MAX_BOXES = 4;

/**
 * The Eventify Console with the applications connected to it: dashes flow along a line from each application up to the console.
 * Decorative; the page says in words what it shows. Without motion the lines stand still.
 */
@Component({
  selector: 'app-connected-apps',
  standalone: true,
  host: { class: 'block', 'aria-hidden': 'true' },
  template: `
    <div class="flex flex-col items-center">
      <div class="flex items-center gap-3 rounded-xl border border-primary-300 bg-primary-50 px-4 py-3 shadow-[0_0_0_4px_color-mix(in_srgb,var(--p-primary-500)_12%,transparent)] dark:border-primary-700 dark:bg-primary-950">
        <span class="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-primary-500 text-white"><i class="pi pi-bolt text-sm"></i></span>
        <span class="text-sm font-semibold text-surface-900 dark:text-surface-0">Eventify Console</span>
      </div>

      <div class="relative h-14 w-full">
        <svg class="absolute inset-0 h-full w-full overflow-visible" viewBox="0 0 300 56" preserveAspectRatio="none" fill="none">
          @for (path of paths(); track $index) {
            <path [attr.d]="path" class="stroke-surface-300 dark:stroke-surface-600" stroke-width="1" vector-effect="non-scaling-stroke" />
            <path [attr.d]="path" class="connection-flow stroke-primary-500" stroke-width="1.5" stroke-dasharray="3 9" stroke-linecap="round" vector-effect="non-scaling-stroke" />
          }
        </svg>
      </div>

      <div class="grid w-full gap-2" [style.grid-template-columns]="'repeat(' + boxes().length + ', minmax(0, 1fr))'">
        @for (box of boxes(); track box.name) {
          <div class="flex min-w-0 flex-col items-center rounded-lg border border-surface-200 bg-surface-0 px-2 py-2 dark:border-surface-700 dark:bg-surface-900">
            <span class="flex max-w-full items-center gap-1.5">
              <span class="h-1.5 w-1.5 shrink-0 rounded-full" [class]="dotClass(box.tone)"></span>
              <span class="truncate text-xs font-medium text-surface-700 dark:text-surface-200">{{ box.name }}</span>
            </span>
            @if (box.note) { <span class="mt-0.5 text-[10px] text-surface-400">{{ box.note }}</span> }
          </div>
        }
      </div>
    </div>
  `,
  styles: `
    .connection-flow { animation: connection-flow 1.2s linear infinite; }
    @keyframes connection-flow { to { stroke-dashoffset: -12; } }
    @media (prefers-reduced-motion: reduce) { .connection-flow { animation: none; } }
  `,
})
export class ConnectedAppsComponent {
  apps = input.required<ConnectedApp[]>();

  readonly boxes = computed<ConnectedApp[]>(() => {
    const apps = this.apps();
    if (apps.length <= MAX_BOXES) return apps;
    const rest = apps.length - (MAX_BOXES - 1);
    // The rest in one box, coloured like the one worst off, so a problem doesn't hide behind it.
    const hidden = apps.slice(MAX_BOXES - 1).map(app => app.tone);
    const tone = hidden.includes('error') ? 'error' : hidden.includes('busy') ? 'busy' : undefined;
    return [...apps.slice(0, MAX_BOXES - 1), { name: `+${rest} more`, note: 'applications', tone }];
  });

  /** The same colours as the application switcher: green running, amber busy, red wrong, grey unknown. */
  dotClass(tone: StatusTone | undefined): string {
    return tone === 'busy' ? 'bg-amber-500' : tone === 'error' ? 'bg-red-500' : tone === 'unknown' ? 'bg-surface-300 dark:bg-surface-600' : 'bg-primary-500';
  }

  /** From the centre of each box, curving up to the console in the middle. */
  readonly paths = computed(() => {
    const count = this.boxes().length;
    return this.boxes().map((_, i) => {
      const x = (i + 0.5) / count * 300;
      return `M${x} 56 C${x} 28 150 28 150 0`;
    });
  });
}
