import { Component, computed, input } from '@angular/core';
import { TooltipModule } from 'primeng/tooltip';
import { AppNode } from '../services/backend.service';
import { InstanceListComponent } from './instance-list.component';
import { StatusTone, TOOLTIP_DELAY_MS, dotClass, worst } from '../status';

/** An application in the picture, with a short note under its name, e.g. its number of instances. */
export interface ConnectedApp {
  name: string;
  note?: string;
  /** How it is doing, as the colour of its dot. */
  tone: StatusTone;
  /** Its instances, listed with how each is doing on hover. */
  instances: AppNode[];
}

/** A box in the picture: one application, or the rest of them together. */
interface Box extends Omit<ConnectedApp, 'instances'> {
  instances?: AppNode[];
  /** For the box of the rest: the applications in it. */
  rest?: ConnectedApp[];
}

/** At most this many boxes; with more applications the last box counts the rest. */
const MAX_BOXES = 4;

/** 0 for an error, 1 for busy, 2 for anything else: the order the boxes are given out in. */
const problem = (app: ConnectedApp) => app.tone === 'error' ? 0 : app.tone === 'busy' ? 1 : 2;

/**
 * The Eventify Console with the applications connected to it: dashes flow along a line from each application up to the console.
 * Decorative; the page says in words what it shows. Without motion the lines stand still.
 */
@Component({
  selector: 'app-connected-apps',
  standalone: true,
  imports: [TooltipModule, InstanceListComponent],
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
          <div class="flex min-w-0 flex-col items-center rounded-lg border border-surface-200 bg-surface-0 px-2 py-2 dark:border-surface-700 dark:bg-surface-900"
               [pTooltip]="box.rest ? rest : instances" tooltipPosition="bottom" tooltipStyleClass="max-w-96" [showDelay]="tooltipDelay">
            <span class="flex max-w-full items-center gap-1.5">
              <span class="h-1.5 w-1.5 shrink-0 rounded-full" [class]="dotClass(box.tone)"></span>
              <span class="truncate text-xs font-medium text-surface-700 dark:text-surface-200">{{ box.name }}</span>
            </span>
            @if (box.note) { <span class="mt-0.5 text-[10px] text-surface-400">{{ box.note }}</span> }
          </div>
          <!-- An application's instances, the same list as in the application switcher -->
          <ng-template #instances><app-instance-list [instances]="box.instances ?? []" /></ng-template>
          <!-- The applications in the box of the rest, each with its state -->
          <ng-template #rest>
            <div class="flex flex-col gap-1 text-xs">
              @for (app of box.rest; track app.name) {
                <!-- A long name wraps, so the tooltip doesn't grow wider than the screen -->
                <div class="flex items-start gap-2">
                  <span class="mt-1 h-1.5 w-1.5 shrink-0 rounded-full" [class]="dotClass(app.tone)"></span>
                  <span class="min-w-0 break-words font-medium">{{ app.name }}</span>
                  <span class="ml-auto shrink-0 whitespace-nowrap pl-6 opacity-80">{{ app.note }}</span>
                </div>
              }
            </div>
          </ng-template>
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

  /**
   * The applications with a problem get a box first, errors before busy ones; the others keep their order. The rest share
   * one box, in the colour of the one worst off, like an application in the switcher; hovering it lists them in their
   * usual order, as the switcher does. Hovering an application's own box lists its instances.
   */
  readonly boxes = computed<Box[]>(() => {
    const apps = this.apps();
    const ordered = [...apps].sort((a, b) => problem(a) - problem(b));
    if (apps.length <= MAX_BOXES) return ordered;
    const shown = ordered.slice(0, MAX_BOXES - 1);
    const rest = apps.filter(app => !shown.includes(app));
    return [...shown, { name: `+${rest.length} more`, note: 'applications', tone: worst(rest).tone, rest }];
  });

  /** The same colours and tooltip delay as the application switcher. */
  readonly dotClass = dotClass;
  readonly tooltipDelay = TOOLTIP_DELAY_MS;

  /** From the centre of each box, curving up to the console in the middle. */
  readonly paths = computed(() => {
    const count = this.boxes().length;
    return this.boxes().map((_, i) => {
      const x = (i + 0.5) / count * 300;
      return `M${x} 56 C${x} 28 150 28 150 0`;
    });
  });
}
