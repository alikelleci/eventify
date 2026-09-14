import { Component, DestroyRef, ElementRef, computed, inject } from '@angular/core';
import { ShowcaseStepsComponent } from './showcase-steps.component';
import { StepPlayer } from './step-player';

interface Field {
  key: string;
  value: string;
  /** The value before this event, when it changed. */
  was?: string;
  /** Added by this event. */
  added?: boolean;
}

interface OrderEvent {
  type: string;
  time: string;
  payload: [string, string][];
  /** The state after this event, compared with the state before it. */
  state: Field[];
}

/** The events of one order, oldest first. */
const EVENTS: OrderEvent[] = [
  {
    type: 'OrderPlaced', time: '09:12',
    payload: [['customerId', '"customer-42"'], ['total', '169.40']],
    state: [
      { key: 'status', value: '"PLACED"', added: true },
      { key: 'customerId', value: '"customer-42"', added: true },
      { key: 'total', value: '169.40', added: true },
    ],
  },
  {
    type: 'OrderConfirmed', time: '09:12',
    payload: [['paymentReference', '"PSP-7F3A-92K1"']],
    state: [
      { key: 'status', value: '"CONFIRMED"', was: '"PLACED"' },
      { key: 'customerId', value: '"customer-42"' },
      { key: 'total', value: '169.40' },
      { key: 'paymentReference', value: '"PSP-7F3A-92K1"', added: true },
    ],
  },
  {
    type: 'OrderShipped', time: '11:02',
    payload: [['carrier', '"DHL"'], ['trackingNumber', '"JD014600003SE"']],
    state: [
      { key: 'status', value: '"SHIPPED"', was: '"CONFIRMED"' },
      { key: 'customerId', value: '"customer-42"' },
      { key: 'total', value: '169.40' },
      { key: 'paymentReference', value: '"PSP-7F3A-92K1"' },
      { key: 'carrier', value: '"DHL"', added: true },
      { key: 'trackingNumber', value: '"JD014600003SE"', added: true },
    ],
  },
];

/**
 * One step per event, in sync with the timeline: the first event is replayed with its payload, the second shows the state
 * as it was after it, and the third highlights what it changed. The part of the card a step is about stays bright, the other dims.
 */
const STEPS = [
  { title: 'Replay the history', text: 'Play the events of an aggregate one by one to see what happened at each step.', focus: 'event', diff: false },
  { title: 'View the state at any moment', text: 'See the state exactly as it was after each event.', focus: 'state', diff: false },
  { title: 'See what changed', text: 'The fields each event added or changed, highlighted.', focus: 'state', diff: true },
];

const STEP_MS = 4000;

/** The event timeline of one order, played as steps, one per event. Picking an event picks its step. */
@Component({
  selector: 'app-aggregate-replay',
  standalone: true,
  imports: [ShowcaseStepsComponent],
  template: `
    <app-showcase-steps [steps]="steps" [active]="player.step()" [playing]="player.playing()" [run]="player.run()"
                        [duration]="player.duration(player.step())" (pick)="player.go($event)" />

    <div class="mt-8 overflow-hidden rounded-2xl border border-surface-200 bg-surface-0 shadow-[0_24px_60px_-30px_rgba(15,23,42,0.35)] dark:border-surface-700 dark:bg-surface-900">

      <!-- Aggregate, and play / pause -->
      <div class="flex items-center gap-3 border-b border-surface-100 px-4 py-3 sm:px-6 dark:border-surface-800">
        <span class="text-xs font-medium uppercase tracking-widest text-surface-400">Order</span>
        <span class="font-mono text-sm text-surface-900 dark:text-surface-0">order-7f3a</span>
        <button type="button" (click)="player.toggle()"
                class="ml-auto inline-flex items-center gap-2 rounded-full border border-surface-200 px-3 py-1 text-xs font-medium text-surface-600 transition-colors hover:border-primary-400 hover:text-primary-600 dark:border-surface-700 dark:text-surface-300 dark:hover:text-primary-400">
          <i class="pi text-[10px]" [class]="player.playing() ? 'pi-pause' : 'pi-play'"></i>{{ player.playing() ? 'Pause' : 'Play' }}
        </button>
      </div>

      <!-- The timeline: one stop per event, the line filled up to the current one -->
      <div class="px-4 pt-6 pb-5 sm:px-6">
        <div class="relative">
          <div class="absolute top-[9px] right-[calc(100%/6)] left-[calc(100%/6)] h-0.5 rounded bg-surface-100 dark:bg-surface-800">
            <div class="h-full rounded bg-primary-500 transition-[width] duration-500 ease-out" [style.width.%]="current() / (events.length - 1) * 100"></div>
          </div>
          <ol class="relative grid grid-cols-3">
            @for (event of events; track event.type; let i = $index) {
              <li>
                <button type="button" (click)="player.go(i)" [attr.aria-label]="event.type" [attr.aria-current]="i === current()"
                        class="group flex w-full cursor-pointer flex-col items-center gap-2.5 outline-none">
                  <span class="flex h-5 items-center justify-center">
                    <span class="h-3.5 w-3.5 rounded-full border-2 transition-all duration-300"
                          [class]="(i <= current() ? 'border-primary-500 bg-primary-500' : 'border-surface-300 bg-surface-0 dark:border-surface-600 dark:bg-surface-900')
                                   + (i === current() ? ' scale-125 ring-4 ring-primary-500/20' : '')"></span>
                  </span>
                  <span class="hidden max-w-full truncate px-1 text-xs transition-colors sm:block"
                        [class]="i === current() ? 'font-semibold text-surface-900 dark:text-surface-0' : i < current() ? 'text-surface-500 group-hover:text-surface-800 dark:text-surface-400 dark:group-hover:text-surface-100' : 'text-surface-400 group-hover:text-surface-700 dark:text-surface-500 dark:group-hover:text-surface-200'">
                    {{ event.type }}
                  </span>
                  <span class="font-mono text-[10px] text-surface-400">{{ event.time }}</span>
                </button>
              </li>
            }
          </ol>
        </div>
      </div>

      <!-- The current event, and the state after it. Rebuilt on every change, so it fades in. -->
      @for (event of [events[current()]]; track event.type) {
        <div class="replay-fade grid border-t border-surface-100 md:grid-cols-2 dark:border-surface-800">
          <div class="p-4 transition-opacity duration-500 sm:p-6" [class.opacity-40]="step().focus === 'state'">
            <div class="flex flex-wrap items-center gap-2">
              <span class="rounded bg-primary-50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-primary-700 dark:bg-primary-950 dark:text-primary-300">Event {{ current() + 1 }}</span>
              <span class="font-semibold text-surface-900 dark:text-surface-0">{{ event.type }}</span>
              <span class="ml-auto font-mono text-[11px] text-surface-400">{{ event.time }}</span>
            </div>
            <div class="mt-4 text-xs font-medium uppercase tracking-widest text-surface-400">Payload</div>
            <div class="mt-2 space-y-1 font-mono text-xs">
              @for (entry of event.payload; track entry[0]) {
                <div class="flex flex-wrap gap-x-1.5"><span class="text-surface-400">{{ entry[0] }}:</span><span class="text-surface-700 dark:text-surface-200">{{ entry[1] }}</span></div>
              }
            </div>
          </div>

          <div class="border-t border-surface-100 bg-surface-50 p-4 transition-opacity duration-500 sm:p-6 md:border-t-0 md:border-l dark:border-surface-800 dark:bg-surface-950/50"
               [class.opacity-40]="step().focus === 'event'">
            <div class="flex items-center justify-between gap-3 text-xs">
              <span class="font-medium uppercase tracking-widest text-surface-400">State</span>
              <span class="font-mono text-surface-400">version {{ current() + 1 }} · {{ event.time }}</span>
            </div>
            <!-- Room for the longest state, so the card keeps its height -->
            <div class="mt-4 min-h-[7.5rem] space-y-1 font-mono text-xs">
              @for (field of event.state; track field.key) {
                <div class="flex flex-wrap items-center gap-x-1.5">
                  <span class="text-surface-400">{{ field.key }}:</span>
                  @if (step().diff && field.was) {
                    <span class="rounded bg-red-100 px-1 text-red-700 line-through dark:bg-red-950 dark:text-red-300">{{ field.was }}</span>
                  }
                  <span class="rounded px-1 transition-colors duration-300"
                        [class]="step().diff && (field.was || field.added) ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300' : 'text-surface-700 dark:text-surface-200'">{{ field.value }}</span>
                </div>
              }
            </div>
          </div>
        </div>
      }
    </div>
  `,
  styles: `
    .replay-fade { animation: replay-fade 0.35s ease-out; }
    @keyframes replay-fade { from { opacity: 0; transform: translateY(4px); } }
    @media (prefers-reduced-motion: reduce) { .replay-fade { animation: none; } }
  `,
})
export class AggregateReplayComponent {
  readonly events = EVENTS;
  readonly steps = STEPS;
  readonly player = new StepPlayer(STEPS.map(() => [STEP_MS]));
  readonly step = computed(() => STEPS[this.player.step()]);
  /** The event of the current step. */
  readonly current = this.player.step;

  constructor() {
    // Plays once it is well in view.
    const observer = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting) {
        this.player.play();
        observer.disconnect();
      }
    }, { threshold: 0.4 });
    observer.observe(inject(ElementRef).nativeElement);

    inject(DestroyRef).onDestroy(() => {
      observer.disconnect();
      this.player.destroy();
    });
  }
}
