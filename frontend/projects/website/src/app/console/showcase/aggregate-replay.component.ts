import { Component, DestroyRef, ElementRef, computed, inject, signal } from '@angular/core';

interface Field {
  key: string;
  value: string;
  /** The value before this moment, when it changed. */
  was?: string;
  /** Added by this moment. */
  added?: boolean;
}

interface Moment {
  kind: 'event' | 'command';
  type: string;
  time: string;
  /** For an event: the command that caused it. */
  via?: string;
  revision?: number;
  retried?: boolean;
  /** For a failed command: the cause. A failed command produces no events. */
  cause?: string;
  payload: [string, string][];
  version: number;
  state: Field[];
  /** The feature of the console this moment shows, an index into FEATURES. */
  feature: number;
}

/** The features of the console, in the order the order's life first shows them. */
const FEATURES = [
  { title: 'Event history', text: 'Every event, in the order it happened, with its payload.' },
  { title: 'Command correlation', text: 'Each event links back to the command that produced it.' },
  { title: 'Command outcomes', text: 'Each command with its outcome, and the cause when it failed.' },
  { title: 'State changes', text: 'The state after any event, and exactly what that event changed.' },
];

/**
 * The life of one order, as the console shows it: each event with the state it produced, and each command with its outcome.
 * Above it, the features of the console; the one the current moment shows is lit, and picking one jumps to it.
 * It plays by itself once it scrolls into view, until someone picks a moment or a feature.
 */
const MOMENTS: Moment[] = [
  {
    kind: 'event', type: 'OrderPlaced', time: '09:12', via: 'PlaceOrder',
    payload: [['customerId', '"customer-42"'], ['total', '169.40']],
    version: 1, feature: 0,
    state: [
      { key: 'status', value: '"PLACED"', added: true },
      { key: 'customerId', value: '"customer-42"', added: true },
      { key: 'total', value: '169.40', added: true },
    ],
  },
  {
    kind: 'event', type: 'OrderConfirmed', time: '09:12', via: 'PlaceOrder',
    payload: [['paymentReference', '"PSP-7F3A"']],
    version: 2, feature: 1,
    state: [
      { key: 'status', value: '"CONFIRMED"', was: '"PLACED"' },
      { key: 'customerId', value: '"customer-42"' },
      { key: 'total', value: '169.40' },
      { key: 'paymentReference', value: '"PSP-7F3A"', added: true },
    ],
  },
  {
    kind: 'command', type: 'ShipOrder', time: '10:47',
    cause: 'Carrier is temporarily unavailable.',
    payload: [['carrier', '"DHL"']],
    version: 2, feature: 2,
    state: [
      { key: 'status', value: '"CONFIRMED"' },
      { key: 'customerId', value: '"customer-42"' },
      { key: 'total', value: '169.40' },
      { key: 'paymentReference', value: '"PSP-7F3A"' },
    ],
  },
  {
    kind: 'event', type: 'OrderShipped', time: '11:02', via: 'ShipOrder', revision: 2, retried: true,
    payload: [['carrier', '"DHL"'], ['trackingNumber', '"JD0146"']],
    version: 3, feature: 3,
    state: [
      { key: 'status', value: '"SHIPPED"', was: '"CONFIRMED"' },
      { key: 'customerId', value: '"customer-42"' },
      { key: 'total', value: '169.40' },
      { key: 'paymentReference', value: '"PSP-7F3A"' },
      { key: 'trackingNumber', value: '"JD0146"', added: true },
    ],
  },
  {
    kind: 'event', type: 'OrderDelivered', time: '14:30', via: 'DeliverOrder',
    payload: [['signedBy', '"J. de Vries"']],
    version: 4, feature: 3,
    state: [
      { key: 'status', value: '"DELIVERED"', was: '"SHIPPED"' },
      { key: 'customerId', value: '"customer-42"' },
      { key: 'total', value: '169.40' },
      { key: 'paymentReference', value: '"PSP-7F3A"' },
      { key: 'trackingNumber', value: '"JD0146"' },
      { key: 'signedBy', value: '"J. de Vries"', added: true },
    ],
  },
  {
    kind: 'command', type: 'CancelOrder', time: '14:41',
    cause: 'Order has already been delivered.',
    payload: [['reason', '"CUSTOMER_REQUEST"']],
    version: 4, feature: 2,
    state: [
      { key: 'status', value: '"DELIVERED"' },
      { key: 'customerId', value: '"customer-42"' },
      { key: 'total', value: '169.40' },
      { key: 'paymentReference', value: '"PSP-7F3A"' },
      { key: 'trackingNumber', value: '"JD0146"' },
      { key: 'signedBy', value: '"J. de Vries"' },
    ],
  },
];

const STEP_MS = 3000;

@Component({
  selector: 'app-aggregate-replay',
  standalone: true,
  template: `
    <!-- The features: two by two on a phone, where only their titles show, and in a row from lg up -->
    <div class="grid grid-cols-2 gap-x-6 gap-y-4 lg:grid-cols-4">
      @for (feature of features; track feature.title; let i = $index) {
        <button type="button" (click)="show(i)" [attr.aria-pressed]="i === current().feature"
                class="group cursor-pointer border-l-2 py-1 pl-4 text-left transition-colors duration-300"
                [class]="i === current().feature ? 'border-primary-500' : 'border-surface-200 hover:border-surface-400 dark:border-surface-700 dark:hover:border-surface-500'">
          <span class="block text-sm font-semibold transition-colors duration-300"
                [class]="i === current().feature ? 'text-surface-900 dark:text-surface-0' : 'text-surface-500 group-hover:text-surface-800 dark:text-surface-400 dark:group-hover:text-surface-100'">{{ feature.title }}</span>
          <span class="mt-1 hidden text-sm leading-relaxed text-surface-500 sm:block dark:text-surface-400">{{ feature.text }}</span>
        </button>
      }
    </div>

    <div class="mt-10 overflow-hidden rounded-2xl border border-surface-200 bg-surface-0 shadow-[0_24px_60px_-30px_rgba(15,23,42,0.35)] dark:border-surface-700 dark:bg-surface-900">

      <!-- Aggregate, and play / pause -->
      <div class="flex items-center gap-3 border-b border-surface-100 px-4 py-3 sm:px-6 dark:border-surface-800">
        <span class="text-xs font-medium uppercase tracking-widest text-surface-400">Order</span>
        <span class="font-mono text-sm text-surface-900 dark:text-surface-0">order-7f3a</span>
        <button type="button" (click)="togglePlay()"
                class="ml-auto inline-flex items-center gap-2 rounded-full border border-surface-200 px-3 py-1 text-xs font-medium text-surface-600 transition-colors hover:border-primary-400 hover:text-primary-600 dark:border-surface-700 dark:text-surface-300 dark:hover:text-primary-400">
          <i class="pi text-[10px]" [class]="playing() ? 'pi-pause' : 'pi-play'"></i>{{ playing() ? 'Pause' : 'Replay' }}
        </button>
      </div>

      <!-- The track: one stop per moment, the line filled up to the selected one -->
      <div class="px-4 pt-10 pb-5 sm:px-6">
        <div class="relative">
          <div class="absolute top-[9px] right-[calc(100%/12)] left-[calc(100%/12)] h-0.5 rounded bg-surface-100 dark:bg-surface-800">
            <div class="h-full rounded bg-primary-500 transition-[width] duration-500 ease-out" [style.width.%]="progress()"></div>
          </div>
          <!-- The failed ShipOrder (third stop) was retried, and produced OrderShipped (fourth stop) -->
          <div class="absolute -top-3 left-[calc(100%*5/12)] right-[calc(100%*5/12)] h-3.5 rounded-t-lg border-x border-t border-dashed transition-colors duration-300"
               [class]="active() >= 3 ? 'border-primary-400' : 'border-surface-300 dark:border-surface-600'">
            <span class="absolute -top-2 left-1/2 -translate-x-1/2 bg-surface-0 px-1.5 text-[10px] leading-none dark:bg-surface-900"
                  [class]="active() >= 3 ? 'text-primary-600 dark:text-primary-400' : 'text-surface-400'">retried</span>
          </div>
          <ol class="relative grid grid-cols-6">
            @for (moment of moments; track $index; let i = $index) {
              <li>
                <button type="button" (click)="select(i)" [attr.aria-label]="moment.type" [attr.aria-current]="i === active()"
                        class="group flex w-full flex-col items-center gap-2.5 outline-none">
                  <span class="flex h-5 items-center justify-center">
                    @if (moment.kind === 'event') {
                      <span class="h-3.5 w-3.5 rounded-full border-2 transition-all duration-300"
                            [class]="(i <= active() ? 'border-primary-500 bg-primary-500' : 'border-surface-300 bg-surface-0 dark:border-surface-600 dark:bg-surface-900')
                                     + (i === active() ? ' scale-125 ring-4 ring-primary-500/20' : '')"></span>
                    } @else {
                      <!-- A command that failed: a red square, it produced nothing on the line -->
                      <span class="h-3 w-3 rotate-45 rounded-[3px] border-2 transition-all duration-300"
                            [class]="(i <= active() ? 'border-red-500 bg-red-500' : 'border-red-300 bg-surface-0 dark:border-red-800 dark:bg-surface-900')
                                     + (i === active() ? ' scale-125 ring-4 ring-red-500/20' : '')"></span>
                    }
                  </span>
                  <span class="hidden max-w-full truncate px-1 text-[11px] transition-colors sm:block"
                        [class]="i === active() ? 'font-semibold text-surface-900 dark:text-surface-0' : 'text-surface-400 group-hover:text-surface-700 dark:group-hover:text-surface-200'">
                    {{ moment.type }}
                  </span>
                  <span class="hidden font-mono text-[10px] text-surface-400 sm:block">{{ moment.time }}</span>
                </button>
              </li>
            }
          </ol>
        </div>
      </div>

      <!-- The selected moment: what happened, and the order after it. Rebuilt on every change, so it fades in. -->
      @for (moment of [current()]; track moment.type + moment.time) {
        <div class="replay-fade grid border-t border-surface-100 md:grid-cols-2 dark:border-surface-800">
          <div class="p-4 sm:p-6">
            <div class="flex flex-wrap items-center gap-2">
              <span class="rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider"
                    [class]="moment.kind === 'event' ? 'bg-primary-50 text-primary-700 dark:bg-primary-950 dark:text-primary-300' : 'bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-300'">
                {{ moment.kind === 'event' ? 'Event' : 'Command failed' }}
              </span>
              <span class="font-semibold text-surface-900 dark:text-surface-0">{{ moment.type }}</span>
              @if (moment.revision) {
                <span class="font-mono text-[11px] text-surface-400">rev {{ moment.revision }}</span>
              }
            </div>

            @if (moment.via) {
              <p class="mt-2 text-xs text-surface-500 dark:text-surface-400">
                Produced by <span class="font-medium text-surface-700 dark:text-surface-200">{{ moment.via }}</span>@if (moment.retried) {, retried from the console}@for (sibling of producedWith(moment); track sibling) {, together with <span class="font-medium text-surface-700 dark:text-surface-200">{{ sibling }}</span>}
              </p>
            } @else {
              <p class="mt-2 text-xs text-surface-500 dark:text-surface-400">No events produced</p>
            }

            <div class="mt-4 space-y-1 font-mono text-xs">
              @for (entry of moment.payload; track entry[0]) {
                <div class="flex gap-1.5"><span class="text-surface-400">{{ entry[0] }}:</span><span class="text-surface-700 dark:text-surface-200">{{ entry[1] }}</span></div>
              }
            </div>

            @if (moment.cause) {
              <div class="mt-4 flex items-center justify-between gap-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs dark:border-red-900 dark:bg-red-950/60">
                <span class="text-red-700 dark:text-red-300">{{ moment.cause }}</span>
                <span class="inline-flex shrink-0 items-center gap-1.5 rounded-full border border-red-300 px-2.5 py-0.5 font-medium text-red-600 dark:border-red-800 dark:text-red-400">
                  <i class="pi pi-refresh text-[10px]"></i>Retry
                </span>
              </div>
            }
          </div>

          <div class="border-t border-surface-100 bg-surface-50 p-4 sm:p-6 md:border-t-0 md:border-l dark:border-surface-800 dark:bg-surface-950/50">
            <div class="flex items-center justify-between text-xs">
              <span class="font-medium uppercase tracking-widest text-surface-400">State</span>
              <span class="font-mono text-surface-400">version {{ moment.version }}</span>
            </div>
            <!-- Room for the longest state, so the card keeps its height -->
            <div class="mt-4 min-h-[8.5rem] space-y-1 font-mono text-xs" [class.opacity-60]="moment.kind === 'command'">
              @for (field of moment.state; track field.key) {
                <div class="flex flex-wrap items-center gap-x-1.5">
                  <span class="text-surface-400">{{ field.key }}:</span>
                  @if (field.was) {
                    <span class="rounded bg-red-100 px-1 text-red-700 line-through dark:bg-red-950 dark:text-red-300">{{ field.was }}</span>
                  }
                  <span class="rounded px-1"
                        [class]="field.was || field.added ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300' : 'text-surface-700 dark:text-surface-200'">{{ field.value }}</span>
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
  readonly moments = MOMENTS;
  readonly features = FEATURES;
  readonly active = signal(0);
  readonly playing = signal(false);
  readonly current = computed(() => MOMENTS[this.active()]);
  /** How far the line is filled: from the first stop to the selected one. */
  readonly progress = computed(() => this.active() / (MOMENTS.length - 1) * 100);

  private timer?: ReturnType<typeof setInterval>;

  constructor() {
    const host = inject(ElementRef).nativeElement as HTMLElement;
    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    // Plays once it is in view, unless motion is reduced.
    const observer = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting && !reducedMotion) {
        this.play();
        observer.disconnect();
      }
    }, { threshold: 0.5 });
    observer.observe(host);

    inject(DestroyRef).onDestroy(() => {
      observer.disconnect();
      this.stop();
    });
  }

  select(index: number) {
    this.stop();
    this.active.set(index);
  }

  /** Jumps to the first moment that shows the feature. */
  show(feature: number) {
    this.select(MOMENTS.findIndex(moment => moment.feature === feature));
  }

  /** The other events the command behind this event produced. */
  producedWith(moment: Moment): string[] {
    return MOMENTS.filter(other => other !== moment && other.kind === 'event' && other.via === moment.via).map(other => other.type);
  }

  togglePlay() {
    if (this.playing()) {
      this.stop();
    } else {
      // At the end, a replay starts from the first event again.
      if (this.active() === MOMENTS.length - 1) this.active.set(0);
      this.play();
    }
  }

  private play() {
    this.stop();
    this.playing.set(true);
    this.timer = setInterval(() => {
      if (this.active() === MOMENTS.length - 1) {
        this.stop();
      } else {
        this.active.update(i => i + 1);
      }
    }, STEP_MS);
  }

  private stop() {
    clearInterval(this.timer);
    this.playing.set(false);
  }
}
