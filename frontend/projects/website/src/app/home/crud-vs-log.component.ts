import { Component, computed } from '@angular/core';
import { StepPlayer, autoplay } from '../shared/step-player';

/** The same order going through the same three changes, once as a row and once as events. */
const CHANGES = [
  { status: 'PLACED', event: 'OrderPlaced', time: '09:12' },
  { status: 'CONFIRMED', event: 'OrderConfirmed', time: '09:14' },
  { status: 'SHIPPED', event: 'OrderShipped', time: '11:02' },
];

/**
 * A row that is updated beside a log that is appended to, in step: the row keeps only the last value,
 * the log keeps every decision and derives the same state from them.
 */
@Component({
  selector: 'app-crud-vs-log',
  standalone: true,
  host: { 'aria-hidden': 'true' },
  template: `
    <div class="grid gap-6 md:grid-cols-2">

      <!-- The row: each change overwrites the one before -->
      <div class="v-card">
        <div class="flex items-center justify-between">
          <span class="v-label">Updating a row</span>
          <span class="v-tag bg-surface-100 text-surface-500 dark:bg-surface-800 dark:text-surface-400">UPDATE orders</span>
        </div>
        <div class="mt-5 overflow-hidden rounded-lg border border-surface-200 font-mono text-xs dark:border-surface-700">
          <div class="grid grid-cols-[1fr_1.2fr_0.8fr] bg-surface-50 px-3 py-2 text-[10px] uppercase tracking-wider text-surface-400 dark:bg-surface-800/60">
            <span>id</span><span>status</span><span>updated</span>
          </div>
          <div class="grid grid-cols-[1fr_1.2fr_0.8fr] items-center border-t border-surface-100 px-3 py-3 dark:border-surface-800">
            <span class="text-surface-700 dark:text-surface-200">order-7f3a</span>
            <span class="relative h-5 overflow-hidden">
              @for (change of [current()]; track change.status) {
                <span class="v-value absolute inset-y-0 left-0 flex items-center rounded bg-primary-50 px-1.5 font-semibold text-primary-700 dark:bg-primary-950 dark:text-primary-300">{{ change.status }}</span>
              }
            </span>
            @for (change of [current()]; track change.time) {
              <span class="v-fade text-surface-500 dark:text-surface-400">{{ change.time }}</span>
            }
          </div>
        </div>
        <!-- What the row forgot: struck out, then gone -->
        <div class="mt-4 flex min-h-7 flex-wrap items-center gap-2 text-xs text-surface-400">
          <span>Previous values:</span>
          @for (change of overwritten(); track change.status) {
            <span class="v-lost rounded border border-dashed border-surface-300 px-1.5 font-mono text-[11px] line-through dark:border-surface-600">{{ change.status }}</span>
          } @empty {
            <span class="italic">none</span>
          }
        </div>
        <p class="mt-4 border-t border-surface-100 pt-4 text-sm text-surface-500 dark:border-surface-800 dark:text-surface-400">
          <i class="pi pi-times-circle mr-1.5 text-xs text-red-400"></i>Only the latest value survives. Why it changed is lost.
        </p>
      </div>

      <!-- The log: each change is a new event, and the state follows from them -->
      <div class="v-card v-card-lit">
        <div class="flex items-center justify-between">
          <span class="v-label">Appending events</span>
          <span class="v-tag bg-primary-50 text-primary-700 dark:bg-primary-950 dark:text-primary-300">orders.events</span>
        </div>
        <ol class="mt-5 space-y-1.5">
          @for (change of appended(); track change.event; let i = $index; let last = $last) {
            <li class="v-append relative flex items-center gap-3 rounded-lg border px-3 py-2 text-xs"
                [class]="last ? 'border-primary-300 bg-primary-50/60 dark:border-primary-800 dark:bg-primary-950/40' : 'border-surface-200 dark:border-surface-700'">
              <span class="font-mono text-[10px] text-surface-400">#{{ i + 1 }}</span>
              <span class="font-semibold text-surface-800 dark:text-surface-100">{{ change.event }}</span>
              <span class="ml-auto font-mono text-[10px] text-surface-400">{{ change.time }}</span>
            </li>
          }
          <!-- Room for the events still to come, so the card keeps its height -->
          @for (slot of empty(); track slot) {
            <li class="rounded-lg border border-dashed border-surface-200 px-3 py-2 text-xs text-transparent dark:border-surface-800">·</li>
          }
        </ol>
        <div class="mt-4 flex min-h-7 items-center gap-2 font-mono text-xs text-surface-500 dark:text-surface-400">
          <span class="font-sans">State:</span>
          @for (change of [current()]; track change.status) {
            <span class="v-fade">status <span class="font-semibold text-primary-700 dark:text-primary-300">{{ change.status }}</span> · from {{ appended().length }} event{{ appended().length > 1 ? 's' : '' }}</span>
          }
        </div>
        <p class="mt-4 border-t border-surface-100 pt-4 text-sm text-surface-500 dark:border-surface-800 dark:text-surface-400">
          <i class="pi pi-check-circle mr-1.5 text-xs text-primary-500"></i>Every decision is kept. The state is derived from them.
        </p>
      </div>
    </div>
  `,
  styles: `
    .v-card { border-radius: 1rem; border: 1px solid var(--p-surface-200); background: var(--p-surface-0); padding: 1.5rem; }
    .v-card-lit { border-color: var(--p-primary-200); box-shadow: 0 24px 60px -32px rgb(20 184 166 / 0.45); }
    .v-label { font-size: 0.875rem; font-weight: 600; color: var(--p-surface-900); }
    .v-tag { border-radius: 0.375rem; padding: 0.125rem 0.5rem; font-family: ui-monospace, monospace; font-size: 10px; }
    @media (prefers-color-scheme: dark) {
      .v-card { border-color: var(--p-surface-800); background: var(--p-surface-900); }
      .v-card-lit { border-color: var(--p-primary-900); box-shadow: 0 24px 60px -32px rgb(20 184 166 / 0.3); }
      .v-label { color: var(--p-surface-0); }
    }

    @media (prefers-reduced-motion: no-preference) {
      .v-value { animation: v-value 0.6s var(--ease-out) both; }
      .v-fade { animation: v-fade 0.5s var(--ease-out) both; }
      .v-lost { animation: v-lost 2.4s var(--ease-out) both; }
      .v-append { animation: v-append 0.6s var(--ease-spring) both; }
    }
    @keyframes v-value { from { transform: translateY(110%); opacity: 0; } }
    @keyframes v-fade { from { opacity: 0; } }
    /* Struck out, and then it fades: the row doesn't keep it */
    @keyframes v-lost { 0% { opacity: 0; transform: translateY(-4px); } 15% { opacity: 1; transform: none; } 70% { opacity: 1; } 100% { opacity: 0.25; } }
    @keyframes v-append { from { opacity: 0; transform: translateY(-8px) scale(0.97); } }
  `,
})
export class CrudVsLogComponent {
  readonly player = new StepPlayer([CHANGES.map((_, i) => i === CHANGES.length - 1 ? 3200 : 1800)]);

  readonly current = computed(() => CHANGES[this.player.frame()]);
  readonly appended = computed(() => CHANGES.slice(0, this.player.frame() + 1));
  readonly empty = computed(() => CHANGES.slice(this.player.frame() + 1).map(change => change.event));
  /** The values this frame's update overwrote: just the one before, the rest the row forgot long ago. */
  readonly overwritten = computed(() => CHANGES.slice(Math.max(0, this.player.frame() - 1), this.player.frame()));

  constructor() {
    autoplay(this.player, 0.35);
  }
}
