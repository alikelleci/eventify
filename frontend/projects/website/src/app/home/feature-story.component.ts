import { Component, DestroyRef, ElementRef, computed, inject, signal } from '@angular/core';

/** Steps per cycle; the cycle is split in four quarters, one per block. */
const STEPS = 40;
const STEP_MS = 300;

/**
 * Four event-sourcing concepts, shown as an animated story.
 * Before it scrolls into view, or with reduced motion, every block shows its end state.
 */
@Component({
  selector: 'app-feature-story',
  standalone: true,
  template: `
    <!-- Two by two on desktop; each block is a wide card with its feature on the left and its scene on the right -->
    <div class="grid gap-6 lg:grid-cols-2">

      <!-- 1. Event replay: the events arrive on the timeline, and the state follows -->
      <article class="s-card" [class.s-lit]="block() === 0">
        <div class="s-stage" aria-hidden="true">
          <div class="flex w-full max-w-44 flex-col items-center">
            <div class="s-box w-full py-1.5">
              @for (event of events; track event; let i = $index, first = $first, last = $last) {
                <div class="s-in s-row" [class.s-out]="before(1 + i * 2)">
                  <span class="s-line" [class]="first ? 'top-1/2 bottom-0' : last ? 'top-0 bottom-1/2' : 'inset-y-0'"></span>
                  <span class="s-dot" [class]="last ? 's-dot-on' : 's-dot-off'"></span>
                  <span [class]="last ? 'font-medium text-primary-700 dark:text-primary-300' : 'text-surface-600 dark:text-surface-300'">{{ event }}</span>
                </div>
              }
            </div>
            <i class="s-in pi pi-arrow-down my-1.5 text-[10px] text-surface-400" [class.s-out]="before(7)"></i>
            <div class="s-in flex items-center gap-2 rounded-full border border-primary-300 bg-primary-50 px-3 py-1 font-medium text-primary-700 dark:border-primary-800 dark:bg-primary-950 dark:text-primary-300"
                 [class.s-out]="before(8)">
              state <span class="font-mono text-[10px] opacity-70">v3</span>
            </div>
          </div>
        </div>
        <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
          <h3 class="s-title">Event replay</h3>
          <p class="s-text">Replay retained events to rebuild state and understand how an aggregate reached it.</p>
        </div>
      </article>

      <!-- 2. Snapshots: a snapshot is taken on the timeline, and the events before it are no longer read -->
      <article class="s-card" [class.s-lit]="block() === 1">
        <div class="s-stage" aria-hidden="true">
          <div class="s-box w-full max-w-44 py-1.5">
            @for (i of [1, 2, 3]; track i; let first = $first) {
              <div class="s-in s-row" [class.s-skipped]="!before(14)">
                <span class="s-line" [class]="first ? 'top-1/2 bottom-0' : 'inset-y-0'"></span>
                <span class="s-dot s-dot-off"></span>
                <span class="text-surface-600 dark:text-surface-300">event {{ i }}</span>
              </div>
            }
            <!-- The line runs on while the snapshot is still to come -->
            <div class="s-row">
              <span class="s-line inset-y-0"></span>
              <span class="s-in absolute left-[9px] flex h-[15px] w-[15px] items-center justify-center rounded-full bg-primary-500 text-white" [class.s-out]="before(12)">
                <i class="pi pi-camera text-[7px]"></i>
              </span>
              <span class="s-in ml-1 font-medium text-primary-700 dark:text-primary-300" [class.s-out]="before(12)">snapshot</span>
            </div>
            @for (i of [4, 5]; track i; let last = $last) {
              <div class="s-row">
                <span class="s-line" [class]="last ? 'top-0 bottom-1/2' : 'inset-y-0'"></span>
                <span class="s-dot s-dot-on"></span>
                <span class="font-medium text-surface-900 dark:text-surface-0">event {{ i }}</span>
              </div>
            }
          </div>
        </div>
        <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
          <h3 class="s-title">Snapshots</h3>
          <p class="s-text">Save the state now and then, so only the latest events need to be replayed.</p>
        </div>
      </article>

      <!-- 3. Upcasting: the old revision is migrated as it is read -->
      <article class="s-card" [class.s-lit]="block() === 2">
        <div class="s-stage" aria-hidden="true">
          <div class="flex w-full max-w-44 flex-col items-center font-mono text-[11px]">
            <div class="s-box w-full px-3 py-2 text-surface-500 dark:text-surface-400">
              <div class="mb-0.5 font-sans text-[10px] font-semibold uppercase tracking-wider">rev 1</div>
              id, customer
            </div>
            <span class="s-in my-1.5 font-sans text-[10px] text-surface-400" [class.s-out]="before(22)"><i class="pi pi-arrow-down mr-1 text-[9px]"></i>upcast</span>
            <div class="s-in s-box s-box-new w-full px-3 py-2" [class.s-out]="before(23)">
              <div class="mb-0.5 font-sans text-[10px] font-semibold uppercase tracking-wider text-primary-600 dark:text-primary-400">rev 2</div>
              id, customer
              <div class="s-in -mx-1 mt-0.5 rounded bg-emerald-100 px-1 text-emerald-700 dark:bg-emerald-900/50 dark:text-emerald-300" [class.s-out]="before(25)">+ address</div>
            </div>
          </div>
        </div>
        <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
          <h3 class="s-title">Upcasting</h3>
          <p class="s-text">Evolve event data deliberately. Older revisions can be upgraded while they are read.</p>
        </div>
      </article>

      <!-- 4. Plain Java: commands become events, then events update state -->
      <article class="s-card" [class.s-lit]="block() === 3">
        <div class="s-stage" aria-hidden="true">
          <div class="flex w-full max-w-44 flex-col items-center gap-2 font-mono text-[10px]">
            <div class="s-in s-box w-full px-3 py-2 text-surface-600 dark:text-surface-300" [class.s-out]="before(31)">
              <span class="text-primary-600 dark:text-primary-400">@CommandHandler</span> PlaceOrder
            </div>
            <i class="s-in pi pi-arrow-down text-[9px] text-surface-400" [class.s-out]="before(33)"></i>
            <div class="s-in w-full rounded bg-primary-100 px-3 py-2 font-semibold text-primary-700 dark:bg-primary-900/60 dark:text-primary-300" [class.s-out]="before(34)">
              OrderPlaced
            </div>
            <i class="s-in pi pi-arrow-down text-[9px] text-surface-400" [class.s-out]="before(36)"></i>
            <div class="s-in s-box w-full px-3 py-2 text-surface-600 dark:text-surface-300" [class.s-out]="before(37)">
              <span class="text-primary-600 dark:text-primary-400">@EventSourcingHandler</span> Order
            </div>
          </div>
        </div>
        <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
          <h3 class="s-title">Plain Java</h3>
          <p class="s-text">Commands decide, events describe what happened, and event handlers rebuild state.</p>
        </div>
      </article>
    </div>
  `,
  styles: `
    .s-card {
      position: relative; display: flex; flex-direction: column; border-radius: 1rem;
      border: 1px solid var(--p-surface-200); background: var(--p-surface-0); transition: box-shadow 0.3s;
    }
    .s-lit { box-shadow: 0 0 0 2px var(--p-primary-400); }
    .s-title { font-size: 1rem; font-weight: 600; letter-spacing: -0.01em; color: var(--p-surface-900); }
    .s-text { margin-top: 0.5rem; font-size: 0.875rem; line-height: 1.6; color: var(--p-surface-500); }
    .s-stage {
      display: flex; align-items: center; justify-content: center; height: 12rem; margin: 0.5rem 0.5rem 0; padding: 0 1rem;
      border-radius: 0.75rem; font-size: 0.75rem; color: var(--p-surface-700);
      background: var(--p-surface-50) radial-gradient(var(--p-surface-200) 1px, transparent 1px) 0 0 / 14px 14px;
    }
    /* From sm up the card is a row: the text on the left, the scene filling the rest */
    @media (min-width: 640px) {
      .s-card { flex-direction: row; }
      .s-stage { flex: 1; height: auto; min-height: 12rem; margin: 0.5rem; }
    }
    .s-box { border: 1px solid var(--p-surface-200); border-radius: 0.5rem; background: var(--p-surface-0); box-shadow: 0 1px 2px rgb(0 0 0 / 0.05); }
    .s-box-new { border-color: var(--p-primary-300); }

    /* A timeline: one row per event, a dot on a line that joins them */
    .s-row { position: relative; display: flex; align-items: center; padding: 0.25rem 0.75rem 0.25rem 1.75rem; }
    .s-line { position: absolute; left: 16px; width: 1px; background: var(--p-surface-200); }
    .s-dot { position: absolute; left: 12px; width: 9px; height: 9px; border-radius: 9999px; border: 2px solid var(--p-surface-0); }
    .s-dot-on { background: var(--p-primary-500); }
    .s-dot-off { background: var(--p-surface-300); }
    @media (prefers-color-scheme: dark) {
      .s-card { border-color: var(--p-surface-800); background: var(--p-surface-900); }
      .s-title { color: var(--p-surface-0); }
      .s-text { color: var(--p-surface-400); }
      .s-stage { color: var(--p-surface-200); background: var(--p-surface-950) radial-gradient(var(--p-surface-800) 1px, transparent 1px) 0 0 / 14px 14px; }
      .s-box { border-color: var(--p-surface-700); background: var(--p-surface-900); }
      .s-box-new { border-color: var(--p-primary-800); }
      .s-line { background: var(--p-surface-700); }
      .s-dot { border-color: var(--p-surface-900); }
      .s-dot-off { background: var(--p-surface-600); }
    }

    /* Something that hasn't happened yet in this cycle, and the events a snapshot made unnecessary to read */
    .s-in { transition: opacity 0.35s, transform 0.35s; }
    .s-out { opacity: 0; transform: translateY(-4px); }
    .s-skipped { opacity: 0.4; text-decoration: line-through; }
  `,
})
export class FeatureStoryComponent {
  readonly events = ['OrderPlaced', 'ItemAdded', 'OrderPaid'];
  /** The step in the cycle, or null while not playing: then everything shows its end state. */
  private readonly step = signal<number | null>(null);
  /** The block whose quarter it is. */
  readonly block = computed(() => { const s = this.step(); return s === null ? -1 : Math.floor(s / (STEPS / 4)); });

  constructor() {
    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    let timer: ReturnType<typeof setInterval> | undefined;

    // The story starts from the beginning once the top of the blocks is well in view
    // (not a share of them: on a phone the blocks are taller than the screen).
    const observer = new IntersectionObserver(([entry]) => {
      if (!entry.isIntersecting) return;
      observer.disconnect();
      if (reducedMotion) return;
      this.step.set(0);
      timer = setInterval(() => this.step.update(s => ((s ?? 0) + 1) % STEPS), STEP_MS);
    }, { rootMargin: '0px 0px -25% 0px' });
    observer.observe(inject(ElementRef).nativeElement);

    inject(DestroyRef).onDestroy(() => {
      observer.disconnect();
      clearInterval(timer);
    });
  }

  /** Whether the cycle hasn't reached this step yet. */
  before(step: number): boolean {
    const s = this.step();
    return s !== null && s < step;
  }
}
