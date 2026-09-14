import { Component, DestroyRef, ElementRef, computed, inject, signal } from '@angular/core';

/** Steps per cycle; the cycle is split in four quarters, one per block. */
const STEPS = 40;
const STEP_MS = 300;

/**
 * The event sourcing features, as a row of four blocks that play in turn: events build the state, a snapshot shortens
 * the replay, an old event is upcast, and the partitions spread over the instances. One 12-second cycle, a quarter per block.
 * Before it scrolls into view, or with reduced motion, every block shows its end state.
 */
@Component({
  selector: 'app-feature-story',
  standalone: true,
  template: `
    <div class="grid gap-6 sm:grid-cols-2 lg:grid-cols-4">

      <!-- 1. State from events: the events arrive, and the state follows -->
      <article class="s-card" [class.s-lit]="block() === 0">
        <div class="s-stage" aria-hidden="true">
          <div class="flex w-full max-w-44 flex-col items-center">
            <div class="s-box w-full py-1">
              @for (event of events; track event; let i = $index) {
                <div class="s-in flex items-center gap-2 px-3 py-1" [class.s-out]="before(1 + i * 2)">
                  <span class="h-1.5 w-1.5 rounded-full bg-primary-500"></span>{{ event }}
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
        <div class="px-6 pt-4 pb-6">
          <h3 class="s-title">State from events</h3>
          <p class="s-text">The state is rebuilt from the events, so the event history is the source of truth.</p>
        </div>
        <span class="s-next" aria-hidden="true"><i class="pi pi-chevron-right text-[9px]"></i></span>
      </article>

      <!-- 2. Snapshots: a snapshot is taken, and the events before it are no longer read -->
      <article class="s-card" [class.s-lit]="block() === 1">
        <div class="s-stage" aria-hidden="true">
          <div class="s-box w-full max-w-44 py-1">
            @for (i of [1, 2, 3]; track i) {
              <div class="s-in flex items-center gap-2 px-3 py-1" [class.s-skipped]="!before(14)">
                <span class="h-1.5 w-1.5 rounded-full bg-surface-300 dark:bg-surface-600"></span>event {{ i }}
              </div>
            }
            <div class="s-in mx-1 flex items-center gap-2 rounded-md bg-primary-500 px-2 py-1 font-medium text-white" [class.s-out]="before(12)">
              <i class="pi pi-camera text-[10px]"></i>snapshot
            </div>
            @for (i of [4, 5]; track i) {
              <div class="flex items-center gap-2 px-3 py-1 font-medium">
                <span class="h-1.5 w-1.5 rounded-full bg-primary-500"></span>event {{ i }}
              </div>
            }
          </div>
        </div>
        <div class="px-6 pt-4 pb-6">
          <h3 class="s-title">Snapshots</h3>
          <p class="s-text">Long histories are read from the latest snapshot instead of from the first event.</p>
        </div>
        <span class="s-next" aria-hidden="true"><i class="pi pi-chevron-right text-[9px]"></i></span>
      </article>

      <!-- 3. Event upcasting: the old revision is migrated as it is read -->
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
        <div class="px-6 pt-4 pb-6">
          <h3 class="s-title">Event upcasting</h3>
          <p class="s-text">Change the structure of an event, and older events are migrated as they are read.</p>
        </div>
        <span class="s-next" aria-hidden="true"><i class="pi pi-chevron-right text-[9px]"></i></span>
      </article>

      <!-- 4. Distributed: the partitions go to the running instances, one instance at a time -->
      <article class="s-card" [class.s-lit]="block() === 3">
        <div class="s-stage" aria-hidden="true">
          <div class="flex gap-2">
            @for (partitions of instances; track $index; let i = $index) {
              <div class="s-box flex w-14 flex-col items-center gap-1.5 px-1.5 py-2">
                <i class="pi pi-server text-[10px] text-surface-400"></i>
                @for (partition of partitions; track partition) {
                  <span class="s-in w-full rounded bg-primary-100 py-1 text-center font-mono font-semibold text-primary-700 dark:bg-primary-900/60 dark:text-primary-300"
                        [class.s-out]="before(31 + i * 2)">{{ partition }}</span>
                }
              </div>
            }
          </div>
        </div>
        <div class="px-6 pt-4 pb-6">
          <h3 class="s-title">Distributed</h3>
          <p class="s-text">Aggregates are spread over Kafka partitions and shared by all running instances.</p>
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
    .s-title { font-size: 1rem; font-weight: 600; color: var(--p-surface-900); }
    .s-text { margin-top: 0.5rem; font-size: 0.875rem; line-height: 1.6; color: var(--p-surface-500); }
    .s-stage {
      display: flex; align-items: center; justify-content: center; height: 11rem; margin: 0.5rem 0.5rem 0; padding: 0 1rem;
      border-radius: 0.75rem; font-size: 0.75rem; color: var(--p-surface-700);
      background: var(--p-surface-50) radial-gradient(var(--p-surface-200) 1px, transparent 1px) 0 0 / 14px 14px;
    }
    .s-box { border: 1px solid var(--p-surface-200); border-radius: 0.5rem; background: var(--p-surface-0); box-shadow: 0 1px 2px rgb(0 0 0 / 0.05); }
    .s-box-new { border-color: var(--p-primary-300); }
    .s-next {
      display: none; position: absolute; z-index: 1; top: 5.25rem; right: -1.1rem; width: 1.25rem; height: 1.25rem;
      align-items: center; justify-content: center; border-radius: 9999px;
      border: 1px solid var(--p-surface-200); background: var(--p-surface-0); color: var(--p-surface-400);
    }
    @media (min-width: 1024px) { .s-next { display: flex; } }
    @media (prefers-color-scheme: dark) {
      .s-card { border-color: var(--p-surface-800); background: var(--p-surface-900); }
      .s-title { color: var(--p-surface-0); }
      .s-text { color: var(--p-surface-400); }
      .s-stage { color: var(--p-surface-200); background: var(--p-surface-950) radial-gradient(var(--p-surface-800) 1px, transparent 1px) 0 0 / 14px 14px; }
      .s-box, .s-next { border-color: var(--p-surface-700); background: var(--p-surface-900); }
      .s-box-new { border-color: var(--p-primary-800); }
    }

    /* Something that hasn't happened yet in this cycle, and the events a snapshot made unnecessary to read */
    .s-in { transition: opacity 0.35s, transform 0.35s; }
    .s-out { opacity: 0; transform: translateY(-4px); }
    .s-skipped { opacity: 0.4; text-decoration: line-through; }
  `,
})
export class FeatureStoryComponent {
  readonly events = ['OrderPlaced', 'ItemAdded', 'OrderPaid'];
  readonly instances = [['P0', 'P3'], ['P1', 'P4'], ['P2', 'P5']];

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
