import { Component, DestroyRef, ElementRef, inject, signal } from '@angular/core';

/**
 * The questions the console answers, as four wide blocks, two by two, that play one story in turn: the events arrive, the state changes,
 * a command fails, and a command is traced to the events it produced. One 12-second cycle, a quarter per block.
 * Without the animation (before it scrolls into view, or with reduced motion) every block shows its end state.
 */
@Component({
  selector: 'app-console-questions',
  standalone: true,
  host: { '[class.playing]': 'playing()' },
  template: `
    <!-- Two by two on desktop; each block is a wide card with its question on the left and its scene on the right -->
    <div class="grid gap-6 lg:grid-cols-2">

      <!-- 1. What happened: the events arrive, oldest first -->
      <article class="q-card q-focus-1 relative flex flex-col rounded-2xl border sm:flex-row border-surface-200 bg-surface-0 dark:border-surface-800 dark:bg-surface-900">
        <div class="q-stage" aria-hidden="true">
          <div class="w-full max-w-48 rounded-lg border border-surface-200 bg-surface-0 py-1.5 text-xs shadow-sm dark:border-surface-700 dark:bg-surface-900">
            @for (event of events; track event; let first = $first, last = $last, i = $index) {
              <div class="relative flex items-center py-1.5 pr-3 pl-7" [class]="'q-arrive-' + (events.length - i)">
                <span class="absolute left-[15px] w-px bg-surface-200 dark:bg-surface-700" [class]="first ? 'top-1/2 bottom-0' : last ? 'top-0 bottom-1/2' : 'inset-y-0'"></span>
                <span class="absolute left-3 h-[9px] w-[9px] rounded-full border-2"
                      [class]="first ? 'border-primary-50 bg-primary-500 dark:border-primary-950' : 'border-surface-0 bg-surface-300 dark:border-surface-900 dark:bg-surface-600'"></span>
                <span [class]="first ? 'font-medium text-primary-700 dark:text-primary-300' : 'text-surface-600 dark:text-surface-300'">{{ event }}</span>
              </div>
            }
          </div>
        </div>
        <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
          <h3 class="q-title">What happened to this aggregate?</h3>
          <p class="q-text">Every event, in the order it happened, with its payload and metadata.</p>
        </div>
      </article>

      <!-- 2. Why it looks like this: the status changes -->
      <article class="q-card q-focus-2 relative flex flex-col rounded-2xl border sm:flex-row border-surface-200 bg-surface-0 dark:border-surface-800 dark:bg-surface-900">
        <div class="q-stage" aria-hidden="true">
          <div class="w-full max-w-48 overflow-hidden rounded-lg border border-surface-200 bg-surface-0 py-1.5 font-mono text-[11px] leading-6 shadow-sm dark:border-surface-700 dark:bg-surface-900">
            <div class="px-3 text-surface-400">&nbsp; total: 169.40</div>
            <!-- The old line and its removed version share a row -->
            <div class="grid">
              <div class="q-before col-start-1 row-start-1 px-3 text-surface-600 dark:text-surface-300">&nbsp; status: "CONFIRMED"</div>
              <div class="q-removed col-start-1 row-start-1 bg-red-50 px-3 text-red-700 dark:bg-red-950/60 dark:text-red-300">- status: "CONFIRMED"</div>
            </div>
            <div class="q-added-1 bg-emerald-50 px-3 text-emerald-700 dark:bg-emerald-950/60 dark:text-emerald-300">+ status: "SHIPPED"</div>
            <div class="q-added-2 bg-emerald-50 px-3 text-emerald-700 dark:bg-emerald-950/60 dark:text-emerald-300">+ carrier: "DHL"</div>
          </div>
        </div>
        <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
          <h3 class="q-title">Why does it look like this?</h3>
          <p class="q-text">The state after any event, and exactly what that event changed.</p>
        </div>
      </article>

      <!-- 3. Did it go through: a command is sent and rejected -->
      <article class="q-card q-focus-3 relative flex flex-col rounded-2xl border sm:flex-row border-surface-200 bg-surface-0 dark:border-surface-800 dark:bg-surface-900">
        <div class="q-stage" aria-hidden="true">
          <div class="w-full max-w-48 rounded-lg border border-surface-200 bg-surface-0 p-3 text-xs shadow-sm dark:border-surface-700 dark:bg-surface-900">
            <div class="flex items-center gap-2">
              <span class="grid">
                <span class="q-pending col-start-1 row-start-1 h-2 w-2 rounded-full bg-surface-300 dark:bg-surface-600"></span>
                <span class="q-failed col-start-1 row-start-1 h-2 w-2 rounded-full bg-red-500"></span>
              </span>
              <span class="font-medium text-surface-900 dark:text-surface-0">ShipOrder</span>
              <span class="ml-auto grid justify-items-end">
                <i class="q-pending pi pi-spin pi-spinner col-start-1 row-start-1 self-center text-[10px] text-surface-400"></i>
                <span class="q-failed col-start-1 row-start-1 rounded-full bg-red-50 px-2 py-0.5 text-[10px] font-semibold text-red-700 dark:bg-red-950 dark:text-red-300">failure</span>
              </span>
            </div>
            <p class="q-cause mt-2 text-[11px] leading-relaxed text-red-600 dark:text-red-400">Carrier is temporarily unavailable.</p>
          </div>
        </div>
        <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
          <h3 class="q-title">Did my command go through?</h3>
          <p class="q-text">Each command with its outcome, and the cause when it was rejected.</p>
        </div>
      </article>

      <!-- 4. Correlate: a command, the events it produced, and the correlation ID that links them -->
      <article class="q-card q-focus-4 relative flex flex-col rounded-2xl border sm:flex-row border-surface-200 bg-surface-0 dark:border-surface-800 dark:bg-surface-900">
        <div class="q-stage" aria-hidden="true">
          <div class="flex flex-col items-center gap-3">
            <div class="flex items-center">
              <span class="q-command rounded-md border border-surface-200 bg-surface-0 px-2.5 py-1.5 text-xs font-medium text-surface-900 shadow-sm dark:border-surface-700 dark:bg-surface-900 dark:text-surface-0">PlaceOrder</span>
              <svg class="h-20 w-10 shrink-0 text-primary-400" viewBox="0 0 40 80" fill="none">
                <path class="q-branch-1" pathLength="1" d="M0 40 C 20 40, 20 14, 40 14" stroke="currentColor" stroke-width="1.5" />
                <path class="q-branch-2" pathLength="1" d="M0 40 C 20 40, 20 66, 40 66" stroke="currentColor" stroke-width="1.5" />
              </svg>
              <div class="flex flex-col gap-7">
                <span class="q-event-1 rounded-full border border-primary-300 bg-primary-50 px-3 py-1 text-xs font-medium text-primary-700 dark:border-primary-800 dark:bg-primary-950 dark:text-primary-300">OrderPlaced</span>
                <span class="q-event-2 rounded-full border border-primary-300 bg-primary-50 px-3 py-1 text-xs font-medium text-primary-700 dark:border-primary-800 dark:bg-primary-950 dark:text-primary-300">OrderConfirmed</span>
              </div>
            </div>
            <span class="q-correlation inline-flex items-center gap-1.5 rounded-full border border-surface-200 bg-surface-0 px-2.5 py-0.5 font-mono text-[10px] text-surface-500 dark:border-surface-700 dark:bg-surface-900 dark:text-surface-400">
              <i class="pi pi-link text-[9px] text-primary-500"></i>$correlationId 5f0c2b1e
            </span>
          </div>
        </div>
        <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
          <h3 class="q-title">Which events did it produce?</h3>
          <p class="q-text">Correlate events back to the command that produced them, and see everything it caused.</p>
        </div>
      </article>
    </div>
  `,
  styles: `
    .q-title { font-size: 1rem; font-weight: 600; letter-spacing: -0.01em; color: var(--p-surface-900); }
    .q-text { margin-top: 0.5rem; font-size: 0.875rem; line-height: 1.6; color: var(--p-surface-500); }
    @media (prefers-color-scheme: dark) {
      .q-title { color: var(--p-surface-0); }
      .q-text { color: var(--p-surface-400); }
    }

    /* The scene: a dotted panel, at the top of a block on a phone and on its right from sm up */
    .q-stage {
      display: flex; align-items: center; justify-content: center;
      height: 10rem; margin: 0.5rem 0.5rem 0; padding: 0 1rem; border-radius: 0.75rem;
      background-color: var(--p-surface-50);
      background-image: radial-gradient(var(--p-surface-200) 1px, transparent 1px);
      background-size: 14px 14px;
    }
    @media (min-width: 640px) {
      .q-stage { flex: 1; height: auto; min-height: 11rem; margin: 0.5rem; }
    }
    @media (prefers-color-scheme: dark) {
      .q-stage { background-color: var(--p-surface-950); background-image: radial-gradient(var(--p-surface-800) 1px, transparent 1px); }
    }

    /* The removed line and the failure only exist once they happen */
    .q-removed, .q-failed { opacity: 1; }
    .q-before, .q-pending { opacity: 0; }

    /* ---- The story, 12s per cycle, only while in view and motion is welcome ---- */
    @media (prefers-reduced-motion: no-preference) {
      :host(.playing) .q-card { animation: 12s infinite; }
      :host(.playing) .q-focus-1 { animation-name: q-focus-1; }
      :host(.playing) .q-focus-2 { animation-name: q-focus-2; }
      :host(.playing) .q-focus-3 { animation-name: q-focus-3; }
      :host(.playing) .q-focus-4 { animation-name: q-focus-4; }

      /* 1. The events arrive: OrderPlaced, OrderConfirmed, then OrderShipped */
      :host(.playing) .q-arrive-1 { animation: q-arrive-1 12s infinite; }
      :host(.playing) .q-arrive-2 { animation: q-arrive-2 12s infinite; }
      :host(.playing) .q-arrive-3 { animation: q-arrive-3 12s infinite; }

      /* 2. The status line is replaced by the removed and added lines */
      :host(.playing) .q-before { animation: q-until-28 12s infinite; }
      :host(.playing) .q-removed { animation: q-from-28 12s infinite; }
      :host(.playing) .q-added-1 { animation: q-from-31 12s infinite; }
      :host(.playing) .q-added-2 { animation: q-from-34 12s infinite; }

      /* 3. Pending, then rejected with its cause */
      :host(.playing) .q-pending { animation: q-until-56 12s infinite; }
      :host(.playing) .q-failed { animation: q-from-56 12s infinite; }
      :host(.playing) .q-cause { animation: q-from-60 12s infinite; }

      /* 4. The command, then each event it produced with its branch; the correlation ID with the last one */
      :host(.playing) .q-command { animation: q-from-76 12s infinite; }
      :host(.playing) :is(.q-branch-1, .q-branch-2) { stroke-dasharray: 1; }
      :host(.playing) .q-branch-1 { animation: q-draw-79 12s infinite; }
      :host(.playing) .q-event-1 { animation: q-from-82 12s infinite; }
      :host(.playing) .q-branch-2 { animation: q-draw-84 12s infinite; }
      :host(.playing) :is(.q-event-2, .q-correlation) { animation: q-from-87 12s infinite; }
    }

    /* Each block is lit during its quarter */
    @keyframes q-focus-1 { 0%, 23% { box-shadow: 0 0 0 2px var(--p-primary-400); } 26%, 100% { box-shadow: 0 0 0 0 transparent; } }
    @keyframes q-focus-2 { 0%, 24% { box-shadow: 0 0 0 0 transparent; } 26%, 48% { box-shadow: 0 0 0 2px var(--p-primary-400); } 51%, 100% { box-shadow: 0 0 0 0 transparent; } }
    @keyframes q-focus-3 { 0%, 49% { box-shadow: 0 0 0 0 transparent; } 51%, 73% { box-shadow: 0 0 0 2px var(--p-primary-400); } 76%, 100% { box-shadow: 0 0 0 0 transparent; } }
    @keyframes q-focus-4 { 0%, 74% { box-shadow: 0 0 0 0 transparent; } 76%, 98% { box-shadow: 0 0 0 2px var(--p-primary-400); } 100% { box-shadow: 0 0 0 0 transparent; } }

    @keyframes q-arrive-1 { 0%, 2% { opacity: 0; transform: translateY(-4px); } 6%, 100% { opacity: 1; transform: none; } }
    @keyframes q-arrive-2 { 0%, 8% { opacity: 0; transform: translateY(-4px); } 12%, 100% { opacity: 1; transform: none; } }
    @keyframes q-arrive-3 { 0%, 14% { opacity: 0; transform: translateY(-4px); } 18%, 100% { opacity: 1; transform: none; } }

    @keyframes q-until-28 { 0%, 28% { opacity: 1; } 30%, 100% { opacity: 0; } }
    @keyframes q-from-28 { 0%, 28% { opacity: 0; } 30%, 100% { opacity: 1; } }
    @keyframes q-from-31 { 0%, 31% { opacity: 0; transform: translateX(-4px); } 34%, 100% { opacity: 1; transform: none; } }
    @keyframes q-from-34 { 0%, 34% { opacity: 0; transform: translateX(-4px); } 37%, 100% { opacity: 1; transform: none; } }

    @keyframes q-until-56 { 0%, 56% { opacity: 1; } 58%, 100% { opacity: 0; } }
    @keyframes q-from-56 { 0%, 56% { opacity: 0; } 58%, 100% { opacity: 1; } }
    @keyframes q-from-60 { 0%, 60% { opacity: 0; } 63%, 100% { opacity: 1; } }

    @keyframes q-from-76 { 0%, 76% { opacity: 0; transform: translateX(-4px); } 78%, 100% { opacity: 1; transform: none; } }
    @keyframes q-from-82 { 0%, 82% { opacity: 0; transform: translateX(-4px); } 84%, 100% { opacity: 1; transform: none; } }
    @keyframes q-from-87 { 0%, 87% { opacity: 0; transform: translateX(-4px); } 89%, 100% { opacity: 1; transform: none; } }
    /* A branch draws from the command to its event */
    @keyframes q-draw-79 { 0%, 79% { stroke-dashoffset: 1; } 82%, 100% { stroke-dashoffset: 0; } }
    @keyframes q-draw-84 { 0%, 84% { stroke-dashoffset: 1; } 87%, 100% { stroke-dashoffset: 0; } }
  `,
})
export class ConsoleQuestionsComponent {
  /** Newest first, like the console's list. */
  readonly events = ['OrderShipped', 'OrderConfirmed', 'OrderPlaced'];
  readonly playing = signal(false);

  constructor() {
    // The story starts from the beginning once the top of the blocks is well in view
    // (not a share of them: on a phone the blocks are taller than the screen).
    const observer = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting) {
        this.playing.set(true);
        observer.disconnect();
      }
    }, { rootMargin: '0px 0px -25% 0px' });
    observer.observe(inject(ElementRef).nativeElement);
    inject(DestroyRef).onDestroy(() => observer.disconnect());
  }
}
