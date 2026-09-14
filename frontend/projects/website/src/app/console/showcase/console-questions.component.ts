import { Component, DestroyRef, ElementRef, inject, signal } from '@angular/core';

/**
 * The questions the console answers, as four blocks that play one story in turn: the events arrive, the state changes,
 * a command fails, and a retry produces the missing event. One 12-second cycle, a quarter per block.
 * Without the animation (before it scrolls into view, or with reduced motion) every block shows its end state.
 */
@Component({
  selector: 'app-console-questions',
  standalone: true,
  host: { '[class.playing]': 'playing()' },
  template: `
    <div class="grid gap-6 sm:grid-cols-2 lg:grid-cols-4">

      <!-- 1. What happened: the events arrive, oldest first -->
      <article class="q-card q-focus-1 relative flex flex-col rounded-2xl border border-surface-200 bg-surface-0 dark:border-surface-800 dark:bg-surface-900">
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
        <div class="px-6 pt-4 pb-6">
          <h3 class="q-title">What happened to this aggregate?</h3>
          <p class="q-text">Every event, in the order it happened, with its payload and metadata.</p>
        </div>
        <span class="q-next" aria-hidden="true"><i class="pi pi-chevron-right text-[9px]"></i></span>
      </article>

      <!-- 2. Why it looks like this: the status changes -->
      <article class="q-card q-focus-2 relative flex flex-col rounded-2xl border border-surface-200 bg-surface-0 dark:border-surface-800 dark:bg-surface-900">
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
        <div class="px-6 pt-4 pb-6">
          <h3 class="q-title">Why does it look like this?</h3>
          <p class="q-text">The state after any event, and exactly what that event changed.</p>
        </div>
        <span class="q-next" aria-hidden="true"><i class="pi pi-chevron-right text-[9px]"></i></span>
      </article>

      <!-- 3. Did it go through: a command is sent and rejected -->
      <article class="q-card q-focus-3 relative flex flex-col rounded-2xl border border-surface-200 bg-surface-0 dark:border-surface-800 dark:bg-surface-900">
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
        <div class="px-6 pt-4 pb-6">
          <h3 class="q-title">Did my command go through?</h3>
          <p class="q-text">Each command with its outcome, and the cause when it was rejected.</p>
        </div>
        <span class="q-next" aria-hidden="true"><i class="pi pi-chevron-right text-[9px]"></i></span>
      </article>

      <!-- 4. Try again: Retry is pressed, and the event follows -->
      <article class="q-card q-focus-4 relative flex flex-col rounded-2xl border border-surface-200 bg-surface-0 dark:border-surface-800 dark:bg-surface-900">
        <div class="q-stage" aria-hidden="true">
          <div class="flex flex-col items-center">
            <span class="q-press inline-flex items-center gap-1.5 rounded-md bg-primary-500 px-3 py-1.5 text-xs font-semibold text-white shadow-sm">
              <i class="pi pi-refresh text-[10px]"></i>Retry
            </span>
            <span class="q-link h-6 border-l border-dashed border-primary-400"></span>
            <span class="q-result flex items-center gap-2 rounded-full border border-primary-300 bg-primary-50 px-3 py-1 text-xs font-medium text-primary-700 dark:border-primary-800 dark:bg-primary-950 dark:text-primary-300">
              <span class="h-1.5 w-1.5 rounded-full bg-primary-500"></span>OrderShipped
            </span>
          </div>
        </div>
        <div class="px-6 pt-4 pb-6">
          <h3 class="q-title">Can I try it again?</h3>
          <p class="q-text">Retry a failed command once the cause is fixed, and follow the events it produces.</p>
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

    /* The scene: a dotted panel at the top of each block */
    .q-stage {
      display: flex; align-items: center; justify-content: center;
      height: 10rem; margin: 0.5rem 0.5rem 0; padding: 0 1rem; border-radius: 0.75rem;
      background-color: var(--p-surface-50);
      background-image: radial-gradient(var(--p-surface-200) 1px, transparent 1px);
      background-size: 14px 14px;
    }
    @media (prefers-color-scheme: dark) {
      .q-stage { background-color: var(--p-surface-950); background-image: radial-gradient(var(--p-surface-800) 1px, transparent 1px); }
    }

    /* Leads to the next block, in the gap between them (desktop only) */
    .q-next {
      display: none; position: absolute; z-index: 1; top: 5.25rem; right: -1.1rem;
      width: 1.25rem; height: 1.25rem; align-items: center; justify-content: center; border-radius: 9999px;
      border: 1px solid var(--p-surface-200); background: var(--p-surface-0); color: var(--p-surface-400);
    }
    @media (prefers-color-scheme: dark) { .q-next { border-color: var(--p-surface-700); background: var(--p-surface-900); } }
    @media (min-width: 1024px) { .q-next { display: flex; } }

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

      /* 4. Retry is pressed, the link draws, the event appears */
      :host(.playing) .q-press { animation: q-press 12s infinite; }
      :host(.playing) .q-link { animation: q-link 12s infinite; transform-origin: top; }
      :host(.playing) .q-result { animation: q-from-86 12s infinite; }
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

    @keyframes q-press { 0%, 79% { transform: none; } 80% { transform: scale(0.92); } 82%, 100% { transform: none; } }
    @keyframes q-link { 0%, 82% { transform: scaleY(0); } 85%, 100% { transform: scaleY(1); } }
    @keyframes q-from-86 { 0%, 85% { opacity: 0; transform: translateY(-4px); } 88%, 100% { opacity: 1; transform: none; } }
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
