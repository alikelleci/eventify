import { Component, DestroyRef, ElementRef, inject } from '@angular/core';
import { ShowcaseStepsComponent } from './showcase-steps.component';
import { StepPlayer } from './step-player';

/**
 * Two steps, each a flow from a command through its outcome to its events:
 * PlaceOrder succeeds and produces two events;
 * ShipOrder fails with its cause and produces nothing, then Retry sends it again and it produces OrderShipped.
 */
const STEPS = [
  { title: 'Trace a command to its events', text: 'See every event a command produced.' },
  { title: 'Retry a failed command', text: 'See why a command failed, and send it again from the console.' },
];

/** For each step, how long each of its frames shows: the retry step first fails, then retries. */
const FRAMES = [[5500], [3500, 4500]];

@Component({
  selector: 'app-command-trace',
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

      <!-- The flow on a dotted stage: the command, its outcome on the line, and the events -->
      <div class="c-stage px-3 py-8 sm:px-8 sm:py-12">
        <div class="mx-auto grid max-w-3xl grid-cols-[minmax(0,1fr)_4rem_minmax(0,1fr)] items-center sm:grid-cols-[minmax(0,1fr)_8rem_minmax(0,1fr)]">
          <div class="pb-3 text-[10px] font-medium uppercase tracking-widest text-surface-400">Command</div>
          <div class="pb-3 text-center text-[10px] font-medium uppercase tracking-widest text-surface-400"><span class="hidden sm:inline">Outcome</span></div>
          <div class="pb-3 text-[10px] font-medium uppercase tracking-widest text-surface-400">Events</div>

          <!-- Rebuilt on every step and frame, so its animation starts over -->
          @for (key of [player.step() + '-' + player.frame()]; track key) {
            @switch (player.step()) {

              <!-- A command with two events -->
              @case (0) {
                <div class="c-in rounded-xl border border-surface-200 bg-surface-0 p-3 shadow-sm sm:p-4 dark:border-surface-700 dark:bg-surface-900">
                  <div class="text-sm font-semibold text-surface-900 dark:text-surface-0">PlaceOrder</div>
                  <div class="mt-2 hidden space-y-0.5 font-mono text-[11px] sm:block">
                    <div><span class="mr-1 text-surface-400">customerId:</span><span class="text-surface-700 dark:text-surface-200">"customer-42"</span></div>
                    <div><span class="mr-1 text-surface-400">total:</span><span class="text-surface-700 dark:text-surface-200">169.40</span></div>
                  </div>
                  <span class="c-chip mt-3">5f0c…0001</span>
                </div>

                <div class="relative h-52 text-primary-400 dark:text-primary-600">
                  <svg class="absolute inset-0 h-full w-full" viewBox="0 0 100 100" preserveAspectRatio="none" fill="none">
                    <path class="c-draw" style="animation-delay: 300ms" pathLength="1" d="M0 50 H50" stroke="currentColor" stroke-width="1.5" vector-effect="non-scaling-stroke" />
                    <path class="c-draw" style="animation-delay: 900ms" pathLength="1" d="M50 50 C 75 50, 75 28.85, 100 28.85" stroke="currentColor" stroke-width="1.5" vector-effect="non-scaling-stroke" />
                    <path class="c-draw" style="animation-delay: 1400ms" pathLength="1" d="M50 50 C 75 50, 75 71.15, 100 71.15" stroke="currentColor" stroke-width="1.5" vector-effect="non-scaling-stroke" />
                  </svg>
                  <span class="c-node c-pop bg-emerald-500 text-white" style="animation-delay: 650ms"><i class="pi pi-check text-[11px]"></i></span>
                </div>

                <div class="flex h-52 flex-col justify-center gap-4">
                  <div class="c-event c-in" style="animation-delay: 1200ms">
                    <span class="c-event-name">OrderPlaced</span>
                    <span class="c-chip">5f0c…0001</span>
                  </div>
                  <div class="c-event c-in" style="animation-delay: 1700ms">
                    <span class="c-event-name">OrderConfirmed</span>
                    <span class="c-chip">5f0c…0001</span>
                  </div>
                </div>
              }

              <!-- A command that fails, and its retry -->
              @case (1) {
                @let retried = player.frame() === 1;
                <div class="rounded-xl border border-surface-200 bg-surface-0 p-3 shadow-sm sm:p-4 dark:border-surface-700 dark:bg-surface-900" [class.c-in]="!retried">
                  <div class="flex flex-wrap items-center gap-x-2 gap-y-1">
                    <span class="text-sm font-semibold text-surface-900 dark:text-surface-0">ShipOrder</span>
                    @if (retried) {
                      <span class="inline-flex items-center gap-1 text-[10px] text-surface-400"><i class="pi pi-refresh text-[9px]"></i>retried</span>
                    }
                  </div>
                  <div class="mt-2 hidden font-mono text-[11px] sm:block">
                    <span class="mr-1 text-surface-400">carrier:</span><span class="text-surface-700 dark:text-surface-200">"DHL"</span>
                  </div>
                  <span class="c-chip mt-3">{{ retried ? '5f0c…0003' : '5f0c…0002' }}</span>

                  @if (!retried) {
                    <!-- The cause, and the button to send the command again -->
                    <div class="c-fade mt-3 rounded-lg bg-red-50 p-2.5 text-[11px] leading-snug text-red-700 dark:bg-red-950/60 dark:text-red-300" style="animation-delay: 1300ms">
                      Carrier DHL is temporarily unavailable.
                      <span class="mt-2 flex w-fit items-center gap-1.5 rounded-full border border-red-300 bg-surface-0 px-2.5 py-0.5 font-medium text-red-600 dark:border-red-800 dark:bg-surface-900 dark:text-red-400">
                        <i class="pi pi-refresh text-[9px]"></i>Retry
                      </span>
                    </div>
                  } @else {
                    <!-- Retry pressed: the box keeps its size, the button spins, then the command goes through -->
                    <div class="mt-3 rounded-lg bg-surface-50 p-2.5 text-[11px] leading-snug text-surface-500 dark:bg-surface-800 dark:text-surface-400">
                      Sent again from the console.
                      <span class="mt-2 grid w-fit">
                        <span class="c-gone col-start-1 row-start-1 flex items-center gap-1.5 rounded-full border border-primary-500 bg-primary-500 px-2.5 py-0.5 font-medium text-white" style="animation-delay: 800ms">
                          <i class="pi pi-spin pi-spinner text-[9px]"></i>Retrying
                        </span>
                        <span class="c-fade col-start-1 row-start-1 flex items-center gap-1.5 rounded-full border border-emerald-300 bg-surface-0 px-2.5 py-0.5 font-medium text-emerald-700 dark:border-emerald-800 dark:bg-surface-900 dark:text-emerald-300" style="animation-delay: 800ms">
                          <i class="pi pi-check text-[9px]"></i>success
                        </span>
                      </span>
                    </div>
                  }
                </div>

                <div class="relative h-52">
                  <svg class="absolute inset-0 h-full w-full" viewBox="0 0 100 100" preserveAspectRatio="none" fill="none">
                    @if (!retried) {
                      <path class="c-draw text-red-300 dark:text-red-800" style="animation-delay: 300ms" pathLength="1" d="M0 50 H50" stroke="currentColor" stroke-width="1.5" vector-effect="non-scaling-stroke" />
                    } @else {
                      <path class="c-draw text-primary-400 dark:text-primary-600" style="animation-delay: 850ms" pathLength="1" d="M0 50 H50" stroke="currentColor" stroke-width="1.5" vector-effect="non-scaling-stroke" />
                      <path class="c-draw text-primary-400 dark:text-primary-600" style="animation-delay: 1300ms" pathLength="1" d="M50 50 H100" stroke="currentColor" stroke-width="1.5" vector-effect="non-scaling-stroke" />
                    }
                  </svg>
                  @if (!retried) {
                    <span class="c-node c-pop bg-red-500 text-white" style="animation-delay: 650ms"><i class="pi pi-times text-[11px]"></i></span>
                  } @else {
                    <span class="c-node c-pop bg-emerald-500 text-white" style="animation-delay: 1100ms"><i class="pi pi-check text-[11px]"></i></span>
                  }
                </div>

                <div class="flex h-52 flex-col justify-center">
                  @if (!retried) {
                    <div class="c-fade flex h-[4.5rem] items-center justify-center rounded-xl border border-dashed border-surface-300 text-xs text-surface-400 dark:border-surface-600" style="animation-delay: 1000ms">
                      No events
                    </div>
                  } @else {
                    <div class="c-event c-in" style="animation-delay: 1600ms">
                      <span class="c-event-name">OrderShipped</span>
                      <span class="c-chip">5f0c…0003</span>
                    </div>
                  }
                </div>
              }
            }
          }
        </div>
      </div>
    </div>
  `,
  styles: `
    .c-stage {
      background-color: var(--p-surface-50);
      background-image: radial-gradient(var(--p-surface-200) 1px, transparent 1px);
      background-size: 14px 14px;
    }
    /* The outcome, on the line between the command and its events */
    .c-node {
      position: absolute; top: calc(50% - 0.875rem); left: calc(50% - 0.875rem);
      display: flex; align-items: center; justify-content: center; width: 1.75rem; height: 1.75rem; border-radius: 9999px;
      box-shadow: 0 0 0 4px var(--p-surface-50);
    }
    .c-event {
      display: flex; flex-direction: column; justify-content: center; gap: 0.375rem; height: 4.5rem; padding: 0 0.75rem;
      border: 1px solid var(--p-primary-200); border-radius: 0.75rem; background: var(--p-surface-0);
      box-shadow: 0 1px 2px rgb(0 0 0 / 0.05);
    }
    .c-event-name { font-size: 0.8125rem; font-weight: 600; color: var(--p-surface-900); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .c-chip {
      display: block; width: fit-content; padding: 0 0.375rem; border-radius: 0.25rem; font-family: ui-monospace, monospace; font-size: 10px;
      line-height: 1.25rem; background: var(--p-primary-50); color: var(--p-primary-700);
    }
    @media (prefers-color-scheme: dark) {
      .c-stage { background-color: var(--p-surface-950); background-image: radial-gradient(var(--p-surface-800) 1px, transparent 1px); }
      .c-node { box-shadow: 0 0 0 4px var(--p-surface-950); }
      .c-event { border-color: var(--p-primary-900); background: var(--p-surface-900); }
      .c-event-name { color: var(--p-surface-0); }
      .c-chip { background: var(--p-primary-950); color: var(--p-primary-300); }
    }

    /* Without motion, each frame shows its end state: what is gone is gone */
    .c-gone { opacity: 0; }

    @media (prefers-reduced-motion: no-preference) {
      .c-in { animation: c-in 0.4s ease-out both; }
      .c-fade { animation: c-fade 0.35s ease-out both; }
      .c-gone { animation: c-gone 0.3s ease-out both; }
      .c-pop { animation: c-pop 0.35s ease-out both; }
      .c-draw { stroke-dasharray: 1; animation: c-draw 0.45s ease-out both; }
    }
    @keyframes c-in { from { opacity: 0; transform: translateX(-6px); } }
    @keyframes c-fade { from { opacity: 0; } }
    @keyframes c-gone { from { opacity: 1; } to { opacity: 0; } }
    @keyframes c-pop { from { opacity: 0; transform: scale(0.5); } }
    @keyframes c-draw { from { stroke-dashoffset: 1; } to { stroke-dashoffset: 0; } }
  `,
})
export class CommandTraceComponent {
  readonly steps = STEPS;
  readonly player = new StepPlayer(FRAMES);

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
