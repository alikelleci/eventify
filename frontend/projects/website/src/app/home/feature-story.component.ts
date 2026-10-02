import { Component, computed } from '@angular/core';
import { StepPlayer, autoplay } from '../shared/step-player';

/** Each card plays its scene in ticks; the scenes below say at which tick something happens. */
const TICKS = 12;
const TICK_MS = 300;

/**
 * Four event-sourcing concepts, each a card with a small scene. The cards take turns: the current one is lit and plays
 * its scene, the others show how theirs ends. Picking a card plays it. With reduced motion every scene shows its end.
 */
@Component({
  selector: 'app-feature-story',
  standalone: true,
  template: `
    <!-- Two by two on desktop; each card has its feature on the left and its scene on the right -->
    <div class="grid gap-6 lg:grid-cols-2">
      @for (card of cards; track card.title; let c = $index) {
        <article class="s-card" [class.s-lit]="lit(c)" (click)="player.go(c)">
          <div class="s-stage" aria-hidden="true">
            @switch (c) {

              <!-- 1. Event replay: the events are applied one by one, and the state's version follows -->
              @case (0) {
                <div class="flex w-full max-w-44 flex-col items-center">
                  <div class="s-box relative w-full py-1.5">
                    <span class="s-cursor" [style.transform]="'translateY(' + (replayed() - 1) * 100 + '%)'" [class.opacity-0]="replayed() === 0"></span>
                    @for (event of events; track event; let i = $index, first = $first, last = $last) {
                      <div class="s-row" [class.s-dim]="i >= replayed()">
                        <span class="s-line" [class]="first ? 'top-1/2 bottom-0' : last ? 'top-0 bottom-1/2' : 'inset-y-0'"></span>
                        <span class="s-dot" [class]="i < replayed() ? 's-dot-on' : 's-dot-off'"></span>
                        <span [class]="i === replayed() - 1 ? 'font-medium text-primary-700 dark:text-primary-300' : 'text-surface-600 dark:text-surface-300'">{{ event }}</span>
                      </div>
                    }
                  </div>
                  <i class="pi pi-arrow-down my-1.5 text-[10px] text-surface-400"></i>
                  @for (v of [replayed()]; track v) {
                    <div class="s-pop flex items-center gap-2 rounded-full border px-3 py-1 font-medium"
                         [class]="v ? 'border-primary-300 bg-primary-50 text-primary-700 dark:border-primary-800 dark:bg-primary-950 dark:text-primary-300' : 'border-surface-200 text-surface-400 dark:border-surface-700'">
                      state <span class="font-mono text-[10px] opacity-70">{{ v ? 'v' + v : 'empty' }}</span>
                    </div>
                  }
                </div>
              }

              <!-- 2. Snapshots: a snapshot is taken after event 3, so loading reads it and only events 4 and 5 -->
              @case (1) {
                <div class="s-box w-full max-w-44 py-1.5">
                  @for (i of [1, 2, 3]; track i; let first = $first) {
                    <div class="s-row s-in" [class.s-skipped]="at(c, 5)">
                      <span class="s-line" [class]="first ? 'top-1/2 bottom-0' : 'inset-y-0'"></span>
                      <span class="s-dot s-dot-off"></span>
                      <span class="text-surface-600 dark:text-surface-300">event {{ i }}</span>
                    </div>
                  }
                  <div class="s-row s-row-new" [class.s-closed]="!at(c, 2)">
                    <span class="s-line inset-y-0"></span>
                    <span class="absolute left-[9px] flex h-[15px] w-[15px] items-center justify-center rounded-full bg-primary-500 text-white" [class.s-flash]="at(c, 2)">
                      <i class="pi pi-camera text-[7px]"></i>
                    </span>
                    <span class="ml-1 font-medium text-primary-700 dark:text-primary-300">snapshot</span>
                  </div>
                  @for (i of [4, 5]; track i; let last = $last) {
                    <div class="s-row">
                      <span class="s-line" [class]="last ? 'top-0 bottom-1/2' : 'inset-y-0'"></span>
                      <span class="s-dot" [class]="at(c, i === 4 ? 7 : 9) ? 's-dot-on' : 's-dot-off'"></span>
                      <span class="transition-colors duration-300" [class]="at(c, i === 4 ? 7 : 9) ? 'font-medium text-surface-900 dark:text-surface-0' : 'text-surface-600 dark:text-surface-300'">event {{ i }}</span>
                    </div>
                  }
                  <div class="s-in mx-3 mt-1 border-t border-surface-100 pt-1.5 text-[10px] text-surface-500 dark:border-surface-800 dark:text-surface-400" [class.s-out]="!at(c, 10)">
                    loads snapshot + 2 events
                  </div>
                </div>
              }

              <!-- 3. Upcasting: the old revision is upgraded as it is read -->
              @case (2) {
                <div class="flex w-full max-w-44 flex-col items-center font-mono text-[11px]">
                  <div class="s-box w-full px-3 py-2 text-surface-500 dark:text-surface-400">
                    <div class="mb-0.5 font-sans text-[10px] font-semibold uppercase tracking-wider">rev 1</div>
                    id, customer
                  </div>
                  <span class="s-in my-1.5 font-sans text-[10px] text-surface-400" [class.s-out]="!at(c, 2)"><i class="pi pi-arrow-down mr-1 text-[9px]"></i>upcast</span>
                  <div class="s-in s-box s-box-new w-full px-3 py-2" [class.s-out]="!at(c, 4)">
                    <div class="mb-0.5 font-sans text-[10px] font-semibold uppercase tracking-wider text-primary-600 dark:text-primary-400">rev 2</div>
                    id, customer
                    <div class="s-grow" [class.s-closed]="!at(c, 6)">
                      <div class="-mx-1 mt-0.5 rounded bg-emerald-100 px-1 text-emerald-700 dark:bg-emerald-900/50 dark:text-emerald-300">+ address</div>
                    </div>
                  </div>
                </div>
              }

              <!-- 4. Plain Java: the command handler decides, the event is returned, the event sourcing handler applies it -->
              @case (3) {
                <div class="flex w-full max-w-44 flex-col items-center gap-2 font-mono text-[10px]">
                  <div class="s-box s-in w-full px-3 py-2 text-surface-600 dark:text-surface-300" [class.s-active]="playsAt(c, 0, 3)">
                    <span class="text-primary-600 dark:text-primary-400">@CommandHandler</span> PlaceOrder
                  </div>
                  <i class="s-in pi pi-arrow-down text-[9px] text-surface-400" [class.s-out]="!at(c, 2)"></i>
                  <div class="s-in w-full rounded bg-primary-100 px-3 py-2 font-semibold text-primary-700 dark:bg-primary-900/60 dark:text-primary-300"
                       [class.s-out]="!at(c, 3)" [class.s-active]="playsAt(c, 3, 6)">
                    OrderPlaced
                  </div>
                  <i class="s-in pi pi-arrow-down text-[9px] text-surface-400" [class.s-out]="!at(c, 5)"></i>
                  <div class="s-box s-in w-full px-3 py-2 text-surface-600 dark:text-surface-300" [class.s-out]="!at(c, 6)" [class.s-active]="playsAt(c, 6, 10)">
                    <span class="text-primary-600 dark:text-primary-400">@EventSourcingHandler</span> Order
                  </div>
                </div>
              }
            }
          </div>
          <div class="px-6 pt-4 pb-6 sm:order-first sm:flex sm:w-5/12 sm:shrink-0 sm:flex-col sm:justify-center sm:p-7">
            <h3 class="s-title">{{ card.title }}</h3>
            <p class="s-text">{{ card.text }}</p>
          </div>
          <!-- How long the current card still plays -->
          @if (lit(c)) {
            @for (r of [player.run()]; track r) {
              <span class="s-progress" [style.animation-duration.ms]="player.duration(c)"></span>
            }
          }
        </article>
      }
    </div>
  `,
  styles: `
    .s-card {
      position: relative; display: flex; flex-direction: column; overflow: hidden; border-radius: 1rem; cursor: pointer;
      border: 1px solid var(--p-surface-200); background: var(--p-surface-0);
      transition: box-shadow 0.5s var(--ease-out), transform 0.5s var(--ease-out), border-color 0.5s;
    }
    .s-card:hover { border-color: var(--p-surface-300); }
    .s-lit {
      border-color: var(--p-primary-300); transform: translateY(-3px);
      box-shadow: 0 0 0 1px var(--p-primary-300), 0 24px 50px -28px rgb(20 184 166 / 0.55);
    }
    .s-lit:hover { border-color: var(--p-primary-300); }
    .s-progress {
      position: absolute; bottom: 0; left: 0; height: 2px; background: var(--p-primary-500);
      animation: s-progress linear both;
    }
    @keyframes s-progress { from { width: 0; } to { width: 100%; } }
    .s-title { font-size: 1rem; font-weight: 600; letter-spacing: -0.01em; color: var(--p-surface-900); }
    .s-text { margin-top: 0.5rem; font-size: 0.875rem; line-height: 1.6; color: var(--p-surface-500); }
    .s-stage {
      display: flex; align-items: center; justify-content: center; height: 13rem; margin: 0.5rem 0.5rem 0; padding: 0 1rem;
      border-radius: 0.75rem; font-size: 0.75rem; color: var(--p-surface-700);
      background: var(--p-surface-50) radial-gradient(var(--p-surface-200) 1px, transparent 1px) 0 0 / 14px 14px;
    }
    /* From sm up the card is a row: the text on the left, the scene filling the rest */
    @media (min-width: 640px) {
      .s-card { flex-direction: row; }
      .s-stage { flex: 1; height: auto; min-height: 13rem; margin: 0.5rem; }
    }
    .s-box { border: 1px solid var(--p-surface-200); border-radius: 0.5rem; background: var(--p-surface-0); box-shadow: 0 1px 2px rgb(0 0 0 / 0.05); }
    .s-box-new { border-color: var(--p-primary-300); }

    /* A timeline: one row per event, a dot on a line that joins them */
    .s-row { position: relative; display: flex; align-items: center; height: 1.625rem; padding: 0 0.75rem 0 1.75rem; transition: opacity 0.4s; }
    .s-line { position: absolute; left: 16px; width: 1px; background: var(--p-surface-200); }
    .s-dot {
      position: absolute; left: 12px; width: 9px; height: 9px; border-radius: 9999px; border: 2px solid var(--p-surface-0);
      transition: background-color 0.3s, transform 0.4s var(--ease-spring);
    }
    .s-dot-on { background: var(--p-primary-500); transform: scale(1.2); }
    .s-dot-off { background: var(--p-surface-300); }
    /* The row being replayed, a band that moves down with it */
    .s-cursor {
      position: absolute; top: 0.375rem; left: 0.25rem; right: 0.25rem; height: 1.625rem; border-radius: 0.375rem;
      background: var(--p-primary-50); transition: transform 0.5s var(--ease-out), opacity 0.3s;
    }
    .s-dim { opacity: 0.45; }

    @media (prefers-color-scheme: dark) {
      .s-card { border-color: var(--p-surface-800); background: var(--p-surface-900); }
      .s-card:hover { border-color: var(--p-surface-700); }
      .s-lit, .s-lit:hover { border-color: var(--p-primary-700); box-shadow: 0 0 0 1px var(--p-primary-700), 0 24px 50px -28px rgb(20 184 166 / 0.4); }
      .s-title { color: var(--p-surface-0); }
      .s-text { color: var(--p-surface-400); }
      .s-stage { color: var(--p-surface-200); background: var(--p-surface-950) radial-gradient(var(--p-surface-800) 1px, transparent 1px) 0 0 / 14px 14px; }
      .s-box { border-color: var(--p-surface-700); background: var(--p-surface-900); }
      .s-box-new { border-color: var(--p-primary-800); }
      .s-line { background: var(--p-surface-700); }
      .s-dot { border-color: var(--p-surface-900); }
      .s-dot-off { background: var(--p-surface-600); }
      .s-cursor { background: rgb(20 184 166 / 0.12); }
    }

    /* Something that hasn't happened yet in this scene */
    .s-in { transition: opacity 0.5s var(--ease-out), transform 0.5s var(--ease-out), filter 0.5s var(--ease-out), box-shadow 0.4s; }
    .s-out { opacity: 0; transform: translateY(-6px) scale(0.97); filter: blur(2px); }
    /* The events a snapshot made unnecessary to read */
    .s-skipped { opacity: 0.35; text-decoration: line-through; }
    /* The part of the flow that runs now */
    .s-active { box-shadow: 0 0 0 2px var(--p-primary-400), 0 8px 20px -10px rgb(20 184 166 / 0.6); }
    /* A row that opens up when it arrives */
    .s-grow { display: grid; grid-template-rows: 1fr; transition: grid-template-rows 0.5s var(--ease-out), opacity 0.5s; }
    .s-grow > * { min-height: 0; overflow: hidden; }
    .s-grow.s-closed { grid-template-rows: 0fr; opacity: 0; }
    .s-row-new { overflow: hidden; transition: height 0.5s var(--ease-out), opacity 0.5s; }
    .s-row-new.s-closed { height: 0; opacity: 0; }

    @media (prefers-reduced-motion: no-preference) {
      .s-pop { animation: s-pop 0.5s var(--ease-spring) both; }
      .s-flash { animation: s-flash 0.7s ease-out both; }
    }
    @keyframes s-pop { from { transform: scale(0.85); opacity: 0.4; } }
    @keyframes s-flash { from { box-shadow: 0 0 0 0 rgb(20 184 166 / 0.7); } to { box-shadow: 0 0 0 10px rgb(20 184 166 / 0); } }
  `,
})
export class FeatureStoryComponent {
  readonly cards = [
    { title: 'Event replay', text: 'Replay retained events to rebuild state and understand how an aggregate reached it.' },
    { title: 'Snapshots', text: 'Save the state now and then, so only the latest events need to be replayed.' },
    { title: 'Upcasting', text: 'Evolve event data deliberately. Older revisions can be upgraded while they are read.' },
    { title: 'Plain Java', text: 'Commands decide, events describe what happened, and event handlers rebuild state.' },
  ];
  readonly events = ['OrderPlaced', 'ItemAdded', 'OrderPaid'];

  readonly player = new StepPlayer(this.cards.map(() => Array(TICKS).fill(TICK_MS)));

  /** How many events the replay has applied: one at tick 1, 4 and 7. */
  readonly replayed = computed(() => this.events.filter((_, i) => this.at(0, 1 + i * 3)).length);

  constructor() {
    autoplay(this.player, 0.15);
  }

  /** Whether the card is the one playing. */
  lit(card: number): boolean {
    return this.player.playing() && this.player.step() === card;
  }

  /** Whether the card's scene has reached this tick: a card that isn't playing shows its end. */
  at(card: number, tick: number): boolean {
    return this.player.step() !== card || this.player.frame() >= tick;
  }

  /** Whether the card is playing and its scene is between these ticks. */
  playsAt(card: number, from: number, to: number): boolean {
    return this.lit(card) && this.player.frame() >= from && this.player.frame() < to;
  }
}
