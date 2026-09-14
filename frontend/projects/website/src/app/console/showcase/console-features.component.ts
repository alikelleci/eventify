import { Component } from '@angular/core';

/**
 * What the console shows, as a bento grid: one large card for events and state, two smaller ones for commands and tracing.
 * Each card has a small scene on a dotted stage, drawn with the console's own styles. Decorative only.
 */
@Component({
  selector: 'app-console-features',
  standalone: true,
  template: `
    <div class="grid gap-4 lg:grid-cols-3 lg:grid-rows-2">

      <!-- Events and state: the large card -->
      <article class="group relative flex flex-col overflow-hidden rounded-2xl border border-surface-200 bg-surface-0 text-xs transition-colors duration-300 hover:border-primary-300 dark:border-surface-800 dark:bg-surface-900 dark:hover:border-primary-800 lg:col-span-2 lg:row-span-2">
        <div class="p-6 pb-5 sm:p-7 sm:pb-5">
          <h2 class="text-base font-semibold text-surface-900 dark:text-surface-0">Events and state</h2>
          <p class="mt-2 max-w-lg text-sm leading-relaxed text-surface-500 dark:text-surface-400">
            Every event of an aggregate with its payload and metadata. Open one to see the state it resulted in, compared with the state before.
          </p>
        </div>

        <div class="stage relative mx-2 mb-2 flex flex-1 rounded-xl bg-surface-50 dark:bg-surface-950 min-h-80 items-center justify-center px-5 py-10 text-[13px] sm:px-10" aria-hidden="true">
          <!-- Three layers: the event list, the opened event, and the state it resulted in on top.
               Stacked with small overlaps on a phone, composed freely from sm up. -->
          <div class="relative w-full max-w-2xl sm:h-80">
            <div class="overflow-hidden rounded-lg border border-surface-200 bg-surface-0 shadow-[0_12px_32px_-16px_rgba(15,23,42,0.3)] dark:border-surface-700 dark:bg-surface-900 relative mr-10 sm:absolute sm:top-8 sm:left-0 sm:mr-0 sm:w-[42%]">
              <div class="border-b border-surface-100 px-3 py-2 text-[10px] font-medium uppercase tracking-widest text-surface-400 dark:border-surface-800">Events</div>
              <div class="py-1.5">
                @for (event of events; track event.type) {
                  <div class="flex items-center gap-2.5 border-l-2 px-4 py-2.5"
                       [class]="event.open ? 'border-primary-500 bg-primary-50 dark:bg-primary-950' : 'border-transparent'">
                    <span class="h-1.5 w-1.5 shrink-0 rounded-full" [class]="event.open ? 'bg-primary-500' : 'bg-surface-300 dark:bg-surface-600'"></span>
                    <span class="font-medium" [class]="event.open ? 'text-primary-700 dark:text-primary-300' : 'text-surface-700 dark:text-surface-200'">{{ event.type }}</span>
                  </div>
                }
              </div>
            </div>

            <div class="overflow-hidden rounded-lg border border-surface-200 bg-surface-0 shadow-[0_12px_32px_-16px_rgba(15,23,42,0.3)] dark:border-surface-700 dark:bg-surface-900 relative -mt-4 ml-6 transition-transform duration-500 group-hover:-translate-y-1 sm:absolute sm:top-0 sm:right-0 sm:mt-0 sm:ml-0 sm:w-[48%]">
              <div class="border-b border-surface-100 px-3 py-2 text-[10px] font-medium uppercase tracking-widest text-surface-400 dark:border-surface-800 flex justify-between"><span>OrderShipped</span><span class="font-mono normal-case tracking-normal">rev 2</span></div>
              <div class="space-y-2 p-4 font-mono text-xs">
                <div class="flex gap-1.5"><span class="text-surface-400">carrier:</span><span class="text-surface-700 dark:text-surface-200">"DHL"</span></div>
                <div class="flex gap-1.5"><span class="text-surface-400">trackingNumber:</span><span class="text-surface-700 dark:text-surface-200">"JD0146"</span></div>
                <div class="flex gap-1.5"><span class="text-surface-400">$correlationId:</span><span class="truncate text-surface-700 dark:text-surface-200">"5f0c2b1e"</span></div>
              </div>
            </div>

            <div class="overflow-hidden rounded-lg border border-surface-200 bg-surface-0 shadow-[0_12px_32px_-16px_rgba(15,23,42,0.3)] dark:border-surface-700 dark:bg-surface-900 relative z-10 -mt-4 mr-4 ml-2 transition-transform duration-500 group-hover:-translate-y-2 sm:absolute sm:right-[12%] sm:bottom-0 sm:m-0 sm:w-[52%]">
              <div class="border-b border-surface-100 px-3 py-2 text-[10px] font-medium uppercase tracking-widest text-surface-400 dark:border-surface-800 flex justify-between"><span>State</span><span class="font-mono normal-case tracking-normal">version 3</span></div>
              <div class="space-y-2 p-4 font-mono text-xs">
                <div class="flex gap-1.5"><span class="text-surface-400">total:</span><span class="text-surface-700 dark:text-surface-200">169.40</span></div>
                <div class="flex flex-wrap gap-1.5"><span class="text-surface-400">status:</span>
                  <span class="rounded bg-red-100 px-1 text-red-700 line-through dark:bg-red-950 dark:text-red-300">"CONFIRMED"</span><span class="rounded bg-emerald-100 px-1 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300">"SHIPPED"</span></div>
                <div class="flex"><span class="rounded bg-emerald-100 px-1 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300">trackingNumber: "JD0146"</span></div>
              </div>
            </div>
          </div>
        </div>
      </article>

      <!-- Commands and outcomes: a small deck, the failed command in front -->
      <article class="group relative flex flex-col overflow-hidden rounded-2xl border border-surface-200 bg-surface-0 text-xs transition-colors duration-300 hover:border-primary-300 dark:border-surface-800 dark:bg-surface-900 dark:hover:border-primary-800">
        <div class="p-6 pb-5 sm:p-7 sm:pb-5">
          <h2 class="text-base font-semibold text-surface-900 dark:text-surface-0">Commands and outcomes</h2>
          <p class="mt-2 max-w-lg text-sm leading-relaxed text-surface-500 dark:text-surface-400">Each command with its result, and the cause when it failed.</p>
        </div>

        <div class="stage relative mx-2 mb-2 flex flex-1 rounded-xl bg-surface-50 dark:bg-surface-950 min-h-52 items-center justify-center px-6 py-6" aria-hidden="true">
          <div class="relative h-36 w-full max-w-72">
            <div class="overflow-hidden rounded-lg border border-surface-200 bg-surface-0 shadow-[0_12px_32px_-16px_rgba(15,23,42,0.3)] dark:border-surface-700 dark:bg-surface-900 absolute inset-x-5 top-0 flex items-center gap-2 px-3 py-2.5 opacity-70 transition-transform duration-500 group-hover:-translate-y-1.5">
              <i class="pi pi-check-circle text-[11px] text-emerald-500"></i><span class="font-medium text-surface-700 dark:text-surface-200">ShipOrder</span>
              <span class="ml-auto rounded-full bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300">success</span>
            </div>
            <div class="overflow-hidden rounded-lg border border-surface-200 bg-surface-0 shadow-[0_12px_32px_-16px_rgba(15,23,42,0.3)] dark:border-surface-700 dark:bg-surface-900 absolute inset-x-2.5 top-9 flex items-center gap-2 px-3 py-2.5 opacity-90 transition-transform duration-500 group-hover:-translate-y-0.5">
              <i class="pi pi-check-circle text-[11px] text-emerald-500"></i><span class="font-medium text-surface-700 dark:text-surface-200">DeliverOrder</span>
              <span class="ml-auto rounded-full bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300">success</span>
            </div>
            <div class="overflow-hidden rounded-lg border border-surface-200 bg-surface-0 shadow-[0_12px_32px_-16px_rgba(15,23,42,0.3)] dark:border-surface-700 dark:bg-surface-900 absolute inset-x-0 top-[4.5rem] p-3">
              <div class="flex items-center gap-2">
                <i class="pi pi-times-circle text-[11px] text-red-500"></i>
                <span class="font-medium text-surface-900 dark:text-surface-0">CancelOrder</span>
                <span class="ml-auto rounded-full bg-red-50 px-2 py-0.5 text-[10px] font-semibold text-red-700 dark:bg-red-950 dark:text-red-300">failure</span>
              </div>
              <p class="mt-2 text-red-600 dark:text-red-400">Order has already been delivered.</p>
            </div>
          </div>
        </div>
      </article>

      <!-- Trace and retry: a command branching into the events it produced -->
      <article class="group relative flex flex-col overflow-hidden rounded-2xl border border-surface-200 bg-surface-0 text-xs transition-colors duration-300 hover:border-primary-300 dark:border-surface-800 dark:bg-surface-900 dark:hover:border-primary-800">
        <div class="p-6 pb-5 sm:p-7 sm:pb-5">
          <h2 class="text-base font-semibold text-surface-900 dark:text-surface-0">Trace and retry</h2>
          <p class="mt-2 max-w-lg text-sm leading-relaxed text-surface-500 dark:text-surface-400">Follow a command to the events it produced, and resubmit a failed one.</p>
        </div>

        <div class="stage relative mx-2 mb-2 flex flex-1 rounded-xl bg-surface-50 dark:bg-surface-950 min-h-52 items-center justify-center px-4 py-6" aria-hidden="true">
          <!-- Linked by their correlation ID -->
          <div class="relative flex flex-col items-center gap-4">
            <div class="flex items-center">
              <span class="overflow-hidden rounded-lg border border-surface-200 bg-surface-0 shadow-[0_12px_32px_-16px_rgba(15,23,42,0.3)] dark:border-surface-700 dark:bg-surface-900 px-2.5 py-1.5 font-medium text-surface-900 dark:text-surface-0">PlaceOrder</span>
              <svg class="h-20 w-12 shrink-0 text-primary-400 dark:text-primary-600" viewBox="0 0 48 80" fill="none">
                <path class="flow" d="M0 40 C 24 40, 24 14, 48 14" stroke="currentColor" stroke-width="1.5" />
                <path class="flow" d="M0 40 C 24 40, 24 66, 48 66" stroke="currentColor" stroke-width="1.5" />
              </svg>
              <div class="flex flex-col gap-7">
                <span class="rounded-md bg-primary-50 px-2.5 py-1.5 font-medium text-primary-700 ring-1 ring-primary-200 dark:bg-primary-950 dark:text-primary-300 dark:ring-primary-900">OrderPlaced</span>
                <span class="rounded-md bg-primary-50 px-2.5 py-1.5 font-medium text-primary-700 ring-1 ring-primary-200 dark:bg-primary-950 dark:text-primary-300 dark:ring-primary-900">OrderConfirmed</span>
              </div>
            </div>
            <span class="inline-flex items-center gap-1.5 rounded-full border border-surface-200 bg-surface-0 px-2.5 py-1 font-mono text-[10px] text-surface-500 dark:border-surface-700 dark:bg-surface-900 dark:text-surface-400">
              <i class="pi pi-link text-[10px] text-primary-500"></i>$correlationId 5f0c2b1e
            </span>
          </div>
        </div>
      </article>
    </div>
  `,
  styles: `
    /* The stage: dots that fade out towards the edges, behind the scene */
    .stage::before {
      content: '';
      position: absolute;
      inset: 0;
      border-radius: inherit;
      background-image: radial-gradient(circle, var(--p-surface-300) 1px, transparent 1px);
      background-size: 16px 16px;
      mask-image: radial-gradient(ellipse at center, black 30%, transparent 75%);
      pointer-events: none;
    }
    @media (prefers-color-scheme: dark) {
      .stage::before { background-image: radial-gradient(circle, var(--p-surface-700) 1px, transparent 1px); }
    }

    /* The branches of the trace, flowing from the command to its events */
    .flow { stroke-dasharray: 4 4; animation: flow 1.2s linear infinite; }
    @keyframes flow { to { stroke-dashoffset: -8; } }
    @media (prefers-reduced-motion: reduce) { .flow { animation: none; } }
  `,
})
export class ConsoleFeaturesComponent {
  readonly events: { type: string; open?: boolean }[] = [
    { type: 'OrderDelivered' },
    { type: 'OrderShipped', open: true },
    { type: 'OrderConfirmed' },
    { type: 'OrderPlaced' },
  ];
}
