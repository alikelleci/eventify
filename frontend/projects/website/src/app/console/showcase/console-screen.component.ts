import { Component, viewChild } from '@angular/core';
import { DatePipe } from '@angular/common';
import { of } from 'rxjs';
import { MessageService } from 'primeng/api';
import { TabsModule } from 'primeng/tabs';
import { TagModule } from 'primeng/tag';
import { AggregateState, EventDetail, EventMessage } from '@eventify/ui/models';
import { EventifyService } from '@eventify/ui/services/eventify.service';
import { EventDetailComponent } from '@eventify/ui/components/event-detail/event-detail.component';
import { TimelineItemComponent } from '@eventify/ui/components/timeline-item.component';

const ORDER_ID = 'order-7f3a';
const NOW = Date.now();
const at = (minutesAgo: number, ms = 0) => new Date(NOW - minutesAgo * 60_000 + ms).toISOString();
const ITEMS = [{ sku: 'CHAIR-OAK', quantity: 2, unitPrice: 64.95 }, { sku: 'LAMP-BRASS', quantity: 1, unitPrice: 39.5 }];

interface Step {
  command: string;
  minutesAgo: number;
  events: { type: string; payload: Record<string, unknown>; revision?: number; apply: (order: Record<string, unknown>, time: string) => Record<string, unknown> }[];
}

/** The events of the same order as the replay, oldest first, grouped by the command that produced them. */
const STEPS: Step[] = [
  {
    command: 'PlaceOrder', minutesAgo: 190,
    events: [
      { type: 'OrderPlaced', payload: { customerId: 'customer-42', items: ITEMS, total: 169.4 },
        apply: (_, time) => ({ id: ORDER_ID, status: 'PLACED', customerId: 'customer-42', total: 169.4, placedAt: time }) },
      { type: 'OrderConfirmed', payload: { paymentReference: 'PSP-7F3A-92K1' },
        apply: (order, time) => ({ ...order, status: 'CONFIRMED', paymentReference: 'PSP-7F3A-92K1', confirmedAt: time }) },
    ],
  },
  {
    command: 'ShipOrder', minutesAgo: 80,
    events: [{ type: 'OrderShipped', revision: 2, payload: { carrier: 'DHL', trackingNumber: 'JD014600003SE' },
      apply: (order, time) => ({ ...order, status: 'SHIPPED', carrier: 'DHL', trackingNumber: 'JD014600003SE', shippedAt: time }) }],
  },
  {
    command: 'DeliverOrder', minutesAgo: 12,
    events: [{ type: 'OrderDelivered', payload: { signedBy: 'J. de Vries' },
      apply: (order, time) => ({ ...order, status: 'DELIVERED', signedBy: 'J. de Vries', deliveredAt: time }) }],
  },
];

/** The events newest first, like the console list, with the state after each one. */
function buildOrder() {
  const events: EventMessage[] = [];
  const states = new Map<number, AggregateState>();
  let order: Record<string, unknown> = {};
  let sequence = 0;

  STEPS.forEach((step, index) => {
    const correlationId = `5f0c2b1e-7d4a-4c8e-9b3f-${String(index + 1).padStart(12, '0')}`;
    step.events.forEach((e, i) => {
      const event: EventMessage = {
        id: `8c1d4e2a-3b5f-4a6c-9d7e-${String(++sequence).padStart(12, '0')}`, sequence, timestamp: at(step.minutesAgo, (i + 1) * 4),
        type: e.type, aggregateType: 'order', aggregateId: ORDER_ID, revision: e.revision ?? 1,
        payload: { id: ORDER_ID, ...e.payload }, metadata: { '$correlationId': correlationId },
      };
      order = e.apply(order, event.timestamp);
      events.push(event);
      states.set(event.sequence, {
        aggregateId: ORDER_ID, type: 'Order', version: event.sequence,
        timestamp: event.timestamp, metadata: {}, payload: order,
      });
    });
  });

  return { events: events.reverse(), states };
}

const ORDER = buildOrder();

/** Stands in for the console API, so the real detail components show the example order. */
const EXAMPLE_API: Partial<EventifyService> = {
  getEventDetail: (_type, _id, sequence) => {
    const detail: EventDetail = {
      event: ORDER.events.find(e => e.sequence === sequence)!,
      state: ORDER.states.get(sequence) ?? null,
      previousState: ORDER.states.get(sequence - 1) ?? null,
      stateKnown: true,
      previousStateKnown: true,
    };
    return of(detail);
  },
};

/**
 * The console's aggregate page, for the landing page: the events list and the real event detail component, on an example order.
 * A picture, not a demo: it can't be clicked, and it shows the state diff of the shipment.
 */
@Component({
  selector: 'app-console-screen',
  standalone: true,
  imports: [DatePipe, TabsModule, TagModule, TimelineItemComponent, EventDetailComponent],
  providers: [MessageService, { provide: EventifyService, useValue: EXAMPLE_API }],
  host: { inert: '', 'aria-hidden': 'true' },
  template: `
    <div class="pointer-events-none overflow-hidden rounded-xl border border-surface-200 bg-surface-0 text-left shadow-[0_40px_100px_-40px_rgba(15,23,42,0.45)] select-none dark:border-surface-700 dark:bg-surface-900">
      <!-- The console header, as in header.component.ts -->
      <div class="flex items-center gap-3 border-b border-slate-700 bg-slate-900 px-5 py-3">
        <i class="pi pi-bolt text-xl text-primary-400"></i>
        <span class="text-lg tracking-tight"><span class="font-semibold text-white">Eventify</span><span class="ml-1.5 font-light text-slate-300">Console</span></span>
        <div class="relative ml-auto w-80">
          <i class="pi pi-search absolute top-1/2 left-2.5 -translate-y-1/2 text-xs text-slate-400"></i>
          <div class="flex h-8 items-center rounded border border-slate-700 bg-slate-800 pl-8 text-sm text-slate-100">{{ orderId }}</div>
        </div>
      </div>

      <!-- The aggregate page, as in aggregate.component.html -->
      <div class="flex h-[34rem]">
        <div class="flex w-[340px] shrink-0 flex-col border-r border-surface-100 dark:border-surface-800">
          <p-tabs value="events" class="!bg-transparent">
            <div class="px-4">
              <p-tablist>
                <p-tab value="events">Events</p-tab>
                <p-tab value="commands">Commands</p-tab>
              </p-tablist>
            </div>
          </p-tabs>
          <div class="min-h-0 flex-1 px-4 pt-3 pb-4">
            <div class="overflow-hidden rounded-lg border border-surface-100 dark:border-surface-800">
              @for (event of events; track event.id) {
                <app-timeline-item [selected]="event === selected" [reached]="$index >= events.indexOf(selected)" [first]="$first" [last]="$last" [interactive]="false">
                  <div class="flex min-w-0 flex-1 flex-col">
                    <div class="flex h-[22px] items-center gap-2">
                      <span class="flex min-w-0 items-baseline gap-2">
                        <span class="shrink-0 font-mono text-xs tabular-nums text-surface-400">#{{ event.sequence }}</span>
                        <span class="truncate text-sm font-medium">{{ event.type }}</span>
                      </span>
                      @if (event.revision > 1) { <p-tag [value]="'rev ' + event.revision" severity="secondary" styleClass="shrink-0" /> }
                    </div>
                    <span class="mt-0.5 text-xs text-surface-400">{{ event.timestamp | date:'MMM d, y · HH:mm:ss' }}</span>
                  </div>
                  <i class="pi pi-chevron-right shrink-0 text-sm text-surface-400"></i>
                </app-timeline-item>
              }
            </div>
          </div>
        </div>

        <div class="flex-1 overflow-hidden px-6 py-5">
          <app-event-detail [event]="selected" (loaded)="showStateDiff()" />
        </div>
      </div>
    </div>
  `,
})
export class ConsoleScreenComponent {
  readonly orderId = ORDER_ID;
  readonly events = ORDER.events;
  /** The shipment: the event with the most to show. */
  readonly selected = ORDER.events[1];

  private readonly eventDetail = viewChild(EventDetailComponent);

  /** Opens the State tab with the diff, so the screen shows what the event changed. */
  showStateDiff() {
    this.eventDetail()?.activeTab.set('state');
    this.eventDetail()?.showDiff.set(true);
  }
}
