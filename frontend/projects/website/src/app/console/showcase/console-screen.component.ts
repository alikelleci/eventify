import { Component, computed, signal, viewChild } from '@angular/core';
import { DatePipe } from '@angular/common';
import { of } from 'rxjs';
import { MessageService } from 'primeng/api';
import { TabsModule } from 'primeng/tabs';
import { TagModule } from 'primeng/tag';
import { AggregateState, CommandMessage, EventDetail, EventMessage } from '@eventify/ui/models';
import { EventifyService } from '@eventify/ui/services/eventify.service';
import { EventDetailComponent } from '@eventify/ui/components/event-detail/event-detail.component';
import { CommandDetailComponent } from '@eventify/ui/components/command-detail/command-detail.component';
import { TimelineItemComponent } from '@eventify/ui/components/timeline-item.component';

const ORDER_ID = 'order-7f3a';
const NOW = Date.now();
const at = (minutesAgo: number, ms = 0) => new Date(NOW - minutesAgo * 60_000 + ms).toISOString();
const ITEMS = [{ sku: 'CHAIR-OAK', quantity: 2, unitPrice: 64.95 }, { sku: 'LAMP-BRASS', quantity: 1, unitPrice: 39.5 }];

interface Step {
  command: string;
  minutesAgo: number;
  payload: Record<string, unknown>;
  failure?: string;
  retried?: boolean;
  events?: { type: string; payload: Record<string, unknown>; revision?: number; apply: (order: Record<string, unknown>, time: string) => Record<string, unknown> }[];
}

/** The same order as the replay, oldest first. */
const STEPS: Step[] = [
  {
    command: 'PlaceOrder', minutesAgo: 190, payload: { customerId: 'customer-42', items: ITEMS },
    events: [
      { type: 'OrderPlaced', payload: { customerId: 'customer-42', items: ITEMS, total: 169.4 },
        apply: (_, time) => ({ id: ORDER_ID, status: 'PLACED', customerId: 'customer-42', total: 169.4, placedAt: time }) },
      { type: 'OrderConfirmed', payload: { paymentReference: 'PSP-7F3A-92K1' },
        apply: (order, time) => ({ ...order, status: 'CONFIRMED', paymentReference: 'PSP-7F3A-92K1', confirmedAt: time }) },
    ],
  },
  { command: 'ShipOrder', minutesAgo: 95, payload: { carrier: 'DHL' }, failure: 'Carrier DHL is temporarily unavailable (HTTP 503). Try again later.' },
  {
    command: 'ShipOrder', minutesAgo: 80, payload: { carrier: 'DHL' }, retried: true,
    events: [{ type: 'OrderShipped', revision: 2, payload: { carrier: 'DHL', trackingNumber: 'JD014600003SE' },
      apply: (order, time) => ({ ...order, status: 'SHIPPED', carrier: 'DHL', trackingNumber: 'JD014600003SE', shippedAt: time }) }],
  },
  {
    command: 'DeliverOrder', minutesAgo: 12, payload: { signedBy: 'J. de Vries' },
    events: [{ type: 'OrderDelivered', payload: { signedBy: 'J. de Vries' },
      apply: (order, time) => ({ ...order, status: 'DELIVERED', signedBy: 'J. de Vries', deliveredAt: time }) }],
  },
  { command: 'CancelOrder', minutesAgo: 3, payload: { reason: 'CUSTOMER_REQUEST' }, failure: 'Order has already been delivered and can no longer be cancelled.' },
];

/** Commands and events newest first, like the console lists, with the state after each event. */
function buildOrder() {
  const commands: CommandMessage[] = [];
  const events: EventMessage[] = [];
  const states = new Map<string, AggregateState>();
  let order: Record<string, unknown> = {};
  let sequence = 0;
  const nextId = () => `${ORDER_ID}@${String(++sequence).padStart(13, '0')}`;

  STEPS.forEach((step, index) => {
    const correlationId = `5f0c2b1e-7d4a-4c8e-9b3f-${String(index + 1).padStart(12, '0')}`;
    commands.push({
      id: nextId(), timestamp: at(step.minutesAgo), type: step.command, aggregateId: ORDER_ID,
      payload: { id: ORDER_ID, ...step.payload },
      metadata: {
        '$correlationId': correlationId,
        ...(step.retried ? { retry: 'true', source: 'console' } : {}),
        ...(step.failure ? { '$result': 'failure', '$cause': step.failure } : { '$result': 'success' }),
      },
    });
    if (step.failure) return;
    step.events?.forEach((e, i) => {
      const event: EventMessage = {
        id: nextId(), timestamp: at(step.minutesAgo, (i + 1) * 4), type: e.type, aggregateId: ORDER_ID, revision: e.revision ?? 1,
        payload: { id: ORDER_ID, ...e.payload }, metadata: { '$correlationId': correlationId },
      };
      order = e.apply(order, event.timestamp);
      events.push(event);
      states.set(event.id, {
        id: event.id, eventId: event.id, aggregateId: ORDER_ID, type: 'Order', version: events.length,
        timestamp: event.timestamp, metadata: {}, payload: order,
      });
    });
  });

  return { commands: commands.reverse(), events: events.reverse(), states };
}

const ORDER = buildOrder();

/** Stands in for the console API, so the real detail components show the example order. */
const EXAMPLE_API: Partial<EventifyService> = {
  getEventDetail: (_, eventId) => {
    const index = ORDER.events.findIndex(e => e.id === eventId);
    const older = ORDER.events[index + 1];
    const detail: EventDetail = {
      event: ORDER.events[index],
      state: ORDER.states.get(eventId) ?? null,
      previousState: older ? ORDER.states.get(older.id) ?? null : null,
    };
    return of(detail);
  },
  getEventsByCorrelation: (_, correlationId) =>
    of({ events: [...ORDER.events].reverse().filter(e => e.metadata['$correlationId'] === correlationId) }),
  retryCommand: () => of(undefined),
};

/**
 * The console's aggregate page, for the landing page: the same lists and the real detail components, on an example order.
 * Clickable like the console itself; it opens on the state diff of the retried shipment.
 */
@Component({
  selector: 'app-console-screen',
  standalone: true,
  imports: [DatePipe, TabsModule, TagModule, TimelineItemComponent, EventDetailComponent, CommandDetailComponent],
  providers: [MessageService, { provide: EventifyService, useValue: EXAMPLE_API }],
  template: `
    <div class="overflow-hidden rounded-xl border border-surface-200 bg-surface-0 text-left shadow-[0_40px_100px_-40px_rgba(15,23,42,0.45)] dark:border-surface-700 dark:bg-surface-900">
      <!-- Browser bar -->
      <div class="flex items-center gap-4 border-b border-surface-200 bg-surface-100 px-4 py-2.5 dark:border-surface-700 dark:bg-surface-800">
        <div class="flex gap-1.5" aria-hidden="true">
          <span class="h-2.5 w-2.5 rounded-full bg-surface-300 dark:bg-surface-600"></span>
          <span class="h-2.5 w-2.5 rounded-full bg-surface-300 dark:bg-surface-600"></span>
          <span class="h-2.5 w-2.5 rounded-full bg-surface-300 dark:bg-surface-600"></span>
        </div>
        <div class="mx-auto w-full max-w-md truncate rounded-md bg-surface-0 px-3 py-1 text-center font-mono text-[11px] text-surface-500 dark:bg-surface-900 dark:text-surface-400">
          localhost:8085/console/aggregates/{{ orderId }}
        </div>
        <div class="w-[46px]" aria-hidden="true"></div>
      </div>

      <!-- The console header, as in header.component.ts -->
      <div class="flex items-center gap-3 border-b border-slate-700 bg-slate-900 px-5 py-3" aria-hidden="true">
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
          <p-tabs [value]="tab()" (valueChange)="switchTab($event)" class="!bg-transparent">
            <div class="px-4">
              <p-tablist>
                <p-tab value="events">Events</p-tab>
                <p-tab value="commands">Commands</p-tab>
              </p-tablist>
            </div>
          </p-tabs>
          <div class="min-h-0 flex-1 px-4 pt-3 pb-4">
            <div class="max-h-full overflow-y-auto rounded-lg border border-surface-100 dark:border-surface-800">
              @if (tab() === 'events') {
                @for (event of events; track event.id) {
                  <app-timeline-item [selected]="selected() === event" [first]="$first" [last]="$last" (click)="selected.set(event)">
                    <div class="flex min-w-0 flex-1 flex-col">
                      <div class="flex h-[22px] items-center gap-2">
                        <span class="truncate text-sm font-medium">{{ event.type }}</span>
                        @if (event.revision > 1) { <p-tag [value]="'rev ' + event.revision" severity="secondary" styleClass="shrink-0" /> }
                      </div>
                      <span class="mt-0.5 text-xs text-surface-400">{{ event.timestamp | date:'MMM d, y · HH:mm:ss' }}</span>
                    </div>
                    <i class="pi pi-chevron-right shrink-0 text-sm text-surface-400"></i>
                  </app-timeline-item>
                }
              } @else {
                @for (command of commands; track command.id) {
                  <app-timeline-item [tone]="command.metadata['$result'] === 'failure' ? 'failure' : 'success'"
                                     [selected]="selected() === command" [first]="$first" [last]="$last" (click)="selected.set(command)">
                    <div class="flex min-w-0 flex-1 flex-col">
                      <div class="flex h-[22px] items-center gap-2">
                        <span class="truncate text-sm font-medium">{{ command.type }}</span>
                        @if (command.metadata['$result'] === 'failure') { <p-tag value="failure" severity="danger" styleClass="shrink-0" /> }
                        @if (command.metadata['retry'] === 'true') { <p-tag value="retried" severity="warn" styleClass="shrink-0" /> }
                      </div>
                      <span class="mt-0.5 text-xs text-surface-400">{{ command.timestamp | date:'MMM d, y · HH:mm:ss' }}</span>
                    </div>
                    <i class="pi pi-chevron-right shrink-0 text-sm text-surface-400"></i>
                  </app-timeline-item>
                }
              }
            </div>
          </div>
        </div>

        <div class="flex-1 overflow-y-auto px-6 py-5">
          @if (selectedEvent(); as event) {
            <app-event-detail [event]="event" (loaded)="onEventLoaded()" />
          } @else if (selectedCommand(); as command) {
            <app-command-detail [command]="command" />
          }
        </div>
      </div>
    </div>
  `,
})
export class ConsoleScreenComponent {
  readonly orderId = ORDER_ID;
  readonly events = ORDER.events;
  readonly commands = ORDER.commands;

  readonly tab = signal<'events' | 'commands'>('events');
  /** Opens on the retried shipment, the event with the most to show. */
  readonly selected = signal<EventMessage | CommandMessage>(ORDER.events[1]);
  readonly selectedEvent = computed(() => { const s = this.selected(); return 'revision' in s ? s : null; });
  readonly selectedCommand = computed(() => { const s = this.selected(); return 'revision' in s ? null : s; });

  private readonly eventDetail = viewChild(EventDetailComponent);
  private firstLoad = true;

  switchTab(tab: string | number | undefined) {
    const next = tab === 'commands' ? 'commands' : 'events';
    this.tab.set(next);
    // Like the console on desktop: the newest item of the tab opens.
    this.selected.set(next === 'events' ? this.events[0] : this.commands[0]);
  }

  /** The first event opens on its State tab with the diff, so the screen shows what the event changed. */
  onEventLoaded() {
    if (!this.firstLoad) return;
    this.firstLoad = false;
    this.eventDetail()?.activeTab.set('state');
    this.eventDetail()?.showDiff.set(true);
  }
}
