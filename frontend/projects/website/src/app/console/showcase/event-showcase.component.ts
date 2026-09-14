import { Component, ViewChild } from '@angular/core';
import { of } from 'rxjs';
import { MessageService } from 'primeng/api';
import { AggregateState, EventDetail, EventMessage } from '@eventify/ui/models';
import { EventDetailComponent } from '@eventify/ui/components/event-detail/event-detail.component';
import { EventifyService } from '@eventify/ui/services/eventify.service';

const AGGREGATE_ID = 'order-7f3a';
const minutesAgo = (m: number) => new Date(Date.now() - m * 60_000).toISOString();

const ORDER = {
  '@class': 'com.example.Order',
  id: AGGREGATE_ID,
  customerId: 'customer-42',
  items: [{ sku: 'CHAIR-OAK', quantity: 2 }],
  total: 129.95,
  currency: 'EUR',
};

function state(version: number, minutes: number, payload: Record<string, unknown>): AggregateState {
  const eventId = `${AGGREGATE_ID}@000000000000${version}`;
  return { id: eventId, eventId, version, type: 'Order', timestamp: minutesAgo(minutes), aggregateId: AGGREGATE_ID, metadata: {}, payload };
}

const EVENT: EventMessage = {
  id: `${AGGREGATE_ID}@0000000000003`,
  type: 'PaymentReceived',
  timestamp: minutesAgo(2),
  aggregateId: AGGREGATE_ID,
  revision: 1,
  payload: { '@class': 'com.example.OrderEvent$PaymentReceived', id: AGGREGATE_ID, amount: 129.95, method: 'CARD' },
  metadata: { '$correlationId': 'corr-capture-payment', '$replyTo': 'orders.replies' },
};

const DETAIL: EventDetail = {
  event: EVENT,
  previousState: state(2, 6, { ...ORDER, status: 'PLACED' }),
  state: state(3, 2, { ...ORDER, status: 'PAID', paidAt: minutesAgo(2) }),
};

/**
 * Landing page showcase: the real event details component on its State tab, showing what changed.
 * Its data comes from a stand-in for the API, so it looks exactly like the console and follows any change to that screen.
 */
@Component({
  selector: 'app-event-showcase',
  standalone: true,
  imports: [EventDetailComponent],
  providers: [
    MessageService,
    { provide: EventifyService, useValue: { getEventDetail: () => of(DETAIL) } },
  ],
  template: `<app-event-detail [event]="event" (loaded)="showStateDiff()" />`,
})
export class EventShowcaseComponent {
  @ViewChild(EventDetailComponent) private detail!: EventDetailComponent;

  readonly event = EVENT;

  showStateDiff() {
    this.detail.activeTab.set('state');
    this.detail.showDiff.set(true);
  }
}
