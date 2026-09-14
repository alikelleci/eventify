import { HttpInterceptorFn, HttpResponse } from '@angular/common/http';
import { of, delay } from 'rxjs';
import { AggregateState, CommandMessage, CommandsPage, EventDetail, EventMessage, EventsPage } from '@eventify/ui/models';

/**
 * Example data for development: the history of an order, as commands with their outcome and the events they produced.
 * The state after each event is derived from the events, so every diff is consistent.
 *
 * Search any ID for a delivered order, or one of these for the other cases:
 * - "cancelled": an order that could not be shipped and was cancelled
 * - "rejected":  no events, only a rejected command
 * - "not-found": nothing at all (like the backend, an unknown aggregate is an empty list, not a 404)
 */
const CANCELLED_ID = 'cancelled';
const REJECTED_ID = 'rejected';
const NOT_FOUND_ID = 'not-found';

interface Step {
  command: string;
  minutesAgo: number;
  payload?: Record<string, unknown>;
  /** The cause. A failed command produces no events. */
  failure?: string;
  /** Resubmitted from the console after an earlier failure. */
  retried?: boolean;
  events?: { type: string; payload?: Record<string, unknown>; revision?: number }[];
}

const ITEMS = [
  { sku: 'CHAIR-OAK', name: 'Oak dining chair', quantity: 2, unitPrice: 64.95 },
  { sku: 'LAMP-BRASS', name: 'Brass table lamp', quantity: 1, unitPrice: 39.50 },
];
const TOTAL = 169.40;
const ADDRESS = { street: 'Keizersgracht 123', postalCode: '1015 CJ', city: 'Amsterdam', country: 'NL' };
const PLACE_ORDER: Step = {
  command: 'PlaceOrder',
  minutesAgo: 190,
  payload: { customerId: 'customer-42', items: ITEMS, shippingAddress: ADDRESS, currency: 'EUR' },
  // Payment is authorised while placing the order, so a valid order is confirmed right away: one command, two events.
  events: [
    { type: 'OrderPlaced', payload: { customerId: 'customer-42', items: ITEMS, total: TOTAL, currency: 'EUR', shippingAddress: ADDRESS } },
    { type: 'OrderConfirmed', payload: { paymentReference: 'PSP-7F3A-92K1' } },
  ],
};

const DELIVERED_ORDER: Step[] = [
  PLACE_ORDER,
  { command: 'ShipOrder', minutesAgo: 95, payload: { carrier: 'DHL' }, failure: 'Carrier DHL is temporarily unavailable (HTTP 503). Try again later.' },
  {
    command: 'ShipOrder', minutesAgo: 80, payload: { carrier: 'DHL' }, retried: true,
    // Revision 2: the event gained the tracking number in a later version of its schema.
    events: [{ type: 'OrderShipped', payload: { carrier: 'DHL', trackingNumber: 'JD014600003SE' }, revision: 2 }],
  },
  { command: 'DeliverOrder', minutesAgo: 12, payload: { signedBy: 'J. de Vries' }, events: [{ type: 'OrderDelivered', payload: { signedBy: 'J. de Vries' } }] },
  { command: 'CancelOrder', minutesAgo: 3, payload: { reason: 'CUSTOMER_REQUEST' }, failure: 'Order has already been delivered and can no longer be cancelled.' },
];

const CANCELLED_ORDER: Step[] = [
  PLACE_ORDER,
  { command: 'ShipOrder', minutesAgo: 140, payload: { carrier: 'DHL' }, failure: 'Item LAMP-BRASS is out of stock.' },
  { command: 'CancelOrder', minutesAgo: 125, payload: { reason: 'OUT_OF_STOCK' }, events: [{ type: 'OrderCancelled', payload: { reason: 'OUT_OF_STOCK' } }] },
];

const REJECTED_ORDER: Step[] = [
  { command: 'PlaceOrder', minutesAgo: 1, payload: { customerId: 'customer-17', items: ITEMS, shippingAddress: ADDRESS, currency: 'EUR' }, failure: 'Customer customer-17 is blocked and cannot place orders.' },
];

// Fixed at startup, so times and IDs stay the same between requests.
const NOW = Date.now();
const REPLY_TO = 'orders.replies';

interface History {
  commands: CommandMessage[];               // newest first, like the backend
  events: EventMessage[];                   // newest first
  states: Record<string, AggregateState>;   // the order after each event, by event ID
}

function buildHistory(orderId: string, steps: Step[]): History {
  let sequence = 0;
  // Fake IDs in the backend's aggregateId@key shape, numbered in the order things happened.
  const nextId = () => `${orderId}@${String(++sequence).padStart(13, '0')}`;
  const fakeUuid = (n: number) => `${(0x5f0c2b1e + n * 0x1a2b3c).toString(16).slice(-8)}-7d4a-4c8e-9b3f-${String(n).padStart(12, '0')}`;

  const commands: CommandMessage[] = [];
  const events: EventMessage[] = [];

  steps.forEach((step, index) => {
    const time = NOW - step.minutesAgo * 60_000;
    // Each command has its own correlation ID, also a retry, so a failed command never shows events.
    const correlationId = fakeUuid(index + 1);
    const metadata: Record<string, string> = step.retried
      // Resubmitted from the console: marked like EventifyService.retryCommand does, without $replyTo.
      ? { '$correlationId': correlationId, 'retry': 'true', 'source': 'console', 'description': 'Retried via Eventify Console' }
      : { '$correlationId': correlationId, '$replyTo': REPLY_TO };

    commands.push({
      id: nextId(),
      timestamp: new Date(time).toISOString(),
      type: step.command,
      aggregateId: orderId,
      payload: { '@class': `com.example.order.OrderCommand$${step.command}`, id: orderId, ...step.payload },
      metadata: { ...metadata, ...(step.failure ? { '$result': 'failure', '$cause': step.failure } : { '$result': 'success' }) },
    });

    if (step.failure) return;
    (step.events ?? []).forEach((e, i) => events.push({
      id: nextId(),
      // A few milliseconds after the command, and apart from each other, like events handled in one go.
      timestamp: new Date(time + (i + 1) * 4).toISOString(),
      type: e.type,
      aggregateId: orderId,
      revision: e.revision ?? 1,
      payload: { '@class': `com.example.order.OrderEvent$${e.type}`, id: orderId, ...e.payload },
      metadata: { '$correlationId': correlationId, '$replyTo': REPLY_TO },
    }));
  });

  // The order after each event, applied oldest first.
  const states: Record<string, AggregateState> = {};
  let order: Record<string, unknown> = {};
  events.forEach((e, index) => {
    order = applyEvent(order, e);
    states[e.id] = {
      id: e.id, eventId: e.id, aggregateId: orderId, type: 'Order', version: index + 1,
      timestamp: e.timestamp, metadata: { '$correlationId': e.metadata['$correlationId'] }, payload: order,
    };
  });

  const newestFirst = <T extends { timestamp: string }>(list: T[]) => [...list].sort((a, b) => b.timestamp.localeCompare(a.timestamp));
  return { commands: newestFirst(commands), events: newestFirst(events), states };
}

function applyEvent(order: Record<string, unknown>, event: EventMessage): Record<string, unknown> {
  const p = event.payload;
  switch (event.type) {
    case 'OrderPlaced':
      return {
        '@class': 'com.example.order.Order', id: event.aggregateId, status: 'PLACED', customerId: p['customerId'],
        items: p['items'], total: p['total'], currency: p['currency'], shippingAddress: p['shippingAddress'], placedAt: event.timestamp,
      };
    case 'OrderConfirmed':
      return { ...order, status: 'CONFIRMED', paymentReference: p['paymentReference'], confirmedAt: event.timestamp };
    case 'OrderShipped':
      return { ...order, status: 'SHIPPED', carrier: p['carrier'], trackingNumber: p['trackingNumber'], shippedAt: event.timestamp };
    case 'OrderDelivered':
      return { ...order, status: 'DELIVERED', signedBy: p['signedBy'], deliveredAt: event.timestamp };
    case 'OrderCancelled':
      return { ...order, status: 'CANCELLED', cancellationReason: p['reason'], cancelledAt: event.timestamp };
    default:
      return order;
  }
}

const histories = new Map<string, History>();

function historyOf(aggregateId: string): History {
  if (!histories.has(aggregateId)) {
    const steps = aggregateId === NOT_FOUND_ID ? []
      : aggregateId === REJECTED_ID ? REJECTED_ORDER
      : aggregateId === CANCELLED_ID ? CANCELLED_ORDER
      : DELIVERED_ORDER;
    histories.set(aggregateId, buildHistory(aggregateId, steps));
  }
  return histories.get(aggregateId)!;
}

const respond = (body: unknown, ms: number) => of(new HttpResponse({ status: 200, body })).pipe(delay(ms));

export const mockInterceptor: HttpInterceptorFn = (req, next) => {
  if (!req.url.includes('/api/aggregates/')) return next(req);

  const aggregateId = decodeURIComponent(req.url.match(/\/api\/aggregates\/([^/?]+)/)?.[1] ?? '');
  const { commands, events, states } = historyOf(aggregateId);

  // Retrying only sends the command to Kafka, so it's fast. The retried command itself isn't added to the example data.
  if (req.method === 'POST' && req.url.endsWith('/retry')) {
    return respond(null, 150);
  }

  if (req.url.includes('/commands')) {
    // Commands are polled from Kafka, which is always slower than reading the event store.
    return respond({ commands } satisfies CommandsPage, 2000);
  }

  if (req.url.includes('/events')) {
    const correlationMatch = req.url.match(/\/events\/by-correlation\/(.+)/);
    if (correlationMatch) {
      const correlationId = decodeURIComponent(correlationMatch[1]);
      // Oldest first, like the real backend, which reads the event store in key order.
      return respond({ events: events.filter(e => e.metadata['$correlationId'] === correlationId).reverse() }, 300);
    }

    const eventDetailMatch = req.url.match(/\/events\/([^?]+)/);
    if (eventDetailMatch) {
      const eventId = decodeURIComponent(eventDetailMatch[1]);
      const index = events.findIndex(e => e.id === eventId);
      if (index === -1) return of(new HttpResponse({ status: 404, body: 'Not Found' }));
      const older = events[index + 1];
      const detail: EventDetail = { event: events[index], state: states[eventId] ?? null, previousState: older ? states[older.id] : null };
      return respond(detail, 300);
    }

    return respond({ events, nextCursor: null } satisfies EventsPage, 400);
  }

  return next(req);
};
