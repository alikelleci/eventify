import { HttpInterceptorFn, HttpResponse } from '@angular/common/http';
import { of, delay } from 'rxjs';
import { AggregateState, CommandEventsPage, CommandMessage, CommandsPage, EventDetail, EventMessage, EventsPage } from '@eventify/ui/models';
import { AppEntry, AppNode } from '@eventify/ui/services/backend.service';
import { InstanceStatus } from '@eventify/ui/status';

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
    const commandId = nextId();
    const time = NOW - step.minutesAgo * 60_000;
    // Each command has its own correlation ID, also a retry. Its events name it as their cause, so a failed command never shows events.
    const correlationId = fakeUuid(index + 1);
    // A retry names the command it retries: the last one of the same type before it.
    const retried = [...commands].reverse().find(command => command.type === step.command);
    const metadata: Record<string, string> = step.retried && retried
      // Resubmitted from the console: marked like ConsoleService.retryCommand does, without $replyTo.
      ? { '$correlationId': correlationId, '$retryOf': retried.id }
      : { '$correlationId': correlationId, '$replyTo': REPLY_TO };

    commands.push({
      id: commandId,
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
      metadata: { '$correlationId': correlationId, '$causationId': commandId, '$replyTo': REPLY_TO },
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

/** An example application, without how its instances are doing: apps() adds that. */
type MockApp = { name: string; nodes: Omit<AppNode, 'status'>[] };

/** Two example applications, as connected to the console. The same example data answers for both. */
const APPS: MockApp[] = [
  { name: 'orders', nodes: [
    { nodeId: 'orders.3f2a9c1e-7b4d-4c1a-9f0e-5d8a2b6c1e44:0', hostname: 'orders-5d8f7-x2k4q', version: '1.0.0', connectedAt: new Date(NOW - 3_600_000).toISOString() },
    { nodeId: 'orders.b81e44d2-1c3f-4e8a-a2b7-9d6c5e4f3a21:0', hostname: 'orders-5d8f7-p9m2z', version: '1.0.0', connectedAt: new Date(NOW - 3_500_000).toISOString() },
  ] },
  { name: 'payments', nodes: [
    { nodeId: 'payments.7c1d2e3f-4a5b-4c6d-8e9f-0a1b2c3d4e5f:0', hostname: 'payments-7b9c6-k4j8w', version: '1.0.0', connectedAt: new Date(NOW - 7_200_000).toISOString() },
  ] },
  // TEMP: many more applications, to preview the home dashboard with a long list. Remove this block afterwards.
  ...[
    { name: 'shipping', instances: 3, versions: ['1.4.0', '1.4.0', '1.5.0'] },   // mixed: an upgrade rolling out (2 old, 1 new)
    { name: 'returns', instances: 3, versions: ['1.3.0', '1.4.0', '1.5.0'] },    // mixed: every instance on its own version
    { name: 'inventory', instances: 2 },
    { name: 'customers', instances: 6 },                                         // more instances than a card lists
    { name: 'events-gateway', instances: 16, versions: ['2.2.0'] },              // mixed: one instance left behind (1 old, 15 new)
    { name: 'billing', instances: 2, versions: ['1.9.0', '1.9.0'] },            // not mixed: an older version, but the same on all instances
    { name: 'notifications', instances: 1 },
    { name: 'catalog', instances: 4 },
    { name: 'pricing', instances: 2 },
    { name: 'loyalty', instances: 1 },
    { name: 'search-indexer', instances: 3 },
    { name: 'fraud-detection', instances: 2 },
    { name: 'customer-notification-preferences-and-consent-management-service', instances: 2 },       // a long name that wraps in the tooltip of "+N more"
    { name: 'warehouse-management-service-with-a-very-long-name', instances: 3 }, // a long name cut off on its home card
  ].map(({ name, instances, versions }): MockApp => ({
    name,
    nodes: Array.from({ length: instances }, (_, i) => ({
      nodeId: `${name}.${crypto.randomUUID()}:0`,
      hostname: `${name}-6c8d9-${(i + 10).toString(36)}x${i}q`,
      version: versions?.[i] ?? '2.3.1',
      connectedAt: new Date(NOW - (i + 1) * 1_700_000 - name.length * 60_000).toISOString(),
    })),
  })),
];

/** The example applications with how each instance is doing, as /api/apps reports them. */
function apps(): AppEntry[] {
  const running: InstanceStatus = { state: 'RUNNING', stateForMs: 3_600_000, restoring: false };
  // Every state shows up somewhere, per application and instance; the rest is running. On the home page, the two with an
  // error and the first busy one get a card; the other busy ones make "+N more" amber.
  const situations: Record<string, (InstanceStatus | null)[]> = {
    returns: [running, { state: 'ERROR', stateForMs: 2_700_000, restoring: false }],                     // error on 1 of 3
    'warehouse-management-service-with-a-very-long-name': [running, running, { state: 'NOT_RUNNING', stateForMs: 95_000, restoring: false }],
    payments: [{ state: 'REBALANCING', stateForMs: 224_000, restoring: false }],
    shipping: [{ state: 'REBALANCING', stateForMs: 1_500_000, restoring: true }, { state: 'REBALANCING', stateForMs: 4_000, restoring: false }],
    catalog: [running, running, running, { state: 'CREATED', stateForMs: 12_000, restoring: false }],     // one just starting
    customers: [running, running, running, running, { state: 'PENDING_SHUTDOWN', stateForMs: 6_000, restoring: false }, null], // one stopping, one doesn't answer
  };
  // By name, as the console sorts them.
  return [...APPS].sort((a, b) => a.name.localeCompare(b.name)).map(app => ({
    ...app,
    nodes: app.nodes.map((node, i) => {
      const situation = situations[app.name];
      return { ...node, status: situation && i < situation.length ? situation[i] : running };
    }),
  }));
}

export const mockInterceptor: HttpInterceptorFn = (req, next) => {
  if (req.url.endsWith('/api/apps')) return respond(apps(), 100);
  if (req.url.endsWith('/api/session')) return respond({ loginEnabled: false, user: null, appTokenRequired: false }, 50);
  // Retrying only sends the command to Kafka, so it's fast. The retried command itself isn't added to the example data.
  if (req.method === 'POST' && req.url.endsWith('/commands/retry')) return respond(null, 150);
  if (!req.url.includes('/api/apps/') || !req.url.includes('/aggregates/')) return next(req);

  const aggregateId = decodeURIComponent(req.url.match(/\/aggregates\/([^/?]+)/)?.[1] ?? '');
  const { commands, events, states } = historyOf(aggregateId);

  const commandEventsMatch = req.url.match(/\/commands\/([^/?]+)\/events/);
  if (commandEventsMatch) {
    const commandId = decodeURIComponent(commandEventsMatch[1]);
    // Oldest first, like the real backend, which reads the event store in key order.
    return respond({ events: events.filter(e => e.metadata['$causationId'] === commandId).reverse() } satisfies CommandEventsPage, 300);
  }

  if (req.url.includes('/commands')) {
    // Commands are polled from Kafka, which is always slower than reading the event store.
    return respond({ commands, lookbackDays: 7, truncated: false } satisfies CommandsPage, 2000);
  }

  if (req.url.includes('/events')) {
    const eventDetailMatch = req.url.match(/\/events\/([^?]+)/);
    if (eventDetailMatch) {
      const eventId = decodeURIComponent(eventDetailMatch[1]);
      const index = events.findIndex(e => e.id === eventId);
      if (index === -1) return of(new HttpResponse({ status: 404, body: 'Not Found' }));
      const older = events[index + 1];
      const detail: EventDetail = { event: events[index], state: states[eventId] ?? null, previousState: older ? states[older.id] : null, stateKnown: true, previousStateKnown: true };
      return respond(detail, 300);
    }

    return respond({ events, nextCursor: null } satisfies EventsPage, 400);
  }

  return next(req);
};
