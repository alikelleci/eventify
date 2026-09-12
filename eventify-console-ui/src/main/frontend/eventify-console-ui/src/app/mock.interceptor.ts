import { HttpInterceptorFn, HttpResponse } from '@angular/common/http';
import { of, delay } from 'rxjs';
import { CommandsPage, CorrelatedEventsPage, EventsPage, EventDetail, AggregateState } from './models';

const AGGREGATE_ID = 'customer-1';

const MOCK_COMMANDS: CommandsPage = {
  commands: [
    {
      id: `${AGGREGATE_ID}@0000000000005`,
      timestamp: new Date(Date.now() - 10_000).toISOString(),
      type: 'CreateCustomer',
      payload: { '@class': 'com.example.CustomerCommand$CreateCustomer', id: AGGREGATE_ID, firstName: 'John', lastName: 'Doe' },
      metadata: { '$correlationId': 'corr-005', '$result': 'success', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
    },
    {
      id: `${AGGREGATE_ID}@0000000000004`,
      timestamp: new Date(Date.now() - 30_000).toISOString(),
      type: 'DeleteCustomer',
      payload: { '@class': 'com.example.CustomerCommand$DeleteCustomer', id: AGGREGATE_ID },
      metadata: { '$correlationId': 'corr-004', '$result': 'success', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
    },
    {
      id: `${AGGREGATE_ID}@0000000000003`,
      timestamp: new Date(Date.now() - 120_000).toISOString(),
      type: 'ChangeFirstName',
      payload: { '@class': 'com.example.CustomerCommand$ChangeFirstName', id: AGGREGATE_ID, firstName: 'Jane' },
      metadata: { '$correlationId': 'corr-003', '$result': 'success', 'retry': 'true', 'source': 'console', 'description': 'Retried via Eventify Console' },
      aggregateId: AGGREGATE_ID,
    },
    {
      id: `${AGGREGATE_ID}@0000000000002`,
      timestamp: new Date(Date.now() - 180_000).toISOString(),
      type: 'ChangeLastName',
      payload: { '@class': 'com.example.CustomerCommand$ChangeLastName', id: AGGREGATE_ID, lastName: 'Smith' },
      metadata: { '$correlationId': 'corr-002', '$result': 'success', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
    },
    {
      id: `${AGGREGATE_ID}@0000000000001`,
      timestamp: new Date(Date.now() - 300_000).toISOString(),
      type: 'CreateCustomer',
      payload: { '@class': 'com.example.CustomerCommand$CreateCustomer', id: AGGREGATE_ID, firstName: 'John', lastName: 'Doe' },
      metadata: { '$correlationId': 'corr-001', '$result': 'failure', '$cause': 'Customer already exists.', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
    },
  ],
};

const MOCK_EVENTS: EventsPage = {
  events: [
    {
      id: `${AGGREGATE_ID}@0000000000005`,
      timestamp: new Date(Date.now() - 10_000).toISOString(),
      type: 'CustomerCreated',
      payload: { '@class': 'com.example.CustomerEvent$CustomerCreated', id: AGGREGATE_ID, firstName: 'John', lastName: 'Doe', email: 'john.doe@example.com' },
      metadata: { '$correlationId': 'corr-005', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
      revision: 1,
    },
    {
      id: `${AGGREGATE_ID}@0000000000004`,
      timestamp: new Date(Date.now() - 30_000).toISOString(),
      type: 'CustomerDeleted',
      payload: { '@class': 'com.example.CustomerEvent$CustomerDeleted', id: AGGREGATE_ID },
      metadata: { '$correlationId': 'corr-004', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
      revision: 1,
    },
    {
      id: `${AGGREGATE_ID}@0000000000003`,
      timestamp: new Date(Date.now() - 120_000).toISOString(),
      type: 'FirstNameChanged',
      payload: { '@class': 'com.example.CustomerEvent$FirstNameChanged', id: AGGREGATE_ID, firstName: 'Jane' },
      metadata: { '$correlationId': 'corr-003-no-match', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
      revision: 1,
    },
    {
      id: `${AGGREGATE_ID}@0000000000002`,
      timestamp: new Date(Date.now() - 180_000).toISOString(),
      type: 'LastNameChanged',
      payload: { '@class': 'com.example.CustomerEvent$LastNameChanged', id: AGGREGATE_ID, lastName: 'Smith' },
      metadata: { '$correlationId': 'corr-002', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
      revision: 1,
    },
    {
      id: `${AGGREGATE_ID}@0000000000001`,
      timestamp: new Date(Date.now() - 300_000).toISOString(),
      type: 'CustomerCreated',
      payload: {
        '@class': 'com.example.CustomerEvent$CustomerCreated',
        id: AGGREGATE_ID,
        firstName: 'John',
        lastName: 'Doe',
        email: 'john.doe@example.com',
        phoneNumber: '+31612345678',
        dateOfBirth: '1985-03-22',
        address: {
          street: 'Keizersgracht 123',
          city: 'Amsterdam',
          postalCode: '1015 CJ',
          country: 'NL',
        },
        preferences: {
          language: 'nl',
          currency: 'EUR',
          newsletterOptIn: true,
          smsOptIn: false,
          theme: 'dark',
        },
        registrationSource: 'WEB',
        referralCode: 'FRIEND2024',
        ipAddress: '84.105.32.17',
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        sessionId: 'sess-1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d',
        auditTrail: [
          { action: 'VALIDATION_PASSED', field: 'email', rule: 'Email', at: new Date(Date.now() - 302_000).toISOString() },
          { action: 'VALIDATION_PASSED', field: 'firstName', rule: 'NotBlank', at: new Date(Date.now() - 301_500).toISOString() },
          { action: 'VALIDATION_PASSED', field: 'lastName', rule: 'NotBlank', at: new Date(Date.now() - 301_000).toISOString() },
          { action: 'COMMAND_ACCEPTED', at: new Date(Date.now() - 300_500).toISOString() },
        ],
        notificationsDispatched: [
          { channel: 'EMAIL', recipient: 'john.doe@example.com', template: 'WELCOME', status: 'SENT' },
          { channel: 'SMS', recipient: '+31612345678', template: 'WELCOME_SMS', status: 'SENT' },
        ],
        tags: ['new-customer', 'web-registration', 'referral'],
      },
      metadata: { '$correlationId': 'corr-001-no-match', '$replyTo': 'my-app.replies' },
      aggregateId: AGGREGATE_ID,
      revision: 1,
    },
  ],
  nextCursor: null,
};

const CUSTOMER_BASE = {
  '@class': 'com.example.Customer',
  id: AGGREGATE_ID,
  firstName: 'John',
  lastName: 'Doe',
  email: 'john.doe@example.com',
  phoneNumber: '+31612345678',
  dateOfBirth: '1985-03-22',
  address: { street: 'Keizersgracht 123', city: 'Amsterdam', postalCode: '1015 CJ', country: 'NL' },
  preferences: { language: 'nl', currency: 'EUR', newsletterOptIn: true, smsOptIn: false, theme: 'dark' },
  registrationSource: 'WEB',
  referralCode: 'FRIEND2024',
  tags: ['new-customer', 'web-registration', 'referral'],
};

const MOCK_STATE_BY_EVENT: Record<string, AggregateState | null> = {
  [`${AGGREGATE_ID}@0000000000001`]: {
    id: `${AGGREGATE_ID}@0000000000001`,
    timestamp: new Date(Date.now() - 300_000).toISOString(),
    type: 'Customer',
    payload: { ...CUSTOMER_BASE },
    metadata: { '$correlationId': 'corr-001' },
    aggregateId: AGGREGATE_ID,
    eventId: `${AGGREGATE_ID}@0000000000001`,
    version: 1,
  },
  [`${AGGREGATE_ID}@0000000000002`]: {
    id: `${AGGREGATE_ID}@0000000000002`,
    timestamp: new Date(Date.now() - 180_000).toISOString(),
    type: 'Customer',
    payload: { ...CUSTOMER_BASE, lastName: 'Smith' },
    metadata: { '$correlationId': 'corr-002' },
    aggregateId: AGGREGATE_ID,
    eventId: `${AGGREGATE_ID}@0000000000002`,
    version: 2,
  },
  [`${AGGREGATE_ID}@0000000000003`]: {
    id: `${AGGREGATE_ID}@0000000000003`,
    timestamp: new Date(Date.now() - 120_000).toISOString(),
    type: 'Customer',
    payload: { ...CUSTOMER_BASE, firstName: 'Jane', lastName: 'Smith' },
    metadata: { '$correlationId': 'corr-003' },
    aggregateId: AGGREGATE_ID,
    eventId: `${AGGREGATE_ID}@0000000000003`,
    version: 3,
  },
  [`${AGGREGATE_ID}@0000000000004`]: null,
  [`${AGGREGATE_ID}@0000000000005`]: {
    id: `${AGGREGATE_ID}@0000000000005`,
    timestamp: new Date(Date.now() - 10_000).toISOString(),
    type: 'Customer',
    payload: { ...CUSTOMER_BASE },
    metadata: { '$correlationId': 'corr-005' },
    aggregateId: AGGREGATE_ID,
    eventId: `${AGGREGATE_ID}@0000000000005`,
    version: 5,
  },
};

export const mockInterceptor: HttpInterceptorFn = (req, next) => {
  if (req.url.includes('/api/aggregates/')) {
    if (req.url.includes('/commands')) {
      return of(new HttpResponse({ status: 200, body: MOCK_COMMANDS })).pipe(delay(400));
    }

    if (req.url.includes('/events')) {
      const correlationMatch = req.url.match(/\/events\/by-correlation\/(.+)/);
      if (correlationMatch) {
        const correlationId = decodeURIComponent(correlationMatch[1]);
        const events = MOCK_EVENTS.events.filter(e => e.metadata['$correlationId'] === correlationId);
        return of(new HttpResponse({ status: 200, body: { events } })).pipe(delay(300));
      }

      const eventDetailMatch = req.url.match(/\/events\/([^?]+)/);
      if (eventDetailMatch) {
        const eventId = decodeURIComponent(eventDetailMatch[1]);
        const eventsList = MOCK_EVENTS.events;
        const idx = eventsList.findIndex(e => e.id === eventId);
        if (idx === -1) return of(new HttpResponse({ status: 404, body: 'Not Found' }));
        const event = eventsList[idx];
        const state = MOCK_STATE_BY_EVENT[eventId] ?? null;
        const previousState = idx < eventsList.length - 1 ? (MOCK_STATE_BY_EVENT[eventsList[idx + 1].id] ?? null) : null;
        const detail: EventDetail = { event, state, previousState };
        return of(new HttpResponse({ status: 200, body: detail })).pipe(delay(300));
      }
      return of(new HttpResponse({ status: 200, body: MOCK_EVENTS })).pipe(delay(400));
    }
  }
  return next(req);
};
