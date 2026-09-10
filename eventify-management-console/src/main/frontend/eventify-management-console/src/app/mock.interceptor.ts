import { HttpInterceptorFn, HttpResponse } from '@angular/common/http';
import { of, delay } from 'rxjs';
import { EventsPage, EventDetail, AggregateState } from './models';

const AGGREGATE_ID = 'customer-1';

const MOCK_EVENTS: EventsPage = {
  events: [
    {
      id: `${AGGREGATE_ID}@0000000000003`,
      timestamp: new Date(Date.now() - 60_000).toISOString(),
      type: 'FirstNameChanged',
      aggregateId: AGGREGATE_ID,
      revision: 1,
      metadata: { '$correlationId': 'corr-003', '$replyTo': 'my-app.replies' },
      payload: { '@type': 'com.example.CustomerEvent$FirstNameChanged', id: AGGREGATE_ID, firstName: 'Jane' },
    },
    {
      id: `${AGGREGATE_ID}@0000000000002`,
      timestamp: new Date(Date.now() - 120_000).toISOString(),
      type: 'CustomerUpdated',
      aggregateId: AGGREGATE_ID,
      revision: 1,
      metadata: { '$correlationId': 'corr-002', '$replyTo': 'my-app.replies' },
      payload: { '@type': 'com.example.CustomerEvent$CustomerUpdated', id: AGGREGATE_ID, firstName: 'John', lastName: 'Smith' },
    },
    {
      id: `${AGGREGATE_ID}@0000000000001`,
      timestamp: new Date(Date.now() - 300_000).toISOString(),
      type: 'CustomerCreated',
      aggregateId: AGGREGATE_ID,
      revision: 1,
      metadata: { '$correlationId': 'corr-001', '$replyTo': 'my-app.replies' },
      payload: {
        '@type': 'com.example.CustomerEvent$CustomerCreated',
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
    },
  ],
  nextCursor: null,
};

const MOCK_STATE_BY_EVENT: Record<string, AggregateState> = {
  [`${AGGREGATE_ID}@0000000000001`]: {
    eventId: `${AGGREGATE_ID}@0000000000001`,
    timestamp: new Date(Date.now() - 300_000).toISOString(),
    version: 1,
    metadata: { '$correlationId': 'corr-001' },
    payload: {
      '@type': 'com.example.Customer',
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
    },
  },
  [`${AGGREGATE_ID}@0000000000002`]: {
    eventId: `${AGGREGATE_ID}@0000000000002`,
    timestamp: new Date(Date.now() - 120_000).toISOString(),
    version: 2,
    metadata: { '$correlationId': 'corr-002' },
    payload: {
      '@type': 'com.example.Customer',
      id: AGGREGATE_ID,
      firstName: 'John',
      lastName: 'Smith',
      email: 'john.doe@example.com',
      phoneNumber: '+31612345678',
      dateOfBirth: '1985-03-22',
      address: { street: 'Keizersgracht 123', city: 'Amsterdam', postalCode: '1015 CJ', country: 'NL' },
      preferences: { language: 'nl', currency: 'EUR', newsletterOptIn: true, smsOptIn: false, theme: 'dark' },
      registrationSource: 'WEB',
      referralCode: 'FRIEND2024',
      tags: ['new-customer', 'web-registration', 'referral'],
    },
  },
  [`${AGGREGATE_ID}@0000000000003`]: {
    eventId: `${AGGREGATE_ID}@0000000000003`,
    timestamp: new Date(Date.now() - 60_000).toISOString(),
    version: 3,
    metadata: { '$correlationId': 'corr-003' },
    payload: {
      '@type': 'com.example.Customer',
      id: AGGREGATE_ID,
      firstName: 'Jane',
      lastName: 'Smith',
      email: 'jane.doe@example.com',
      phoneNumber: '+31612345678',
      dateOfBirth: '1985-03-22',
      address: { street: 'Prinsengracht 456', city: 'Amsterdam', postalCode: '1016 HV', country: 'NL' },
      preferences: { language: 'en', currency: 'EUR', newsletterOptIn: false, smsOptIn: true, theme: 'light' },
      registrationSource: 'WEB',
      referralCode: 'FRIEND2024',
      tags: ['new-customer', 'web-registration', 'referral', 'updated'],
    },
  },
};

export const mockInterceptor: HttpInterceptorFn = (req, next) => {
  if (req.url.includes('/api/aggregates') && req.url.includes('/events')) {
    const eventDetailMatch = req.url.match(/\/api\/aggregates\/[^/]+\/events\/(.+)/);
    if (eventDetailMatch) {
      const eventId = decodeURIComponent(eventDetailMatch[1]);
      const eventsList = MOCK_EVENTS.events;
      const idx = eventsList.findIndex(e => e.id === eventId);
      if (idx === -1) return of(new HttpResponse({ status: 404, body: 'Not Found' }));
      const event = eventsList[idx];
      const state = MOCK_STATE_BY_EVENT[eventId];
      const previousState = idx < eventsList.length - 1 ? MOCK_STATE_BY_EVENT[eventsList[idx + 1].id] : null;
      const detail: EventDetail = { event, state, previousState };
      return of(new HttpResponse({ status: 200, body: detail })).pipe(delay(300));
    }
    return of(new HttpResponse({ status: 200, body: MOCK_EVENTS })).pipe(delay(400));
  }
  return next(req);
};
