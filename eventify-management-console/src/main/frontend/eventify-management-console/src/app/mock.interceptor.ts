import { HttpInterceptorFn, HttpResponse } from '@angular/common/http';
import { of, delay } from 'rxjs';
import { EventsPage, AggregateState } from './models';

const AGGREGATE_ID = 'customer-1';

const MOCK_EVENTS: EventsPage = {
  events: [
    {
      id: `${AGGREGATE_ID}@0000000000003`,
      timestamp: new Date(Date.now() - 60_000).toISOString(),
      type: 'FirstNameChanged',
      aggregateId: AGGREGATE_ID,
      eventId: `${AGGREGATE_ID}@0000000000003`,
      revision: 1,
      metadata: { '$correlationId': 'corr-003', '$causationId': 'cmd-003' },
      payload: { '@type': 'com.example.CustomerEvent$FirstNameChanged', id: AGGREGATE_ID, firstName: 'Jane' },
    },
    {
      id: `${AGGREGATE_ID}@0000000000002`,
      timestamp: new Date(Date.now() - 120_000).toISOString(),
      type: 'CustomerUpdated',
      aggregateId: AGGREGATE_ID,
      eventId: `${AGGREGATE_ID}@0000000000002`,
      revision: 1,
      metadata: { '$correlationId': 'corr-002', '$causationId': 'cmd-002' },
      payload: { '@type': 'com.example.CustomerEvent$CustomerUpdated', id: AGGREGATE_ID, firstName: 'John', lastName: 'Smith' },
    },
    {
      id: `${AGGREGATE_ID}@0000000000001`,
      timestamp: new Date(Date.now() - 300_000).toISOString(),
      type: 'CustomerCreated',
      aggregateId: AGGREGATE_ID,
      eventId: `${AGGREGATE_ID}@0000000000001`,
      revision: 1,
      metadata: { '$correlationId': 'corr-001', '$causationId': 'cmd-001' },
      payload: { '@type': 'com.example.CustomerEvent$CustomerCreated', id: AGGREGATE_ID, firstName: 'John', lastName: 'Doe' },
    },
  ],
  nextCursor: null,
};

const MOCK_STATE: AggregateState = {
  eventId: `${AGGREGATE_ID}@0000000000003`,
  timestamp: new Date(Date.now() - 60_000).toISOString(),
  version: 3,
  metadata: { '$correlationId': 'corr-003' },
  payload: { '@type': 'com.example.Customer', id: AGGREGATE_ID, firstName: 'Jane', lastName: 'Smith' },
};

export const mockInterceptor: HttpInterceptorFn = (req, next) => {
  if (req.url.includes('/api/aggregates') && req.url.includes('/events')) {
    return of(new HttpResponse({ status: 200, body: MOCK_EVENTS })).pipe(delay(400));
  }
  if (req.url.includes('/api/aggregates') && req.url.includes('/state')) {
    return of(new HttpResponse({ status: 200, body: MOCK_STATE })).pipe(delay(300));
  }
  return next(req);
};
