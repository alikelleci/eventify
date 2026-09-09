export interface EventMessage {
  id: string;
  timestamp: string;
  type: string;
  aggregateId: string;
  eventId: string;
  version: number;
  metadata: Record<string, string>;
  payload: Record<string, unknown> & { '@type'?: string; '@class'?: string };
}

export interface EventsPage {
  events: EventMessage[];
  nextCursor: string | null;
}

export interface AggregateState {
  eventId: string;
  timestamp: string;
  version: number;
  metadata: Record<string, string>;
  payload: Record<string, unknown> & { '@type'?: string };
}
