export interface CommandMessage {
  id: string;
  timestamp: string;
  type: string;
  payload: Record<string, unknown> & { '@class'?: string };
  metadata: Record<string, string>;
  aggregateId: string;
}

export interface CommandsPage {
  commands: CommandMessage[];
}

export interface EventMessage {
  id: string;
  timestamp: string;
  type: string;
  payload: Record<string, unknown> & { '@class'?: string };
  metadata: Record<string, string>;
  aggregateId: string;
  revision: number;
}

export interface EventsPage {
  events: EventMessage[];
  nextCursor: string | null;
}

export interface AggregateState {
  id: string;
  timestamp: string;
  type: string;
  payload: Record<string, unknown> & { '@class'?: string };
  metadata: Record<string, string>;
  aggregateId: string;
  eventId: string;
  version: number;
}

export interface EventDetail {
  event: EventMessage;
  state: AggregateState | null;
  previousState: AggregateState | null;
}
