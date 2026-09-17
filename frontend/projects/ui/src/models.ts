/** The events one command produced, oldest first. */
export interface CommandEventsPage {
  events: EventMessage[];
}

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
  /** How many days back the commands were read; older ones are not in the page. Missing from older console clients. */
  lookbackDays?: number;
  /** Whether more commands were found than the page holds: the oldest ones are left out. */
  truncated?: boolean;
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
  /** False when the state after the event can't be rebuilt: the events before it were deleted at a snapshot. */
  stateKnown: boolean;
  /** False when the state before the event can't be rebuilt, for the same reason. A known null state is no state. */
  previousStateKnown: boolean;
}
