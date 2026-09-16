/** How an application is doing, as /api/apps reports it with each application. */
export interface AppStatus {
  /** The Kafka Streams state of the instance worst off, or null when no instance answered. */
  state: string | null;
  /** How long the application has been in that state. */
  stateForMs: number;
  /** How many instances are in that state; fewer than `answered` means only some of them are. */
  inState: number;
  /** How many instances are restoring state stores. */
  restoring: number;
  /** How many instances answered. */
  answered: number;
}

export type StatusTone = 'ok' | 'busy' | 'error' | 'unknown';

/** From this long on, a state shows how long it has lasted: a short rebalance or restart is normal, a long one is not. */
const SHOW_DURATION_FROM_MS = 60_000;

/**
 * What to show for a status: a short text, and how alarming it is. The tone is there from the start. How many instances
 * it is about is only said where that points somewhere: an error on one instance, or one instance restoring. A
 * rebalance is about all of them.
 */
export function statusLabel(status: AppStatus | undefined | null): { text: string; tone: StatusTone } {
  if (!status || !status.state) return { text: 'No status', tone: 'unknown' };
  if (status.restoring > 0) {
    return { text: `Restoring${lasting(status)}${some(status.restoring, status.answered)}`, tone: 'busy' };
  }

  switch (status.state) {
    case 'RUNNING':
      return { text: 'Running', tone: 'ok' };
    case 'REBALANCING':
      return { text: `Rebalancing${lasting(status)}`, tone: 'busy' };
    case 'CREATED':
      return { text: 'Starting', tone: 'busy' };
    case 'PENDING_SHUTDOWN':
    case 'NOT_RUNNING':
      return { text: `Stopped${lasting(status)}${some(status.inState, status.answered)}`, tone: 'error' };
    default:
      // How long it has been wrong matters: seconds means a restart, hours means nobody noticed.
      return { text: `Error${lasting(status)}${some(status.inState, status.answered)}`, tone: 'error' };
  }
}

/** Only the state, without numbers: for where there is no room for more, like the cards on the home page. */
export function stateLabel(status: AppStatus | undefined | null): { text: string; tone: StatusTone } {
  const { tone } = statusLabel(status);
  if (!status || !status.state) return { text: 'No status', tone };
  if (status.restoring > 0) return { text: 'Restoring', tone };
  switch (status.state) {
    case 'RUNNING': return { text: 'Running', tone };
    case 'REBALANCING': return { text: 'Rebalancing', tone };
    case 'CREATED': return { text: 'Starting', tone };
    case 'PENDING_SHUTDOWN':
    case 'NOT_RUNNING': return { text: 'Stopped', tone };
    default: return { text: 'Error', tone };
  }
}

/** " for 4 min", once the state has lasted long enough to be worth saying. */
function lasting(status: AppStatus): string {
  return status.stateForMs >= SHOW_DURATION_FROM_MS ? ` for ${duration(status.stateForMs)}` : '';
}

/** "(1 of 3 instances)" when it is about some of the instances only; nothing when it is about every one that answered. */
function some(count: number, answered: number): string {
  return count > 0 && count < answered ? ` (${count} of ${answered} instances)` : '';
}

/** 40s, 4 min, 2 h, 3 days. */
export function duration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} h`;
  const days = Math.floor(hours / 24);
  return `${days} ${days === 1 ? 'day' : 'days'}`;
}
