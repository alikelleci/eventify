/** How an application is doing, as /api/status reports it. */
export interface AppStatus {
  name: string;
  /** The Kafka Streams state of the instance worst off, or null when no instance answered. */
  state: string | null;
  /** How long the application has been in that state. */
  stateForMs: number;
  /** How many instances are in that state; fewer than `answered` means only some of them are. */
  inState: number;
  restore: { restored: number; total: number; percentage: number } | null;
  /** The instances connected right now, and how many of them answered. */
  instances: number;
  answered: number;
}

export type StatusTone = 'ok' | 'busy' | 'error' | 'unknown';

/** What to show for a status: a short text, and how alarming it is. */
export function statusLabel(status: AppStatus | undefined | null): { text: string; tone: StatusTone } {
  if (!status || !status.state) return { text: 'No status', tone: 'unknown' };
  if (status.restore) return { text: `Restoring ${status.restore.percentage}%${some(status)}`, tone: 'busy' };

  switch (status.state) {
    case 'RUNNING':
      return { text: 'Running', tone: 'ok' };
    case 'REBALANCING':
      return { text: `Rebalancing for ${duration(status.stateForMs)}${some(status)}`, tone: 'busy' };
    case 'CREATED':
      return { text: `Starting${some(status)}`, tone: 'busy' };
    case 'PENDING_SHUTDOWN':
    case 'NOT_RUNNING':
      return { text: `Stopped for ${duration(status.stateForMs)}${some(status)}`, tone: 'error' };
    default:
      // How long it has been wrong matters: seconds means a restart, hours means nobody noticed.
      return { text: `Error for ${duration(status.stateForMs)}${some(status)}`, tone: 'error' };
  }
}

/**
 * The state is the one of the instance worst off, so it can be about some of them only: "Error (1 of 3)".
 * Nothing when every instance that answered is in it.
 */
function some(status: AppStatus): string {
  return status.inState > 0 && status.inState < status.answered ? ` (${status.inState} of ${status.answered})` : '';
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
