/** How one instance is doing, as /api/apps reports it with each instance. */
export interface InstanceStatus {
  /** The Kafka Streams state: RUNNING, REBALANCING, ERROR, … */
  state: string;
  /** How long the instance has been in that state. */
  stateForMs: number;
  /** Whether it is restoring state stores. */
  restoring: boolean;
}

export type StatusTone = 'ok' | 'busy' | 'error' | 'unknown';

export interface StatusLabel {
  text: string;
  tone: StatusTone;
}

/** How long the mouse rests on a status before its tooltip shows: moving past it shows nothing. */
export const TOOLTIP_DELAY_MS = 400;

/** Worst first: which instance speaks for the application. */
const TONES: StatusTone[] = ['error', 'busy', 'unknown', 'ok'];

/** The state of one instance in a word, and how alarming it is; null when it didn't answer. */
export function instanceState(status: InstanceStatus | null): StatusLabel {
  if (!status) return { text: 'No answer', tone: 'unknown' };
  if (status.restoring) return { text: 'Restoring', tone: 'busy' };
  switch (status.state) {
    case 'RUNNING': return { text: 'Running', tone: 'ok' };
    case 'REBALANCING': return { text: 'Rebalancing', tone: 'busy' };
    case 'CREATED': return { text: 'Starting', tone: 'busy' };
    // Every instance passes through this during a normal deploy: on its way down, not broken.
    case 'PENDING_SHUTDOWN': return { text: 'Stopping', tone: 'busy' };
    case 'NOT_RUNNING': return { text: 'Stopped', tone: 'error' };
    default: return { text: 'Error', tone: 'error' };
  }
}

/** From this long on, a state says how long it has lasted: before that, the state itself says enough. */
const SHOW_DURATION_FROM_MS = 60_000;

/**
 * "Error for 3 min": the state of one instance, with how long it has lasted once that is a minute or more. Running
 * needs no duration.
 */
export function instanceLabel(status: InstanceStatus | null): string {
  const { text, tone } = instanceState(status);
  return status && tone !== 'ok' && status.stateForMs >= SHOW_DURATION_FROM_MS ? `${text} for ${duration(status.stateForMs)}` : text;
}

/** The state of an application: the one of its instance worst off. */
export function appState(instances: { status: InstanceStatus | null }[]): StatusLabel {
  if (instances.length === 0) return { text: 'No instances', tone: 'unknown' };
  return worst(instances.map(instance => instanceState(instance.status)));
}

/** The one worst off: an error before busy, busy before unknown, unknown before running. At least one is needed. */
export function worst<T extends { tone: StatusTone }>(labels: T[]): T {
  return labels.reduce((worst, label) => TONES.indexOf(label.tone) < TONES.indexOf(worst.tone) ? label : worst);
}

/**
 * The instances numbered 1, 2, 3 in that order, from the one connected longest. An instance keeps its number while it
 * stays connected; one that restarts comes last.
 */
export function numbered<T extends { nodeId: string; connectedAt: string }>(instances: T[]): { number: number; instance: T }[] {
  return [...instances]
    .sort((a, b) => a.connectedAt.localeCompare(b.connectedAt) || a.nodeId.localeCompare(b.nodeId))
    .map((instance, index) => ({ number: index + 1, instance }));
}

/** The colour of a status dot: green running, amber busy, red wrong, grey unknown. */
export function dotClass(tone: StatusTone | undefined): string {
  return tone === 'busy' ? 'bg-amber-500'
    : tone === 'error' ? 'bg-red-500'
    : tone === 'unknown' ? 'bg-surface-300 dark:bg-surface-600'
    : 'bg-primary-500';
}

/** 4 min, 2 h, 3 days: only used from a minute on. */
function duration(ms: number): string {
  const minutes = Math.floor(ms / 60_000);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} h`;
  const days = Math.floor(hours / 24);
  return `${days} ${days === 1 ? 'day' : 'days'}`;
}
