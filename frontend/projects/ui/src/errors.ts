import { HttpErrorResponse } from '@angular/common/http';

/**
 * The statuses the console explains in plain text: the request was wrong (400), the stored data can't be answered from,
 * e.g. an aggregate's events are incomplete (422), or the application can't answer right now (503).
 */
const EXPLAINED = [400, 422, 503];

/**
 * The message to show for a failed request. When the console says why, e.g. that the application is rebalancing, that
 * is more useful than a generic failure.
 */
export function errorDetail(err: unknown, fallback: string): string {
  if (err instanceof HttpErrorResponse && EXPLAINED.includes(err.status) && typeof err.error === 'string' && err.error.trim()) {
    return err.error;
  }
  return fallback;
}
