import { HttpErrorResponse } from '@angular/common/http';

/**
 * The message to show for a failed request. When the application can't answer right now (503), the console says why,
 * e.g. that it's rebalancing, which is more useful than a generic failure.
 */
export function errorDetail(err: unknown, fallback: string): string {
  if (err instanceof HttpErrorResponse && err.status === 503 && typeof err.error === 'string' && err.error.trim()) {
    return err.error;
  }
  return fallback;
}
