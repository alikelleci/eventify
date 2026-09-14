/** Loading states (skeletons, and buttons such as Retry) stay visible at least this long, so a fast response doesn't make them flicker. */
export const MIN_LOADING_MS = 300;

/** Runs `done` once MIN_LOADING_MS has passed since `startedAt` (right away if it already has). */
export function afterMinLoading(startedAt: number, done: () => void) {
  setTimeout(done, Math.max(0, MIN_LOADING_MS - (Date.now() - startedAt)));
}
