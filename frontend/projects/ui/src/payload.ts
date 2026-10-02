/** A message payload without the Java type name, which is noise in the console. */
export function cleanPayload(payload: Record<string, unknown>): Record<string, unknown> {
  const cleaned = { ...payload };
  delete cleaned['@class'];
  return cleaned;
}

/** A payload as indented JSON, for display and copying. */
export function formatPayload(payload: Record<string, unknown>): string {
  return JSON.stringify(cleanPayload(payload), null, 2);
}

/** Metadata as rows for the metadata table. */
export function metadataEntries(metadata: Record<string, string> | null | undefined): { key: string; value: string }[] {
  return Object.entries(metadata ?? {}).map(([key, value]) => ({ key, value }));
}
