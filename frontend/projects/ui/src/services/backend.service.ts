import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { catchError, of, switchMap, timer } from 'rxjs';

/** An instance of an application, connected to the console right now. */
export interface AppNode {
  nodeId: string;
  hostname: string | null;
  version: string | null;
  connectedAt: string;
}

/** An application: the instances with the same application id. */
export interface AppEntry {
  name: string;
  nodes: AppNode[];
}

/** A version the instances of an application run, and how many of them run it. */
export interface VersionCount {
  version: string;
  instances: number;
}

/**
 * The plugin versions an application's instances run, newest first, when they differ (e.g. while an upgrade rolls out);
 * otherwise empty. Only the instances of one application are compared, not applications with each other or with the console.
 */
export function mixedVersions(app: AppEntry): VersionCount[] {
  const counts = new Map<string, number>();
  for (const node of app.nodes) {
    if (node.version) counts.set(node.version, (counts.get(node.version) ?? 0) + 1);
  }
  if (counts.size < 2) return [];
  return [...counts].map(([version, instances]) => ({ version, instances })).sort((a, b) => compareVersions(b.version, a.version));
}

/** Compares versions like 1.10.0 and 1.9.2 part by part, as numbers where they are numbers. */
function compareVersions(a: string, b: string): number {
  const pa = a.split(/[.-]/), pb = b.split(/[.-]/);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const x = pa[i] ?? '', y = pb[i] ?? '';
    const diff = /^\d+$/.test(x) && /^\d+$/.test(y) ? Number(x) - Number(y) : x.localeCompare(y);
    if (diff) return diff;
  }
  return 0;
}

/** How often the list of applications is refreshed, so new and stopped applications show up by themselves. */
const REFRESH_MS = 5000;

@Injectable({ providedIn: 'root' })
export class BackendService {
  private readonly http = inject(HttpClient);

  private readonly _apps = signal<AppEntry[]>([]);
  private readonly _loaded = signal(false);
  /** The chosen application's name; null until the first list arrives. */
  private readonly _activeName = signal<string | null>(null);

  /**
   * The connected applications. The chosen one stays in the list while none of its instances is connected
   * (e.g. during a restart), so the page doesn't jump to another application.
   */
  readonly apps = computed<AppEntry[]>(() => {
    const apps = this._apps();
    const active = this._activeName();
    return active && !apps.some(app => app.name === active)
      ? [...apps, { name: active, nodes: [] }].sort((a, b) => a.name.localeCompare(b.name))
      : apps;
  });
  /** No application has connected to the console (yet). */
  readonly noApps = computed(() => this._loaded() && this.apps().length === 0);
  // Equal by name: the list is refreshed every few seconds, which must not count as picking another application.
  readonly activeApp = computed<AppEntry | null>(
    () => this.apps().find(app => app.name === this._activeName()) ?? null,
    { equal: (a, b) => a?.name === b?.name },
  );
  readonly baseUrl = computed(() => `/api/apps/${encodeURIComponent(this.activeApp()?.name ?? '')}`);

  /** Loads the applications, and keeps them up to date. Resolves once the first list is there. */
  load(): Promise<void> {
    return new Promise<void>(resolve => {
      timer(0, REFRESH_MS).pipe(
        // Keep the last list when the console is briefly unreachable.
        switchMap(() => this.http.get<AppEntry[]>('/api/apps').pipe(catchError(() => of(null)))),
      ).subscribe(apps => {
        if (apps) this.update(apps);
        this._loaded.set(true);
        resolve();
      });
    });
  }

  setActiveApp(app: AppEntry): void {
    this._activeName.set(app.name);
  }

  private update(apps: AppEntry[]): void {
    this._apps.set(apps);
    if (this._activeName() === null && apps.length > 0) this._activeName.set(apps[0].name);
  }
}
