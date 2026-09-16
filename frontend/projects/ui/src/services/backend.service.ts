import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { catchError, filter, fromEvent, merge, of, switchMap, timer } from 'rxjs';
import { InstanceStatus } from '../status';

/** An instance of an application, connected to the console right now. */
export interface AppNode {
  nodeId: string;
  hostname: string | null;
  version: string | null;
  connectedAt: string;
  /** How it is doing; null when it didn't answer. */
  status: InstanceStatus | null;
}

/** An application: the instances with the same application id. */
export interface AppEntry {
  name: string;
  nodes: AppNode[];
}

/**
 * How often the applications are refreshed, so new and stopped applications show up by themselves and their status
 * stays current. Only while the page is in view.
 */
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

  /**
   * Loads the applications, and keeps them up to date. Resolves once the first list is there. A hidden page (another
   * tab, or minimised) asks nothing after that, and asks again as soon as it is shown.
   */
  load(): Promise<void> {
    const visible = () => document.visibilityState === 'visible';
    return new Promise<void>(resolve => {
      merge(timer(0, REFRESH_MS), fromEvent(document, 'visibilitychange')).pipe(
        // The first time always: the console starts once the list is there, also in a tab opened in the background.
        filter((_, index) => index === 0 || visible()),
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
