import { Injectable, signal, computed, inject } from '@angular/core';
import { ConfigService, AppEntry } from './config.service';

@Injectable({ providedIn: 'root' })
export class BackendService {
  private readonly config = inject(ConfigService);
  /** Position of the chosen app in the list; outside this service only the app itself matters. */
  private readonly _activeIndex = signal(0);

  readonly isEmbedded = computed(() => this.config.config().mode === 'embedded');
  readonly apps = computed<AppEntry[]>(() => this.config.config().apps ?? []);
  /** Standalone without any applications: there is nothing to connect to. */
  readonly unconfigured = computed(() => !this.isEmbedded() && this.apps().length === 0);
  readonly activeApp = computed<AppEntry | null>(() => this.apps()[this._activeIndex()] ?? null);
  readonly baseUrl = computed(() => this.isEmbedded() ? '' : (this.activeApp()?.url ?? ''));

  setActiveApp(app: AppEntry): void {
    const index = this.apps().indexOf(app);
    if (index >= 0) this._activeIndex.set(index);
  }
}
