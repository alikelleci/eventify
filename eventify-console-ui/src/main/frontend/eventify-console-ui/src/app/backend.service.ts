import { Injectable, signal, computed, inject } from '@angular/core';
import { ConfigService, AppEntry } from './config.service';

@Injectable({ providedIn: 'root' })
export class BackendService {
  private readonly config = inject(ConfigService);
  private readonly _activeIndex = signal(0);

  readonly isEmbedded = computed(() => this.config.config().mode === 'embedded');
  readonly apps = computed<AppEntry[]>(() => this.config.config().apps ?? []);
  readonly activeApp = computed<AppEntry | null>(() => this.apps()[this._activeIndex()] ?? null);
  readonly baseUrl = computed(() => this.isEmbedded() ? '' : (this.activeApp()?.url ?? ''));

  setActive(index: number): void {
    this._activeIndex.set(index);
  }
}
