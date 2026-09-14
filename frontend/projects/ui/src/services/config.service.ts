import { Injectable, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';

export interface AppEntry {
  name: string;
  url: string;
}

export interface AppConfig {
  mode: 'embedded' | 'standalone';
  apps?: AppEntry[];
}

@Injectable({ providedIn: 'root' })
export class ConfigService {
  private readonly _config = signal<AppConfig>({ mode: 'embedded' });
  readonly config = this._config.asReadonly();

  constructor(private readonly http: HttpClient) {}

  load(): Promise<void> {
    return firstValueFrom(this.http.get<AppConfig>('config.json'))
      // A trailing slash would turn every API call into //api/..., which the server doesn't match.
      .then(cfg => this._config.set({ ...cfg, apps: cfg.apps?.map(app => ({ ...app, url: app.url.replace(/\/+$/, '') })) }))
      .catch(() => {});
  }
}
