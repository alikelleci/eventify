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
      .then(cfg => this._config.set(cfg))
      .catch(() => {});
  }
}
