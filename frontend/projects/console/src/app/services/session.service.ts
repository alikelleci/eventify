import { Injectable, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { catchError, firstValueFrom, of } from 'rxjs';

/** What the console tells about itself and the person using it (GET /api/session). */
export interface Session {
  /** People log in with the identity provider: show who is logged in and a log out button. */
  loginEnabled: boolean;
  user: string | null;
  /** Applications need the application token to connect. */
  appTokenRequired: boolean;
}

@Injectable({ providedIn: 'root' })
export class SessionService {
  private readonly http = inject(HttpClient);

  readonly session = signal<Session>({ loginEnabled: false, user: null, appTokenRequired: false });

  load(): Promise<void> {
    return firstValueFrom(this.http.get<Session>('/api/session').pipe(catchError(() => of(null))))
      .then(session => { if (session) this.session.set(session); });
  }

  /** A plain form post, like the log out buttons of server-rendered pages: the server then redirects the browser. */
  logout(): void {
    const form = document.createElement('form');
    form.method = 'post';
    form.action = '/logout';
    const csrf = document.createElement('input');
    csrf.type = 'hidden';
    csrf.name = '_csrf';
    csrf.value = decodeURIComponent(document.cookie.match(/(?:^|;\s*)XSRF-TOKEN=([^;]*)/)?.[1] ?? '');
    form.appendChild(csrf);
    document.body.appendChild(form);
    form.submit();
  }
}
