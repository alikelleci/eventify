import { ApplicationConfig, provideBrowserGlobalErrorListeners, APP_INITIALIZER } from '@angular/core';
import { PreloadAllModules, provideRouter, withPreloading } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';
import { environment } from '../environments/environment';
import { mockInterceptor } from './mock.interceptor';
import { loginInterceptor } from './login.interceptor';
import { SessionService } from './services/session.service';
import { provideEventifyTheme } from '@eventify/ui/theme';
import { routes } from './app.routes';
import { BackendService } from '@eventify/ui/services/backend.service';

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    // Lazy routes load in the background right after startup, so the first search doesn't wait for them.
    provideRouter(routes, withPreloading(PreloadAllModules)),
    provideHttpClient(withInterceptors(environment.useMockData ? [mockInterceptor] : [loginInterceptor])),
    provideAnimationsAsync(),
    {
      provide: APP_INITIALIZER,
      // The connected applications, before the first page shows: an aggregate link then opens in the right application.
      useFactory: (backend: BackendService, session: SessionService) => () => Promise.all([backend.load(), session.load()]),
      deps: [BackendService, SessionService],
      multi: true,
    },
    provideEventifyTheme(),
  ],
};
