import { ApplicationConfig, provideBrowserGlobalErrorListeners, APP_INITIALIZER } from '@angular/core';
import { PreloadAllModules, provideRouter, withPreloading } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';
import { environment } from '../environments/environment';
import { mockInterceptor } from './mock.interceptor';
import { provideEventifyTheme } from '@eventify/ui/theme';
import { routes } from './app.routes';
import { ConfigService } from '@eventify/ui/services/config.service';

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    // Lazy routes load in the background right after startup, so the first search doesn't wait for them.
    provideRouter(routes, withPreloading(PreloadAllModules)),
    provideHttpClient(withInterceptors(environment.useMockData ? [mockInterceptor] : [])),
    provideAnimationsAsync(),
    {
      provide: APP_INITIALIZER,
      useFactory: (cfg: ConfigService) => () => cfg.load(),
      deps: [ConfigService],
      multi: true,
    },
    provideEventifyTheme(),
  ],
};
