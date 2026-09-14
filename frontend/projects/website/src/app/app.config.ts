import { ApplicationConfig, provideBrowserGlobalErrorListeners } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';
import { provideEventifyTheme } from '@eventify/ui/theme';
import { routes } from './app.routes';

// No backend: unlike the console, no config.json, HTTP client or mock data. The showcases bring their own example data.
export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideRouter(routes),
    provideAnimationsAsync(),
    provideEventifyTheme(),
  ],
};
