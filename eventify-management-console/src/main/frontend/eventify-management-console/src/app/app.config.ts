import { ApplicationConfig, provideBrowserGlobalErrorListeners } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';
import { environment } from '../environments/environment';
import { mockInterceptor } from './mock.interceptor';
import { providePrimeNG } from 'primeng/config';
import Aura from '@primeuix/themes/aura';
import { routes } from './app.routes';

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideRouter(routes),
    provideHttpClient(withInterceptors(environment.useMockData ? [mockInterceptor] : [])),
    provideAnimationsAsync(),
    providePrimeNG({
      ripple: true,
      theme: {
        preset: Aura,
        options: {
          prefix: 'p',
          darkModeSelector: 'system',
          cssLayer: {
            name: 'primeng',
            order: 'theme, base, primeng',
          },
        },
      },
      license: "eyJpZCI6ImViNjcxMTM0LWVkY2ItNDViYi1iZTM0LTk1MDg5OWI0NWMxZCIsInByb2R1Y3QiOiJwcmltZXVpIiwidGllciI6ImNvbW11bml0eSIsInR5cGUiOiJkZXYiLCJpYXQiOjE3ODg5OTYzMzgsImV4cCI6MTgyMDUzMjMzOH0.PRIwr-nmo0kgSy0ZRlMFNXPdsEermcSuOssLiFM-aPotlmpyd9p-NtjkfiYbtgOZbkyDHpOaZ_v5Jfpd0Dg4Bg"
    }),
  ],
};
