import { providePrimeNG } from 'primeng/config';
import Aura from '@primeuix/themes/aura';

/** The PrimeNG theme shared by the console and the website, so both look the same. */
export function provideEventifyTheme() {
  return providePrimeNG({
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
  });
}
