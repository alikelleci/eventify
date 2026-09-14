import { Component, inject } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { BackendService } from '@eventify/ui/services/backend.service';
import { HeaderComponent } from './header.component';
import { NoAppsComponent } from './no-apps.component';

/** The console shell: header on top, the routed page in the content area below. */
@Component({
  selector: 'app-console-layout',
  standalone: true,
  imports: [RouterOutlet, HeaderComponent, NoAppsComponent],
  template: `
    <div class="flex flex-col h-dvh bg-surface-0 dark:bg-surface-900 text-surface-900 dark:text-surface-100">
      <app-header class="shrink-0" />
      <main class="flex-1 overflow-hidden">
        @if (backend.unconfigured()) {
          <app-no-apps />
        } @else {
          <router-outlet />
        }
      </main>
    </div>
  `,
})
export class ConsoleLayoutComponent {
  readonly backend = inject(BackendService);
}
