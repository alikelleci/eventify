import { Component, inject } from '@angular/core';
import { RouterLink } from '@angular/router';
import { BackendService } from '@eventify/ui/services/backend.service';
import { SearchService } from '../services/search.service';
import { SessionService } from '../services/session.service';
import { AppSwitcherComponent } from './app-switcher.component';
import { HeaderSearchComponent } from './header-search.component';

@Component({
  selector: 'app-header',
  standalone: true,
  imports: [RouterLink, AppSwitcherComponent, HeaderSearchComponent],
  host: { class: 'relative block' },
  template: `
    <header class="relative flex items-center gap-3 px-5 py-3 border-b border-slate-700 bg-slate-900">
      <a routerLink="/" class="flex items-center gap-3 rounded outline-none focus-visible:ring-2 focus-visible:ring-primary-400">
        <i class="pi pi-bolt inline-block w-5 shrink-0 text-center text-xl text-primary-400"></i>
        <span class="text-lg tracking-tight"><span class="font-semibold text-white">Eventify</span><span class="font-light text-slate-300 ml-1.5">Console</span></span>
      </a>

      <!-- Without connected applications there is nothing to search -->
      @if (!backend.noApps()) {
        <app-header-search class="hidden sm:block ml-auto w-80" />
      }

      @if (backend.apps().length > 0) {
        <app-app-switcher class="ml-auto w-44 shrink-0 sm:ml-0" />
      }

      <!-- With login: who is logged in, and log out -->
      @if (session.session().loginEnabled) {
        <span class="h-5 w-px shrink-0 bg-slate-700" aria-hidden="true" [class.ml-auto]="backend.apps().length === 0"></span>
        <button type="button" (click)="session.logout()" title="Log out"
                class="flex items-center gap-2 h-8 px-2.5 rounded text-sm text-slate-300 hover:text-white hover:bg-slate-800 transition-colors cursor-pointer outline-none focus-visible:ring-2 focus-visible:ring-primary-400"
                >
          <span class="hidden md:inline max-w-40 truncate">{{ session.session().user }}</span>
          <i class="pi pi-sign-out text-xs"></i>
        </button>
      }
    </header>

    <!-- On mobile the search gets its own white bar below the header -->
    @if (!backend.noApps()) {
      <div class="sm:hidden px-4 py-3 border-b border-surface-200 dark:border-surface-800">
        <app-header-search variant="light" />
      </div>
    }

    @if (search.loading()) {
      <div class="absolute inset-x-0 -bottom-px h-0.5 overflow-hidden">
        <div class="header-progress h-full w-1/3 bg-primary-400"></div>
      </div>
    }
  `,
})
export class HeaderComponent {
  readonly backend = inject(BackendService);
  readonly search = inject(SearchService);
  readonly session = inject(SessionService);
}
